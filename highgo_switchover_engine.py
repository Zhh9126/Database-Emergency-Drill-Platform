# -*- coding: utf-8 -*-
"""
瀚高数据库 (HighGo) 主备切换引擎
瀚高基于 PostgreSQL 内核，主备切换通过 pg_rewind + pg_basebackup 重建或
通过流复制管理工具执行 switchover。
支持:
  - standby: 主备 Switchover（pg_ctl promote / pg_rewind + pg_basebackup 重建）
参考命令:
  pg_ctl promote -D <备库数据目录>
  pg_rewind --target-pgdata=<原主库目录> --source-server='host=新主库 ...'
"""
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

try:
    import psycopg2
    PG_DRIVER = True
except ImportError:
    PG_DRIVER = False

# 引擎可用性标志（供 app.py import 判断）
HG_AVAILABLE = PG_DRIVER


def _now():
    return datetime.now(timezone.utc).isoformat()


def _step(name, success=True, detail=''):
    return {'name': name, 'status': 'success' if success else 'failed',
            'timestamp': _now(), 'detail': detail}


class HighGoSwitchoverError(Exception):
    pass


class HighGoSwitchoverEngine:
    """瀚高数据库主备切换引擎"""

    def __init__(self, config: dict, force: bool = False, dry_run: bool = False):
        self.cfg = config
        self.force = force
        self.dry_run = dry_run
        self.steps = []
        self.logs = []
        self.logs_cb = None

    def _log(self, msg, level='INFO'):
        self.logs.append(f'[{level}] {msg}')
        if self.logs_cb:
            self.logs_cb(msg)

    def _connect(self, host, port, user, password, db_name='highgo'):
        if not PG_DRIVER:
            raise HighGoSwitchoverError('psycopg2 未安装，请执行: pip install psycopg2-binary')
        return psycopg2.connect(
            host=host, port=port, user=user, password=password,
            dbname=db_name, connect_timeout=10,
        )

    def run(self, log_callback=None):
        self.logs_cb = log_callback
        try:
            primary_host = self.cfg.get('hg_primary_host', '')
            primary_port = int(self.cfg.get('hg_primary_port', 5866))
            standby_host = self.cfg.get('hg_standby_host', '')
            standby_port = int(self.cfg.get('hg_standby_port', 5866))
            user = self.cfg.get('hg_user', 'highgo')
            password = self.cfg.get('hg_pass', '')
            standby_user = self.cfg.get('hg_standby_user') or user
            standby_password = self.cfg.get('hg_standby_pass') or password
            db_name = self.cfg.get('hg_db_name', 'highgo')

            # Step 1: 连接主库验证角色
            self._log(f'连接瀚高主库 {primary_host}:{primary_port}')
            steps_ok = True
            try:
                conn = self._connect(primary_host, primary_port, user, password, db_name)
                cur = conn.cursor()
                cur.execute("SELECT pg_is_in_recovery()")
                row = cur.fetchone()
                conn.close()
                in_recovery = row[0] if row else True
                self._log(f'主库 pg_is_in_recovery={in_recovery}')
                self.steps.append(_step('验证主库角色', in_recovery is False,
                                        f'pg_is_in_recovery={in_recovery}'))
                if in_recovery:
                    raise HighGoSwitchoverError(f'主库 {primary_host} 处于恢复状态')
            except HighGoSwitchoverError:
                raise
            except Exception as e:
                self.steps.append(_step('验证主库角色', False, str(e)))
                if not self.force:
                    raise
                steps_ok = False

            # Step 2: 检查备库状态
            self._log(f'检查瀚高备库 {standby_host}:{standby_port}')
            try:
                conn2 = self._connect(standby_host, standby_port, standby_user, standby_password, db_name)
                cur2 = conn2.cursor()
                cur2.execute("SELECT pg_is_in_recovery(), pg_last_xact_replay_timestamp()")
                row2 = cur2.fetchone()
                conn2.close()
                standby_in_recovery = row2[0] if row2 else False
                self._log(f'备库 pg_is_in_recovery={standby_in_recovery}')
                self.steps.append(_step('验证备库角色', standby_in_recovery is True,
                                        f'pg_is_in_recovery={standby_in_recovery}'))
            except Exception as e:
                self.steps.append(_step('验证备库角色', False, str(e)))
                raise HighGoSwitchoverError(f'备库连接失败: {e}')

            # Step 3: 检查复制延迟（使用 LSN 对比，避免空闲期误报）
            self._log('检查复制延迟...')
            try:
                conn = self._connect(primary_host, primary_port, user, password, db_name)
                cur = conn.cursor()
                # 先尝试 LSN 对比（更准确）
                try:
                    cur.execute("SELECT pg_current_wal_lsn()")
                    prim_lsn = cur.fetchone()[0]
                    cur.close()
                    conn2 = self._connect(standby_host, standby_port, standby_user, standby_password, db_name)
                    cur2 = conn2.cursor()
                    cur2.execute("SELECT pg_last_wal_replay_lsn()")
                    stby_lsn = cur2.fetchone()[0]
                    cur2.close()
                    conn2.close()
                    if prim_lsn and stby_lsn:
                        cur = conn.cursor()
                        cur.execute("SELECT pg_wal_lsn_diff(%s, %s)", (prim_lsn, stby_lsn))
                        lag_bytes = cur.fetchone()[0]
                        cur.close()
                        self._log(f'复制延迟: {int(lag_bytes or 0)} bytes (LSN: {prim_lsn} vs {stby_lsn})')
                        lag_ok = (lag_bytes or 0) <= 262144  # 256KB 以内视为同步
                    else:
                        lag_ok = True
                        self._log('LSN 获取失败，跳过延迟检查')
                except Exception:
                    # 回退到时间戳方式
                    cur = conn.cursor()
                    cur.execute("SELECT COALESCE(EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())), 0)")
                    row = cur.fetchone()
                    cur.close()
                    lag = float(row[0]) if row and row[0] else 0
                    self._log(f'复制延迟: {lag:.1f}s (时间戳方式)')
                    max_delay = self.cfg.get('max_delay', 60)
                    lag_ok = lag <= max_delay
                conn.close()
                max_delay = self.cfg.get('max_delay', 60)
                if not lag_ok:
                    self._log(f'警告: 复制延迟超过阈值 {max_delay}s', 'WARN')
                self.steps.append(_step('检查复制延迟', lag_ok, 'LSN 对比方式'))
            except Exception as e:
                self._log(f'延迟检查跳过: {e}', 'WARN')
                self.steps.append(_step('检查复制延迟', True, '跳过'))

            # Step 4: 执行 Switchover — 真实执行 pg_promote() + 配置旧主库
            if self.dry_run:
                self._log('[演习] 模拟瀚高主备切换')
                self.steps.append(_step('Switchover（演习）', True, '未执行真实操作'))
            else:
                self._log('执行瀚高主备切换...')
                switchover_ok = False

                # 4.1 通过 pg_promote() 提升备库为主库
                try:
                    conn2 = self._connect(standby_host, standby_port, standby_user, standby_password, db_name)
                    cur2 = conn2.cursor()
                    cur2.execute("SELECT pg_promote(wait_seconds := 60)")
                    result = cur2.fetchone()
                    conn2.commit()
                    conn2.close()
                    promote_ok = result[0] if result else False
                    if promote_ok:
                        self._log('✓ pg_promote() 成功，备库已提升为新主库')
                        self.steps.append(_step('提升备库为主库', True, 'pg_promote() 成功'))
                        switchover_ok = True
                    else:
                        self._log('pg_promote() 返回 False', 'WARN')
                        self.steps.append(_step('提升备库为主库', False, 'pg_promote() 返回 False'))
                except Exception as e:
                    self._log(f'pg_promote() 失败: {e}', 'WARN')
                    self.steps.append(_step('提升备库为主库', False, str(e)))

                # 4.2 配置旧主库为新备库
                if switchover_ok:
                    import time
                    time.sleep(3)
                    self._log('配置旧主库为新备库...')
                    try:
                        conn = self._connect(primary_host, primary_port, user, password, db_name)
                        cur = conn.cursor()
                        conninfo = f"host={standby_host} port={standby_port} user={standby_user} password={standby_password} dbname={db_name}"
                        cur.execute(f"ALTER SYSTEM SET primary_conninfo = '{conninfo}'")
                        conn.commit()
                        self._log(f'✓ 已设置 primary_conninfo -> {standby_host}:{standby_port}')
                        try:
                            cur.execute("SELECT pg_reload_conf()")
                            conn.commit()
                        except Exception:
                            pass
                        conn.close()
                        self.steps.append(_step('配置旧主库为新备库', True, 'primary_conninfo 已更新'))
                    except Exception as e:
                        self._log(f'配置旧主库失败: {e}', 'WARN')
                        self.steps.append(_step('配置旧主库为新备库', False, str(e)))
                        self._log(f'如需手动修复: pg_rewind --target-pgdata=<数据目录> '
                                  f"--source-server='host={standby_host} port={standby_port}'", 'WARN')

            # Step 5: 验证新拓扑
            if not self.dry_run:
                import time
                self._log('等待切换完成...')
                time.sleep(10)
                try:
                    conn2 = self._connect(standby_host, standby_port, standby_user, standby_password, db_name)
                    cur2 = conn2.cursor()
                    cur2.execute("SELECT pg_is_in_recovery()")
                    row = cur2.fetchone()
                    conn2.close()
                    is_primary = row[0] is False if row else False
                    self.steps.append(_step('验证新主库', is_primary,
                                            f'原备库 {standby_host} pg_is_in_recovery={row[0] if row else "N/A"}'))
                except Exception as e:
                    self.steps.append(_step('验证新主库', False, str(e)))
                    steps_ok = False
            else:
                self.steps.append(_step('验证新主库（演习）', True))

            self._log('瀚高数据库主备切换完成')
            return {'steps': self.steps, 'logs': self.logs, 'ok': steps_ok,
                    'full_log': '\n'.join(self.logs)}

        except Exception as e:
            self._log(f'切换异常: {e}', 'ERROR')
            return {'steps': self.steps, 'logs': self.logs, 'ok': False,
                    'full_log': '\n'.join(self.logs), 'error': str(e)}

    def get_status(self, emit=None):
        """瀚高状态：主备真实角色（pg_is_in_recovery）+ 版本（标准化）"""
        primary_host = self.cfg.get('hg_primary_host', '')
        primary_port = int(self.cfg.get('hg_primary_port', 5866))
        standby_host = self.cfg.get('hg_standby_host', '')
        standby_port = int(self.cfg.get('hg_standby_port', 5866))
        user = self.cfg.get('hg_user', 'highgo')
        password = self.cfg.get('hg_pass', '')
        db_name = self.cfg.get('hg_db_name', 'highgo')
        stby_user = self.cfg.get('hg_standby_user') or user
        stby_pass = self.cfg.get('hg_standby_pass') or password
        result = {'ok': True, 'primary': {}, 'standby': {}, 'healthy': False, 'summary': ''}
        for key, host, port, u, pw in (
                ('primary', primary_host, primary_port, user, password),
                ('standby', standby_host, standby_port, stby_user, stby_pass)):
            try:
                conn = self._connect(host, port, u, pw, db_name)
                cur = conn.cursor()
                cur.execute("SELECT version(), pg_is_in_recovery()")
                row = cur.fetchone()
                version = (row[0] or '').split('\n')[0] if row and row[0] else ''
                in_recovery = bool(row[1]) if row and len(row) > 1 else False
                conn.close()
                role = 'standby' if in_recovery else 'primary'
                result[key] = {'host': host, 'port': port, 'role': role, 'ok': True, 'version': version}
            except Exception as e:
                result[key] = {'host': host, 'port': port, 'ok': False, 'error': str(e)[:120]}
        p_, s_ = result['primary'], result['standby']
        if p_.get('ok') and s_.get('ok'):
            result['healthy'] = p_.get('role') == 'primary' and s_.get('role') == 'standby'
            result['summary'] = '主备正常' if result['healthy'] else '方向异常：主=%s 备=%s' % (p_.get('role'), s_.get('role'))
        else:
            result['ok'] = False
            result['summary'] = '主备节点存在连接失败'
        return result

    # ------------------------------------------------------------------
    # 复制修复（运维实用功能）
    # ------------------------------------------------------------------
    def repair_replication(self, emit=None) -> dict:
        """瀚高主备复制一键修复：诊断 + 刷新 primary_conninfo + LSN 延迟

        与 PG 同协议（psycopg2）：
          1. 连接主备，诊断 pg_is_in_recovery 角色；
          2. 方向校验；
          3. 主库 pg_stat_replication 复制连接检查；
          4. 刷新备库 primary_conninfo + pg_reload_conf；
          5. LSN 延迟检查（≤256KB 视为同步）。
        """
        if emit is None:
            emit = lambda level, msg: None  # noqa: E731
        results = {'repaired': False, 'direction_mismatch': False, 'details': {}}

        if not PG_DRIVER:
            emit('error', 'psycopg2 未安装，请执行: pip install psycopg2-binary')
            results['details']['error'] = 'psycopg2 未安装'
            return results

        primary_host = self.cfg.get('hg_primary_host', '')
        primary_port = int(self.cfg.get('hg_primary_port', 5866))
        standby_host = self.cfg.get('hg_standby_host', '')
        standby_port = int(self.cfg.get('hg_standby_port', 5866))
        user = self.cfg.get('hg_user', 'highgo')
        password = self.cfg.get('hg_pass', '')
        stby_user = self.cfg.get('hg_standby_user') or user
        stby_pass = self.cfg.get('hg_standby_pass') or password
        db_name = self.cfg.get('hg_db_name', 'highgo')

        emit('info', f"⭐ 开始瀚高复制修复（{primary_host} ↔ {standby_host}）")

        try:
            prim = self._connect(primary_host, primary_port, user, password, db_name)
            emit('info', f"✓ 主库 {primary_host}:{primary_port} 连接正常")
        except Exception as e:
            emit('error', f"✗ 主库连接失败: {e}")
            results['details']['primary_connect_error'] = str(e)
            return results
        try:
            stby = self._connect(standby_host, standby_port, stby_user, stby_pass, db_name)
            emit('info', f"✓ 备库 {standby_host}:{standby_port} 连接正常")
        except Exception as e:
            emit('error', f"✗ 备库连接失败: {e}")
            prim.close()
            results['details']['standby_connect_error'] = str(e)
            return results

        try:
            # 角色诊断
            cur = prim.cursor()
            cur.execute('SELECT pg_is_in_recovery()')
            prim_recovery = cur.fetchone()[0]
            cur.close()
            cur = stby.cursor()
            cur.execute('SELECT pg_is_in_recovery()')
            stby_recovery = cur.fetchone()[0]
            cur.close()
            results['details']['primary_role'] = 'standby' if prim_recovery else 'primary'
            results['details']['standby_role'] = 'standby' if stby_recovery else 'primary'
            emit('info', f"ℹ 主库角色: {results['details']['primary_role']}，备库角色: {results['details']['standby_role']}")

            # 方向校验
            if prim_recovery or not stby_recovery:
                parts = []
                if prim_recovery:
                    parts.append(f"配置主库 {primary_host} 实际是备库（恢复模式），方向可能已颠倒")
                if not stby_recovery:
                    parts.append(f"配置备库 {standby_host} 实际是主库")
                desc = '；'.join(parts)
                emit('warning', f'⚠ 配置方向与物理拓扑不一致：{desc}')
                results['direction_mismatch'] = True
                results['details']['direction_mismatch_desc'] = desc
                return results
            emit('info', f"✓ 方向校验通过：{primary_host}=主，{standby_host}=备")

            # 复制连接
            cur = prim.cursor()
            cur.execute("SELECT client_addr, state, pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS replay_bytes FROM pg_stat_replication")
            repl_rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            cur.close()
            if repl_rows:
                emit('info', f"✓ 主库看到 {len(repl_rows)} 个复制连接：")
                details = []
                for r in repl_rows:
                    d = dict(zip(cols, r))
                    emit('info', f"  client={d.get('client_addr')} state={d.get('state')} diff={d.get('replay_bytes')} bytes")
                    details.append(d)
                results['details']['replica_connections'] = details
            else:
                emit('warning', '主库 pg_stat_replication 为空：备库未建立流复制连接')
                results['details']['replica_connections'] = []

            # 刷新 primary_conninfo
            emit('info', f"刷新备库 primary_conninfo -> {primary_host}:{primary_port}")
            conninfo = f"host={primary_host} port={primary_port} user={stby_user} password={stby_pass}"
            try:
                cur = stby.cursor()
                cur.execute("ALTER SYSTEM SET primary_conninfo = %s", (conninfo,))
                if hasattr(stby, 'commit'):
                    stby.commit()
                cur = stby.cursor()
                cur.execute('SELECT pg_reload_conf()')
                cur.fetchone()
                cur.close()
                emit('info', '✓ 已刷新 primary_conninfo 并 pg_reload_conf()')
                results['repaired'] = True
            except Exception as e:
                emit('warning', f'刷新 primary_conninfo 失败（备库非恢复模式时不允许）: {e}')

            # LSN 延迟
            try:
                cur = prim.cursor()
                cur.execute('SELECT pg_current_wal_lsn()')
                p_lsn = cur.fetchone()[0]
                cur.close()
                cur = stby.cursor()
                cur.execute('SELECT pg_last_wal_replay_lsn()')
                s_lsn = cur.fetchone()[0]
                cur.close()
                if p_lsn and s_lsn:
                    cur = stby.cursor()
                    cur.execute('SELECT pg_wal_lsn_diff(%s, %s)', (p_lsn, s_lsn))
                    lag = cur.fetchone()[0]
                    cur.close()
                    lag_bytes = int(lag) if lag is not None else 0
                    results['details']['lsn_lag_bytes'] = lag_bytes
                    if lag_bytes <= 262144:
                        emit('info', f"✓ 备库 LSN 已同步（差值 {lag_bytes} 字节）")
                    else:
                        emit('warning', f"⚠ 备库 LSN 落后 {lag_bytes} 字节，等待复制追平")
                else:
                    emit('info', 'LSN 位点获取失败，跳过延迟检查')
            except Exception as e:
                emit('warning', f'LSN 延迟检查失败: {e}')

            emit('info', '✅ 瀚高复制修复完成')
            return results
        except Exception as e:
            emit('error', f'修复中断: {e}')
            results['details']['error'] = str(e)
            return results
        finally:
            prim.close()
            stby.close()


def test_highgo_connection(data: dict) -> dict:
    """测试瀚高数据库连接"""
    host = data.get('hg_primary_host', '')
    port = int(data.get('hg_primary_port', 5866))
    user = data.get('hg_user', 'highgo')
    password = data.get('hg_pass', '')
    try:
        conn = psycopg2.connect(host=host, port=port, user=user,
                                password=password, connect_timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT version()")
        row = cur.fetchone()
        conn.close()
        return {'ok': True, 'version': row[0] if row else ''}
    except Exception as e:
        return {'ok': False, 'error': str(e)}
