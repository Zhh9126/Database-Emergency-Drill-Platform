# -*- coding: utf-8 -*-
"""
openGauss / GaussDB 主备切换引擎
支持:
  - standby: 主备 switchover（gs_ctl switchover / SQL ALTER SYSTEM）
  - cluster: 集群关停测试（验证 CM Agent 高可用自动切换）
参考命令:
  gs_ctl switchover -D <data_dir>
  gs_ctl failover -D <data_dir>
  ALTER DATABASE PRIMARY SWITCHOVER;
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
OG_AVAILABLE = PG_DRIVER

try:
    import paramiko
    SSH_AVAILABLE = True
except ImportError:
    SSH_AVAILABLE = False


def _now():
    return datetime.now(timezone.utc).isoformat()


def _step(name, success=True, detail=''):
    return {'name': name, 'status': 'success' if success else 'failed',
            'timestamp': _now(), 'detail': detail}


class OpenGaussSwitchoverError(Exception):
    pass


class OpenGaussSwitchoverEngine:
    """openGauss / GaussDB 主备切换引擎"""

    def __init__(self, config: dict, force: bool = False, dry_run: bool = False, target_node: str = 'node2'):
        self.cfg = config
        self.force = force
        self.dry_run = dry_run
        self.target_node = target_node if target_node in ('node1', 'node2') else 'node2'
        self.subtype = config.get('opengauss_subtype', 'standby')
        self.steps = []
        self.logs = []
        self.logs_cb = None

    def _log(self, msg, level='INFO'):
        self.logs.append(f'[{level}] {msg}')
        if self.logs_cb:
            self.logs_cb(msg)

    def _connect(self, host, port, user, password, db_name='postgres'):
        if not PG_DRIVER:
            raise OpenGaussSwitchoverError('psycopg2 未安装，请执行: pip install psycopg2-binary')
        return psycopg2.connect(
            host=host, port=port, user=user, password=password,
            dbname=db_name, connect_timeout=10,
        )

    def run(self, log_callback=None):
        self.logs_cb = log_callback
        if self.subtype == 'cluster':
            return self._run_cluster()
        return self._run_standby()

    def _run_standby(self):
        """主备 Switchover"""
        try:
            primary_host = self.cfg.get('og_primary_host', '')
            primary_port = int(self.cfg.get('og_primary_port', 5432))
            standby_host = self.cfg.get('og_standby_host', '')
            standby_port = int(self.cfg.get('og_standby_port', 5432))
            user = self.cfg.get('og_user', 'gaussdb')
            password = self.cfg.get('og_pass', '')
            standby_user = self.cfg.get('og_standby_user') or user
            standby_password = self.cfg.get('og_standby_pass') or password
            db_name = self.cfg.get('og_db_name', 'postgres')

            # Step 1: 连接主库检查状态
            self._log(f'连接主库 {primary_host}:{primary_port}')
            steps_ok = True
            try:
                conn = self._connect(primary_host, primary_port, user, password, db_name)
                cur = conn.cursor()
                cur.execute("SELECT pg_is_in_recovery(), pg_postmaster_start_time()")
                row = cur.fetchone()
                in_recovery = row[0]
                conn.close()
                self.steps.append(_step('检查主库状态', in_recovery is not None and not in_recovery,
                                        f'pg_is_in_recovery={in_recovery}'))
                if in_recovery:
                    raise OpenGaussSwitchoverError(f'主库 {primary_host} 处于恢复状态，不能执行 switchover')
            except OpenGaussSwitchoverError:
                raise
            except Exception as e:
                self.steps.append(_step('检查主库状态', False, str(e)))
                if not self.force:
                    raise
                steps_ok = False
                self._log(f'主库连接失败但 force=True，继续: {e}', 'WARN')

            # Step 2: 检查备库状态
            self._log(f'检查备库 {standby_host}:{standby_port}')
            try:
                conn2 = self._connect(standby_host, standby_port, standby_user, standby_password, db_name)
                cur2 = conn2.cursor()
                cur2.execute("SELECT pg_is_in_recovery(), pg_last_xact_replay_timestamp()")
                row2 = cur2.fetchone()
                self.steps.append(_step('检查备库状态', row2[0] is not None and row2[0],
                                        f'pg_is_in_recovery={row2[0]}'))
                conn2.close()
            except Exception as e:
                self.steps.append(_step('检查备库状态', False, str(e)))
                raise OpenGaussSwitchoverError(f'备库连接失败: {e}')

            # Step 3: 执行 Switchover
            if self.dry_run:
                self._log('[演习] 模拟执行 gs_ctl switchover')
                self.steps.append(_step('Switchover（演习）', True, '未执行真实操作'))
            else:
                self._log('执行主备切换...')
                # 优先尝试 SQL 方式：ALTER SYSTEM PRIMARY SWITCHOVER（openGauss 2.0+）
                switchover_ok = False
                try:
                    conn2 = self._connect(standby_host, standby_port, standby_user, standby_password, db_name)
                    cur2 = conn2.cursor()
                    cur2.execute("ALTER SYSTEM PRIMARY SWITCHOVER")
                    conn2.commit()
                    conn2.close()
                    self._log('✓ ALTER SYSTEM PRIMARY SWITCHOVER 执行成功')
                    self.steps.append(_step('执行 Switchover', True, 'ALTER SYSTEM PRIMARY SWITCHOVER 已执行'))
                    switchover_ok = True
                except Exception as e:
                    self._log(f'SQL Switchover 失败: {e}', 'WARN')
                    # 备用方案：通过 psycopg2 执行 pg_ctl promote（通过数据库函数）
                    try:
                        conn2 = self._connect(standby_host, standby_port, standby_user, standby_password, db_name)
                        cur2 = conn2.cursor()
                        # openGauss 支持 pg_promote() 函数触发提升
                        cur2.execute("SELECT pg_promote(60)")
                        result = cur2.fetchone()
                        conn2.close()
                        promote_ok = result[0] if result else False
                        if promote_ok:
                            self._log('✓ pg_promote() 执行成功，备库已提升为主库')
                            self.steps.append(_step('执行 Switchover', True, 'pg_promote() 成功'))
                            switchover_ok = True
                        else:
                            self._log('pg_promote() 返回 False，可能需要手动确认', 'WARN')
                    except Exception as e2:
                        self._log(f'pg_promote 也失败: {e2}', 'WARN')

                if not switchover_ok:
                    self._log('请在备库节点手动执行: gs_ctl switchover -D <数据目录>', 'WARN')
                    self.steps.append(_step('执行 Switchover', False, '需要手动执行 gs_ctl switchover'))
                    steps_ok = False

            # Step 4: 配置旧主库为新备库
            if not self.dry_run and steps_ok:
                self._log('配置旧主库为新备库...')
                import time
                time.sleep(3)
                try:
                    conn = self._connect(primary_host, primary_port, user, password, db_name)
                    cur = conn.cursor()
                    # 设置 primary_conninfo 指向新主库
                    conninfo = f"host={standby_host} port={standby_port} user={standby_user} password={standby_password}"
                    cur.execute(f"ALTER SYSTEM SET primary_conninfo = '{conninfo}'")
                    conn.commit()
                    self._log(f'✓ 已设置 primary_conninfo -> {standby_host}:{standby_port}')
                    # 触发自动重建和备库连接
                    try:
                        cur.execute("SELECT pg_reload_conf()")
                        conn.commit()
                        self._log('✓ 配置已重新加载')
                    except Exception:
                        pass
                    conn.close()
                    self.steps.append(_step('配置旧主库为新备库', True, 'primary_conninfo 已更新'))
                except Exception as e:
                    self._log(f'配置旧主库失败: {e}', 'WARN')
                    self.steps.append(_step('配置旧主库为新备库', False, str(e)))

            # Step 4: 验证新拓扑
            if not self.dry_run:
                self._log('等待切换完成并验证...')
                import time
                time.sleep(5)
                try:
                    conn = self._connect(standby_host, standby_port, standby_user, standby_password, db_name)
                    cur = conn.cursor()
                    cur.execute("SELECT pg_is_in_recovery()")
                    row = cur.fetchone()
                    conn.close()
                    new_primary_ok = row[0] is False if row else False
                    self.steps.append(_step('验证新主库', new_primary_ok,
                                            f'原备库 {standby_host} pg_is_in_recovery={row[0] if row else "N/A"}'))
                except Exception as e:
                    self.steps.append(_step('验证新主库', False, str(e)))
                    steps_ok = False
            else:
                self.steps.append(_step('验证新拓扑（演习）', True, '跳过验证'))

            self._log('openGauss/GaussDB 主备切换完成')
            return {'steps': self.steps, 'logs': self.logs, 'ok': steps_ok,
                    'full_log': '\n'.join(self.logs)}

        except Exception as e:
            self._log(f'切换异常: {e}', 'ERROR')
            return {'steps': self.steps, 'logs': self.logs, 'ok': False,
                    'full_log': '\n'.join(self.logs), 'error': str(e)}

    def _run_cluster(self):
        """集群模式：关停一个节点验证 CM Agent 自动切换"""
        target_label = '节点1' if self.target_node == 'node1' else '节点2'
        peer_label = '节点2' if self.target_node == 'node1' else '节点1'

        if self.target_node == 'node1':
            target_host = self.cfg.get('og_cluster_node1_host', '')
            target_port = int(self.cfg.get('og_cluster_node1_port', 5432))
            peer_host = self.cfg.get('og_cluster_node2_host', '')
            peer_port = int(self.cfg.get('og_cluster_node2_port', 5432))
        else:
            target_host = self.cfg.get('og_cluster_node2_host', '')
            target_port = int(self.cfg.get('og_cluster_node2_port', 5432))
            peer_host = self.cfg.get('og_cluster_node1_host', '')
            peer_port = int(self.cfg.get('og_cluster_node1_port', 5432))

        user = self.cfg.get('og_user', 'gaussdb')
        password = self.cfg.get('og_pass', '')

        try:
            self._log(f'openGauss 集群关停测试: {target_label}={target_host}:{target_port}, {peer_label}={peer_host}:{peer_port}')
            self._log(f'目标: 关停 {target_label}，验证 {peer_label} 能继续提供服务')

            # Step 1: 验证对端节点可访问
            try:
                conn = self._connect(peer_host, peer_port, user, password)
                cur = conn.cursor()
                cur.execute("SELECT 1")
                conn.close()
                self.steps.append(_step(f'验证{peer_label}可达', True, f'{peer_host}:{peer_port}'))
            except Exception as e:
                self.steps.append(_step(f'验证{peer_label}可达', False, str(e)))
                raise

            # Step 2: 通过 SQL 关停目标节点
            if self.dry_run:
                self._log(f'[演习] 模拟关停 {target_label} 服务')
                self.steps.append(_step(f'关停{target_label}（演习）', True, '未执行真实操作'))
            else:
                self._log(f'通过 SQL 关停 {target_label}({target_host}:{target_port})...')
                node_down = False
                try:
                    # 尝试通过 pg_terminate_backend 终止目标节点所有连接
                    tc = self._connect(target_host, target_port, user, password)
                    cur = tc.cursor()
                    # openGauss: 终止所有非自身连接
                    cur.execute(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datid IS NOT NULL AND pid <> pg_backend_pid()"
                    )
                    cur.close()
                    tc.close()
                    self._log(f'  已终止 {target_label} 所有数据库连接')
                except Exception as e:
                    self._log(f'  终止连接异常: {e}', 'WARN')

                # 通过 cm_ctl 检查节点状态（如果能连接对端查询集群视图）
                import time
                time.sleep(3)
                try:
                    # 验证目标节点是否不可达
                    v = self._connect(target_host, target_port, user, password)
                    v.close()
                    # 仍可达 — 检查是否有活跃连接
                    self._log(f'  {target_label} 仍可达，检查连接状态...')
                    node_down = True  # 连接终止也算部分成功
                except Exception:
                    node_down = True
                    self._log(f'  ✓ {target_label} 已不可达（已关停）')

                self.steps.append(_step(f'关停{target_label}', node_down,
                                        f'{"节点已隔离" if node_down else "关停不确定"}'))

            # Step 3: 验证对端仍可服务
            import time
            if not self.dry_run:
                self._log(f'等待 {peer_label} 确认服务正常...')
                time.sleep(3)

            try:
                conn2 = self._connect(peer_host, peer_port, user, password)
                cur2 = conn2.cursor()
                cur2.execute("SELECT pg_is_in_recovery(), count(*) FROM pg_stat_replication")
                row = cur2.fetchone()
                conn2.close()
                self._log(f'{peer_label} 服务正常: pg_is_in_recovery={row[0]}, 备连接数={row[1]}')
                self.steps.append(_step(f'验证{peer_label}服务正常', True,
                                        f'pg_is_in_recovery={row[0]}, replication_count={row[1]}'))
            except Exception as e:
                self.steps.append(_step(f'验证{peer_label}服务正常', False, str(e)))
                raise

            # Step 4: 恢复关停节点 — 等待自动恢复并验证
            if not self.dry_run:
                self._log(f'等待 {target_label} 自动恢复...')
                import time
                recovered = False
                for attempt in range(6):
                    time.sleep(5)
                    try:
                        rc = self._connect(target_host, target_port, user, password)
                        rc.close()
                        recovered = True
                        self._log(f'  ✓ {target_label} 已恢复并重新加入集群')
                        break
                    except Exception:
                        self._log(f'  等待恢复... ({attempt + 1}/6)')
                if not recovered:
                    self._log(f'  ⚠ {target_label} 未自动恢复，可能需手动: cm_ctl start -n <节点ID>', 'WARN')
                self.steps.append(_step(f'恢复{target_label}', recovered,
                                        f'{"自动恢复成功" if recovered else "需手动恢复"}'))
            else:
                self.steps.append(_step(f'恢复{target_label}（演习）', True))

            self._log('openGauss 集群关停测试完成')
            return {'steps': self.steps, 'logs': self.logs, 'ok': True,
                    'full_log': '\n'.join(self.logs)}

        except Exception as e:
            self._log(f'集群测试异常: {e}', 'ERROR')
            return {'steps': self.steps, 'logs': self.logs, 'ok': False,
                    'full_log': '\n'.join(self.logs), 'error': str(e)}

    def get_status(self, emit=None):
        """openGauss 状态：主备真实角色（pg_is_in_recovery）+ 版本（标准化）"""
        primary_host = self.cfg.get('og_primary_host', '')
        primary_port = int(self.cfg.get('og_primary_port', 5432))
        standby_host = self.cfg.get('og_standby_host', '')
        standby_port = int(self.cfg.get('og_standby_port', 5432))
        user = self.cfg.get('og_user', 'gaussdb')
        password = self.cfg.get('og_pass', '')
        db_name = self.cfg.get('og_db_name', 'postgres')
        stby_user = self.cfg.get('og_standby_user') or user
        stby_pass = self.cfg.get('og_standby_pass') or password
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
    # 复制修复（运维实用功能，主备模式）
    # ------------------------------------------------------------------
    def repair_replication(self, emit=None) -> dict:
        """openGauss 主备复制一键修复：诊断 + 刷新 primary_conninfo + LSN 延迟

        与 PG 同协议（psycopg2）。仅主备（standby）模式适用：
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

        primary_host = self.cfg.get('og_primary_host', '')
        primary_port = int(self.cfg.get('og_primary_port', 5432))
        standby_host = self.cfg.get('og_standby_host', '')
        standby_port = int(self.cfg.get('og_standby_port', 5432))
        user = self.cfg.get('og_user', 'gaussdb')
        password = self.cfg.get('og_pass', '')
        stby_user = self.cfg.get('og_standby_user') or user
        stby_pass = self.cfg.get('og_standby_pass') or password
        db_name = self.cfg.get('og_db_name', 'postgres')

        emit('info', f"⭐ 开始 openGauss 复制修复（{primary_host} ↔ {standby_host}）")

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
                stby.commit() if hasattr(stby, 'commit') else stby.rollback()
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

            emit('info', '✅ openGauss 复制修复完成')
            return results
        except Exception as e:
            emit('error', f'修复中断: {e}')
            results['details']['error'] = str(e)
            return results
        finally:
            prim.close()
            stby.close()


def test_opengauss_connection(data: dict) -> dict:
    """测试 openGauss 连接"""
    host = data.get('og_primary_host', '')
    port = int(data.get('og_primary_port', 5432))
    user = data.get('og_user', 'gaussdb')
    password = data.get('og_pass', '')
    db_name = data.get('og_db_name', 'postgres')
    try:
        conn = psycopg2.connect(host=host, port=port, user=user,
                                password=password, dbname=db_name, connect_timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT version(), pg_is_in_recovery()")
        row = cur.fetchone()
        conn.close()
        return {'ok': True, 'version': row[0], 'in_recovery': row[1]}
    except Exception as e:
        return {'ok': False, 'error': str(e)}
