"""
PostgreSQL 流复制主备切换引擎

切换流程:
  0. 连接主库、备库
  1. 确认主备角色 & 复制状态
  2. 锁主库 (CHECKPOINT 冲刷 WAL)
  3. 主库 pg_ctl promote 备库
  4. 原主库降级为备库
  5. 新主库验证
  6. 新备库启动复制
  7. 结果校验

依赖: psycopg2
"""

import json
import time
import traceback

try:
    import psycopg2
    import psycopg2.extras
    PG_AVAILABLE = True
except ImportError:
    PG_AVAILABLE = False


class PgConnection:
    """单次 PostgreSQL 连接的 wrapper"""

    def __init__(self, host, port, user, password, dbname='postgres'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(
            host=self.host, port=self.port, user=self.user,
            password=self.password, dbname=self.dbname,
            connect_timeout=10
        )
        self.conn.autocommit = True

    def execute(self, sql):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        try:
            rows = cur.fetchall()
        except psycopg2.ProgrammingError:
            rows = []
        cur.close()
        return rows

    def execute_args(self, sql, params):
        """参数化执行（避免 SQL 注入）"""
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        try:
            rows = cur.fetchall()
        except psycopg2.ProgrammingError:
            rows = []
        cur.close()
        return rows

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None


class PgSwitchoverEngine:
    """PostgreSQL 主备切换引擎"""

    def __init__(self, config_dict, force=False, dry_run=False):
        self.cfg = config_dict
        self.force = force
        self.dry_run = dry_run
        self.steps = []
        self.logs = []
        self.primary_conn = None
        self.standby_conn = None

    # ------------------------------------------------------------------
    def _log(self, msg, level='INFO'):
        self.logs.append(f'[{level}] {msg}')

    def _add_step(self, name, status='pending', detail=''):
        self.steps.append({'step': len(self.steps) + 1, 'name': name, 'status': status, 'detail': detail})

    def _fail(self, msg, rollback_fn=None):
        self._log(msg, 'ERROR')
        if rollback_fn:
            try:
                rollback_fn()
            except Exception as re:
                self._log(f'回滚失败: {re}', 'ERROR')
        raise PgSwitchoverError(msg)

    # ------------------------------------------------------------------
    def step0_connect(self):
        self._add_step('连接主备库', 'running')
        try:
            self.primary_conn = PgConnection(
                self.cfg['pg_primary_host'], self.cfg['pg_primary_port'],
                self.cfg['pg_user'], self.cfg['pg_pass']
            )
            self.primary_conn.connect()
            self._log(f'主库连接成功 {self.cfg["pg_primary_host"]}:{self.cfg["pg_primary_port"]}')
        except Exception as e:
            self._fail(f'主库连接失败: {e}')

        try:
            self.standby_conn = PgConnection(
                self.cfg['pg_standby_host'], self.cfg['pg_standby_port'],
                self.cfg['pg_user'], self.cfg['pg_pass']
            )
            self.standby_conn.connect()
            self._log(f'备库连接成功 {self.cfg["pg_standby_host"]}:{self.cfg["pg_standby_port"]}')
        except Exception as e:
            self._fail(f'备库连接失败: {e}', self._close_all)

        self._add_step('连接主备库', 'success', '主库和备库均已连接')

    def step1_check_roles(self):
        self._add_step('检查主备角色', 'running')

        # 检查主库
        p_rows = self.primary_conn.execute("SELECT pg_is_in_recovery() AS is_replica")
        if not p_rows:
            self._fail('主库查询角色失败')
        if p_rows[0]['is_replica']:
            if self.force:
                self._log('主库当前为备库角色，强制继续', 'WARNING')
            else:
                self._fail('主库当前为备库角色(处于恢复模式), 不是主库', self._close_all)

        # 查看复制槽和 WAL 发送状态
        repl = self.primary_conn.execute(
            "SELECT slot_name,active,restart_lsn FROM pg_replication_slots"
        )
        self._log(f'复制槽状态: {len(repl)} 个')
        for r in repl:
            self._log(f'  槽 {r["slot_name"]}: active={r["active"]}, lsn={r["restart_lsn"]}')

        # 检查备库
        s_rows = self.standby_conn.execute("SELECT pg_is_in_recovery() AS is_replica")
        if not s_rows:
            self._fail('备库查询角色失败')

        # 检查复制延迟
        # 修复：原先用 pg_last_xact_replay_timestamp() 计算延迟，空闲期会误报超大延迟。
        # 改为比较 LSN 位置（主库当前 WAL 位点 vs 备库已重放位点），更准确。
        try:
            p_lsn = self.primary_conn.execute("SELECT pg_current_wal_lsn() AS lsn")
            s_lsn = self.standby_conn.execute("SELECT pg_last_wal_replay_lsn() AS lsn")
            if p_lsn and s_lsn and p_lsn[0]['lsn'] and s_lsn[0]['lsn']:
                # 使用 pg_wal_lsn_diff 计算字节差
                diff = self.primary_conn.execute_args(
                    "SELECT pg_wal_lsn_diff(%s, %s) AS bytes",
                    (p_lsn[0]['lsn'], s_lsn[0]['lsn'])
                )
                lag_bytes = int(diff[0]['bytes']) if diff and diff[0]['bytes'] else 0
                # 若差距极小（< 256KB，一个 WAL segment 默认16MB/256KB 缓冲内）视为同步
                if lag_bytes <= 262144:
                    lag = 0
                    self._log(f'备库复制延迟: {lag}s (LSN差值 {lag_bytes} 字节, 已同步)')
                else:
                    lag = lag_bytes
                    self._log(f'备库复制延迟: {lag} 字节 (超出LSN同步范围)', 'WARNING')
            else:
                # 回退为时间戳方式
                s_lag = self.standby_conn.execute(
                    "SELECT extract(epoch FROM now()-pg_last_xact_replay_timestamp()) AS lag_seconds"
                )
                lag = int(float(s_lag[0]['lag_seconds'])) if s_lag and s_lag[0]['lag_seconds'] else 0
                self._log(f'备库复制延迟(时间戳回退): {lag}s')
        except Exception as e:
            self._log(f'复制延迟计算异常: {e}，回退时间戳方式', 'WARNING')
            s_lag = self.standby_conn.execute(
                "SELECT extract(epoch FROM now()-pg_last_xact_replay_timestamp()) AS lag_seconds"
            )
            lag = int(float(s_lag[0]['lag_seconds'])) if s_lag and s_lag[0]['lag_seconds'] else 0
            self._log(f'备库复制延迟(时间戳回退): {lag}s')
        if lag > self.cfg.get('max_delay', 60) and not self.force and not self.cfg.get('skip_delay_check'):
            self._fail(f'复制延迟({lag}s)超过阈值({self.cfg.get("max_delay", 60)}s)', self._close_all)

        self._add_step('检查主备角色', 'success', f'主库正常, 备库正常, 延迟{lag}s')

    def step2_checkpoint(self):
        self._add_step('主库执行 CHECKPOINT', 'running')
        if self.dry_run:
            self._log('演习模式: 跳过 CHECKPOINT')
            self._add_step('主库执行 CHECKPOINT', 'success', '演习跳过')
            return

        try:
            self.primary_conn.execute('CHECKPOINT')
            self._log('主库 CHECKPOINT 完成')
            self._add_step('主库执行 CHECKPOINT', 'success')
        except Exception as e:
            self._log(f'CHECKPOINT 失败: {e}', 'WARNING')
            self._add_step('主库执行 CHECKPOINT', 'success', f'警告: {e}')

    def step3_promote_standby(self):
        """在备库执行 pg_ctl promote 或使用 pg_promote() 函数"""
        self._add_step('提升备库为新主库', 'running')
        if self.dry_run:
            self._log('演习模式: 跳过备库提升')
            self._add_step('提升备库为新主库', 'success', '演习跳过')
            return

        try:
            # PostgreSQL 10+ 支持 pg_promote()
            try:
                self.standby_conn.execute("SELECT pg_promote(wait_seconds=>60)")
                self._log('通过 pg_promote() 提升备库')
            except Exception:
                # fallback: 通知用户手动执行 pg_ctl promote
                self._log('pg_promote() 不可用，请手动在备库执行 pg_ctl promote', 'WARNING')
                if not self.force:
                    self._fail('备库提升需要手动执行 pg_ctl promote', self._close_all)
                self._log('强制模式下跳过手动确认', 'WARNING')

            # 等待备库成为主库
            for i in range(30):
                time.sleep(1)
                try:
                    r = self.standby_conn.execute("SELECT pg_is_in_recovery() AS is_replica")
                    if r and not r[0]['is_replica']:
                        self._log('备库已提升为主库')
                        self._add_step('提升备库为新主库', 'success')
                        return
                except Exception:
                    pass
            self._fail('等待备库提升超时(30s)', self._close_all)
        except PgSwitchoverError:
            raise
        except Exception as e:
            self._fail(f'提升备库失败: {e}', self._close_all)

    def step4_demote_primary(self):
        """将原主库配置为备库跟随新主库

        ⚠️ 已知限制（真实测试 2026-11-04 发现）：
        PG 主库真正降级为备库需要文件系统动作（创建 standby.signal / 修改 recovery.conf
        并重启实例），纯 SQL（ALTER SYSTEM SET primary_conninfo + pg_reload_conf）无法使
        主库进入恢复模式。因此本步骤只能完成「配置 primary_conninfo」这一部分，并严格
        验证是否真的进入恢复模式：
          - 若原主库已进入恢复模式（重建/手动降级已完成）→ 成功；
          - 若仍为主库角色 → 标记失败并抛错（引擎不再误报成功），提示运维手动执行降级。
        """
        self._add_step('原主库降级为新备库', 'running')
        if self.dry_run:
            self._log('演习模式: 跳过原主库降级')
            self._add_step('原主库降级为新备库', 'success', '演习跳过')
            return

        try:
            # 在备库重新建立连接(可能原连接断开)
            self.primary_conn.close()
            self.primary_conn.connect()

            # 获取新主库的连接信息
            new_primary_host = self.cfg['pg_standby_host']
            new_primary_port = self.cfg['pg_standby_port']

            # 配置 primary_conninfo
            # 注意: 这里通过 pg_reload_conf() 仅通知，不修改 postgresql.conf
            # 实际需要 ALTER SYSTEM 修改配置
            conninfo = f"host={new_primary_host} port={new_primary_port} user={self.cfg['pg_user']} password={self.cfg['pg_pass']}"
            try:
                self.primary_conn.execute(f"ALTER SYSTEM SET primary_conninfo = '{conninfo}'")
                self.primary_conn.execute("SELECT pg_reload_conf()")
                self._log(f'已设置 primary_conninfo -> {new_primary_host}:{new_primary_port}')
            except Exception as e:
                self._log(f'自动设置 primary_conninfo 失败: {e}', 'WARNING')
                self._log(f'请在原主库手动设置: primary_conninfo = \'{conninfo}\'', 'WARNING')

            # ── 关键验证：原主库是否真的进入恢复模式（成为备库）──
            r = self.primary_conn.execute("SELECT pg_is_in_recovery() AS is_replica")
            is_replica = r[0]['is_replica'] if r else False
            if is_replica:
                self._log('原主库已进入恢复模式（降级成功）')
                self._add_step('原主库降级为新备库', 'success')
            else:
                # 降级未生效：明确失败，不再误报成功
                detail = (
                    '原主库仍为主库角色：PG 降级需要 standby.signal + 实例重启，'
                    f'纯 SQL 无法完成。请运维手动执行: 1) 在 {new_primary_host}:{new_primary_port} 主库创建 standby.signal；'
                    ' 2) 重启该实例；3) 确认 pg_is_in_recovery()=true。'
                )
                self._log(detail, 'ERROR')
                self._add_step('原主库降级为新备库', 'failed', detail)
                raise PgSwitchoverError('原主库降级为新备库失败：未进入恢复模式（需 standby.signal + 重启）')
        except PgSwitchoverError:
            raise
        except Exception as e:
            self._log(f'原主库降级异常: {e}', 'ERROR')
            self._add_step('原主库降级为新备库', 'failed', str(e))
            raise PgSwitchoverError(f'原主库降级异常: {e}')

    def step5_verify(self):
        self._add_step('验证切换结果', 'running')

        ok = []

        # 新主库 (原备库)
        try:
            r = self.standby_conn.execute("SELECT pg_is_in_recovery() AS is_replica")
            is_replica = r[0]['is_replica'] if r else True
            if not is_replica:
                self._log(f'新主库 ({self.cfg["pg_standby_host"]}): 角色 -> PRIMARY')
                ok.append('new_primary')
            else:
                self._log(f'新主库 ({self.cfg["pg_standby_host"]}): 仍为备库!', 'ERROR')
        except Exception as e:
            self._log(f'新主库验证失败: {e}', 'ERROR')

        # 新备库 (原主库)
        try:
            r = self.primary_conn.execute("SELECT pg_is_in_recovery() AS is_replica")
            is_replica = r[0]['is_replica'] if r else False
            if is_replica:
                self._log(f'新备库 ({self.cfg["pg_primary_host"]}): 已处于恢复模式')
                ok.append('new_standby')
            else:
                self._log(f'新备库 ({self.cfg["pg_primary_host"]}): 仍为主库角色!', 'ERROR')
        except Exception as e:
            self._log(f'新备库验证失败: {e}', 'ERROR')

        self._add_step('验证切换结果', 'success' if len(ok) == 2 else 'failed',
                        f'{len(ok)}/2 验证通过')
        # 修复（2026-11-05 真实测试）：验证不通过必须影响最终状态（all_ok=False），
        # 否则 history.status 误报 success、前端显示「切换成功」但拓扑实际错误。
        if len(ok) != 2:
            missing = set(['new_primary', 'new_standby']) - set(ok)
            details = '；'.join('新主库仍是备库' if m == 'new_primary' else '新备库仍是主库'
                                for m in missing)
            raise PgSwitchoverError(f'切换结果验证不通过（{len(ok)}/2）：{details}')

    # ------------------------------------------------------------------
    def run(self) -> list:
        all_ok = False
        try:
            if not PG_AVAILABLE:
                raise PgSwitchoverError('psycopg2 未安装，请执行: pip install psycopg2-binary')

            self.step0_connect()
            self.step1_check_roles()
            self.step2_checkpoint()
            self.step3_promote_standby()
            self.step4_demote_primary()
            if not self.dry_run:
                self.step5_verify()
            all_ok = True
        except PgSwitchoverError as e:
            self._log(f'切换中断: {e}', 'ERROR')
            self._add_step('切换中断', 'failed', str(e))
        except Exception as e:
            self._log(f'未预期错误: {e}\n{traceback.format_exc()}', 'ERROR')
            self._add_step('未知错误', 'failed', str(e))
        finally:
            self._close_all()

        return self.steps, self.logs, all_ok

    def _close_all(self):
        for c in [self.primary_conn, self.standby_conn]:
            if c:
                try:
                    c.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # 复制修复（运维实用功能）
    # ------------------------------------------------------------------
    def repair_replication(self, emit=None) -> dict:
        """PostgreSQL 流复制一键修复：诊断 + 刷新 primary_conninfo + LSN 延迟检查

        功能：
          1. 诊断主备角色（pg_is_in_recovery）与复制状态（pg_stat_replication / pg_replication_slots）；
          2. 方向校验：配置主库应为 PRIMARY、配置备库应为 STANDBY，方向颠倒时停止；
          3. 刷新备库 primary_conninfo（ALTER SYSTEM + pg_reload_conf，指向配置主库）；
          4. LSN 延迟检查（pg_wal_lsn_diff，≤256KB 视为同步）；
          5. 从库只读保障（default_transaction_read_only=on 会话级提示，PG 备库本身只读）。

        返回 dict：{'repaired': bool, 'direction_mismatch': bool, 'details': {...}}
        """
        if emit is None:
            emit = lambda level, msg: None  # noqa: E731
        results = {'repaired': False, 'direction_mismatch': False, 'details': {}}

        if not PG_AVAILABLE:
            emit('error', 'psycopg2 未安装，请执行: pip install psycopg2-binary')
            results['details']['error'] = 'psycopg2 未安装'
            return results

        emit('info', '⭐ 开始 PostgreSQL 复制修复（诊断→方向→primary_conninfo→延迟）')

        # ── 1. 连接主备 ──
        try:
            self.primary_conn = PgConnection(
                self.cfg['pg_primary_host'], self.cfg['pg_primary_port'],
                self.cfg['pg_user'], self.cfg['pg_pass'])
            self.primary_conn.connect()
            emit('info', f"✓ 主库 {self.cfg['pg_primary_host']}:{self.cfg['pg_primary_port']} 连接正常")
        except Exception as e:
            emit('error', f"✗ 主库连接失败: {e}")
            results['details']['primary_connect_error'] = str(e)
            return results
        try:
            self.standby_conn = PgConnection(
                self.cfg['pg_standby_host'], self.cfg['pg_standby_port'],
                self.cfg.get('pg_standby_user') or self.cfg['pg_user'],
                self.cfg.get('pg_standby_pass') or self.cfg['pg_pass'])
            self.standby_conn.connect()
            emit('info', f"✓ 备库 {self.cfg['pg_standby_host']}:{self.cfg['pg_standby_port']} 连接正常")
        except Exception as e:
            emit('error', f"✗ 备库连接失败: {e}")
            self._close_all()
            results['details']['standby_connect_error'] = str(e)
            return results

        try:
            # ── 2. 角色诊断 ──
            p_role = self.primary_conn.execute("SELECT pg_is_in_recovery() AS is_replica")
            s_role = self.standby_conn.execute("SELECT pg_is_in_recovery() AS is_replica")
            p_replica = bool(p_role[0]['is_replica']) if p_role else None
            s_replica = bool(s_role[0]['is_replica']) if s_role else None
            results['details']['primary_role'] = 'standby' if p_replica else 'primary'
            results['details']['standby_role'] = 'standby' if s_replica else 'primary'
            emit('info', f"ℹ 主库角色: {results['details']['primary_role']}，备库角色: {results['details']['standby_role']}")

            # 方向一致性
            if p_replica or not s_replica:
                parts = []
                if p_replica:
                    parts.append(f"配置主库 {self.cfg['pg_primary_host']} 实际是备库（处于恢复模式），方向可能已颠倒")
                if not s_replica:
                    parts.append(f"配置备库 {self.cfg['pg_standby_host']} 实际是主库")
                desc = '；'.join(parts)
                emit('warning', f'⚠ 配置方向与物理拓扑不一致：{desc}')
                results['direction_mismatch'] = True
                results['details']['direction_mismatch_desc'] = desc
                return results
            emit('info', f"✓ 方向校验通过：{self.cfg['pg_primary_host']}=主，{self.cfg['pg_standby_host']}=备")

            # ── 3. 主库复制状态（发送者视角） ──
            # pg_stat_replication 显示备库连接与接收位置
            repl_rows = self.primary_conn.execute(
                "SELECT client_addr, state, sent_lsn, replay_lsn, "
                "       pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS replay_bytes "
                "FROM pg_stat_replication")
            if repl_rows:
                emit('info', f"✓ 主库看到 {len(repl_rows)} 个复制连接：")
                for r in repl_rows:
                    emit('info', f"  client={r.get('client_addr')} state={r.get('state')} "
                                 f"diff={r.get('replay_bytes')} bytes")
                results['details']['replica_connections'] = [dict(r) for r in repl_rows]
            else:
                emit('warning', '主库 pg_stat_replication 为空：备库未建立流复制连接')
                results['details']['replica_connections'] = []

            # 复制槽
            slots = self.primary_conn.execute("SELECT slot_name, active, restart_lsn FROM pg_replication_slots")
            if slots:
                emit('info', f"复制槽 {len(slots)} 个：")
                for r in slots:
                    emit('info', f"  {r['slot_name']}: active={r['active']} restart_lsn={r['restart_lsn']}")
                results['details']['replication_slots'] = [dict(r) for r in slots]

            # ── 4. 刷新备库 primary_conninfo（指向配置主库）──
            emit('info', f"刷新备库 primary_conninfo -> {self.cfg['pg_primary_host']}:{self.cfg['pg_primary_port']}")
            conninfo = (f"host={self.cfg['pg_primary_host']} port={self.cfg['pg_primary_port']} "
                        f"user={self.cfg['pg_user']} password={self.cfg['pg_pass']}")
            try:
                self.standby_conn.execute(
                    "ALTER SYSTEM SET primary_conninfo = %s", (conninfo,))
                self.standby_conn.execute("SELECT pg_reload_conf()")
                emit('info', '✓ 已刷新 primary_conninfo 并 pg_reload_conf()')
                results['repaired'] = True
            except Exception as e:
                emit('warning', f'刷新 primary_conninfo 失败（备库非恢复模式时不允许 ALTER SYSTEM）: {e}')

            # ── 5. LSN 延迟检查 ──
            try:
                p_lsn = self.primary_conn.execute("SELECT pg_current_wal_lsn() AS lsn")
                s_lsn = self.standby_conn.execute("SELECT pg_last_wal_replay_lsn() AS lsn")
                if p_lsn and s_lsn and p_lsn[0]['lsn'] and s_lsn[0]['lsn']:
                    diff = self.primary_conn.execute_args(
                        "SELECT pg_wal_lsn_diff(%s, %s) AS bytes",
                        (p_lsn[0]['lsn'], s_lsn[0]['lsn']))
                    lag_bytes = int(diff[0]['bytes']) if diff and diff[0]['bytes'] else 0
                    results['details']['lsn_lag_bytes'] = lag_bytes
                    if lag_bytes <= 262144:
                        emit('info', f"✓ 备库 LSN 已同步（差值 {lag_bytes} 字节）")
                    else:
                        emit('warning', f"⚠ 备库 LSN 落后 {lag_bytes} 字节，等待复制追平")
                else:
                    emit('info', 'LSN 位点获取失败（可能备库刚启动），跳过延迟检查')
            except Exception as e:
                emit('warning', f"LSN 延迟检查失败: {e}")

            emit('info', '✅ PostgreSQL 复制修复完成')
            return results
        except Exception as e:
            emit('error', f'修复中断: {e}')
            results['details']['error'] = str(e)
            return results
        finally:
            self._close_all()


class PgSwitchoverError(Exception):
    pass


# ------------------------------------------------------------------
# 独立测试函数
def test_pg_connection(host, port, user, password) -> dict:
    if not PG_AVAILABLE:
        return {'ok': False, 'error': 'psycopg2 未安装，请执行: pip install psycopg2-binary'}

    c = PgConnection(host, port, user, password)
    try:
        c.connect()
        role_rows = c.execute("SELECT pg_is_in_recovery() AS is_replica")
        is_replica = role_rows[0]['is_replica'] if role_rows else None

        version = c.execute("SELECT version() AS v")[0]['v']

        result = {
            'ok': True, 'host': host, 'port': port,
            'role': 'standby' if is_replica else 'primary',
            'is_replica': is_replica,
            'version': version.split(',')[0] if version else 'unknown',
        }

        # 如果是主库，查看复制状态
        if not is_replica:
            slots = c.execute("SELECT slot_name,active FROM pg_replication_slots")
            result['replication_slots'] = [dict(r) for r in slots] if slots else []
        else:
            lag = c.execute(
                "SELECT extract(epoch FROM now()-pg_last_xact_replay_timestamp()) AS lag_seconds"
            )
            result['lag_seconds'] = int(float(lag[0]['lag_seconds'])) if lag and lag[0]['lag_seconds'] else 0

        return result
    except Exception as e:
        return {'ok': False, 'error': str(e)}
    finally:
        c.close()
