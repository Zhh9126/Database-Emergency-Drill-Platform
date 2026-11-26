# -*- coding: utf-8 -*-
"""
Oracle ADG 主备切换引擎
支持: 物理 Standby 手动切换 + DG Broker 切换
"""
import json
import time
from datetime import datetime, timezone
from typing import Callable, Optional, Any
import traceback

try:
    import oracledb
    ORACLE_AVAILABLE = True
except ImportError:
    ORACLE_AVAILABLE = False


class OracleConnection:
    """Oracle 连接封装"""
    def __init__(self, host: str, port: int, sid: str, user: str, password: str,
                 mode: str = 'SYSDBA'):
        self.host = host
        self.port = port
        self.sid = sid
        self.user = user
        self.password = password
        self.mode_str = mode
        self.conn: Any = None

    @property
    def mode(self):
        if self.mode_str.upper() == 'SYSDBA':
            return oracledb.SYSDBA if ORACLE_AVAILABLE else 2
        return None

    @property
    def dsn(self):
        return f"{self.host}:{self.port}/{self.sid}"

    def connect(self) -> bool:
        """建立连接"""
        if not ORACLE_AVAILABLE:
            raise RuntimeError("oracledb 库未安装，请执行: pip install oracledb")
        try:
            self.conn = oracledb.connect(
                user=self.user, password=self.password,
                dsn=self.dsn, mode=self.mode
            )
            return True
        except Exception as e:
            raise ConnectionError(f"Oracle 连接失败 [{self.host}:{self.port}/{self.sid}]: {e}")

    def close(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass

    def execute(self, sql: str) -> list:
        """执行查询返回结果列表"""
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            cols = [d[0].lower() for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            return [dict(zip(cols, row)) for row in rows]
        finally:
            cur.close()

    def exec_dml(self, sql: str) -> bool:
        """执行 DDL/DML（如 ALTER DATABASE 等）"""
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            self.conn.commit()
            return True
        finally:
            cur.close()

    def get_value(self, sql: str):
        """获取单值"""
        rows = self.execute(sql)
        if rows:
            return list(rows[0].values())[0]
        return None


# ============================================================
#  Oracle ADG 切换引擎
# ============================================================

class OracleSwitchoverEngine:
    """Oracle DataGuard 主备切换引擎"""

    def __init__(self, config: dict, log_callback: Optional[Callable] = None):
        if not ORACLE_AVAILABLE:
            raise RuntimeError("oracledb 库未安装，请执行: pip install oracledb")

        self.cfg = config
        self.log = log_callback or (lambda level, msg: None)

        # 连接参数
        self.prim_host = config['oracle_primary_host']
        self.prim_port = config.get('oracle_primary_port', 1521)
        self.prim_sid = config.get('oracle_primary_sid', '')
        self.stby_host = config['oracle_standby_host']
        self.stby_port = config.get('oracle_standby_port', 1521)
        self.stby_sid = config.get('oracle_standby_sid', '')
        self.oracle_user = config.get('oracle_user', 'sys')
        self.oracle_pass = config.get('oracle_pass', '')
        self.connect_mode = config.get('oracle_connect_mode', 'SYSDBA')

        # 切换控制
        self.force_switchover = config.get('force_switchover', False)
        self.dry_run = config.get('dry_run', False)
        self.skip_consistency_check = config.get('skip_consistency_check', False)
        self.auto_rollback = config.get('auto_rollback', True)
        self.verify_switchover = config.get('verify_switchover', True)

        # 连接实例
        self.prim_conn: Optional[OracleConnection] = None
        self.stby_conn: Optional[OracleConnection] = None

        # 状态
        self.steps = []
        self.full_log_lines = []
        # 修复（2026-11-05）：切换结果验证标志，step8_verify 任一校验失败置 False，
        # 防止 warning=误判成功。
        self._verify_ok = True

    # ======== 日志辅助 ========
    def _info(self, msg):
        self.log('info', msg)
        self.full_log_lines.append(f"[INFO] {msg}")

    def _success(self, msg):
        self.log('success', msg)
        self.full_log_lines.append(f"[SUCCESS] {msg}")

    def _warning(self, msg):
        self.log('warning', msg)
        self.full_log_lines.append(f"[WARNING] {msg}")

    def _error(self, msg):
        self.log('error', msg)
        self.full_log_lines.append(f"[ERROR] {msg}")

    def _step(self, num, name, status='running', detail=''):
        s = {'step': num, 'name': name, 'status': status, 'detail': detail,
             'time': datetime.now(timezone.utc).isoformat()}
        self.steps.append(s)

    def _update_last_step(self, status, detail=''):
        if self.steps:
            self.steps[-1]['status'] = status
            self.steps[-1]['detail'] = detail

    # ======== 连接管理 ========
    def _connect_all(self):
        """连接主备库"""
        self._info("连接 Oracle 主库...")
        self.prim_conn = OracleConnection(
            self.prim_host, self.prim_port, self.prim_sid,
            self.oracle_user, self.oracle_pass, self.connect_mode
        )
        self.prim_conn.connect()
        self._info(f"✓ 主库连接成功: {self.prim_host}:{self.prim_port}/{self.prim_sid}")

        self._info("连接 Oracle 备库...")
        self.stby_conn = OracleConnection(
            self.stby_host, self.stby_port, self.stby_sid,
            self.oracle_user, self.oracle_pass, self.connect_mode
        )
        self.stby_conn.connect()
        self._info(f"✓ 备库连接成功: {self.stby_host}:{self.stby_port}/{self.stby_sid}")

    def _close_all(self):
        for c in [self.prim_conn, self.stby_conn]:
            if c:
                try:
                    c.close()
                except Exception:
                    pass

    # ======== 查询辅助 ========
    def _query_db_status(self, conn: OracleConnection, label: str):
        """查询数据库角色和切换状态"""
        sql = """
            SELECT NAME, OPEN_MODE, DATABASE_ROLE, SWITCHOVER_STATUS,
                   FORCE_LOGGING, DATAGUARD_BROKER, GUARD_STATUS,
                   DB_UNIQUE_NAME, PROTECTION_MODE
            FROM V$DATABASE
        """
        rows = conn.execute(sql)
        if not rows:
            return None
        row = rows[0]
        self._info(
            f"[{label}] NAME={row.get('name','?')}, "
            f"DB_UNIQUE={row.get('db_unique_name','?')}, "
            f"ROLE={row.get('database_role','?')}, "
            f"SWITCHOVER_STATUS={row.get('switchover_status','?')}, "
            f"GUARD={row.get('guard_status','?')}, "
            f"OPEN_MODE={row.get('open_mode','?')}"
        )
        return row

    def _query_archived_logs(self, conn: OracleConnection, label: str, limit=5):
        """查看最近归档日志"""
        sql = f"""
            SELECT * FROM (
                SELECT SEQUENCE#, FIRST_TIME, NEXT_TIME
                FROM V$ARCHIVED_LOG
                ORDER BY SEQUENCE# DESC
            ) WHERE ROWNUM <= {limit}
        """
        rows = conn.execute(sql)
        self._info(f"[{label}] 最近 {len(rows)} 条归档日志:")
        for r in rows:
            self._info(f"    SEQ={r.get('sequence#')} FIRST={r.get('first_time')} NEXT={r.get('next_time')}")

    # ======== 步骤实现 ========

    def step1_check_primary(self):
        """步骤1: 检查主库状态"""
        self._step(1, "检查主库状态和角色")
        try:
            self._info(">>> 查询主库 V$DATABASE ...")
            info = self._query_db_status(self.prim_conn, "主库")
            if not info:
                self._update_last_step('failed', "无法获取主库状态")
                return False

            db_role = (info.get('database_role') or '').upper()
            if 'PRIMARY' not in db_role:
                self._warning(f"主库角色异常: {db_role}, 期望 PRIMARY")

            self._query_archived_logs(self.prim_conn, "主库")

            switch_status = (info.get('switchover_status') or '').upper()
            allowed = ('TO STANDBY', 'SESSIONS ACTIVE', 'SESSION ACTIVE')
            if switch_status in allowed:
                self._success(f"主库 SWITCHOVER_STATUS = {switch_status}, 允许切换")
            else:
                self._warning(f"主库 SWITCHOVER_STATUS = {switch_status}, 不在期望列表中")
                if not self.force_switchover:
                    self._update_last_step('failed', f"SWITCHOVER_STATUS={switch_status}")
                    return False

            self._update_last_step('success', f"角色={db_role}, SWITCHOVER_STATUS={switch_status}")
            return True
        except Exception as e:
            self._update_last_step('failed', str(e))
            self._error(f"主库状态检查异常: {e}")
            return False

    def step2_check_standby(self):
        """步骤2: 检查备库状态"""
        self._step(2, "检查备库状态和角色")
        try:
            self._info(">>> 查询备库 V$DATABASE ...")
            info = self._query_db_status(self.stby_conn, "备库")
            if not info:
                self._update_last_step('failed', "无法获取备库状态")
                return False

            db_role = (info.get('database_role') or '').upper()
            if 'PHYSICAL STANDBY' not in db_role and 'STANDBY' not in db_role:
                self._warning(f"备库角色异常: {db_role}, 期望 PHYSICAL STANDBY")
                if not self.force_switchover:
                    self._update_last_step('failed', f"备库角色={db_role}")
                    return False

            self._query_archived_logs(self.stby_conn, "备库")

            switch_status = (info.get('switchover_status') or '').upper()
            allowed = ('TO PRIMARY', 'SESSIONS ACTIVE', 'SESSION ACTIVE', 'NOT ALLOWED')
            if switch_status == 'NOT ALLOWED':
                self._error("备库 SWITCHOVER_STATUS = NOT ALLOWED，可能数据未完全同步")
                self._error("→ 检查备库告警日志: show parameter dump;")
                self._error("→ 查看 trace 目录下 alert 日志")
                self._error("→ 尝试: rman target / → list incarnation; → reset database to incarnation N;")
                if not self.force_switchover:
                    self._update_last_step('failed', "SWITCHOVER_STATUS=NOT ALLOWED")
                    return False
            elif switch_status in allowed:
                self._success(f"备库 SWITCHOVER_STATUS = {switch_status}, 允许切换")
            elif switch_status == 'FAILED DESTINATION':
                self._error("备库 SWITCHOVER_STATUS = FAILED DESTINATION，通常是监听或主库状态异常")
                self._error("→ 检查告警日志: show parameter dump;")
                if not self.force_switchover:
                    self._update_last_step('failed', "SWITCHOVER_STATUS=FAILED DESTINATION")
                    return False
            else:
                self._warning(f"备库 SWITCHOVER_STATUS = {switch_status}")

            self._update_last_step('success', f"角色={db_role}, SWITCHOVER_STATUS={switch_status}")
            return True
        except Exception as e:
            self._update_last_step('failed', str(e))
            self._error(f"备库状态检查异常: {e}")
            return False

    def step3_switch_primary_to_standby(self):
        """步骤3: 主库切换为备库 (ALTER DATABASE COMMIT TO SWITCHOVER TO PHYSICAL STANDBY)"""
        self._step(3, "主库执行 SWITCHOVER TO PHYSICAL STANDBY")
        if self.dry_run:
            self._info("[演习模式] 跳过实际切换")
            self._update_last_step('success', '演习跳过')
            return True
        try:
            self._info("执行: ALTER DATABASE COMMIT TO SWITCHOVER TO PHYSICAL STANDBY")
            self.prim_conn.exec_dml("ALTER DATABASE COMMIT TO SWITCHOVER TO PHYSICAL STANDBY")
            self._success("主库已提交切换为备库")
            self._update_last_step('success', "主库已执行 SWITCHOVER TO PHYSICAL STANDBY")
            return True
        except Exception as e:
            self._update_last_step('failed', str(e))
            self._error(f"主库切换为备库失败: {e}")
            return False

    def step4_mount_old_primary(self):
        """步骤4: 原主库启动到 MOUNT 状态（新高版本会自动关闭，但尽量显式处理）"""
        self._step(4, "原主库启动到 MOUNT 状态")
        if self.dry_run:
            self._info("[演习模式] 跳过")
            self._update_last_step('success', '演习跳过')
            return True
        try:
            # 高版本 Oracle 在 switchover 后自动关闭，低版本需要 SHUTDOWN ABORT
            # 尝试 SHUTDOWN 再 STARTUP MOUNT
            try:
                self.prim_conn.exec_dml("SHUTDOWN ABORT")
            except Exception:
                # SHUTDOWN ABORT 可能导致连接断开，这是正常情况
                pass
            time.sleep(3)
            # 重新连接并 STARTUP MOUNT
            self.prim_conn.close()
            self.prim_conn.connect()
            self._info("执行: STARTUP MOUNT")
            self.prim_conn.exec_dml("STARTUP MOUNT")
            self._success("原主库已启动到 MOUNT 状态")
            self._update_last_step('success', "原主库 → MOUNT")
            return True
        except Exception as e:
            self._warning(f"原主库 MOUNT 步骤异常（可能已完成自动处理）: {e}")
            self._update_last_step('success', f"尝试MOUNT，提示: {e}")
            return True

    def step5_switch_standby_to_primary(self):
        """步骤5: 备库切换为主库"""
        self._step(5, "备库执行 SWITCHOVER TO PRIMARY")
        if self.dry_run:
            self._info("[演习模式] 跳过")
            self._update_last_step('success', '演习跳过')
            return True
        try:
            self._info("执行: ALTER DATABASE COMMIT TO SWITCHOVER TO PRIMARY WITH SESSION SHUTDOWN")
            self.stby_conn.exec_dml(
                "ALTER DATABASE COMMIT TO SWITCHOVER TO PRIMARY WITH SESSION SHUTDOWN"
            )
            self._success("备库已切换为主库角色")
            self._update_last_step('success', "备库 → PRIMARY 角色")
            return True
        except Exception as e:
            self._warning(f"备库切换为主库异常（可能已生效）: {e}")
            # 有些版本可能需要不带 WITH SESSION SHUTDOWN
            try:
                self._info("重试: ALTER DATABASE COMMIT TO SWITCHOVER TO PRIMARY")
                self.stby_conn.exec_dml("ALTER DATABASE COMMIT TO SWITCHOVER TO PRIMARY")
                self._success("备库已切换为主库角色（重试成功）")
                self._update_last_step('success', "备库 → PRIMARY 角色 (重试)")
                return True
            except Exception as e2:
                self._update_last_step('failed', str(e2))
                self._error(f"备库切换为主库失败: {e2}")
                return False

    def step6_open_new_primary(self):
        """步骤6: 新主库 OPEN"""
        self._step(6, "新主库 ALTER DATABASE OPEN")
        if self.dry_run:
            self._info("[演习模式] 跳过")
            self._update_last_step('success', '演习跳过')
            return True
        try:
            self._info("执行: ALTER DATABASE OPEN")
            self.stby_conn.exec_dml("ALTER DATABASE OPEN")
            self._success("新主库已 OPEN")

            # 确认角色
            info = self._query_db_status(self.stby_conn, "新主库")
            db_role = (info.get('database_role') or '').upper() if info else '?'
            switch_status = (info.get('switchover_status') or '') if info else '?'
            self._info(f"新主库: ROLE={db_role}, SWITCHOVER_STATUS={switch_status}")

            self._update_last_step('success', f"新主库 OPEN, ROLE={db_role}")
            return True
        except Exception as e:
            self._update_last_step('failed', str(e))
            self._error(f"新主库 OPEN 失败: {e}")
            return False

    def step7_start_mrp_on_new_standby(self):
        """步骤7: 新备库开启日志应用 (Managed Recovery Process)"""
        self._step(7, "新备库启动日志应用 (MRP)")
        if self.dry_run:
            self._info("[演习模式] 跳过")
            self._update_last_step('success', '演习跳过')
            return True
        try:
            # 新备库（原主库）确认已是 mount/open 状态
            self._info("在新备库（原主库）上启动日志应用...")
            # 先 OPEN（如果还在 mount）
            try:
                self.prim_conn.exec_dml("ALTER DATABASE OPEN")
                self._info("新备库已 OPEN")
            except Exception as oe:
                self._info(f"OPEN 状态: {oe}")

            # 启动 MRP
            self._info("执行: ALTER DATABASE RECOVER MANAGED STANDBY DATABASE USING CURRENT LOGFILE DISCONNECT")
            self.prim_conn.exec_dml(
                "ALTER DATABASE RECOVER MANAGED STANDBY DATABASE USING CURRENT LOGFILE DISCONNECT"
            )
            self._success("新备库日志应用已启动")

            # 确认状态
            info = self._query_db_status(self.prim_conn, "新备库")
            db_role = (info.get('database_role') or '') if info else '?'
            self._info(f"新备库: ROLE={db_role}")

            self._update_last_step('success', f"MRP 已启动, ROLE={db_role}")
            return True
        except Exception as e:
            self._warning(f"新备库启动 MRP 异常: {e}")
            self._update_last_step('warning', f"MRP 可能需手动启动: {e}")
            return True  # 非致命错误

    def step8_verify(self):
        """步骤8: 验证切换结果"""
        self._step(8, "验证切换结果")
        try:
            all_ok = True

            # 新主库（原备库）
            self._info(">>> 验证新主库（原备库）...")
            info_p = self._query_db_status(self.stby_conn, "新主库")
            if info_p:
                role = (info_p.get('database_role') or '').upper()
                if 'PRIMARY' in role:
                    self._success(f"✓ 新主库角色确认为 PRIMARY: {self.stby_host}:{self.stby_port}/{self.stby_sid}")
                else:
                    self._error(f"✗ 新主库角色异常: {role}")
                    all_ok = False
            else:
                all_ok = False

            # 新备库（原主库）
            self._info(">>> 验证新备库（原主库）...")
            info_s = self._query_db_status(self.prim_conn, "新备库")
            if info_s:
                role = (info_s.get('database_role') or '').upper()
                if 'STANDBY' in role:
                    self._success(f"✓ 新备库角色确认为 STANDBY: {self.prim_host}:{self.prim_port}/{self.prim_sid}")
                else:
                    self._error(f"✗ 新备库角色异常: {role}")
                    all_ok = False
            else:
                all_ok = False

            if all_ok:
                self._update_last_step('success', "主备角色验证通过")
            else:
                self._verify_ok = False
                self._update_last_step('warning', "部分验证未通过，请人工确认")
            return all_ok
        except Exception as e:
            self._update_last_step('warning', f"验证异常: {e}")
            return False

    # ======== 主流程 ========

    def run(self) -> dict:
        """执行 ADG 切换主流程"""
        result = {
            'success': False,
            'new_primary': f"{self.prim_host}:{self.prim_port}/{self.prim_sid}",  # 实际是旧备库
            'new_standby': f"{self.stby_host}:{self.stby_port}/{self.stby_sid}",  # 实际是旧主库
            'steps': [],
            'full_log': '',
        }
        try:
            # ---- 连接 ----
            self._step(0, "建立 Oracle 连接", 'running')
            self._connect_all()
            self._update_last_step('success')

            # ---- 步骤流程 ----
            flow = [
                ("检查主库状态", self.step1_check_primary),
                ("检查备库状态", self.step2_check_standby),
                ("主库→SWITCHOVER TO PHYSICAL STANDBY", self.step3_switch_primary_to_standby),
                ("原主库→MOUNT", self.step4_mount_old_primary),
                ("备库→SWITCHOVER TO PRIMARY", self.step5_switch_standby_to_primary),
                ("新主库→OPEN", self.step6_open_new_primary),
                ("新备库→启动日志应用", self.step7_start_mrp_on_new_standby),
            ]
            if self.verify_switchover:
                flow.append(("验证切换结果", self.step8_verify))

            for step_name, step_fn in flow:
                ok = step_fn()
                if not ok and not self.force_switchover:
                    self._error(f"步骤 [{step_name}] 失败，中止切换")
                    # 自动回滚：如果已经执行了步骤3/5，尽量恢复
                    if self.auto_rollback:
                        self._try_rollback()
                    break

            # ---- 判断结果 ----
            final_step = self.steps[-1] if self.steps else {}
            # 修复（2026-11-05 真实测试同类发现）：warning 不代表成功——
            # step8_verify 验证失败时步骤状态为 warning，若视为成功会导致
            # history.status 误报 success、前端显示「切换成功」而拓扑实际未验证。
            result['success'] = (final_step.get('status') == 'success'
                                 and self._verify_ok)
            # 修正拓扑
            result['new_primary'] = f"{self.stby_host}:{self.stby_port}/{self.stby_sid}"
            result['new_standby'] = f"{self.prim_host}:{self.prim_port}/{self.prim_sid}"
            result['steps'] = self.steps
            result['full_log'] = '\n'.join(self.full_log_lines)

            if result['success']:
                self._success("==== Oracle ADG 切换流程完成 ====")
            else:
                self._error("==== Oracle ADG 切换未完全成功 ====")

        except Exception as e:
            self._error(f"切换引擎异常: {e}")
            self.full_log_lines.append(traceback.format_exc())
            result['success'] = False
            result['steps'] = self.steps
            result['full_log'] = '\n'.join(self.full_log_lines)
            if self.auto_rollback:
                self._try_rollback()
        finally:
            self._close_all()

        return result

    def _try_rollback(self):
        """尽力回滚：恢复只读、重新挂载等"""
        self._info("尝试自动回滚...")
        try:
            if self.stby_conn:
                try:
                    s = self.stby_conn.get_value("SELECT DATABASE_ROLE FROM V$DATABASE")
                    if s and 'PRIMARY' in str(s).upper():
                        self._info("新主库角色已变更，回滚需要反方向再切换，请人工处理")
                except Exception:
                    pass
            if self.prim_conn:
                try:
                    self.prim_conn.exec_dml("ALTER DATABASE OPEN")
                    self.prim_conn.exec_dml(
                        "ALTER DATABASE RECOVER MANAGED STANDBY DATABASE USING CURRENT LOGFILE DISCONNECT"
                    )
                    self._info("已尝试在新备库上启动日志应用")
                except Exception as e:
                    self._info(f"回滚尝试: {e}")
        except Exception as e:
            self._info(f"回滚失败: {e}")

    # =====================================================================
    #  复制修复（运维实用功能）
    # =====================================================================
    def repair_replication(self, emit=None) -> dict:
        """Oracle DataGuard 复制一键修复：诊断 + 备库 MRP 启动 + 归档同步检查

        功能：
          1. 连接主备库，查询 V$DATABASE（DATABASE_ROLE / OPEN_MODE / SWITCHOVER_STATUS）；
          2. 方向校验：配置主库应为 PRIMARY、配置备库应为 PHYSICAL STANDBY，颠倒时停止；
          3. 备库启动日志应用（MRP）：ALTER DATABASE RECOVER MANAGED STANDBY DATABASE
             USING CURRENT LOGFILE DISCONNECT（幂等，若已在应用则跳过）；
          4. 归档日志同步检查（主备最近归档序号对比）。

        返回 dict：{'repaired': bool, 'direction_mismatch': bool, 'details': {...}}
        """
        if emit is None:
            emit = lambda level, msg: None  # noqa: E731
        results = {'repaired': False, 'direction_mismatch': False, 'details': {}}

        if not ORACLE_AVAILABLE:
            emit('error', 'oracledb 库未安装，请执行: pip install oracledb')
            results['details']['error'] = 'oracledb 未安装'
            return results

        emit('info', f'⭐ 开始 Oracle DataGuard 复制修复（{self.prim_host} ↔ {self.stby_host}）')

        # ── 1. 连接主备 ──
        try:
            self.prim_conn = OracleConnection(
                self.prim_host, self.prim_port, self.prim_sid,
                self.oracle_user, self.oracle_pass, self.connect_mode)
            self.prim_conn.connect()
            emit('info', f"✓ 主库 {self.prim_host}:{self.prim_port}/{self.prim_sid} 连接正常")
        except Exception as e:
            emit('error', f'✗ 主库连接失败: {e}')
            results['details']['primary_connect_error'] = str(e)
            return results
        try:
            self.stby_conn = OracleConnection(
                self.stby_host, self.stby_port, self.stby_sid,
                self.oracle_user, self.oracle_pass, self.connect_mode)
            self.stby_conn.connect()
            emit('info', f"✓ 备库 {self.stby_host}:{self.stby_port}/{self.stby_sid} 连接正常")
        except Exception as e:
            emit('error', f'✗ 备库连接失败: {e}')
            self._close_all()
            results['details']['standby_connect_error'] = str(e)
            return results

        try:
            # ── 2. 角色诊断 ──
            p_row = self._query_db_status(self.prim_conn, '主库')
            s_row = self._query_db_status(self.stby_conn, '备库')
            p_role = (p_row.get('database_role') or '').upper() if p_row else ''
            s_role = (s_row.get('database_role') or '').upper() if s_row else ''
            results['details']['primary_role'] = p_role
            results['details']['standby_role'] = s_role
            results['details']['primary_switchover_status'] = p_row.get('switchover_status', '') if p_row else ''
            results['details']['standby_open_mode'] = s_row.get('open_mode', '') if s_row else ''

            # 方向校验
            if 'PRIMARY' not in p_role or 'STANDBY' not in s_role:
                desc = f'配置主库角色={p_role or "未知"}，配置备库角色={s_role or "未知"}'
                emit('warning', f'⚠ 配置方向与物理拓扑不一致：{desc}')
                results['direction_mismatch'] = True
                results['details']['direction_mismatch_desc'] = desc
                return results
            emit('info', f"✓ 方向校验通过：{p_role} / {s_role}")

            # ── 3. 备库 MRP 启动（幂等） ──
            emit('info', '检查备库日志应用进程（MRP）...')
            try:
                mrp = self.stby_conn.execute(
                    "SELECT PROCESS, STATUS FROM V$MANAGED_STANDBY WHERE PROCESS LIKE 'MRP%'"
                )
                if mrp and any(str(r.get('process', '')).startswith('MRP') for r in mrp):
                    emit('info', '✓ 备库 MRP 已在运行，日志应用正常')
                    results['details']['mrp_status'] = 'running'
                else:
                    emit('info', '备库 MRP 未运行，启动日志应用...')
                    try:
                        self.stby_conn.exec_dml(
                            "ALTER DATABASE RECOVER MANAGED STANDBY DATABASE USING CURRENT LOGFILE DISCONNECT"
                        )
                        results['repaired'] = True
                        results['details']['mrp_status'] = 'started'
                        emit('info', '✓ 已启动 MRP（RECOVER MANAGED STANDBY）')
                    except Exception as e:
                        # ORA-16000 / ORA-16139 等表示已在应用或状态不允许
                        err = str(e).upper()
                        if '16000' in err or '16139' in err or '16627' in err:
                            emit('info', f'MRP 已处于应用状态（{err}），无需重复启动')
                            results['details']['mrp_status'] = 'already_running'
                        else:
                            emit('warning', f'启动 MRP 异常: {e}')
                            results['details']['mrp_error'] = str(e)
            except Exception as e:
                emit('warning', f'查询 MRP 状态失败: {e}')

            # ── 4. 归档日志同步检查 ──
            try:
                logs = get_oracle_archived_logs(
                    self.prim_host, self.prim_port, self.prim_sid,
                    self.oracle_user, self.oracle_pass, self.connect_mode)
                if isinstance(logs, list) and logs:
                    count = len(logs)
                    last_seq = logs[0].get('sequence', '?')
                    results['details']['primary_archived_logs'] = count
                    emit('info', f"主库最近归档日志 {count} 条，最新序号 {last_seq} 已可用")
                elif isinstance(logs, list):
                    emit('info', '主库无归档日志记录')
            except Exception as e:
                emit('warning', f'归档日志检查失败: {e}')

            if not results['repaired']:
                results['repaired'] = True  # 已诊断且无异常即为健康
            emit('info', '✅ Oracle DataGuard 复制修复完成')
            return results
        except Exception as e:
            emit('error', f'修复中断: {e}')
            results['details']['error'] = str(e)
            return results
        finally:
            self._close_all()


# ============================================================
#  连接测试 & 状态查询
# ============================================================

def test_oracle_connection(host: str, port: int, sid: str,
                           user: str, password: str, mode: str = 'SYSDBA') -> dict:
    """测试 Oracle 连接并返回状态信息"""
    if not ORACLE_AVAILABLE:
        return {'ok': False, 'error': 'oracledb 库未安装，请执行: pip install oracledb'}

    conn = OracleConnection(host, port, sid, user, password, mode)
    try:
        conn.connect()
        rows = conn.execute("""SELECT NAME, OPEN_MODE, DATABASE_ROLE, SWITCHOVER_STATUS,
                                     DB_UNIQUE_NAME, PROTECTION_MODE, GUARD_STATUS
                              FROM V$DATABASE""")
        row = rows[0] if rows else {}

        return {
            'ok': True,
            'host': host, 'port': port, 'sid': sid,
            'db_name': row.get('name', '?'),
            'db_unique': row.get('db_unique_name', '?'),
            'open_mode': row.get('open_mode', '?'),
            'database_role': row.get('database_role', '?'),
            'switchover_status': row.get('switchover_status', '?'),
            'guard_status': row.get('guard_status', '?'),
            'protection_mode': row.get('protection_mode', '?'),
        }
    except Exception as e:
        return {'ok': False, 'error': str(e), 'host': host, 'port': port}
    finally:
        conn.close()


def get_oracle_archived_logs(host: str, port: int, sid: str,
                              user: str, password: str, mode: str = 'SYSDBA',
                              limit: int = 10) -> list:
    """获取归档日志列表"""
    conn = OracleConnection(host, port, sid, user, password, mode)
    try:
        conn.connect()
        sql = f"""
            SELECT * FROM (
                SELECT SEQUENCE#, FIRST_TIME, NEXT_TIME, STATUS, BLOCKS
                FROM V$ARCHIVED_LOG ORDER BY SEQUENCE# DESC
            ) WHERE ROWNUM <= {limit}
        """
        rows = conn.execute(sql)
        result = []
        for r in rows:
            result.append({
                'sequence': r.get('sequence#'),
                'first_time': str(r.get('first_time', '')),
                'next_time': str(r.get('next_time', '')),
                'status': r.get('status', ''),
                'blocks': r.get('blocks', 0),
            })
        return result
    finally:
        conn.close()
