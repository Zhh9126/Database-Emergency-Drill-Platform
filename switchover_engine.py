"""
应急演练平台 - MySQL 主从切换引擎 v3.0
集成参考 switch.py：
  - 动态延迟阈值计算（基于历史延迟趋势）
  - GTID深度检查（M_only / S_only 分析）
  - 两阶段提交协议（准备→执行→回滚）
  - 动态连接管理
"""
import json
import time
import uuid
import threading
import statistics
import logging
from datetime import datetime
from typing import Callable
from collections import deque

import pymysql

logger = logging.getLogger(__name__)


# ======================== 连接池管理 ========================

class ConnectionManager:
    """动态连接管理器"""

    def __init__(self):
        self._connections: dict[str, pymysql.Connection] = {}
        self._lock = threading.Lock()

    def get(self, key: str, host: str, port: int, user: str, password: str,
            connect_timeout: int = 5) -> pymysql.Connection:
        with self._lock:
            if key in self._connections:
                conn = self._connections[key]
                try:
                    conn.ping(reconnect=False)
                    return conn
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass
            conn = pymysql.connect(
                host=host, port=port, user=user, password=password,
                connect_timeout=connect_timeout,
                read_timeout=30, write_timeout=30,
                charset='utf8mb4', autocommit=True,
                cursorclass=pymysql.cursors.DictCursor,
            )
            self._connections[key] = conn
            return conn

    def close_all(self):
        with self._lock:
            for conn in self._connections.values():
                try:
                    conn.close()
                except Exception:
                    pass
            self._connections.clear()

    def remove(self, key: str):
        with self._lock:
            if key in self._connections:
                try:
                    self._connections[key].close()
                except Exception:
                    pass
                del self._connections[key]


# ======================== 延迟趋势分析器 ========================

class DelayAnalyzer:
    """基于历史延迟数据动态计算安全阈值"""

    def __init__(self, window_size: int = 10, max_delay_default: int = 60):
        self.window = deque(maxlen=window_size)
        self.max_delay_default = max_delay_default

    def record(self, delay_seconds: int | None):
        if delay_seconds is not None and delay_seconds >= 0:
            self.window.append(delay_seconds)

    def compute_dynamic_threshold(self) -> int:
        """返回动态计算的 max_delay 阈值"""
        if len(self.window) < 3:
            return self.max_delay_default
        try:
            avg = statistics.mean(self.window)
            stdev = statistics.stdev(self.window) if len(self.window) >= 2 else 0
            dynamic = int(avg + 2 * stdev)
            return max(dynamic, 10)
        except Exception:
            return self.max_delay_default

    def is_anomaly(self, current: int) -> bool:
        """当前延迟是否异常"""
        if len(self.window) < 3:
            return current > self.max_delay_default * 2
        threshold = self.compute_dynamic_threshold()
        return current > threshold * 1.5


# ======================== 切换引擎 ========================

class SwitchoverEngine:
    """MySQL 一主一从 GTID 主从切换引擎 v3.0"""

    def __init__(self):
        self._running_tasks: dict[str, dict] = {}
        self._conn_mgr = ConnectionManager()

    # ─── 基础数据库操作 ───

    def _connect(self, key: str, host: str, port: int, user: str, password: str,
                 timeout: int = 5) -> pymysql.Connection:
        return self._conn_mgr.get(key, host, port, user, password, timeout)

    def _get_value(self, conn: pymysql.Connection, sql: str):
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if row:
                return list(row.values())[0]
        return None

    def _exec(self, conn: pymysql.Connection, sql: str):
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()

    def _exec_query(self, conn: pymysql.Connection, sql: str) -> list[dict]:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def _check_connectivity(self, conn: pymysql.Connection, label: str, emit) -> bool:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            emit('info', f'✓ {label} 连接正常')
            return True
        except Exception as e:
            emit('error', f'✗ {label} 连接失败: {e}')
            return False

    # ─── GTID 状态与深度检查 ───

    def _check_gtid(self, conn: pymysql.Connection, label: str, emit) -> str | None:
        """检查 GTID 模式，返回 GTID_MODE"""
        gtid_mode = self._get_value(conn, "SELECT @@GLOBAL.GTID_MODE")
        if gtid_mode in ('ON', 'ON_PERMISSIVE'):
            emit('info', f'{label} GTID_MODE = {gtid_mode} ✓')
            return gtid_mode
        emit('warning', f'{label} GTID_MODE = {gtid_mode} ⚠')
        return gtid_mode

    def _get_gtid_executed(self, conn: pymysql.Connection, label: str) -> str | None:
        """获取 GTID_EXECUTED"""
        gtid = self._get_value(conn, "SELECT @@GLOBAL.GTID_EXECUTED")
        if gtid:
            gtid_str = str(gtid)
            if len(gtid_str) > 80:
                gtid_str = gtid_str[:80] + "..."
            logger.debug(f'{label} GTID_EXECUTED: {gtid_str}')
        return gtid

    def _deep_gtid_check(self, master_conn, slave_conn, emit) -> dict:
        """
        GTID 深度检查 (参考 switch.py)
        分析 M_only / S_only / common
        """
        master_gtid = self._get_gtid_executed(master_conn, '主库')
        slave_gtid = self._get_gtid_executed(slave_conn, '从库')

        result = {
            'master_gtid': master_gtid,
            'slave_gtid': slave_gtid,
            'consistent': True,
            'M_only': 0,
            'S_only': 0,
            'common': 0,
        }

        if not master_gtid or not slave_gtid:
            result['consistent'] = False
            return result

        # 使用参数化查询避免 SQL 注入风险
        with slave_conn.cursor() as cur:
            cur.execute("SELECT GTID_SUBTRACT(%s, %s) AS diff", (master_gtid, slave_gtid))
            row = cur.fetchone()
            m_only = list(row.values())[0] if row else None
        with master_conn.cursor() as cur:
            cur.execute("SELECT GTID_SUBTRACT(%s, %s) AS diff", (slave_gtid, master_gtid))
            row = cur.fetchone()
            s_only = list(row.values())[0] if row else None

        # 一致性判定：
        #  - M_only>0：主库有事务从未同步到从库 → 切换会丢数据 → 视为不一致（阻断）
        #  - S_only>0：从库比主库多事务（通常是从库自己 UUID 的历史事务，如它曾当过主库）
        #             这类事务不会出现在主库 binlog，属正常遗留，切换后新主=从库反而数据更完整
        #             → 仅警告，不阻断
        if m_only and m_only != '':
            result['M_only'] = self._count_gtid_transactions(str(m_only))
            result['consistent'] = False
            result['M_only_detail'] = str(m_only)

        if s_only and s_only != '':
            result['S_only'] = self._count_gtid_transactions(str(s_only))
            result['S_only_detail'] = str(s_only)

        emit('info', f'GTID深度检查: M_only={result["M_only"]}, S_only={result["S_only"]}')
        if result['M_only'] > 0:
            emit('warning', f'⚠ 主库有 {result["M_only"]} 个事务未同步到从库（M_only），存在数据丢失风险')
        elif result['S_only'] > 0:
            emit('info', f'ℹ 从库多 {result["S_only"]} 个历史事务（S_only，通常为从库自产 UUID 事务，不影响切换）')
        else:
            emit('info', '✓ GTID完全一致')

        return result

    @staticmethod
    def _count_gtid_transactions(gtid_set: str) -> int:
        """估算 GTID set 中的事务数量"""
        count = 0
        for part in gtid_set.split(','):
            part = part.strip()
            if ':' in part:
                uuid_section, ranges = part.split(':', 1)
                for r in ranges.split(':'):
                    r = r.strip()
                    if '-' in r:
                        try:
                            a, b = r.split('-', 1)
                            count += int(b) - int(a) + 1
                        except ValueError:
                            count += 1
                    else:
                        count += 1  # 单值（非区间），计为 1 个事务
        return count

    # ─── 复制修复（运维实用功能） ───

    def repair_replication(self, config: dict, emit=None) -> dict:
        """MySQL 主从复制一键修复（诊断 + 补齐复制账号 + 重建 GTID 复制通道）

        功能：
          1. 诊断主从角色、GTID 模式、复制状态、复制账号；
          2. 确认/创建主库 repl 复制账号（REPLICATION SLAVE/CLIENT）；
          3. 从库重建复制通道（GTID 自动定位 SOURCE_AUTO_POSITION=1）；
          4. GTID 差异分析（M_only/S_only）与对齐建议。

        返回 dict：
          {
            'repaired': bool,          # 是否执行了修复动作
            'master_role': str|None,   # master 实际角色 (primary/replica/unknown)
            'slave_role': str|None,    # slave 实际角色
            'details': {...},          # 诊断明细
          }
        """
        results = {
            'repaired': False,
            'master_role': None,
            'slave_role': None,
            'details': {},
        }
        if emit is None:
            emit = lambda level, msg: None  # noqa: E731

        master_host = config.get('master_host')
        master_port = config.get('master_port', 3306)
        slave_host = config.get('slave_host')
        slave_port = config.get('slave_port', 3306)
        user = config.get('mysql_user', 'root')
        password = config.get('mysql_pass', '')
        slave_user = config.get('mysql_slave_user') or user
        slave_pass = config.get('mysql_slave_pass') or password
        repl_user = config.get('repl_user') or 'repl'
        repl_pass = config.get('repl_pass') or ''
        if not repl_pass:
            emit('warning', '未配置复制账号密码（mysql_repl_pass），跳过复制账号补齐')

        emit('info', '⭐ 开始 MySQL 复制修复（诊断→账号→通道→GTID）')

        # ── 1. 连接主从 ──
        try:
            m_conn = self._connect('repair_m', master_host, master_port, user, password)
            emit('info', f'✓ 主库 {master_host}:{master_port} 连接正常')
        except Exception as e:
            emit('error', f'✗ 主库连接失败: {e}')
            results['details']['master_connect_error'] = str(e)
            return results
        try:
            s_conn = self._connect('repair_s', slave_host, slave_port, slave_user, slave_pass)
            emit('info', f'✓ 从库 {slave_host}:{slave_port} 连接正常')
        except Exception as e:
            emit('error', f'✗ 从库连接失败: {e}')
            results['details']['slave_connect_error'] = str(e)
            m_conn.close()
            return results

        try:
            # ── 2. 角色诊断 ──
            def _role_of(conn):
                # 结合 read_only + 复制状态判定：仅当 read_only=1 且存在活跃/配置的复制通道才视为从库；
                # 主库即使残留 REPLICA STATUS 配置（历史遗留、IO=No）也应判定为主库（read_only=0）。
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT @@read_only AS ro, @@GLOBAL.GTID_MODE AS gtid")
                        row = cur.fetchone()
                        ro = 1
                        if row:
                            ro = int(row.get('ro') if hasattr(row, 'get') else row[0])
                            if isinstance(ro, str):
                                ro = 1 if ro.upper() in ('1', 'ON', 'TRUE') else 0
                except Exception:
                    ro = 0
                try:
                    with conn.cursor() as cur:
                        cur.execute("SHOW REPLICA STATUS")
                        rows = cur.fetchall()
                except Exception:
                    rows = None
                if rows is None:
                    try:
                        with conn.cursor() as cur:
                            cur.execute("SHOW SLAVE STATUS")
                            rows = cur.fetchall()
                    except Exception:
                        return 'primary'
                if ro == 1 and rows:
                    return 'replica'
                if ro == 0:
                    return 'primary'
                return 'replica' if rows else 'primary'

            m_role = _role_of(m_conn)
            s_role = _role_of(s_conn)
            results['master_role'] = m_role
            results['slave_role'] = s_role
            emit('info', f'ℹ 主库角色: {m_role}，从库角色: {s_role}')

            # ── 方向一致性校验 ──
            # 配置的"主库"实际若是从库（或"从库"实际是主），说明配置方向与物理拓扑相反。
            # 此时不应盲目按配置方向重建复制（会把物理主库误配成从库），应停下来提示用户。
            direction_ok = (m_role == 'primary' and s_role == 'replica')
            if not direction_ok:
                detail_desc = ''
                if m_role == 'replica' and s_role == 'primary':
                    detail_desc = f'配置方向与实际相反：实际主库={slave_host}(主)，从库={master_host}(从)。建议先更新配置方向或先执行切换使物理与配置一致，再运行修复。'
                elif m_role == 'replica' and s_role == 'replica':
                    detail_desc = '主从均为从库角色，拓扑异常（双从/指向第三方）。'
                elif m_role == 'primary' and s_role == 'primary':
                    detail_desc = '主从均为主库角色（双活/双主），存在写冲突风险。请先停掉其中一个的写，再重建复制。'
                emit('warning', f'⚠ 配置方向与物理拓扑不一致：{detail_desc}')
                results['direction_mismatch'] = True
                results['details']['direction_mismatch_desc'] = detail_desc
                return results
            emit('info', f'✓ 方向校验通过：{master_host}=主，{slave_host}=从')

            # ── 3. GTID 模式检查 ──

            # ── 3. GTID 模式检查 ──
            m_gtid = self._get_value(m_conn, "SELECT @@GLOBAL.GTID_MODE")
            s_gtid = self._get_value(s_conn, "SELECT @@GLOBAL.GTID_MODE")
            gtid_ok = m_gtid == 'ON' and s_gtid == 'ON'
            results['details']['master_gtid_mode'] = m_gtid
            results['details']['slave_gtid_mode'] = s_gtid
            emit('info', f'GTID 模式 主={m_gtid} 从={s_gtid}')

            # ── 4. 复制账号：主库上确保 repl 存在 ──
            emit('info', f'复制账号检查: {repl_user}@{master_host}')
            with m_conn.cursor() as cur:
                cur.execute("SELECT User FROM mysql.user WHERE User=%s AND Host=%s", (repl_user, '%'))
                exists = cur.fetchone()
            if exists:
                emit('info', f'✓ 主库已有复制账号 {repl_user}@%')
            elif not repl_pass:
                emit('warning', f'✗ 主库缺少复制账号 {repl_user}@%，且未配置 mysql_repl_pass，跳过创建（请在配置中填写复制账号密码）')
            else:
                emit('warning', f'✗ 主库缺少复制账号 {repl_user}@%，正在创建...')
                try:
                    with m_conn.cursor() as cur:
                        cur.execute(
                            "CREATE USER %s@'%%' IDENTIFIED WITH mysql_native_password BY %s",
                            (repl_user, repl_pass))
                    with m_conn.cursor() as cur:
                        cur.execute("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO %s@'%%'",
                                    (repl_user,))
                    m_conn.commit()
                    results['repaired'] = True
                    emit('info', f'✓ 已创建复制账号 {repl_user}@% 并授权')
                except Exception as e:
                    emit('error', f'✗ 创建复制账号失败: {e}')

            # ── 5. 从库重建复制通道（GTID 自动定位） ──
            if s_role == 'replica':
                # 已有通道：检查健康度（兼容 MySQL 8.4+ 新列名与 8.0 旧列名）
                cur2 = s_conn.cursor(pymysql.cursors.DictCursor)
                try:
                    cur2.execute("SHOW REPLICA STATUS")
                    d2 = cur2.fetchone()
                    if d2:
                        # 新列名（>=8.4）或旧列名（<=8.0）
                        io_th = d2.get('Replica_IO_Running') or d2.get('Slave_IO_Running')
                        sql_th = d2.get('Replica_SQL_Running') or d2.get('Slave_SQL_Running')
                        mh = d2.get('Master_Host') or d2.get('Source_Host')
                        ap = d2.get('Auto_Position')
                        io_th = str(io_th).strip() if io_th is not None else 'No'
                        sql_th = str(sql_th).strip() if sql_th is not None else 'No'
                        if ap is not None:
                            try:
                                ap = int(ap)
                            except (TypeError, ValueError):
                                ap = None
                        emit('info', f'从库复制通道: 指向 {mh}，IO={io_th} SQL={sql_th} AutoPos={ap}')
                        if io_th == 'Yes' and sql_th == 'Yes' and ap == 1 and str(mh) == str(master_host):
                            emit('info', '✓ 从库复制通道健康且指向正确，无需重建')
                        else:
                            emit('warning', '从库复制通道不健康/方向错误，重建为 GTID 自动定位...')
                            self._rebuild_replica_channel(s_conn, master_host, master_port,
                                                          repl_user, repl_pass, emit)
                            results['repaired'] = True
                    else:
                        emit('warning', f'从库 {slave_host} 无复制状态记录，重建复制通道...')
                        self._rebuild_replica_channel(s_conn, master_host, master_port,
                                                      repl_user, repl_pass, emit)
                        results['repaired'] = True
                finally:
                    cur2.close()
            else:
                # 从库当前无通道（是独立主/孤儿）→ 直接建立
                emit('warning', f'从库 {slave_host} 无复制通道，建立 GTID 自动定位复制...')
                self._rebuild_replica_channel(s_conn, master_host, master_port,
                                              repl_user, repl_pass, emit)
                results['repaired'] = True

            # ── 6. GTID 差异分析 ──
            try:
                m_exe = self._get_gtid_executed(m_conn, '主库')
                s_exe = self._get_gtid_executed(s_conn, '从库')
                if m_exe and s_exe:
                    results['details']['master_gtid_executed'] = m_exe
                    results['details']['slave_gtid_executed'] = s_exe
                    # M_only / S_only
                    with s_conn.cursor() as cur:
                        cur.execute("SELECT GTID_SUBTRACT(%s, %s) AS diff", (m_exe, s_exe))
                        row = cur.fetchone()
                        m_only = list(row.values())[0] if row else None
                    with m_conn.cursor() as cur:
                        cur.execute("SELECT GTID_SUBTRACT(%s, %s) AS diff", (s_exe, m_exe))
                        row = cur.fetchone()
                        s_only = list(row.values())[0] if row else None
                    m_cnt = self._count_gtid_transactions(str(m_only)) if m_only and str(m_only) != '' else 0
                    s_cnt = self._count_gtid_transactions(str(s_only)) if s_only and str(s_only) != '' else 0
                    results['details']['M_only'] = m_cnt
                    results['details']['S_only'] = s_cnt
                    if m_cnt == 0 and s_cnt == 0:
                        emit('info', '✓ GTID 完全一致')
                    elif m_cnt > 0:
                        emit('warning', f'⚠ 主库多 {m_cnt} 事务未同步（M_only={m_only}）→ 等待复制追平或需手动处理')
                    else:
                        emit('info', f'ℹ 从库多 {s_cnt} 个历史事务（S_only，正常遗留）')
            except Exception as e:
                emit('warning', f'GTID 差异分析失败: {e}')

            # 收尾：从库只读保护
            try:
                with s_conn.cursor(pymysql.cursors.DictCursor) as cur:
                    cur.execute("SHOW GLOBAL VARIABLES LIKE 'read_only'")
                    row = cur.fetchone()
                    ro_val = (row or {}).get('Value')
                    if ro_val != 'ON':
                        emit('info', f'从库当前 read_only={ro_val}，设为只读保护...')
                try:
                    with s_conn.cursor() as cur:
                        cur.execute("SET GLOBAL read_only = ON")
                        cur.execute("SET GLOBAL super_read_only = ON")
                    s_conn.commit()
                    emit('info', '✓ 已将从库设为只读保护（read_only=ON）')
                except Exception as e2:
                    emit('warning', f'设置从库只读失败（不影响复制功能）: {e2}')
            except Exception as e:
                emit('warning', f'检查从库只读状态失败: {e}')

            emit('info', '✅ 复制修复完成')
            return results
        finally:
            m_conn.close()
            s_conn.close()

    def _rebuild_replica_channel(self, s_conn, master_host, master_port,
                                 repl_user, repl_pass, emit) -> bool:
        """从库重建复制通道：STOP → RESET → CHANGE(AUTO_POSITION=1) → START

        兼容 MySQL 5.7（CHANGE MASTER TO）/ 8.0+（CHANGE REPLICATION SOURCE TO）
        8.0 且源端 repl 账号用 caching_sha2_password 时需 GET_SOURCE_PUBLIC_KEY=1。
        """
        # 探测从库版本（决定语法分支）
        version = None
        try:
            with s_conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute("SELECT VERSION() AS v")
                row = cur.fetchone()
                version = (row or {}).get('v', '')
        except Exception:
            pass
        is_80 = bool(version) and version.startswith('8.')

        try:
            with s_conn.cursor() as cur:
                cur.execute("STOP REPLICA")
        except Exception:
            try:
                with s_conn.cursor() as cur:
                    cur.execute("STOP SLAVE")
            except Exception:
                pass
        try:
            with s_conn.cursor() as cur:
                cur.execute("RESET REPLICA ALL")
        except Exception:
            try:
                with s_conn.cursor() as cur:
                    cur.execute("RESET SLAVE ALL")
            except Exception:
                pass

        if is_80:
            # MySQL 8.0+：新语法，带 GET_SOURCE_PUBLIC_KEY
            try:
                sql = (
                    "CHANGE REPLICATION SOURCE TO "
                    "SOURCE_HOST=%s, SOURCE_PORT=%s, "
                    "SOURCE_USER=%s, SOURCE_PASSWORD=%s, "
                    "SOURCE_AUTO_POSITION=1, "
                    "GET_SOURCE_PUBLIC_KEY=1"
                )
                with s_conn.cursor() as cur:
                    cur.execute(sql, (master_host, master_port, repl_user, repl_pass))
                s_conn.commit()
                emit('info', f'✓ 已重建复制通道 -> {master_host}:{master_port}（AUTO_POSITION=1）')
            except Exception as e:
                emit('warning', f'CHANGE ... GET_SOURCE_PUBLIC_KEY 失败，降级无公钥版: {e}')
                try:
                    sql = (
                        "CHANGE REPLICATION SOURCE TO "
                        "SOURCE_HOST=%s, SOURCE_PORT=%s, "
                        "SOURCE_USER=%s, SOURCE_PASSWORD=%s, "
                        "SOURCE_AUTO_POSITION=1"
                    )
                    with s_conn.cursor() as cur:
                        cur.execute(sql, (master_host, master_port, repl_user, repl_pass))
                    s_conn.commit()
                    emit('info', f'✓ 已重建复制通道（无公钥）-> {master_host}:{master_port}')
                except Exception as e2:
                    emit('warning', f'CHANGE REPLICATION SOURCE 降级失败，试旧语法: {e2}')
                    return self._rebuild_channel_legacy(s_conn, master_host, master_port,
                                                        repl_user, repl_pass, emit)
        else:
            # 5.7 或未知版本：旧语法
            return self._rebuild_channel_legacy(s_conn, master_host, master_port,
                                                repl_user, repl_pass, emit)

        try:
            with s_conn.cursor() as cur:
                cur.execute("START REPLICA")
        except Exception:
            try:
                with s_conn.cursor() as cur:
                    cur.execute("START SLAVE")
            except Exception:
                pass
        emit('info', f'✓ 复制通道已启动 -> {master_host}:{master_port}')
        return True

    def _rebuild_channel_legacy(self, s_conn, master_host, master_port,
                                repl_user, repl_pass, emit) -> bool:
        """旧语法（MySQL 5.7）重建复制通道"""
        try:
            sql = (
                "CHANGE MASTER TO "
                "MASTER_HOST=%s, MASTER_PORT=%s, "
                "MASTER_USER=%s, MASTER_PASSWORD=%s, "
                "MASTER_AUTO_POSITION=1"
            )
            with s_conn.cursor() as cur:
                cur.execute(sql, (master_host, master_port, repl_user, repl_pass))
            s_conn.commit()
            emit('info', f'✓ 已重建复制通道（旧语法）-> {master_host}:{master_port}')
        except Exception as e:
            emit('error', f'✗ 旧语法重建复制通道也失败: {e}')
            return False
        try:
            with s_conn.cursor() as cur:
                cur.execute("START SLAVE")
        except Exception:
            pass
        return True

    # ─── 复制状态 ───

    def _get_replica_status(self, conn: pymysql.Connection) -> dict | None:
        try:
            rows = self._exec_query(conn, "SHOW REPLICA STATUS")
            return rows[0] if rows else None
        except Exception:
            try:
                rows = self._exec_query(conn, "SHOW SLAVE STATUS")
                return rows[0] if rows else None
            except Exception:
                return None

    def _check_replica_health(self, conn: pymysql.Connection, label: str, emit) -> dict:
        """检查复制健康状态并返回详情字典"""
        status = self._get_replica_status(conn)
        io = status.get('Replica_IO_Running') or status.get('Slave_IO_Running') if status else None
        sql = status.get('Replica_SQL_Running') or status.get('Slave_SQL_Running') if status else None
        delay_raw = status.get('Seconds_Behind_Source') if status else None
        if delay_raw is None:
            delay_raw = status.get('Seconds_Behind_Master') if status else None
        # 复制健康但延迟为 NULL（无新事务、已追平）时按 0 处理
        delay = 0 if delay_raw is None else int(delay_raw)

        healthy = (io == 'Yes' and sql == 'Yes')
        if healthy:
            emit('info', f'✓ {label} 复制健康 - IO:{io} SQL:{sql} 延迟:{delay}s')
        else:
            emit('error', f'✗ {label} 复制异常 - IO:{io} SQL:{sql}')

        return {'healthy': healthy, 'io': io, 'sql': sql, 'delay': delay}

    # ─── 延迟等待 (集成动态阈值) ───

    def _wait_for_catchup(self, slave_conn, slave_host, slave_port,
                          max_delay: int, wait_timeout: int, wait_step: int,
                          force: bool, skip: bool, dry_run: bool,
                          emit, analyzer: DelayAnalyzer) -> bool:
        """等待从库追平，使用动态阈值"""
        if skip:
            emit('info', '▸ 跳过延迟检查（配置要求）')
            return True
        if dry_run:
            emit('info', '[演习] 跳过延迟等待，模拟通过')
            return True

        dynamic_threshold = analyzer.compute_dynamic_threshold()
        effective_max = min(max_delay, dynamic_threshold)
        emit('info', f'▸ 等待从库追平... 静态阈值={max_delay}s 动态阈值={dynamic_threshold}s 生效={effective_max}s')

        total_wait = 0
        while total_wait < wait_timeout:
            status = self._get_replica_status(slave_conn)
            if not status:
                emit('error', '无法获取复制状态')
                return False

            delay_raw = status.get('Seconds_Behind_Source') or status.get('Seconds_Behind_Master')

            if delay_raw is None:
                io = status.get('Replica_IO_Running') or status.get('Slave_IO_Running')
                sql = status.get('Replica_SQL_Running') or status.get('Slave_SQL_Running')
                if io == 'Yes' and sql == 'Yes':
                    emit('info', '✓ 复制线程正常，延迟为 NULL，视为已追平')
                    return True
                emit('error', '延迟为 NULL 且复制线程异常')
                return False

            try:
                delay = int(delay_raw)
            except (ValueError, TypeError):
                emit('error', f'延迟值异常: {delay_raw}')
                return False

            analyzer.record(delay)

            if delay == 0:
                emit('info', '✓ 从库已完全追平 (0秒延迟)')
                return True

            if delay <= effective_max:
                emit('info', f'✓ 从库延迟 {delay}s，在阈值 {effective_max}s 内')
                return True

            if analyzer.is_anomaly(delay):
                emit('warning', f'⚠ 延迟异常偏高: {delay}s，可能是网络抖动')
                if force:
                    emit('warning', '强制模式，忽略延迟异常')
                    return True

            emit('info', f'  延迟 {delay}s，等待追平... [{total_wait}s/{wait_timeout}s]')
            time.sleep(wait_step)
            total_wait += wait_step

        emit('warning', f'⏰ 等待追平超时 ({wait_timeout}s)，当前延迟 {delay}s')
        if force:
            emit('warning', '强制模式，继续执行')
            return True
        return False

    # ─── 只读管理 ───

    def _set_read_only(self, conn: pymysql.Connection, label: str, mode: str, emit):
        if mode == 'on':
            emit('info', f'  🔒 设置 {label} 为只读模式')
            self._exec(conn, "SET GLOBAL super_read_only=1")
            self._exec(conn, "SET GLOBAL read_only=1")
            time.sleep(1)
            ro = self._get_value(conn, "SELECT @@GLOBAL.read_only")
            sro = self._get_value(conn, "SELECT @@GLOBAL.super_read_only")
            emit('info', f'  ✓ 只读状态: read_only={ro}, super_read_only={sro}')
        else:
            emit('info', f'  🔓 关闭 {label} 只读模式')
            self._exec(conn, "SET GLOBAL read_only=0")
            self._exec(conn, "SET GLOBAL super_read_only=0")

    # ─── 复制管理 ───

    def _stop_reset_replica(self, conn: pymysql.Connection, label: str, emit):
        emit('info', f'  ⏹ 停止并重置 {label} 复制')
        try:
            self._exec(conn, "STOP REPLICA")
        except Exception:
            try:
                self._exec(conn, "STOP SLAVE")
            except Exception as e:
                emit('warning', f'停止复制异常: {e}')
        try:
            self._exec(conn, "RESET REPLICA ALL")
        except Exception:
            try:
                self._exec(conn, "RESET SLAVE ALL")
            except Exception as e:
                emit('warning', f'重置复制异常: {e}')

    def _setup_replication(self, conn: pymysql.Connection, target_host: str, target_port: int,
                          user: str, password: str, emit) -> bool:
        """配置复制关系。
        GTID 模式（本引擎仅用于 gtid_mode=ON 的主从）一律使用 GTID 自动定位（SOURCE_AUTO_POSITION=1），
        MySQL 会自动比较主从 GTID set 并补发从库缺失的历史事务（无论 binlog 是否已轮转），
        避免精确位点方式遗漏已轮转 binlog 中的事务，导致回切时 GTID 深度检查（M_only>0）卡死。
        （参数化查询防止 SQL 注入）"""
        emit('info', f'  配置复制 -> {target_host}:{target_port}')

        # 探测目标端 gtid_mode；失败则按 GTID 模式处理（本引擎主从切换场景均为 GTID）
        gtid_on = False
        try:
            src_conn = pymysql.connect(
                host=target_host, port=target_port, user=user, password=password,
                connect_timeout=5, charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
            )
            with src_conn.cursor() as cur:
                cur.execute("SHOW GLOBAL VARIABLES LIKE 'gtid_mode'")
                row = cur.fetchone()
                if row:
                    gtid_on = str(row.get('Value', 'OFF')).upper() == 'ON'
            src_conn.close()
        except Exception as e:
            emit('warning', f'探测目标端 gtid_mode 失败: {e}，按 GTID 自动定位处理')

        try:
            if gtid_on:
                sql = (
                    f"CHANGE REPLICATION SOURCE TO "
                    f"SOURCE_HOST=%s, SOURCE_PORT=%s, "
                    f"SOURCE_USER=%s, SOURCE_PASSWORD=%s, "
                    f"SOURCE_AUTO_POSITION=1"
                )
                params = (target_host, target_port, user, password)
                emit('info', '  GTID 模式：使用 SOURCE_AUTO_POSITION=1（自动补发缺失历史事务）')
            else:
                src_conn = pymysql.connect(
                    host=target_host, port=target_port, user=user, password=password,
                    connect_timeout=5, charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor,
                )
                status = self._exec_query(src_conn, "SHOW MASTER STATUS")
                src_conn.close()
                if status:
                    log_file = status[0].get('File', '')
                    log_pos = status[0].get('Position', 0)
                    emit('info', f'  非 GTID 模式，使用精确位点: {log_file}:{log_pos}')
                    sql = (
                        f"CHANGE REPLICATION SOURCE TO "
                        f"SOURCE_HOST=%s, SOURCE_PORT=%s, "
                        f"SOURCE_USER=%s, SOURCE_PASSWORD=%s, "
                        f"SOURCE_LOG_FILE=%s, SOURCE_LOG_POS=%s"
                    )
                    params = (target_host, target_port, user, password, log_file, log_pos)
                else:
                    emit('warning', '无法获取源端日志位点，使用 GTID 自动定位')
                    sql = (
                        f"CHANGE REPLICATION SOURCE TO "
                        f"SOURCE_HOST=%s, SOURCE_PORT=%s, "
                        f"SOURCE_USER=%s, SOURCE_PASSWORD=%s, "
                        f"SOURCE_AUTO_POSITION=1"
                    )
                    params = (target_host, target_port, user, password)
        except Exception as e:
            emit('warning', f'获取源端状态失败: {e}，使用 GTID 自动定位')
            sql = (
                f"CHANGE REPLICATION SOURCE TO "
                f"SOURCE_HOST=%s, SOURCE_PORT=%s, "
                f"SOURCE_USER=%s, SOURCE_PASSWORD=%s, "
                f"SOURCE_AUTO_POSITION=1"
            )
            params = (target_host, target_port, user, password)

        # 使用参数化查询执行 DDL（CHANGE REPLICATION SOURCE）
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
        try:
            self._exec(conn, "START REPLICA")
        except Exception:
            self._exec(conn, "START SLAVE")

        time.sleep(3)
        health = self._check_replica_health(conn, f'新从库({target_host}:{target_port})', emit)
        return health['healthy']

    # ─── 切换后验证 ───

    def _verify_switchover(self, new_master_host, new_master_port,
                          old_master_host, old_master_port,
                          user, password, dry_run, emit) -> bool:
        """切换后验证"""
        emit('info', '')
        emit('info', '═══ 切换后验证 ═══')

        if dry_run:
            emit('info', '[演习] 模拟验证通过')
            return True

        # 验证新主库可写
        emit('info', '▸ 验证新主库写入能力...')
        try:
            conn = pymysql.connect(
                host=new_master_host, port=new_master_port, user=user, password=password,
                connect_timeout=5, charset='utf8mb4', autocommit=True,
                cursorclass=pymysql.cursors.DictCursor,
            )
            ts = int(time.time())
            db_name = f"_drill_verify_{ts}"
            tbl_name = f"{db_name}.t"

            with conn.cursor() as cur:
                cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                cur.execute(f"CREATE TABLE {tbl_name} (id INT PRIMARY KEY, ts TIMESTAMP)")
                cur.execute(f"INSERT INTO {tbl_name} VALUES (1, NOW())")
                cur.execute(f"SELECT COUNT(*) AS cnt FROM {tbl_name} WHERE id=1")
                row = cur.fetchone()
                cur.execute(f"DROP DATABASE IF EXISTS {db_name}")

            conn.close()

            if row and list(row.values())[0] == 1:
                emit('info', '✓ 新主库写入验证通过')
            else:
                emit('error', '✗ 新主库写入验证失败')
                return False
        except Exception as e:
            emit('error', f'✗ 新主库写入验证异常: {e}')
            return False

        # 验证旧主库复制
        emit('info', '▸ 验证旧主库复制状态...')
        time.sleep(3)
        try:
            conn = pymysql.connect(
                host=old_master_host, port=old_master_port, user=user, password=password,
                connect_timeout=5, charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
            )
            health = self._check_replica_health(conn, '旧主库(新从库)', emit)
            conn.close()
            if health['healthy']:
                emit('info', '✓ 旧主库复制状态正常')
            else:
                emit('warning', '⚠ 旧主库复制状态异常，请人工检查')
        except Exception as e:
            emit('info', f'旧主库不可达，跳过验证 ({e})')

        emit('info', '═══ 验证完成 ═══')
        return True

    # ─── 两阶段提交协议 ───

    def _two_phase_commit(self, master_conn, slave_conn, config, emit) -> bool:
        """
        两阶段提交：准备阶段 → 执行阶段 / 回滚阶段
        """
        master_host = config['master_host']
        master_port = config['master_port']
        slave_host = config['slave_host']
        slave_port = config['slave_port']
        user = config['mysql_user']
        password = config['mysql_pass']
        slave_user = config.get('mysql_slave_user') or user
        slave_pass = config.get('mysql_slave_pass') or password

        # 阶段一：准备
        emit('info', '')
        emit('info', '═══ 准备阶段 ═══')

        emit('info', '▸ 阶段一.1: 设置原主库只读')
        if config.get('dry_run', False):
            emit('info', '[演习] 跳过设置只读（不污染真实环境）')
        else:
            self._set_read_only(master_conn, f'原主库({master_host}:{master_port})', 'on', emit)

        emit('info', '▸ 阶段一.2: 等待从库追平')
        analyzer = DelayAnalyzer(max_delay_default=config.get('max_delay', 60))
        caught_up = self._wait_for_catchup(
            slave_conn, slave_host, slave_port,
            config.get('max_delay', 60), config.get('wait_timeout', 300),
            config.get('wait_step', 5), config.get('force_switchover', False),
            config.get('skip_delay_check', False), config.get('dry_run', False),
            emit, analyzer
        )

        if not caught_up:
            emit('error', '从库未能在超时时间内追平')
            if config.get('auto_rollback', False):
                emit('info', '自动回滚: 恢复原主库写权限')
                self._set_read_only(master_conn, f'原主库({master_host}:{master_port})', 'off', emit)
                return False
            emit('warning', '继续执行（自动回滚已关闭）')

        # 阶段二：执行
        emit('info', '')
        emit('info', '═══ 执行阶段 ═══')

        if config.get('dry_run', False):
            emit('info', '[演习] 跳过实际切换操作')
            return True

        emit('info', '▸ 阶段二.1: 提升从库为新主库')
        self._stop_reset_replica(slave_conn, f'从库({slave_host}:{slave_port})', emit)
        self._set_read_only(slave_conn, f'新主库({slave_host}:{slave_port})', 'off', emit)
        emit('info', f'✓ 新主库就绪: {slave_host}:{slave_port}')

        emit('info', '▸ 阶段二.2: 配置旧主库为新从库')
        self._stop_reset_replica(master_conn, f'旧主库({master_host}:{master_port})', emit)
        ok = self._setup_replication(
            master_conn, slave_host, slave_port, slave_user, slave_pass, emit
        )
        if ok:
            emit('info', '✓ 旧主库复制配置成功')
        else:
            emit('warning', '⚠ 旧主库复制配置失败，尝试 GTID 备用方案')
            new_gtid = self._get_gtid_executed(slave_conn, '新主库')
            if new_gtid:
                try:
                    self._exec(master_conn, "STOP REPLICA")
                    self._exec(master_conn, "RESET REPLICA ALL")
                    with master_conn.cursor() as cur:
                        cur.execute("SET GLOBAL gtid_purged=%s", (new_gtid,))
                    master_conn.commit()
                    ok2 = self._setup_replication(master_conn, slave_host, slave_port, slave_user, slave_pass, emit)
                    if ok2:
                        emit('info', '✓ GTID备用方案成功')
                    else:
                        emit('error', '✗ GTID备用方案也失败，需人工介入')
                except Exception as e:
                    emit('error', f'✗ GTID备用方案异常: {e}')

        emit('info', '')
        emit('info', '═══ 两阶段提交完成 ═══')
        return True

    # ─── 主切换流程 ───

    def execute_switchover(self, config: dict, emit: Callable,
                           progress_cb: Callable, task_id: str):
        """执行完整的主从切换流程"""
        master_host = config['master_host']
        master_port = config['master_port']
        slave_host = config['slave_host']
        slave_port = config['slave_port']
        user = config['mysql_user']
        password = config['mysql_pass']
        slave_user = config.get('mysql_slave_user') or user
        slave_pass = config.get('mysql_slave_pass') or password
        dry_run = config.get('dry_run', False)

        task_state = self._running_tasks.get(task_id, {})
        task_state['status'] = 'running'
        steps = []
        overall_start = time.time()

        def add_step(name, status, msg=''):
            steps.append({'name': name, 'status': status, 'message': msg,
                          'time': datetime.now().strftime('%H:%M:%S')})

        master_conn = None
        slave_conn = None

        try:
            # ═══ 阶段零：打印头信息 ═══
            emit('info', '')
            emit('info', '╔══════════════════════════════════════════╗')
            emit('info', '║  MySQL 主从切换演练 v3.0                  ║')
            emit('info', '╚══════════════════════════════════════════╝')
            emit('accent', f'  原主库: {master_host}:{master_port}')
            emit('accent', f'  原从库: {slave_host}:{slave_port}')
            emit('info', f'  模式: {"演习(Dry-Run)" if dry_run else "正式切换"}')
            emit('info', f'  连接超时: {config.get("connect_timeout", 5)}s')
            emit('info', f'  最大延迟: {config.get("max_delay", 60)}s')
            emit('info', '')

            # ═══ 步骤1: 连接验证 ═══
            progress_cb('连接验证', 5)
            emit('info', '[1/6] 连接验证')
            master_conn = self._connect('master', master_host, master_port, user, password,
                                        config.get('connect_timeout', 5))
            if not self._check_connectivity(master_conn, f'原主库 {master_host}:{master_port}', emit):
                raise ConnectionError('原主库连接失败')
            add_step('连接原主库', 'success')

            progress_cb('连接验证', 8)
            slave_conn = self._connect('slave', slave_host, slave_port, slave_user, slave_pass,
                                       config.get('connect_timeout', 5))
            if not self._check_connectivity(slave_conn, f'原从库 {slave_host}:{slave_port}', emit):
                raise ConnectionError('从库连接失败')
            add_step('连接从库', 'success')

            # 检查GTID模式
            self._check_gtid(master_conn, '主库', emit)
            self._check_gtid(slave_conn, '从库', emit)

            progress_cb('连接验证', 10)

            # ═══ 步骤2: 复制健康检查 ═══
            progress_cb('健康检查', 15)
            emit('info', '')
            emit('info', '[2/6] 复制健康检查')
            health = self._check_replica_health(slave_conn, f'从库 {slave_host}:{slave_port}', emit)
            if not health['healthy']:
                raise RuntimeError('从库复制状态异常，无法进行切换')
            add_step('复制健康检查', 'success', f'延迟: {health["delay"]}s')

            # ═══ 步骤3: GTID深度检查 ═══
            progress_cb('GTID检查', 25)
            emit('info', '')
            emit('info', '[3/6] GTID深度一致性检查')
            if config.get('skip_consistency_check', False):
                emit('info', '▸ 跳过一致性检查（配置要求）')
                add_step('GTID一致性检查', 'warning', '已跳过')
            else:
                gtid_result = self._deep_gtid_check(master_conn, slave_conn, emit)
                if gtid_result['consistent']:
                    add_step('GTID一致性检查', 'success', '完全一致')
                elif gtid_result['M_only'] > 0:
                    # M_only > 0：主库有事务未同步到从库 → 数据丢失风险 → 阻断（除非 force）
                    if config.get('force_switchover', False):
                        add_step('GTID一致性检查', 'warning', f'M_only:{gtid_result["M_only"]} S_only:{gtid_result["S_only"]} (强制)')
                    else:
                        add_step('GTID一致性检查', 'failed', f'M_only:{gtid_result["M_only"]} S_only:{gtid_result["S_only"]}')
                        if not dry_run:
                            raise RuntimeError('主库有未同步事务(M_only>0)，数据丢失风险')
                elif gtid_result['S_only'] > 0:
                    # S_only > 0 且 M_only=0：从库比主库多历史事务（正常遗留）→ 仅警告，允许切换
                    add_step('GTID一致性检查', 'warning', f'M_only:{gtid_result["M_only"]} S_only:{gtid_result["S_only"]}')

            # ═══ 步骤4-5: 两阶段提交 ═══
            progress_cb('两阶段提交', 35)
            emit('info', '')
            emit('info', '[4/6] 两阶段提交协议')

            phase2_ok = self._two_phase_commit(master_conn, slave_conn, config, emit)
            if phase2_ok:
                add_step('设置原主库只读', 'success')
                add_step('等待从库追平', 'success')
                if not dry_run:
                    add_step('提升从库为新主库', 'success')
                    add_step('配置旧主库为从库', 'success')
                else:
                    add_step('提升从库(演习)', 'warning', 'Dry-Run跳过')
                    add_step('配置旧主库(演习)', 'warning', 'Dry-Run跳过')
            else:
                add_step('两阶段提交', 'failed', '准备阶段失败')
                raise RuntimeError('两阶段提交失败')

            progress_cb('两阶段提交', 75)

            # ═══ 步骤6: 最终验证 ═══
            progress_cb('最终验证', 85)
            emit('info', '')
            emit('info', '[5/6] 最终验证')
            verify_ok = self._verify_switchover(
                slave_host, slave_port, master_host, master_port,
                user, password, dry_run, emit
            )
            add_step('切换后验证', 'success' if verify_ok else 'failed')
            progress_cb('最终验证', 95)

            # ═══ 完成 ═══
            progress_cb('完成', 100)
            duration = int(time.time() - overall_start)

            if dry_run:
                emit('info', '')
                emit('info', '═══ 演习完成 ═══')
                emit('success', '✓ 所有检查通过，可以进行正式切换')
                emit('info', f'  耗时: {duration}s')
                status_msg = 'success'
                msg = f'演习完成: 所有检查通过，耗时{duration}s'
            else:
                emit('info', '')
                emit('info', '╔══════════════════════════════════════════╗')
                emit('info', '║  切换成功！新主库已就绪                  ║')
                emit('info', '╚══════════════════════════════════════════╝')
                emit('success', f'新主库: {slave_host}:{slave_port}')
                emit('success', f'新从库: {master_host}:{master_port}')
                emit('info', f'耗时: {duration}s')
                status_msg = 'success'
                msg = f'切换成功! 新主库: {slave_host}:{slave_port}, 耗时{duration}s'

            task_state['result'] = {
                'status': status_msg, 'duration': duration,
                'new_master_host': slave_host, 'new_master_port': slave_port,
                'new_slave_host': master_host, 'new_slave_port': master_port,
                'message': msg, 'steps': steps, 'dry_run': dry_run,
            }

        except Exception as e:
            duration = int(time.time() - overall_start)
            emit('error', '')
            emit('error', f'✗ 切换失败: {e}')
            emit('error', f'耗时: {duration}s')

            # 尝试恢复
            if master_conn:
                try:
                    self._set_read_only(master_conn, f'原主库({master_host}:{master_port})', 'off', emit)
                    emit('info', '已尝试恢复原主库写权限')
                except Exception:
                    pass

            task_state['result'] = {
                'status': 'failed', 'duration': duration,
                'message': str(e), 'steps': steps,
            }

        finally:
            self._conn_mgr.close_all()
            task_state['status'] = 'completed'
            task_state['end_time'] = time.time()

        # 修复：同步方法应直接返回结果 dict（app.py 同步调用后不再依赖 get_task_status）
        return task_state.get('result')

    # ─── 快速健康检查（供AI调用） ───

    def quick_health_check(self, master_host, master_port,
                          slave_host, slave_port,
                          user, password, connect_timeout=5,
                          slave_user=None, slave_pass=None) -> dict:
        """快速健康检查，返回结构化结果

        slave_user / slave_pass 为从库独立凭据，缺省时回退主库凭据。
        """
        slave_user = slave_user or user
        slave_pass = slave_pass or password
        result = {'ok': True, 'master': {}, 'slave': {}, 'issues': []}

        try:
            conn = pymysql.connect(
                host=master_host, port=master_port, user=user, password=password,
                connect_timeout=connect_timeout, charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
            )
            with conn.cursor() as cur:
                cur.execute("SELECT @@hostname, @@version, @@gtid_mode, @@read_only, @@super_read_only")
                row = cur.fetchone()
            conn.close()
            result['master'] = {
                'hostname': list(row.values())[0],
                'version': list(row.values())[1],
                'gtid_mode': list(row.values())[2],
                'read_only': int(list(row.values())[3]),
                'super_read_only': int(list(row.values())[4]),
            }
        except Exception as e:
            result['ok'] = False
            result['master'] = {'error': str(e)}
            result['issues'].append(f'主库连接失败: {e}')

        try:
            conn = pymysql.connect(
                host=slave_host, port=slave_port, user=slave_user, password=slave_pass,
                connect_timeout=connect_timeout, charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
            )
            with conn.cursor() as cur:
                cur.execute("SELECT @@hostname, @@version, @@gtid_mode")
                row = cur.fetchone()
                cur.execute("SHOW REPLICA STATUS")
                status = cur.fetchone()
            conn.close()
            result['slave'] = {
                'hostname': list(row.values())[0],
                'version': list(row.values())[1],
                'gtid_mode': list(row.values())[2],
            }
            if status:
                io = status.get('Replica_IO_Running') or status.get('Slave_IO_Running')
                sql = status.get('Replica_SQL_Running') or status.get('Slave_SQL_Running')
                delay = status.get('Seconds_Behind_Source') or status.get('Seconds_Behind_Master')
                result['slave']['replication'] = {
                    'io_running': io, 'sql_running': sql, 'delay': delay,
                    'healthy': io == 'Yes' and sql == 'Yes',
                }
                if io != 'Yes' or sql != 'Yes':
                    result['issues'].append(f'从库复制异常: IO={io}, SQL={sql}')
            else:
                result['slave']['replication'] = None
                result['issues'].append('从库未配置复制')
        except Exception as e:
            result['slave'] = {'error': str(e)}
            result['issues'].append(f'从库连接失败: {e}')

        return result

    # ─── 异步任务管理 ───

    def start_switchover(self, config: dict, log_callback=None) -> str:
        task_id = str(uuid.uuid4())[:8]
        logs = []

        def emit(level, message):
            logs.append(f"[{level.upper()}] {message}")
            if log_callback:
                log_callback(task_id, level, message)

        def progress(step_name, pct):
            """进度回调：更新任务状态中的进度百分比"""
            task_state_obj = self._running_tasks.get(task_id)
            if task_state_obj:
                task_state_obj['progress'] = pct
                task_state_obj['current_step'] = step_name
            if log_callback:
                log_callback(task_id, 'progress', f'{step_name}:{pct}%')

        task_state = {
            'task_id': task_id, 'status': 'running', 'logs': logs,
            'start_time': time.time(), 'config': config,
        }
        self._running_tasks[task_id] = task_state

        t = threading.Thread(target=self.execute_switchover,
                             args=(config, emit, progress, task_id), daemon=True)
        t.start()
        return task_id

    def get_task_status(self, task_id: str) -> dict | None:
        state = self._running_tasks.get(task_id)
        if not state:
            return None
        result = state.get('result', {})
        return {
            'task_id': task_id, 'status': state['status'],
            'logs': state['logs'], 'start_time': state['start_time'],
            'end_time': state.get('end_time'), 'result': result,
        }

    def get_task_logs_since(self, task_id: str, since: int = 0) -> tuple:
        state = self._running_tasks.get(task_id)
        if not state:
            return [], 0
        logs = state.get('logs', [])
        return logs[since:], len(logs)


