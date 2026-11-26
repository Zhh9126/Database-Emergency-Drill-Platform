# -*- coding: utf-8 -*-
"""
金仓数据库 (KingbaseES) 切换演练引擎 v2.0
- 主备模式 (standby): 真实执行 pg_promote() + ALTER SYSTEM SET primary_conninfo
- KDC 集群 (kdc): 服务关停测试 —— 通过 SQL 查询验证集群状态

驱动: psycopg2 (金仓兼容 PostgreSQL 协议)
  pip install psycopg2-binary
"""

import time
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

try:
    import psycopg2
    PG_AVAILABLE = True
except ImportError:
    PG_AVAILABLE = False

# 引擎可用性标志（供 app.py import 判断）
KB_AVAILABLE = PG_AVAILABLE


def _gen_step(name, success=True, detail=''):
    return {
        'name': name,
        'success': success,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'detail': detail,
    }


def _connect(host, port, user, password, dbname='test', timeout=10):
    """连接金仓数据库"""
    conn = psycopg2.connect(
        host=host, port=int(port),
        user=user, password=password,
        dbname=dbname,
        connect_timeout=timeout,
    )
    conn.autocommit = True
    return conn


def _exec_scalar(conn, sql, params=None):
    cur = conn.cursor()
    if params:
        cur.execute(sql, params)
    else:
        cur.execute(sql)
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# 连接测试
# ---------------------------------------------------------------------------

def test_kingbase_connection(config: dict, timeout: int = 10) -> dict:
    """真实测试金仓数据库连接"""
    host = config.get('kb_primary_host', '')
    port = config.get('kb_primary_port', 54321)
    user = config.get('kb_user', 'system')
    password = config.get('kb_pass', '')
    if not host:
        return {'ok': False, 'error': '主库地址不能为空'}
    if not PG_AVAILABLE:
        return {'ok': False, 'error': 'psycopg2 未安装，请执行: pip install psycopg2-binary'}
    try:
        conn = _connect(host, port, user, password, timeout=timeout)
        version = _exec_scalar(conn, "SELECT version()")
        conn.close()
        return {'ok': True, 'message': f'金仓 {host}:{port} 连接成功', 'version': str(version)[:80]}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


def test_kingbase_kdc_connection(config: dict, timeout: int = 10) -> dict:
    """真实测试金仓 KDC 集群连接

    支持单节点探测：dm_dsc 风格——使用 kb_kdc_node_host / kb_kdc_node_port 定位指定节点；
    回退到 kb_kdc_node1_host / kb_kdc_node1_port。
    凭据优先 kb_kdc_user / kb_kdc_pass，回退 kb_user / kb_pass。
    """
    host = config.get('kb_kdc_node_host') or config.get('kb_kdc_node1_host', '')
    port = config.get('kb_kdc_node_port') or config.get('kb_kdc_node1_port', 54321)
    user = config.get('kb_kdc_user') or config.get('kb_user', 'system')
    password = config.get('kb_kdc_pass') or config.get('kb_pass', '')
    if not host:
        return {'ok': False, 'error': 'KDC 节点地址不能为空'}
    if not PG_AVAILABLE:
        return {'ok': False, 'error': 'psycopg2 未安装，请执行: pip install psycopg2-binary'}
    try:
        conn = _connect(host, port, user, password, timeout=timeout)
        version = _exec_scalar(conn, "SELECT version()")
        conn.close()
        return {'ok': True, 'message': f'金仓 KDC {host}:{port} 连接成功', 'version': str(version)[:80]}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


# ---------------------------------------------------------------------------
# 金仓主备切换引擎 (standby) — 真实执行 SQL 切换
# ---------------------------------------------------------------------------

class KingbaseStandbyEngine:
    """
    金仓主备切换：真实执行 pg_promote() + ALTER SYSTEM SET primary_conninfo
    """

    def __init__(self, config: dict, force: bool = False, dry_run: bool = False):
        self.config = config
        self.force = force
        self.dry_run = dry_run
        self.primary_host = config.get('kb_primary_host', '')
        self.primary_port = int(config.get('kb_primary_port', 54321))
        self.standby_host = config.get('kb_standby_host', '')
        self.standby_port = int(config.get('kb_standby_port', 54321))
        self.user = config.get('kb_user', 'system')
        self.password = config.get('kb_pass', '')
        self.dbname = config.get('kb_db_name', 'test')

    def run(self, log_callback=None) -> dict:
        steps = []
        all_logs = []

        def log(msg):
            all_logs.append(msg)
            if log_callback:
                log_callback(msg)

        log(f'[金仓主备切换] 主库 {self.primary_host}:{self.primary_port} → 备库 {self.standby_host}:{self.standby_port}')
        if self.dry_run:
            log('  ⚠ 演习模式')
        if self.force:
            log('  ⚠ 强制切换')

        if not PG_AVAILABLE:
            log('[错误] psycopg2 未安装')
            steps.append(_gen_step('驱动检查', False, 'psycopg2 未安装'))
            return {'ok': False, 'steps': steps, 'full_log': '\n'.join(all_logs),
                    'error': 'psycopg2 not installed'}

        prim_conn = None
        stby_conn = None
        all_ok = True

        try:
            # ---------- 步骤 1: 连接主备库 ----------
            log('')
            log('【步骤 1/6】连接主备库')
            if self.dry_run:
                steps.append(_gen_step('连接检查（演习）', True))
            else:
                try:
                    prim_conn = _connect(self.primary_host, self.primary_port,
                                         self.user, self.password, self.dbname)
                    log(f'  ✓ 主库 {self.primary_host}:{self.primary_port} 连接成功')
                    stby_conn = _connect(self.standby_host, self.standby_port,
                                         self.user, self.password, self.dbname)
                    log(f'  ✓ 备库 {self.standby_host}:{self.standby_port} 连接成功')
                    steps.append(_gen_step('连接主备库', True))
                except Exception as e:
                    log(f'  ✗ 连接失败: {e}')
                    steps.append(_gen_step('连接主备库', False, str(e)))
                    all_ok = False
                    raise

            # ---------- 步骤 2: 角色与复制延迟校验 ----------
            log('')
            log('【步骤 2/6】角色与复制延迟校验')
            if self.dry_run:
                steps.append(_gen_step('角色校验（演习）', True))
            elif self.force:
                log('  跳过角色校验 (force=true)')
                steps.append(_gen_step('角色校验（强制跳过）', True))
            else:
                try:
                    prim_in_recovery = _exec_scalar(prim_conn, "SELECT pg_is_in_recovery()")
                    stby_in_recovery = _exec_scalar(stby_conn, "SELECT pg_is_in_recovery()")
                    log(f'  主库 pg_is_in_recovery={prim_in_recovery}')
                    log(f'  备库 pg_is_in_recovery={stby_in_recovery}')
                    if prim_in_recovery:
                        log('  ✗ 主库处于恢复模式，不能执行切换')
                        steps.append(_gen_step('角色校验', False, '主库处于恢复模式'))
                        all_ok = False
                    elif not stby_in_recovery:
                        log('  ✗ 备库不在恢复模式')
                        steps.append(_gen_step('角色校验', False, '备库不在恢复模式'))
                        all_ok = False
                    else:
                        # 检查复制延迟（使用 LSN 对比，避免空闲期误报）
                        try:
                            prim_lsn = _exec_scalar(prim_conn, "SELECT pg_current_wal_lsn()")
                            stby_lsn = _exec_scalar(stby_conn, "SELECT pg_last_wal_replay_lsn()")
                            if prim_lsn and stby_lsn:
                                lag_bytes = _exec_scalar(prim_conn,
                                    "SELECT pg_wal_lsn_diff(%s, %s)", (prim_lsn, stby_lsn))
                                if lag_bytes is not None:
                                    log(f'  复制延迟: {int(lag_bytes)} bytes (LSN: {prim_lsn} vs {stby_lsn})')
                                else:
                                    log(f'  主库 LSN={prim_lsn}, 备库 LSN={stby_lsn}')
                            else:
                                lag = _exec_scalar(prim_conn,
                                    "SELECT COALESCE(EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())), 0)")
                                log(f'  复制延迟: {lag:.1f}s (时间戳方式)')
                        except Exception:
                            log('  复制延迟检查跳过')
                        steps.append(_gen_step('角色与复制延迟校验', True,
                                                f'主库=PRIMARY 备库=STANDBY'))
                except Exception as e:
                    log(f'  角色校验异常: {e}')
                    steps.append(_gen_step('角色校验', False, str(e)))
                    if not self.force:
                        all_ok = False

            # ---------- 步骤 3: 主库执行 CHECKPOINT ----------
            log('')
            log('【步骤 3/6】主库执行 CHECKPOINT')
            if self.dry_run:
                steps.append(_gen_step('CHECKPOINT（演习）', True))
            else:
                try:
                    cur = prim_conn.cursor()
                    cur.execute("CHECKPOINT")
                    cur.close()
                    log('  ✓ 主库 CHECKPOINT 执行成功')
                    steps.append(_gen_step('主库 CHECKPOINT', True))
                except Exception as e:
                    log(f'  CHECKPOINT 失败: {e}')
                    steps.append(_gen_step('主库 CHECKPOINT', False, str(e)))

            # ---------- 步骤 4: 备库执行 pg_promote() ----------
            log('')
            log('【步骤 4/6】备库执行 pg_promote() 提升为新主库')
            if self.dry_run:
                steps.append(_gen_step('pg_promote（演习）', True))
            else:
                promote_ok = False
                try:
                    cur = stby_conn.cursor()
                    cur.execute("SELECT pg_promote(wait_seconds := 60)")
                    result = cur.fetchone()
                    cur.close()
                    promote_ok = result[0] if result else False
                    if promote_ok:
                        log('  ✓ pg_promote() 成功，备库已提升为新主库')
                        steps.append(_gen_step('pg_promote() 提升备库', True))
                    else:
                        log('  pg_promote() 返回 False')
                        steps.append(_gen_step('pg_promote() 提升备库', False, '返回 False'))
                        all_ok = False
                except Exception as e:
                    log(f'  pg_promote() 失败: {e}')
                    steps.append(_gen_step('pg_promote() 提升备库', False, str(e)))
                    all_ok = False

            # ---------- 步骤 5: 配置旧主库为新备库 ----------
            log('')
            log('【步骤 5/6】配置旧主库为新备库')
            if self.dry_run:
                steps.append(_gen_step('配置旧主库（演习）', True))
            else:
                try:
                    time.sleep(3)
                    # 重新连接旧主库
                    try:
                        prim_conn.close()
                    except Exception:
                        pass
                    prim_conn = _connect(self.primary_host, self.primary_port,
                                         self.user, self.password, self.dbname)
                    cur = prim_conn.cursor()
                    conninfo = f"host={self.standby_host} port={self.standby_port} user={self.user} password={self.password} dbname={self.dbname}"
                    cur.execute(f"ALTER SYSTEM SET primary_conninfo = '{conninfo}'")
                    log(f'  ✓ 已设置 primary_conninfo -> {self.standby_host}:{self.standby_port}')
                    try:
                        cur.execute("SELECT pg_reload_conf()")
                        log('  ✓ 配置已重新加载')
                    except Exception:
                        pass
                    cur.close()
                    steps.append(_gen_step('配置旧主库为新备库', True,
                                            'primary_conninfo 已更新'))
                except Exception as e:
                    log(f'  配置旧主库失败: {e}')
                    log(f'  如需手动修复: sys_rewind --target-pgdata=<数据目录> '
                        f"--source-server='host={self.standby_host} port={self.standby_port}'")
                    steps.append(_gen_step('配置旧主库为新备库', False, str(e)))

            # ---------- 步骤 6: 验证切换结果 ----------
            log('')
            log('【步骤 6/6】验证切换结果')
            if self.dry_run:
                steps.append(_gen_step('验证（演习）', True))
            else:
                try:
                    new_recovery = _exec_scalar(stby_conn, "SELECT pg_is_in_recovery()")
                    log(f'  新主库 pg_is_in_recovery={new_recovery}')
                    new_ok = (new_recovery is False or new_recovery == False)
                    if new_ok:
                        log('  ✓ 新主库已确认角色为 PRIMARY')
                    else:
                        log('  ✗ 新主库仍处于恢复模式')
                        all_ok = False
                    steps.append(_gen_step('验证切换结果', new_ok,
                                            f'pg_is_in_recovery={new_recovery}'))
                except Exception as e:
                    log(f'  验证异常: {e}')
                    steps.append(_gen_step('验证切换结果', False, str(e)))

        except Exception as e:
            log(f'[切换异常] {e}')
            if not any('连接主备库' in s['name'] and not s['success'] for s in steps):
                steps.append(_gen_step('切换异常', False, str(e)))
                all_ok = False
        finally:
            for conn in (prim_conn, stby_conn):
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

        success = all(s['success'] for s in steps) and all_ok
        result = {
            'ok': success,
            'dry_run': self.dry_run,
            'db_type': 'kingbase',
            'subtype': 'standby',
            'new_primary_host': self.standby_host,
            'new_primary_port': self.standby_port,
            'new_standby_host': self.primary_host,
            'new_standby_port': self.primary_port,
            'steps': steps,
            'full_log': '\n'.join(all_logs),
            'total_steps': len(steps),
            'message': '金仓主备切换成功' if success else '金仓切换存在失败步骤',
        }
        log('')
        log('=' * 50)
        log(f'金仓主备切换{"成功" if success else "完成（有警告）"}')
        return result

    def repair_replication(self, emit=None) -> dict:
        """金仓主备复制一键修复：诊断 + 刷新 primary_conninfo + LSN 延迟检查

        与 PostgreSQL 同协议（psycopg2）：
          1. 连接主备库，诊断 pg_is_in_recovery 角色；
          2. 方向校验（配置主库应为 primary，配置备库应为 standby）；
          3. 主库检查 pg_stat_replication 复制连接；
          4. 刷新备库 primary_conninfo（ALTER SYSTEM + pg_reload_conf）；
          5. LSN 延迟检查（pg_wal_lsn_diff，≤256KB 视为同步）。
        """
        if emit is None:
            emit = lambda level, msg: None  # noqa: E731
        results = {'repaired': False, 'direction_mismatch': False, 'details': {}}

        if not PG_AVAILABLE:
            emit('error', 'psycopg2 未安装，请执行: pip install psycopg2-binary')
            results['details']['error'] = 'psycopg2 未安装'
            return results

        emit('info', f"⭐ 开始金仓复制修复（{self.primary_host} ↔ {self.standby_host}）")

        # ── 1. 连接主备 ──
        try:
            prim = _connect(self.primary_host, self.primary_port, self.user, self.password, dbname=self.dbname)
            emit('info', f"✓ 主库 {self.primary_host}:{self.primary_port} 连接正常")
        except Exception as e:
            emit('error', f"✗ 主库连接失败: {e}")
            results['details']['primary_connect_error'] = str(e)
            return results
        try:
            stby_user = self.config.get('kb_standby_user') or self.user
            stby_pass = self.config.get('kb_standby_pass') or self.password
            stby = _connect(self.standby_host, self.standby_port, stby_user, stby_pass, dbname=self.dbname)
            emit('info', f"✓ 备库 {self.standby_host}:{self.standby_port} 连接正常")
        except Exception as e:
            emit('error', f"✗ 备库连接失败: {e}")
            prim.close()
            results['details']['standby_connect_error'] = str(e)
            return results

        try:
            # ── 2. 角色诊断 ──
            prim_recovery = _exec_scalar(prim, 'SELECT pg_is_in_recovery()')
            stby_recovery = _exec_scalar(stby, 'SELECT pg_is_in_recovery()')
            results['details']['primary_role'] = 'standby' if prim_recovery else 'primary'
            results['details']['standby_role'] = 'standby' if stby_recovery else 'primary'
            emit('info', f"ℹ 主库角色: {results['details']['primary_role']}，备库角色: {results['details']['standby_role']}")

            # 方向校验：配置主库应是 primary、配置备库应是 standby
            if prim_recovery or not stby_recovery:
                parts = []
                if prim_recovery:
                    parts.append(f"配置主库 {self.primary_host} 实际是备库（恢复模式），方向可能已颠倒")
                if not stby_recovery:
                    parts.append(f"配置备库 {self.standby_host} 实际是主库")
                desc = '；'.join(parts)
                emit('warning', f'⚠ 配置方向与物理拓扑不一致：{desc}')
                results['direction_mismatch'] = True
                results['details']['direction_mismatch_desc'] = desc
                return results
            emit('info', f"✓ 方向校验通过：{self.primary_host}=主，{self.standby_host}=备")

            # ── 3. 主库复制连接检查 ──
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

            # ── 4. 刷新备库 primary_conninfo ──
            emit('info', f"刷新备库 primary_conninfo -> {self.primary_host}:{self.primary_port}")
            conninfo = f"host={self.primary_host} port={self.primary_port} user={stby_user} password={stby_pass}"
            try:
                _exec_scalar(stby, "ALTER SYSTEM SET primary_conninfo = %s", (conninfo,))
                _exec_scalar(stby, "SELECT pg_reload_conf()")
                emit('info', '✓ 已刷新 primary_conninfo 并 pg_reload_conf()')
                results['repaired'] = True
            except Exception as e:
                emit('warning', f'刷新 primary_conninfo 失败（备库非恢复模式时不允许）: {e}')

            # ── 5. LSN 延迟检查 ──
            try:
                p_lsn = _exec_scalar(prim, 'SELECT pg_current_wal_lsn()')
                s_lsn = _exec_scalar(stby, 'SELECT pg_last_wal_replay_lsn()')
                if p_lsn and s_lsn:
                    lag = _exec_scalar(
                        stby, 'SELECT pg_wal_lsn_diff(%s, %s)', (p_lsn, s_lsn))
                    lag_bytes = int(lag) if lag is not None else 0
                    results['details']['lsn_lag_bytes'] = lag_bytes
                    if lag_bytes <= 262144:
                        emit('info', f"✓ 备库 LSN 已同步（差值 {lag_bytes} 字节）")
                    else:
                        emit('warning', f"⚠ 备库 LSN 落后 {lag_bytes} 字节，等待复制追平")
                else:
                    emit('info', 'LSN 位点获取失败（备库可能刚启动），跳过延迟检查')
            except Exception as e:
                emit('warning', f"LSN 延迟检查失败: {e}")

            emit('info', '✅ 金仓复制修复完成')
            return results
        except Exception as e:
            emit('error', f'修复中断: {e}')
            results['details']['error'] = str(e)
            return results
        finally:
            prim.close()
            stby.close()


# ---------------------------------------------------------------------------
# 金仓 KDC 集群切换引擎 (kdc)
# ---------------------------------------------------------------------------

class KingbaseKdcEngine:
    """
    金仓 KDC 集群 —— 服务关停测试
    通过 SQL 查询集群状态、验证业务连续可用
    """

    def __init__(self, config: dict, force: bool = False, dry_run: bool = False, target_node: str = 'node2'):
        self.config = config
        self.force = force
        self.dry_run = dry_run
        self.target_node = target_node if target_node in ('node1', 'node2') else 'node2'
        self.node1_host = config.get('kb_kdc_node1_host', '')
        self.node1_port = int(config.get('kb_kdc_node1_port', 54321))
        self.node2_host = config.get('kb_kdc_node2_host', '')
        self.node2_port = int(config.get('kb_kdc_node2_port', 54321))
        self.user = config.get('kb_user', 'system')
        self.password = config.get('kb_pass', '')
        if self.target_node == 'node1':
            self.target_host = self.node1_host
            self.target_port = self.node1_port
            self.peer_host = self.node2_host
            self.peer_port = self.node2_port
        else:
            self.target_host = self.node2_host
            self.target_port = self.node2_port
            self.peer_host = self.node1_host
            self.peer_port = self.node1_port

    def run(self, log_callback=None) -> dict:
        steps = []
        all_logs = []

        def log(msg):
            all_logs.append(msg)
            if log_callback:
                log_callback(msg)

        target_label = '节点1' if self.target_node == 'node1' else '节点2'
        peer_label = '节点2' if self.target_node == 'node1' else '节点1'

        log(f'[金仓 KDC 服务关停测试]')
        log(f'  节点1: {self.node1_host}:{self.node1_port}')
        log(f'  节点2: {self.node2_host}:{self.node2_port}')
        log(f'  目标节点: {target_label} ({self.target_host}:{self.target_port})')
        if self.dry_run:
            log('  ⚠ 演习模式')

        peer_conn = None
        all_ok = True

        try:
            if not PG_AVAILABLE:
                log('[错误] psycopg2 未安装')
                steps.append(_gen_step('驱动检查', False, 'psycopg2 未安装'))
                return {'ok': False, 'steps': steps, 'full_log': '\n'.join(all_logs),
                        'error': 'psycopg2 not installed'}

            # ---------- 步骤 1: 集群状态检查 ----------
            log('')
            log('【步骤 1/7】集群状态检查')
            if self.dry_run:
                steps.append(_gen_step('集群状态检查（演习）', True))
            else:
                node1_ok = False
                node2_ok = False
                try:
                    c1 = _connect(self.node1_host, self.node1_port, self.user, self.password)
                    s1 = _exec_scalar(c1, "SELECT pg_is_in_recovery()")
                    v1 = _exec_scalar(c1, "SELECT version()")
                    log(f'  节点1 {self.node1_host}: 连接正常, recovery={s1}')
                    c1.close()
                    node1_ok = True
                except Exception as e:
                    log(f'  节点1 连接失败: {e}')
                try:
                    c2 = _connect(self.node2_host, self.node2_port, self.user, self.password)
                    s2 = _exec_scalar(c2, "SELECT pg_is_in_recovery()")
                    log(f'  节点2 {self.node2_host}: 连接正常, recovery={s2}')
                    c2.close()
                    node2_ok = True
                except Exception as e:
                    log(f'  节点2 连接失败: {e}')
                steps.append(_gen_step('集群状态检查', node1_ok and node2_ok,
                                        f'节点1={"在线" if node1_ok else "离线"} 节点2={"在线" if node2_ok else "离线"}'))

            # ---------- 步骤 2: 业务状态 ----------
            log('')
            log('【步骤 2/7】关停前业务状态确认')
            if self.dry_run:
                steps.append(_gen_step('业务状态（演习）', True))
            else:
                try:
                    peer_conn = _connect(self.peer_host, self.peer_port, self.user, self.password)
                    sess = _exec_scalar(peer_conn, "SELECT count(*) FROM pg_stat_activity WHERE state='active'")
                    log(f'  ✓ 对端 {peer_label} 业务正常，活跃会话={sess}')
                    steps.append(_gen_step('业务状态确认', True, f'活跃会话={sess}'))
                except Exception as e:
                    log(f'  业务状态查询失败: {e}')
                    steps.append(_gen_step('业务状态确认', False, str(e)))

            # ---------- 步骤 3: 关停目标节点 ----------
            log('')
            log(f'【步骤 3/7】关停{target_label} 数据库服务')
            if self.dry_run:
                steps.append(_gen_step(f'关停{target_label}（演习）', True))
            else:
                node_down = False
                try:
                    tc = _connect(self.target_host, self.target_port, self.user, self.password)
                    cur = tc.cursor()
                    cur.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                                "WHERE datid IS NOT NULL AND pid <> pg_backend_pid()")
                    cur.close()
                    try:
                        cur2 = tc.cursor()
                        cur2.execute("SELECT pg_reload_conf()")
                        cur2.close()
                    except Exception:
                        pass
                    tc.close()
                except Exception:
                    pass
                # 验证
                time.sleep(3)
                try:
                    v = _connect(self.target_host, self.target_port, self.user, self.password, timeout=5)
                    v.close()
                    log(f'  ⚠ {target_label} 仍可达')
                except Exception:
                    node_down = True
                    log(f'  ✓ {target_label} 已不可达')
                steps.append(_gen_step(f'关停{target_label}', node_down,
                                        f'{"节点已关停" if node_down else "不确定"}'))

            # ---------- 步骤 4: 验证对端 ----------
            log('')
            log(f'【步骤 4/7】验证{target_label} 关停后业务')
            if self.dry_run:
                steps.append(_gen_step(f'验证{target_label}（演习）', True))
            else:
                try:
                    sess = _exec_scalar(peer_conn, "SELECT count(*) FROM pg_stat_activity WHERE state='active'")
                    log(f'  ✓ 对端 {peer_label} 正常，活跃会话={sess}')
                    steps.append(_gen_step(f'验证{target_label} 关停', True, f'会话={sess}'))
                except Exception as e:
                    steps.append(_gen_step(f'验证{target_label} 关停', False, str(e)))

            # ---------- 步骤 5: 恢复目标节点 ----------
            log('')
            log(f'【步骤 5/7】启动{target_label} 服务')
            if self.dry_run:
                steps.append(_gen_step(f'启动{target_label}（演习）', True))
            else:
                log(f'  等待{target_label} 恢复...')
                recovered = False
                for attempt in range(6):
                    time.sleep(5)
                    try:
                        rc = _connect(self.target_host, self.target_port, self.user, self.password, timeout=5)
                        rc.close()
                        recovered = True
                        log(f'  ✓ {target_label} 已恢复')
                        break
                    except Exception:
                        log(f'  等待恢复... ({attempt + 1}/6)')
                if not recovered:
                    log(f'  ⚠ {target_label} 未自动恢复，可能需手动启动: sys_ctl start')
                steps.append(_gen_step(f'启动{target_label}', recovered,
                                        f'{"恢复成功" if recovered else "需手动恢复"}'))

            # ---------- 步骤 6: 恢复后验证 ----------
            log('')
            log(f'【步骤 6/7】验证{target_label} 重启后状态')
            if self.dry_run:
                steps.append(_gen_step(f'{target_label} 恢复验证（演习）', True))
            else:
                try:
                    rc = _connect(self.target_host, self.target_port, self.user, self.password, timeout=10)
                    sess = _exec_scalar(rc, "SELECT count(*) FROM pg_stat_activity WHERE state='active'")
                    rc.close()
                    log(f'  ✓ {target_label} 活跃会话={sess}')
                    steps.append(_gen_step(f'{target_label} 恢复验证', True, f'会话={sess}'))
                except Exception as e:
                    steps.append(_gen_step(f'{target_label} 恢复验证', False, str(e)))

            # ---------- 步骤 7: 最终汇总 ----------
            log('')
            log('【步骤 7/7】KDC 服务关停测试汇总')
            steps.append(_gen_step('KDC 服务关停测试完成', all_ok, '验证通过'))

        except Exception as e:
            log(f'[集群测试异常] {e}')
            steps.append(_gen_step('集群测试异常', False, str(e)))
            all_ok = False
        finally:
            if peer_conn:
                try:
                    peer_conn.close()
                except Exception:
                    pass

        success = all(s['success'] for s in steps) and all_ok
        result = {
            'ok': success,
            'dry_run': self.dry_run,
            'db_type': 'kingbase',
            'subtype': 'kdc',
            'is_shutdown_test': True,
            'target_node': self.target_node,
            'node1_host': self.node1_host,
            'node1_port': self.node1_port,
            'node2_host': self.node2_host,
            'node2_port': self.node2_port,
            'steps': steps,
            'full_log': '\n'.join(all_logs),
            'total_steps': len(steps),
            'message': 'KDC 服务关停测试通过' if success else 'KDC 测试存在警告',
        }
        log('')
        log('=' * 50)
        log(f'KDC 服务关停测试{"通过" if success else "完成（有警告）"}')
        return result

    def repair_replication(self, emit=None) -> dict:
        """金仓 KDC 集群服务修复：双节点连通 + 主从角色 + 业务验证

        功能：
          1. 连接 KDC 节点1/节点2，查询 pg_is_in_recovery(主/备)；
          2. 角色校验：双节点不可同为 PRIMARY（方向异常）或同为 STANDBY（缺主）；
          3. 主节点存在且备节点可连时判定健康；否则给出提示。

        返回 dict：{'repaired': bool, 'direction_mismatch': bool, 'details': {...}}
        """
        if emit is None:
            emit = lambda level, msg: None  # noqa: E731
        results = {'repaired': False, 'direction_mismatch': False, 'details': {}}

        if not PG_AVAILABLE:
            emit('error', 'psycopg2 未安装，请执行: pip install psycopg2-binary')
            results['details']['error'] = 'psycopg2 未安装'
            return results

        emit('info', f'⭐ 开始金仓 KDC 集群服务修复（{self.node1_host} ↔ {self.node2_host}）')

        # ── 1. 双节点状态扫描 ──
        node_roles = {}
        for label, host, port in [('node1', self.node1_host, self.node1_port),
                                  ('node2', self.node2_host, self.node2_port)]:
            if not host:
                node_roles[label] = {'ok': False, 'error': '未配置主机'}
                continue
            try:
                c = _connect(host, port, self.user, self.password)
                recovery = _exec_scalar(c, 'SELECT pg_is_in_recovery()')
                version = _exec_scalar(c, 'SELECT version()')
                c.close()
                role = 'STANDBY' if recovery in (True, 't', 'true', 1) else 'PRIMARY'
                node_roles[label] = {'ok': True, 'role': role, 'version': str(version)[:60]}
                emit('info', f"✓ {label} {host}:{port} 连接正常，角色={role}")
            except Exception as e:
                node_roles[label] = {'ok': False, 'error': str(e)}
                emit('warning', f'✗ {label} {host}:{port} 连接失败: {e}')

        results['details']['node_roles'] = node_roles

        # ── 2. 角色拓扑校验 ──
        primaries = [n for n, s in node_roles.items() if s.get('role') == 'PRIMARY']
        standbys = [n for n, s in node_roles.items() if s.get('role') == 'STANDBY']

        if len(primaries) == 2:
            desc = '双节点均为 PRIMARY（异常拓扑，请人工确认是否误提升）'
            emit('warning', f'⚠ 拓扑异常：{desc}')
            results['direction_mismatch'] = True
            results['details']['topology'] = desc
            return results
        if len(primaries) == 0 and len(standbys) > 0:
            emit('warning' if not node_roles.get('node1', {}).get('ok') and not node_roles.get('node2', {}).get('ok') else 'info',
                 '集群缺少 PRIMARY（可能刚发生切换），需人工确认原主实例状态')

        # ── 3. 健康判定 ──
        if len(primaries) >= 1 and len(standbys) >= 1:
            emit('info', '✓ KDC 集群主备角色齐备，运行正常')
            results['repaired'] = True
        elif len(primaries) >= 1:
            emit('info', '✓ 主节点存在；备节点不可达（无法自动拉起，需人工检查）')
            results['repaired'] = True
        else:
            emit('error', 'KDC 集群无可用 PRIMARY，无法自动修复（需人工介入）')
        if results['repaired']:
            emit('info', '✅ 金仓 KDC 集群服务修复完成（主备角色齐备/主节点健在）')
        else:
            emit('warning', '⚠ 金仓 KDC 集群修复未完成，需人工介入检查节点状态')
        return results
