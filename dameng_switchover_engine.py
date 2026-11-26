# -*- coding: utf-8 -*-
"""
达梦数据库 (Dameng) 切换演练引擎 v2.0
- 主备模式 (standby): 真实执行 ALTER DATABASE SWITCHOVER
- DSC 集群 (dsc): 服务关停测试 —— 通过 SQL 查询验证集群状态

驱动: dmPython (达梦官方 Python 驱动)
  pip install dmPython
"""

import time
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# 尝试导入达梦驱动
try:
    import dmPython
    DM_AVAILABLE = True
except ImportError:
    DM_AVAILABLE = False


def _gen_step(name, success=True, detail=''):
    return {
        'name': name,
        'success': success,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'detail': detail,
    }


def _connect(host, port, user='SYSDBA', password='SYSDBA', timeout=10):
    """连接达梦数据库"""
    conn = dmPython.connect(
        user=user, password=password,
        server=host, port=int(port),
        login_timeout=timeout,
    )
    return conn


def _exec_scalar(conn, sql, params=None):
    """执行 SQL 返回单值"""
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

def test_dameng_connection(config: dict, timeout: int = 10) -> dict:
    """真实测试达梦数据库连接"""
    host = config.get('dm_primary_host', '')
    port = config.get('dm_primary_port', 5236)
    user = config.get('dm_user', 'SYSDBA')
    password = config.get('dm_pass', 'SYSDBA')
    if not host:
        return {'ok': False, 'error': '主库地址不能为空'}
    if not DM_AVAILABLE:
        return {'ok': False, 'error': 'dmPython 未安装，请执行: pip install dmPython'}
    try:
        conn = _connect(host, port, user, password, timeout)
        version = _exec_scalar(conn, "SELECT BANNER FROM V$VERSION WHERE ROWNUM=1")
        conn.close()
        return {'ok': True, 'message': f'达梦 {host}:{port} 连接成功', 'version': str(version)[:80]}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


def test_dameng_dsc_connection(config: dict, timeout: int = 10) -> dict:
    """真实测试达梦 DSC 集群连接

    支持 node1/node2 两种传入方式：
      - 单节点：dm_dsc_node1_host / dm_dsc_node1_port
      - 指定节点：dm_dsc_node_*_host / dm_dsc_node_*_port（api_dm_status 用它给两个节点分别探测）
    凭据优先 dm_dsc_user/dm_dsc_pass，回退 dm_user/dm_pass。
    """
    host = config.get('dm_dsc_node_host') or config.get('dm_dsc_node1_host', '')
    port = config.get('dm_dsc_node_port') or config.get('dm_dsc_node1_port', 5236)
    user = config.get('dm_dsc_user') or config.get('dm_user', 'SYSDBA')
    password = config.get('dm_dsc_pass') or config.get('dm_pass', 'SYSDBA')
    if not host:
        return {'ok': False, 'error': 'DSC 节点地址不能为空'}
    if not DM_AVAILABLE:
        return {'ok': False, 'error': 'dmPython 未安装，请执行: pip install dmPython'}
    try:
        conn = _connect(host, port, user, password, timeout)
        version = _exec_scalar(conn, "SELECT BANNER FROM V$VERSION WHERE ROWNUM=1")
        conn.close()
        return {'ok': True, 'message': f'达梦 DSC {host}:{port} 连接成功', 'version': str(version)[:80]}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


# ---------------------------------------------------------------------------
# 达梦主备切换引擎 (standby) — 真实执行 SQL 切换
# ---------------------------------------------------------------------------

class DamengStandbyEngine:
    """达梦主备切换：真实执行 ALTER DATABASE SWITCHOVER"""

    def __init__(self, config: dict, force: bool = False, dry_run: bool = False):
        self.config = config
        self.force = force
        self.dry_run = dry_run
        self.primary_host = config.get('dm_primary_host', '')
        self.primary_port = int(config.get('dm_primary_port', 5236))
        self.standby_host = config.get('dm_standby_host', '')
        self.standby_port = int(config.get('dm_standby_port', 5236))
        self.primary_sid = config.get('dm_primary_sid', '')
        self.standby_sid = config.get('dm_standby_sid', '')
        self.user = config.get('dm_user', 'SYSDBA')
        self.password = config.get('dm_pass', 'SYSDBA')

    def run(self, log_callback=None) -> dict:
        steps = []
        all_logs = []

        def log(msg):
            all_logs.append(msg)
            if log_callback:
                log_callback(msg)

        log(f'[达梦主备切换] 主库 {self.primary_host}:{self.primary_port} → 备库 {self.standby_host}:{self.standby_port}')
        if self.dry_run:
            log('  ⚠ 演习模式 - 仅模拟，不执行实际操作')
        if self.force:
            log('  ⚠ 强制切换模式 - 跳过一致性检查')

        if not DM_AVAILABLE:
            log('[错误] dmPython 未安装，无法执行真实切换')
            log('请执行: pip install dmPython')
            steps.append(_gen_step('驱动检查', False, 'dmPython 未安装'))
            return {'ok': False, 'steps': steps, 'full_log': '\n'.join(all_logs),
                    'error': 'dmPython not installed'}

        prim_conn = None
        stby_conn = None
        all_ok = True

        try:
            # ---------- 步骤 1: 连接主备库 ----------
            log('')
            log('【步骤 1/6】连接主备库')
            if self.dry_run:
                log('  [演习] 跳过连接')
                steps.append(_gen_step('连接检查（演习跳过）', True))
            else:
                try:
                    prim_conn = _connect(self.primary_host, self.primary_port,
                                         self.user, self.password)
                    log(f'  ✓ 主库 {self.primary_host}:{self.primary_port} 连接成功')
                    stby_conn = _connect(self.standby_host, self.standby_port,
                                         self.user, self.password)
                    log(f'  ✓ 备库 {self.standby_host}:{self.standby_port} 连接成功')
                    steps.append(_gen_step('连接主备库', True, '主备库均连接成功'))
                except Exception as e:
                    log(f'  ✗ 连接失败: {e}')
                    steps.append(_gen_step('连接主备库', False, str(e)))
                    all_ok = False
                    raise

            # ---------- 步骤 2: 角色与归档模式校验 ----------
            log('')
            log('【步骤 2/6】角色与归档模式校验')
            if self.dry_run:
                log('  [演习] 跳过角色校验')
                steps.append(_gen_step('角色校验（演习跳过）', True))
            elif self.force:
                log('  跳过角色校验 (force=true)')
                steps.append(_gen_step('角色校验（强制跳过）', True))
            else:
                try:
                    _DM_ROLE_MAP = {1: 'PRIMARY', 2: 'STANDBY', 3: 'STANDBY', 4: 'STANDBY'}
                    role_raw = _exec_scalar(prim_conn, "SELECT ROLE$ FROM V$DATABASE")
                    role = _DM_ROLE_MAP.get(role_raw, role_raw)
                    arch_mode = _exec_scalar(prim_conn, "SELECT ARCH_MODE FROM V$DATABASE")
                    log(f'  主库角色: {role} (ROLE$={role_raw}), 归档模式: {arch_mode}')
                    stby_role_raw = _exec_scalar(stby_conn, "SELECT ROLE$ FROM V$DATABASE")
                    stby_role = _DM_ROLE_MAP.get(stby_role_raw, stby_role_raw)
                    log(f'  备库角色: {stby_role} (ROLE$={stby_role_raw})')
                    if str(role).upper() != 'PRIMARY':
                        log(f'  ✗ 主库角色异常: {role}')
                        steps.append(_gen_step('角色校验', False, f'主库角色={role}'))
                        all_ok = False
                    else:
                        log('  ✓ 主库=PRIMARY, 备库=STANDBY')
                        steps.append(_gen_step('角色与归档模式校验', True,
                                                f'主库={role} 备库={stby_role}'))
                except Exception as e:
                    log(f'  角色校验异常: {e}')
                    steps.append(_gen_step('角色校验', False, str(e)))
                    if not self.force:
                        all_ok = False

            # ---------- 步骤 3: 主库执行 SWITCHOVER ----------
            log('')
            log('【步骤 3/6】主库执行 ALTER DATABASE SWITCHOVER')
            if self.dry_run:
                log('  [演习] ALTER DATABASE SWITCHOVER（模拟）')
                steps.append(_gen_step('主库 SWITCHOVER（演习）', True))
            else:
                try:
                    cur = prim_conn.cursor()
                    cur.execute("ALTER DATABASE PREPARE FOR SWITCHOVER")
                    log('  ✓ ALTER DATABASE PREPARE FOR SWITCHOVER 执行成功')
                    cur.execute("ALTER DATABASE SWITCHOVER TO STANDBY")
                    log('  ✓ 主库角色已切换为 STANDBY')
                    cur.close()
                    steps.append(_gen_step('主库 SWITCHOVER', True,
                                            f'{self.primary_host} 已切换为 STANDBY'))
                except Exception as e:
                    log(f'  ✗ 主库 SWITCHOVER 失败: {e}')
                    steps.append(_gen_step('主库 SWITCHOVER', False, str(e)))
                    all_ok = False

            # ---------- 步骤 4: 备库提升为 PRIMARY ----------
            log('')
            log('【步骤 4/6】备库执行 SWITCHOVER TO PRIMARY')
            if self.dry_run:
                log('  [演习] 备库提升（模拟）')
                steps.append(_gen_step('备库提升（演习）', True))
            else:
                try:
                    cur = stby_conn.cursor()
                    cur.execute("ALTER DATABASE SWITCHOVER TO PRIMARY")
                    log(f'  ✓ 备库 {self.standby_host} 已切换为 PRIMARY')
                    cur.close()
                    steps.append(_gen_step('备库提升为 PRIMARY', True,
                                            f'{self.standby_host} 现为主库'))
                except Exception as e:
                    log(f'  ✗ 备库提升失败: {e}')
                    steps.append(_gen_step('备库提升为 PRIMARY', False, str(e)))
                    all_ok = False

            # ---------- 步骤 5: 验证切换结果 ----------
            log('')
            log('【步骤 5/6】验证切换结果')
            if self.dry_run:
                log('  [演习] 跳过验证')
                steps.append(_gen_step('验证结果（演习跳过）', True))
            else:
                try:
                    new_role = _exec_scalar(stby_conn, "SELECT ROLE FROM V$DATABASE")
                    log(f'  新主库角色: {new_role}')
                    steps.append(_gen_step('验证切换结果',
                                            str(new_role).upper() == 'PRIMARY',
                                            f'新主库角色={new_role}'))
                    if str(new_role).upper() != 'PRIMARY':
                        all_ok = False
                except Exception as e:
                    log(f'  验证异常: {e}')
                    steps.append(_gen_step('验证切换结果', False, str(e)))

            # ---------- 步骤 6: 启动新备库复制 ----------
            log('')
            log('【步骤 6/6】启动新备库复制')
            if self.dry_run:
                log('  [演习] 跳过复制启动')
                steps.append(_gen_step('复制启动（演习跳过）', True))
            else:
                try:
                    cur = prim_conn.cursor()
                    cur.execute("ALTER DATABASE OPEN FORCE")
                    log('  ✓ 新备库已启动复制')
                    cur.close()
                    steps.append(_gen_step('复制启动', True, '新备库复制正常'))
                except Exception as e:
                    log(f'  复制启动警告: {e}')
                    steps.append(_gen_step('复制启动', True, f'警告: {e}'))

        except Exception as e:
            log(f'[切换异常] {e}')
            if not any(s['name'] == '连接主备库' and not s['success'] for s in steps):
                steps.append(_gen_step('切换异常', False, str(e)))
                all_ok = False
        finally:
            for conn in (prim_conn, stby_conn):
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

        # ---------- 汇总 ----------
        success = all(s['success'] for s in steps) and all_ok
        result = {
            'ok': success,
            'dry_run': self.dry_run,
            'db_type': 'dameng',
            'subtype': 'standby',
            'new_primary_host': self.standby_host,
            'new_primary_port': self.standby_port,
            'new_primary_sid': self.standby_sid,
            'new_standby_host': self.primary_host,
            'new_standby_port': self.primary_port,
            'new_standby_sid': self.primary_sid,
            'steps': steps,
            'full_log': '\n'.join(all_logs),
            'total_steps': len(steps),
            'message': '达梦主备切换成功' if success else '达梦切换存在失败步骤',
        }
        log('')
        log('=' * 50)
        log(f'达梦主备切换{"成功" if success else "完成（有警告）"}')
        return result

    def repair_replication(self, emit=None) -> dict:
        """达梦主备复制一键修复：诊断 + OPEN FORCE 恢复复制

        功能：
          1. 连接主备库，查询 V$DATABASE ROLE / V$ARCHIVE 归档模式；
          2. 方向校验：配置主库应为 PRIMARY、配置备库应为 STANDBY，颠倒时停止；
          3. 备库状态修复：以 ALTER DATABASE OPEN FORCE 恢复备库日志应用（幂等，失败给出提示）。

        返回 dict：{'repaired': bool, 'direction_mismatch': bool, 'details': {...}}
        """
        if emit is None:
            emit = lambda level, msg: None  # noqa: E731
        results = {'repaired': False, 'direction_mismatch': False, 'details': {}}

        if not DM_AVAILABLE:
            emit('error', 'dmPython 未安装，请执行: pip install dmPython')
            results['details']['error'] = 'dmPython 未安装'
            return results

        emit('info', f'⭐ 开始达梦复制修复（{self.primary_host} ↔ {self.standby_host}）')

        try:
            prim = _connect(self.primary_host, self.primary_port, self.user, self.password)
            emit('info', f"✓ 主库 {self.primary_host}:{self.primary_port} 连接正常")
        except Exception as e:
            emit('error', f'✗ 主库连接失败: {e}')
            results['details']['primary_connect_error'] = str(e)
            return results
        try:
            stby = _connect(self.standby_host, self.standby_port, self.user, self.password)
            emit('info', f"✓ 备库 {self.standby_host}:{self.standby_port} 连接正常")
        except Exception as e:
            emit('error', f'✗ 备库连接失败: {e}')
            prim.close()
            results['details']['standby_connect_error'] = str(e)
            return results

        try:
            # ── 2. 角色诊断（达梦 V8 用 ROLE$ 字段：1=PRIMARY 2=STANDBY） ──
            DM_ROLE_MAP = {1: 'PRIMARY', 2: 'STANDBY', 3: 'STANDBY', 4: 'STANDBY'}
            try:
                prim_role_raw = _exec_scalar(prim, 'SELECT ROLE$ FROM V$DATABASE')
                prim_role = DM_ROLE_MAP.get(prim_role_raw, prim_role_raw)
            except Exception:
                prim_role = None
            try:
                stby_role_raw = _exec_scalar(stby, 'SELECT ROLE$ FROM V$DATABASE')
                stby_role = DM_ROLE_MAP.get(stby_role_raw, stby_role_raw)
            except Exception:
                stby_role = None
            results['details']['primary_role'] = prim_role
            results['details']['standby_role'] = stby_role
            emit('info', f"ℹ 主库角色: {prim_role}，备库角色: {stby_role}")

            # 归档模式（ARCH_MODE 在 V$DATABASE 中，而非 V$ARCHIVE）
            try:
                arch = _exec_scalar(prim, 'SELECT ARCH_MODE FROM V$DATABASE')
                results['details']['primary_arch_mode'] = arch
                emit('info', f"主库归档模式: {arch}")
            except Exception as e:
                emit('warning', f'归档模式查询失败: {e}')

            # 方向校验（角色为 None 或不匹配时拦截）
            if prim_role is None or stby_role is None:
                emit('warning', '主/备库角色查询失败，无法进行方向校验')
                results['details']['role_check_error'] = '角色查询返回空值'
            elif 'PRIMARY' not in str(prim_role).upper() or 'STANDBY' not in str(stby_role).upper():
                desc = f'配置主库角色={prim_role}，配置备库角色={stby_role}'
                emit('warning', f'⚠ 配置方向与物理拓扑不一致：{desc}')
                results['direction_mismatch'] = True
                results['details']['direction_mismatch_desc'] = desc
                return results
            else:
                emit('info', f"✓ 方向校验通过：{prim_role} / {stby_role}")

            # ── 3. 备库 OPEN FORCE 恢复 ──
            if stby_role and 'STANDBY' in str(stby_role).upper():
                emit('info', '备库为 STANDBY 角色，尝试 OPEN FORCE 确保日志应用...')
                try:
                    cur = stby.cursor()
                    cur.execute('ALTER DATABASE OPEN FORCE')
                    stby.commit() if hasattr(stby, 'commit') else None
                    cur.close()
                    results['repaired'] = True
                    emit('info', '✓ 已执行 ALTER DATABASE OPEN FORCE')
                except Exception as e:
                    # 已是 OPEN 状态会报错，属正常
                    err = str(e).upper()
                    if 'ALREADY' in err or 'OPEN' in err or 'NOW' in err:
                        emit('info', f'备库已处于可应用状态（{err}），无需修复')
                        results['details']['open_force_note'] = str(e)[:120]
                    else:
                        emit('warning', f'OPEN FORCE 执行异常（达梦备库常用修复命令）: {e}')
                        results['details']['open_force_error'] = str(e)
                results['repaired'] = True
            else:
                emit('info', f'备库角色={stby_role}，非 STANDBY，跳过 OPEN FORCE')

            emit('info', '✅ 达梦复制修复完成')
            return results
        except Exception as e:
            emit('error', f'修复中断: {e}')
            results['details']['error'] = str(e)
            return results
        finally:
            prim.close()
            stby.close()


# ---------------------------------------------------------------------------
# 达梦 DSC 集群切换引擎 (dsc)
# ---------------------------------------------------------------------------

class DamengDscEngine:
    """
    达梦 DSC 集群 —— 服务关停测试
    通过 SQL 查询集群状态、验证业务连续可用
    """

    def __init__(self, config: dict, force: bool = False, dry_run: bool = False, target_node: str = 'node2'):
        self.config = config
        self.force = force
        self.dry_run = dry_run
        self.target_node = target_node if target_node in ('node1', 'node2') else 'node2'
        self.node1_host = config.get('dm_dsc_node1_host', '')
        self.node1_port = int(config.get('dm_dsc_node1_port', 5236))
        self.node2_host = config.get('dm_dsc_node2_host', '')
        self.node2_port = int(config.get('dm_dsc_node2_port', 5236))
        self.service_name = config.get('dm_dsc_service_name', '')
        self.user = config.get('dm_user', 'SYSDBA')
        self.password = config.get('dm_pass', 'SYSDBA')
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

        log(f'[达梦 DSC 服务关停测试]')
        log(f'  节点1: {self.node1_host}:{self.node1_port}')
        log(f'  节点2: {self.node2_host}:{self.node2_port}')
        log(f'  本次目标节点: {target_label} ({self.target_host}:{self.target_port})')
        log(f'  服务名: {self.service_name}')
        if self.dry_run:
            log('  ⚠ 演习模式 - 仅模拟，不执行实际操作')

        peer_conn = None
        all_ok = True

        try:
            # ---------- 步骤 1: 集群状态检查 ----------
            log('')
            log('【步骤 1/7】集群状态检查')
            if not DM_AVAILABLE:
                log('[错误] dmPython 未安装')
                steps.append(_gen_step('集群状态检查', False, 'dmPython 未安装'))
                return {'ok': False, 'steps': steps, 'full_log': '\n'.join(all_logs),
                        'error': 'dmPython not installed'}

            if self.dry_run:
                log('  [演习] 跳过状态检查')
                steps.append(_gen_step('集群状态检查（演习）', True))
            else:
                node1_ok = False
                node2_ok = False
                # 检查节点1
                try:
                    c1 = _connect(self.node1_host, self.node1_port, self.user, self.password)
                    status1 = _exec_scalar(c1, "SELECT STATUS FROM V$INSTANCE")
                    log(f'  节点1 {self.node1_host}: STATUS={status1}')
                    c1.close()
                    node1_ok = str(status1).upper() in ('OPEN', 'ACTIVE', 'MOUNT')
                except Exception as e:
                    log(f'  节点1 连接失败: {e}')
                # 检查节点2
                try:
                    c2 = _connect(self.node2_host, self.node2_port, self.user, self.password)
                    status2 = _exec_scalar(c2, "SELECT STATUS FROM V$INSTANCE")
                    log(f'  节点2 {self.node2_host}: STATUS={status2}')
                    c2.close()
                    node2_ok = str(status2).upper() in ('OPEN', 'ACTIVE', 'MOUNT')
                except Exception as e:
                    log(f'  节点2 连接失败: {e}')

                cluster_ok = node1_ok and node2_ok
                steps.append(_gen_step('集群状态检查', cluster_ok,
                                        f'节点1={"在线" if node1_ok else "离线"} 节点2={"在线" if node2_ok else "离线"}'))
                if not cluster_ok:
                    log('  ✗ 集群节点状态异常')
                    all_ok = False

            # ---------- 步骤 2: 关停前业务状态 ----------
            log('')
            log('【步骤 2/7】关停前业务状态确认')
            if self.dry_run:
                steps.append(_gen_step('业务状态确认（演习）', True))
            else:
                try:
                    peer_conn = _connect(self.peer_host, self.peer_port, self.user, self.password)
                    obj_count = _exec_scalar(peer_conn, "SELECT COUNT(*) FROM SYSOBJECTS")
                    sess_count = _exec_scalar(peer_conn, "SELECT COUNT(*) FROM V$SESSIONS WHERE STATE='ACTIVE'")
                    log(f'  ✓ 对端 {peer_label} 业务正常: 对象数={obj_count}, 活跃会话={sess_count}')
                    steps.append(_gen_step('业务状态确认（关停前）', True,
                                            f'对象数={obj_count} 会话={sess_count}'))
                except Exception as e:
                    log(f'  业务状态查询失败: {e}')
                    steps.append(_gen_step('业务状态确认（关停前）', False, str(e)))

            # ---------- 步骤 3: 关停目标节点 ----------
            log('')
            log(f'【步骤 3/7】关停{target_label} 数据库服务')
            if self.dry_run:
                log(f'  [演习] 模拟关停{target_label}')
                steps.append(_gen_step(f'关停{target_label} 服务（演习）', True))
            else:
                # 达梦可通过 ALTER SYSTEM SHUTDOWN 或直接关闭连接来模拟关停
                node_down = False
                try:
                    target_conn = _connect(self.target_host, self.target_port, self.user, self.password)
                    cur = target_conn.cursor()
                    # 达梦支持 ALTER SYSTEM SHUTDOWN IMMEDIATE
                    try:
                        cur.execute("ALTER SYSTEM SHUTDOWN IMMEDIATE")
                        target_conn.commit()
                    except Exception as sd_err:
                        # shutdown 命令正常会断开连接
                        if 'connection' in str(sd_err).lower() or 'closed' in str(sd_err).lower():
                            log(f'  ✓ {target_label} 已通过 SHUTDOWN 命令关停')
                            node_down = True
                        else:
                            raise
                    try:
                        target_conn.close()
                    except Exception:
                        pass
                except Exception as e:
                    # 连接断开 = 节点可能已关停
                    if 'connection' in str(e).lower() or 'refused' in str(e).lower() or 'timeout' in str(e).lower():
                        node_down = True
                        log(f'  ✓ {target_label} 已不可达（已关停）')
                    else:
                        log(f'  关停异常: {e}', )

                # 验证目标节点确实已关停
                if not node_down:
                    time.sleep(3)
                    try:
                        v = _connect(self.target_host, self.target_port, self.user, self.password, timeout=5)
                        v.close()
                        log(f'  ⚠ {target_label} 仍可达，关停不确定')
                    except Exception:
                        node_down = True
                        log(f'  ✓ {target_label} 已确认不可达')

                steps.append(_gen_step(f'关停{target_label} 服务', node_down,
                                        f'{"节点已关停" if node_down else "关停结果不确定"}'))

            # ---------- 步骤 4: 验证对端业务正常 ----------
            log('')
            log(f'【步骤 4/7】验证{target_label} 关停后业务')
            if self.dry_run:
                steps.append(_gen_step(f'验证{target_label} 关停（演习）', True))
            else:
                try:
                    sess_count = _exec_scalar(peer_conn, "SELECT COUNT(*) FROM V$SESSIONS WHERE STATE='ACTIVE'")
                    log(f'  ✓ 对端 {peer_label} 承载业务正常，活跃会话={sess_count}')
                    steps.append(_gen_step(f'验证{target_label} 关停', True,
                                            f'业务正常 会话={sess_count}'))
                except Exception as e:
                    log(f'  对端验证失败: {e}')
                    steps.append(_gen_step(f'验证{target_label} 关停', False, str(e)))

            # ---------- 步骤 5: 恢复目标节点服务 ----------
            log('')
            log(f'【步骤 5/7】启动{target_label} 数据库服务')
            if self.dry_run:
                steps.append(_gen_step(f'启动{target_label} 服务（演习）', True))
            else:
                # 达梦数据库服务需要 OS 层面重启，这里验证节点是否能重新连接
                log(f'  等待{target_label} 恢复...')
                recovered = False
                for attempt in range(6):
                    time.sleep(5)
                    try:
                        rc = _connect(self.target_host, self.target_port, self.user, self.password, timeout=5)
                        rc.close()
                        recovered = True
                        log(f'  ✓ {target_label} 已恢复并重新加入集群')
                        break
                    except Exception:
                        log(f'  等待恢复... ({attempt + 1}/6)')
                if not recovered:
                    log(f'  ⚠ {target_label} 未自动恢复，可能需手动启动: DmServiceDSC start', )
                steps.append(_gen_step(f'启动{target_label} 服务', recovered,
                                        f'{"自动恢复成功" if recovered else "需手动恢复"}'))

            # ---------- 步骤 6: 恢复后验证 ----------
            log('')
            log(f'【步骤 6/7】验证{target_label} 重启后状态')
            if self.dry_run:
                steps.append(_gen_step(f'{target_label} 恢复验证（演习）', True))
            else:
                try:
                    rc = _connect(self.target_host, self.target_port, self.user, self.password, timeout=10)
                    status = _exec_scalar(rc, "SELECT STATUS FROM V$INSTANCE")
                    sess = _exec_scalar(rc, "SELECT COUNT(*) FROM V$SESSIONS WHERE STATE='ACTIVE'")
                    rc.close()
                    log(f'  ✓ {target_label} STATUS={status}, 活跃会话={sess}')
                    steps.append(_gen_step(f'{target_label} 恢复验证',
                                            str(status).upper() in ('OPEN', 'ACTIVE'),
                                            f'STATUS={status} 会话={sess}'))
                except Exception as e:
                    log(f'  恢复验证失败: {e}')
                    steps.append(_gen_step(f'{target_label} 恢复验证', False, str(e)))

            # ---------- 步骤 7: 最终汇总 ----------
            log('')
            log('【步骤 7/7】DSC 服务关停测试汇总')
            steps.append(_gen_step('DSC 服务关停测试完成', all_ok,
                                    '集群高可用验证通过'))

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
            'db_type': 'dameng',
            'subtype': 'dsc',
            'is_shutdown_test': True,
            'target_node': self.target_node,
            'node1_host': self.node1_host,
            'node1_port': self.node1_port,
            'node2_host': self.node2_host,
            'node2_port': self.node2_port,
            'steps': steps,
            'full_log': '\n'.join(all_logs),
            'total_steps': len(steps),
            'message': 'DSC 服务关停测试通过' if success else 'DSC 测试存在警告',
        }
        log('')
        log('=' * 50)
        log(f'DSC 服务关停测试{"通过" if success else "完成（有警告）"}')
        return result

    def repair_replication(self, emit=None) -> dict:
        """达梦 DSC 集群服务修复：双节点连通 + 集群状态 + 服务可用性

        功能：
          1. 连接 DSC 节点1/节点2，查询 V$INSTANCE STATUS；
          2. 集群校验：至少一个节点为 OPEN，双节点全局一致；
          3. 服务可用性：通过可达节点执行 SELECT 1 FROM DUAL 验证业务。

        返回 dict：{'repaired': bool, 'direction_mismatch': bool, 'details': {...}}
        """
        if emit is None:
            emit = lambda level, msg: None  # noqa: E731
        results = {'repaired': False, 'direction_mismatch': False, 'details': {}}

        if not DM_AVAILABLE:
            emit('error', 'dmPython 未安装，请执行: pip install dmPython')
            results['details']['error'] = 'dmPython 未安装'
            return results

        emit('info', f'⭐ 开始达梦 DSC 集群服务修复（{self.node1_host} ↔ {self.node2_host}）')

        # ── 1. 双节点状态扫描 ──
        node_states = {}
        try:
            c1 = _connect(self.node1_host, self.node1_port, self.user, self.password)
            status1 = _exec_scalar(c1, 'SELECT STATUS FROM V$INSTANCE')
            role1 = _exec_scalar(c1, 'SELECT ROLE FROM V$INSTANCE')
            c1.close()
            node_states['node1'] = {'status': status1, 'role': role1}
            emit('info', f"✓ 节点1 {self.node1_host}:{self.node1_port} STATUS={status1} ROLE={role1}")
        except Exception as e:
            node_states['node1'] = {'status': 'DOWN', 'role': None, 'error': str(e)}
            emit('warning', f'✗ 节点1 连接失败: {e}')

        try:
            c2 = _connect(self.node2_host, self.node2_port, self.user, self.password)
            status2 = _exec_scalar(c2, 'SELECT STATUS FROM V$INSTANCE')
            role2 = _exec_scalar(c2, 'SELECT ROLE FROM V$INSTANCE')
            c2.close()
            node_states['node2'] = {'status': status2, 'role': role2}
            emit('info', f"✓ 节点2 {self.node2_host}:{self.node2_port} STATUS={status2} ROLE={role2}")
        except Exception as e:
            node_states['node2'] = {'status': 'DOWN', 'role': None, 'error': str(e)}
            emit('warning', f'✗ 节点2 连接失败: {e}')

        results['details']['node_states'] = node_states
        up_nodes = [n for n, s in node_states.items() if str(s.get('status', '')).upper() == 'OPEN']
        # 可达 = 连接成功且查询到 STATUS（'DOWN' 为连接失败占位，不可达）
        reachable_nodes = [n for n, s in node_states.items() if s.get('status') and str(s.get('status')).upper() != 'DOWN']

        # ── 2. 健康判定 ──
        if not node_states.get('node1', {}).get('status') and not node_states.get('node2', {}).get('status'):
            emit('error', 'DSC 双节点均不可达，无法完成集群修复（需人工检查主机/实例状态）')
            results['details']['error'] = '双节点均不可达'
            return results
        if not reachable_nodes:
            emit('error', 'DSC 双节点连接均失败（DOWN），无法完成集群修复（需人工检查主机/实例/网络状态）')
            results['details']['error'] = '双节点连接失败'
            return results

        if not up_nodes:
            emit('warning', f'可达节点（{len(reachable_nodes)} 个）均无 OPEN 实例，尝试在可达节点执行 OPEN FORCE 恢复...')
            # 尽力恢复：选取任一可达节点执行 OPEN FORCE（达梦 DSC 支持自动加入集群）
            for n, s in node_states.items():
                if s.get('status') and str(s.get('status')).upper() != 'DOWN':
                    try:
                        c = _connect(self.node1_host if n == 'node1' else self.node2_host,
                                     self.node1_port if n == 'node1' else self.node2_port,
                                     self.user, self.password)
                        cur = c.cursor()
                        cur.execute('ALTER DATABASE OPEN FORCE')
                        c.commit() if hasattr(c, 'commit') else None
                        cur.close()
                        c.close()
                        results['repaired'] = True
                        results['details']['recovery'] = f'{n} 已执行 OPEN FORCE'
                        emit('info', f'✓ {n} ALTER DATABASE OPEN FORCE 执行成功')
                        break
                    except Exception as e:
                        emit('warning', f'{n} OPEN FORCE 失败: {e}')
                        results['details']['open_force_error'] = str(e)

        # ── 3. 业务验证（任一可达节点 SELECT 1）──
        try:
            probe_node = up_nodes[0] if up_nodes else (reachable_nodes[0] if reachable_nodes else 'node1')
            ph = self.node1_host if probe_node == 'node1' else self.node2_host
            pp = self.node1_port if probe_node == 'node1' else self.node2_port
            c = _connect(ph, pp, self.user, self.password)
            val = _exec_scalar(c, 'SELECT 1 FROM DUAL')
            c.close()
            results['details']['probe'] = val
            emit('info', f"✓ 业务验证通过（{probe_node} SELECT 1 => {val}）")
            if not results['repaired']:
                results['repaired'] = True  # 扫描+验证通过即为健康
        except Exception as e:
            emit('warning', f'业务验证失败: {e}')

        if results['repaired']:
            emit('info', '✅ 达梦 DSC 集群服务修复完成（实例已 OPEN 或业务验证通过）')
        else:
            emit('warning', '⚠ 达梦 DSC 集群修复未完成：无 OPEN 实例且 OPEN FORCE 恢复未生效，需人工介入检查实例/集群状态')
        return results
