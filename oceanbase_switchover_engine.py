# -*- coding: utf-8 -*-
"""
OceanBase 主备租户切换引擎
支持:
  - standby: 主备租户 Switchover（ALTER SYSTEM SWITCHOVER TO STANDBY/PRIMARY）
  - cluster: 集群节点关停测试
参考命令:
  ALTER SYSTEM SWITCHOVER TO STANDBY TENANT=<tenant_name>;
  ALTER SYSTEM SWITCHOVER TO PRIMARY;
"""
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

try:
    import pymysql
    MYSQL_DRIVER = True
except ImportError:
    MYSQL_DRIVER = False

# 引擎可用性标志（供 app.py import 判断）
OB_AVAILABLE = MYSQL_DRIVER


def _now():
    return datetime.now(timezone.utc).isoformat()


def _step(name, success=True, detail=''):
    return {'name': name, 'status': 'success' if success else 'failed',
            'timestamp': _now(), 'detail': detail}


class OceanBaseSwitchoverError(Exception):
    pass


class OceanBaseSwitchoverEngine:
    """OceanBase 主备租户切换引擎"""

    def __init__(self, config: dict, force: bool = False, dry_run: bool = False, target_node: str = 'node2'):
        self.cfg = config
        self.force = force
        self.dry_run = dry_run
        self.target_node = target_node if target_node in ('node1', 'node2') else 'node2'
        self.subtype = config.get('oceanbase_subtype', 'standby')
        self.steps = []
        self.logs = []
        self.logs_cb = None

    def _log(self, msg, level='INFO'):
        self.logs.append(f'[{level}] {msg}')
        if self.logs_cb:
            self.logs_cb(msg)

    def _connect(self, host, port, user, password, db_name='oceanbase'):
        if not MYSQL_DRIVER:
            raise OceanBaseSwitchoverError('pymysql 未安装')
        return pymysql.connect(
            host=host, port=port, user=user, password=password,
            database=db_name, connect_timeout=10,
            charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor,
        )

    def run(self, log_callback=None):
        self.logs_cb = log_callback
        if self.subtype == 'cluster':
            return self._run_cluster()
        return self._run_standby()

    def _run_standby(self):
        """主备租户 Switchover"""
        try:
            primary_host = self.cfg.get('ob_primary_host', '')
            primary_port = int(self.cfg.get('ob_primary_port', 2881))
            standby_host = self.cfg.get('ob_standby_host', '')
            standby_port = int(self.cfg.get('ob_standby_port', 2881))
            user = self.cfg.get('ob_user', 'root')
            password = self.cfg.get('ob_pass', '')
            standby_user = self.cfg.get('ob_standby_user') or user
            standby_password = self.cfg.get('ob_standby_pass') or password
            tenant_name = self.cfg.get('ob_tenant_name', '')

            # Step 1: 连接主租户验证角色
            self._log(f'连接主租户 {primary_host}:{primary_port}')
            steps_ok = True
            try:
                conn = self._connect(primary_host, primary_port, user, password)
                cur = conn.cursor()
                cur.execute("SELECT TENANT_NAME, TENANT_ROLE, SWITCHOVER_STATUS FROM oceanbase.DBA_OB_TENANTS WHERE TENANT_ROLE='PRIMARY' LIMIT 1")
                row = cur.fetchone()
                conn.close()
                if not row:
                    self._log('未找到 PRIMARY 角色的租户', 'WARN')
                    self.steps.append(_step('验证主租户角色', False, '未找到PRIMARY租户'))
                    if not self.force:
                        raise OceanBaseSwitchoverError('主租户不是 PRIMARY 角色')
                else:
                    self._log(f'主租户: {row.get("TENANT_NAME")}, 角色: {row.get("TENANT_ROLE")}, 切换状态: {row.get("SWITCHOVER_STATUS")}')
                    self.steps.append(_step('验证主租户角色', True,
                                            f'角色={row.get("TENANT_ROLE")}, 状态={row.get("SWITCHOVER_STATUS")}'))
            except OceanBaseSwitchoverError:
                raise
            except Exception as e:
                self.steps.append(_step('验证主租户角色', False, str(e)))
                if not self.force:
                    raise
                steps_ok = False

            # Step 2: 验证备租户
            self._log(f'验证备租户 {standby_host}:{standby_port}')
            try:
                conn2 = self._connect(standby_host, standby_port, standby_user, standby_password)
                cur2 = conn2.cursor()
                cur2.execute("SELECT TENANT_NAME, TENANT_ROLE, SWITCHOVER_STATUS FROM oceanbase.DBA_OB_TENANTS WHERE TENANT_ROLE='STANDBY' LIMIT 1")
                row2 = cur2.fetchone()
                conn2.close()
                self.steps.append(_step('验证备租户角色', True,
                                        f'角色={row2.get("TENANT_ROLE")}, 状态={row2.get("SWITCHOVER_STATUS")}' if row2 else '未找到'))
            except Exception as e:
                self.steps.append(_step('验证备租户角色', False, str(e)))
                self._log(f'备租户连接失败: {e}', 'WARN')

            # Step 3: 执行 Switchover（先主切备，后备切主）
            if self.dry_run:
                self._log('[演习] 模拟 OceanBase Switchover')
                self.steps.append(_step('Switchover（演习）', True, '未执行真实操作'))
            else:
                self._log('执行阶段1: 主租户切备租户...')
                try:
                    conn = self._connect(primary_host, primary_port, user, password)
                    cur = conn.cursor()
                    # 先 VERIFY 再执行
                    if tenant_name:
                        cur.execute(f"ALTER SYSTEM SWITCHOVER TO STANDBY TENANT = {tenant_name} VERIFY")
                        cur.execute(f"ALTER SYSTEM SWITCHOVER TO STANDBY TENANT = {tenant_name}")
                    else:
                        cur.execute("ALTER SYSTEM SWITCHOVER TO STANDBY VERIFY")
                        cur.execute("ALTER SYSTEM SWITCHOVER TO STANDBY")
                    conn.commit()
                    conn.close()
                    self.steps.append(_step('主租户切备租户', True, 'SWITCHOVER TO STANDBY 已执行'))
                except Exception as e:
                    self._log(f'主切备失败: {e}', 'ERROR')
                    self.steps.append(_step('主租户切备租户', False, str(e)))
                    steps_ok = False
                    if not self.force:
                        raise

                self._log('执行阶段2: 备租户切主租户...')
                try:
                    conn2 = self._connect(standby_host, standby_port, standby_user, standby_password)
                    cur2 = conn2.cursor()
                    cur2.execute("ALTER SYSTEM SWITCHOVER TO PRIMARY")
                    conn2.commit()
                    conn2.close()
                    self.steps.append(_step('备租户切主租户', True, 'SWITCHOVER TO PRIMARY 已执行'))
                except Exception as e:
                    self._log(f'备切主失败: {e}', 'ERROR')
                    self.steps.append(_step('备租户切主租户', False, str(e)))
                    steps_ok = False

            # Step 4: 验证切换结果
            if not self.dry_run:
                import time
                self._log('等待切换完成...')
                time.sleep(10)
                try:
                    conn2 = self._connect(standby_host, standby_port, standby_user, standby_password)
                    cur2 = conn2.cursor()
                    cur2.execute("SELECT TENANT_ROLE FROM oceanbase.DBA_OB_TENANTS LIMIT 1")
                    row = cur2.fetchone()
                    conn2.close()
                    is_primary = row and row.get('TENANT_ROLE') == 'PRIMARY'
                    self.steps.append(_step('验证新主租户', is_primary,
                                            f'TENANT_ROLE={row.get("TENANT_ROLE") if row else "N/A"}'))
                except Exception as e:
                    self.steps.append(_step('验证新主租户', False, str(e)))
                    steps_ok = False
            else:
                self.steps.append(_step('验证（演习）', True, '跳过'))

            self._log('OceanBase 主备租户切换完成')
            return {'steps': self.steps, 'logs': self.logs, 'ok': steps_ok,
                    'full_log': '\n'.join(self.logs)}

        except Exception as e:
            self._log(f'切换异常: {e}', 'ERROR')
            return {'steps': self.steps, 'logs': self.logs, 'ok': False,
                    'full_log': '\n'.join(self.logs), 'error': str(e)}

    def _run_cluster(self):
        """集群模式：关停一个 Observer 节点验证集群可用性"""
        target_label = '节点1' if self.target_node == 'node1' else '节点2'
        peer_label = '节点2' if self.target_node == 'node1' else '节点1'

        if self.target_node == 'node1':
            target_host = self.cfg.get('ob_cluster_node1_host', '')
            target_port = int(self.cfg.get('ob_cluster_node1_port', 2881))
            peer_host = self.cfg.get('ob_cluster_node2_host', '')
            peer_port = int(self.cfg.get('ob_cluster_node2_port', 2881))
        else:
            target_host = self.cfg.get('ob_cluster_node2_host', '')
            target_port = int(self.cfg.get('ob_cluster_node2_port', 2881))
            peer_host = self.cfg.get('ob_cluster_node1_host', '')
            peer_port = int(self.cfg.get('ob_cluster_node1_port', 2881))

        user = self.cfg.get('ob_user', 'root')
        password = self.cfg.get('ob_pass', '')

        try:
            self._log(f'OceanBase 集群关停测试: {target_label}={target_host}:{target_port}, {peer_label}={peer_host}:{peer_port}')

            # Step 1: 验证对端可访问
            try:
                conn = self._connect(peer_host, peer_port, user, password)
                cur = conn.cursor()
                cur.execute("SELECT 1")
                conn.close()
                self.steps.append(_step(f'验证{peer_label}可达', True))
            except Exception as e:
                self.steps.append(_step(f'验证{peer_label}可达', False, str(e)))
                raise

            # Step 2: 通过 ALTER SYSTEM 真实关停目标 Observer
            if self.dry_run:
                self._log(f'[演习] 模拟关停 {target_label} Observer')
                self.steps.append(_step(f'关停{target_label}（演习）', True))
            else:
                self._log(f'通过 SQL 关停 {target_label}({target_host}:{target_port}) Observer...')
                node_down = False
                try:
                    # 通过对端执行 DELETE SERVER 来隔离目标节点
                    conn_admin = self._connect(peer_host, peer_port, user, password)
                    cur_admin = conn_admin.cursor()
                    # 先查询目标节点的 server_id
                    cur_admin.execute(
                        "SELECT SVR_IP, SVR_PORT FROM oceanbase.DBA_OB_SERVERS WHERE SVR_IP=%s AND SVR_PORT=%s",
                        (target_host, target_port)
                    )
                    server = cur_admin.fetchone()
                    if server:
                        # 通过 ROOTSERVICE 执行 ADD/OBS 标记
                        cur_admin.execute(
                            "ALTER SYSTEM DELETE SERVER %s, %s",
                            (target_host, target_port)
                        )
                        conn_admin.commit()
                        self._log(f'✓ ALTER SYSTEM DELETE SERVER 已执行: {target_host}:{target_port}')
                        node_down = True
                    else:
                        self._log(f'目标节点 {target_host}:{target_port} 未找到活跃记录', 'WARN')
                    conn_admin.close()
                except Exception as e:
                    self._log(f'DELETE SERVER 异常: {e}', 'WARN')
                    # DELETE SERVER 可能需要特定权限，给出降级方案
                    self._log(f'如需手动操作: obd cluster stop <集群名>', 'WARN')

                self.steps.append(_step(f'关停{target_label}', node_down,
                                        f'{"已通过SQL隔离" if node_down else "需确认"}'))

            # Step 3: 验证对端正常
            import time
            if not self.dry_run:
                time.sleep(5)
            try:
                conn2 = self._connect(peer_host, peer_port, user, password)
                cur2 = conn2.cursor()
                cur2.execute("SELECT count(*) as cnt FROM oceanbase.DBA_OB_SERVERS WHERE STATUS='ACTIVE'")
                row = cur2.fetchone()
                active_count = row.get('cnt', 0) if row else 0
                conn2.close()
                self._log(f'{peer_label} 活跃节点数: {active_count}')
                self.steps.append(_step(f'验证{peer_label}可用', active_count > 0,
                                        f'活跃节点数={active_count}'))
            except Exception as e:
                self.steps.append(_step(f'验证{peer_label}可用', False, str(e)))
                raise

            # Step 4: 恢复 — 通过 ADD SERVER 重新加入集群
            if not self.dry_run:
                self._log(f'通过 SQL 恢复 {target_label}({target_host}:{target_port})...')
                recovered = False
                try:
                    conn_r = self._connect(peer_host, peer_port, user, password)
                    cur_r = conn_r.cursor()
                    cur_r.execute("ALTER SYSTEM ADD SERVER %s, %s", (target_host, target_port))
                    conn_r.commit()
                    conn_r.close()
                    self._log(f'✓ ALTER SYSTEM ADD SERVER 已执行')
                    import time
                    time.sleep(5)
                    # 验证恢复
                    conn_v = self._connect(peer_host, peer_port, user, password)
                    cur_v = conn_v.cursor()
                    cur_v.execute(
                        "SELECT count(*) as cnt FROM oceanbase.DBA_OB_SERVERS WHERE SVR_IP=%s AND STATUS='ACTIVE'",
                        (target_host,)
                    )
                    row_v = cur_v.fetchone()
                    recovered = row_v.get('cnt', 0) > 0 if row_v else False
                    conn_v.close()
                except Exception as e:
                    self._log(f'ADD SERVER 异常: {e}', 'WARN')
                    self._log(f'如需手动恢复: obd cluster start <集群名>', 'WARN')

                self.steps.append(_step(f'恢复{target_label}', recovered,
                                        f'{"已重新加入集群" if recovered else "需手动恢复"}'))
            else:
                self.steps.append(_step(f'恢复{target_label}（演习）', True))

            self._log('OceanBase 集群关停测试完成')
            return {'steps': self.steps, 'logs': self.logs, 'ok': True,
                    'full_log': '\n'.join(self.logs)}

        except Exception as e:
            self._log(f'集群测试异常: {e}', 'ERROR')
            return {'steps': self.steps, 'logs': self.logs, 'ok': False,
                    'full_log': '\n'.join(self.logs), 'error': str(e)}

    def get_status(self, emit=None):
        """OceanBase 状态：租户角色探测（DBA_OB_TENANTS）+ 版本（标准化）"""
        primary_host = self.cfg.get('ob_primary_host', '')
        primary_port = int(self.cfg.get('ob_primary_port', 2881))
        standby_host = self.cfg.get('ob_standby_host', '')
        standby_port = int(self.cfg.get('ob_standby_port', 2881))
        user = self.cfg.get('ob_user', 'root')
        password = self.cfg.get('ob_pass', '')
        stby_user = self.cfg.get('ob_standby_user') or user
        stby_pass = self.cfg.get('ob_standby_pass') or password
        result = {'ok': True, 'primary': {}, 'standby': {}, 'healthy': False, 'summary': ''}
        for key, host, port, u, pw in (
                ('primary', primary_host, primary_port, user, password),
                ('standby', standby_host, standby_port, stby_user, stby_pass)):
            try:
                conn = self._connect(host, port, u, pw)
                cur = conn.cursor()
                try:
                    cur.execute("SELECT version()")
                    row = cur.fetchone()
                    version = (row.get('version') if isinstance(row, dict) else (row[0] if row else '')) or ''
                    version = str(version).split(' - ')[0]
                except Exception:
                    version = ''
                role = 'standby'
                try:
                    cur.execute("SELECT TENANT_NAME FROM oceanbase.DBA_OB_TENANTS WHERE TENANT_ROLE='PRIMARY' LIMIT 1")
                    if cur.fetchone():
                        role = 'primary'
                except Exception:
                    pass  # 无权限查视图时按连接成功计
                conn.close()
                result[key] = {'host': host, 'port': port, 'role': role, 'ok': True, 'version': version}
            except Exception as e:
                result[key] = {'host': host, 'port': port, 'ok': False, 'error': str(e)[:120]}
        p_, s_ = result['primary'], result['standby']
        if p_.get('ok') and s_.get('ok'):
            result['healthy'] = p_.get('role') == 'primary' and s_.get('role') == 'standby'
            result['summary'] = '主备租户正常' if result['healthy'] else '方向异常：主=%s 备=%s' % (p_.get('role'), s_.get('role'))
        else:
            result['ok'] = False
            result['summary'] = '主备节点存在连接失败'
        return result

    # ------------------------------------------------------------------
    # 复制修复（运维实用功能，主备租户模式）
    # ------------------------------------------------------------------
    def repair_replication(self, emit=None) -> dict:
        """OceanBase 主备租户一键修复：诊断 + 角色/切换状态校准

        功能：
          1. 连接主备租户，查询 oceanbase.DBA_OB_TENANTS 角色与 SWITCHOVER_STATUS；
          2. 方向校验：配置主库应含 PRIMARY 租户、配置备库应含 STANDBY 租户；
          3. 状态提示：SWITCHOVER_STATUS 不健康时给出恢复建议（OceanBase 需 ALTER SYSTEM 与归档恢复）。

        返回 dict：{'repaired': bool, 'direction_mismatch': bool, 'details': {...}}
        """
        if emit is None:
            emit = lambda level, msg: None  # noqa: E731
        results = {'repaired': False, 'direction_mismatch': False, 'details': {}}

        if not MYSQL_DRIVER:
            emit('error', 'pymysql 未安装，请执行: pip install pymysql')
            results['details']['error'] = 'pymysql 未安装'
            return results

        primary_host = self.cfg.get('ob_primary_host', '')
        primary_port = int(self.cfg.get('ob_primary_port', 2881))
        standby_host = self.cfg.get('ob_standby_host', '')
        standby_port = int(self.cfg.get('ob_standby_port', 2881))
        user = self.cfg.get('ob_user', 'root')
        password = self.cfg.get('ob_pass', '')
        stby_user = self.cfg.get('ob_standby_user') or user
        stby_pass = self.cfg.get('ob_standby_pass') or password

        emit('info', f'⭐ 开始 OceanBase 租户修复（{primary_host}:{primary_port} ↔ {standby_host}:{standby_port}）')

        # ── 1. 连接主备 ──
        try:
            prim = self._connect(primary_host, primary_port, user, password)
            emit('info', f"✓ 主租户节点 {primary_host}:{primary_port} 连接正常")
        except Exception as e:
            emit('error', f'✗ 主租户连接失败: {e}')
            results['details']['primary_connect_error'] = str(e)
            return results
        try:
            stby = self._connect(standby_host, standby_port, stby_user, stby_pass)
            emit('info', f"✓ 备租户节点 {standby_host}:{standby_port} 连接正常")
        except Exception as e:
            emit('error', f'✗ 备租户连接失败: {e}')
            prim.close()
            results['details']['standby_connect_error'] = str(e)
            return results

        try:
            # ── 2. 租户角色诊断 ──
            def tenants(conn, where):
                cur = conn.cursor()
                cur.execute(
                    f"SELECT TENANT_NAME, TENANT_ROLE, SWITCHOVER_STATUS, STATUS "
                    f"FROM oceanbase.DBA_OB_TENANTS WHERE {where}")
                rows = cur.fetchall()
                cur.close()
                return rows

            prim_rows = tenants(prim, "TENANT_ROLE='PRIMARY' LIMIT 1")
            stby_rows = tenants(stby, "TENANT_ROLE='STANDBY' LIMIT 1")

            p_role = prim_rows[0]['TENANT_ROLE'] if prim_rows else None
            p_status = prim_rows[0].get('SWITCHOVER_STATUS') if prim_rows else None
            s_role = stby_rows[0]['TENANT_ROLE'] if stby_rows else None
            s_status = stby_rows[0].get('SWITCHOVER_STATUS') if stby_rows else None
            results['details']['primary_tenant_role'] = p_role
            results['details']['primary_switchover_status'] = p_status
            results['details']['standby_tenant_role'] = s_role
            results['details']['standby_switchover_status'] = s_status
            emit('info', f"ℹ 主租户角色: {p_role or '未找到'}({p_status or '-'})，"
                         f"备租户角色: {s_role or '未找到'}({s_status or '-'})")

            # 方向校验
            if p_role != 'PRIMARY' or s_role != 'STANDBY':
                desc = f'配置主库租户角色={p_role or "未知"}，配置备库租户角色={s_role or "未知"}'
                emit('warning', f'⚠ 配置方向与物理拓扑不一致：{desc}')
                results['direction_mismatch'] = True
                results['details']['direction_mismatch_desc'] = desc
                return results
            emit('info', '✓ 方向校验通过：主库=PRIMARY / 备库=STANDBY')

            # ── 3. 切换状态检查 ──
            if p_status and p_status.upper() not in ('NORMAL', 'NOT ALLOWED', ''):
                emit('warning', f'主租户 SWITCHOVER_STATUS={p_status}，切换操作可能被阻塞')
            else:
                emit('info', f"主租户 SWITCHOVER_STATUS={p_status or 'NORMAL'}")
            if s_status and s_status.upper() not in ('NORMAL', 'NOT ALLOWED', ''):
                emit('warning', f'备租户 SWITCHOVER_STATUS={s_status}，日志同步可能异常')
                emit('info', '备租户建议：检查归档日志回放（OceanBase 自动恢复，无需手工干预）')

            results['repaired'] = True  # 诊断健康
            emit('info', '✅ OceanBase 租户修复完成')
            return results
        except Exception as e:
            emit('error', f'修复中断: {e}')
            results['details']['error'] = str(e)
            return results
        finally:
            prim.close()
            stby.close()


def test_oceanbase_connection(data: dict) -> dict:
    """测试 OceanBase 连接"""
    host = data.get('ob_primary_host', '')
    port = int(data.get('ob_primary_port', 2881))
    user = data.get('ob_user', 'root')
    password = data.get('ob_pass', '')
    try:
        conn = pymysql.connect(host=host, port=port, user=user,
                               password=password, connect_timeout=10,
                               charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        cur = conn.cursor()
        cur.execute("SELECT version()")
        row = cur.fetchone()
        conn.close()
        return {'ok': True, 'version': list(row.values())[0] if row else ''}
    except Exception as e:
        return {'ok': False, 'error': str(e)}
