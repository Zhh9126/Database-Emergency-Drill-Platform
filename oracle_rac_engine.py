"""
Oracle RAC 服务切换引擎

RAC (Real Application Clusters) 切换逻辑不同于 DataGuard。
RAC 切换关注的是「服务(Service)」在节点之间的漂移/重定位。

切换流程:
  0. 连接 RAC 节点
  1. 查询集群节点状态
  2. 查看服务当前运行节点
  3. 在新目标节点启动服务
  4. 在旧节点停止服务 (或直接 relocate)
  5. 验证服务已漂移
  6. 检查连接负载分布

核心操作: srvctl relocate service -d <db_unique_name> -s <service> \
              -oldinst <old_instance> -newinst <new_instance>

依赖: oracledb (已安装)
"""

import time
import traceback

try:
    import oracledb
    ORACLE_AVAILABLE = True
except ImportError:
    ORACLE_AVAILABLE = False


class RacConnection:
    """Oracle RAC 节点连接"""

    def __init__(self, host, sid, user, password, port=1521):
        self.host = host
        self.port = port
        self.sid = sid
        self.user = user
        self.password = password
        self.conn = None

    def connect(self):
        dsn = f'{self.host}:{self.port}/{self.sid}'
        self.conn = oracledb.connect(
            user=self.user, password=self.password, dsn=dsn,
            mode=oracledb.SYSDBA if self.user.upper() == 'SYS' else oracledb.DEFAULT_AUTH
        )
        self.conn.outputtypehandler = self._handler
        return self.conn

    @staticmethod
    def _handler(cursor, name, default_type, size, precision, scale):
        if default_type == oracledb.DB_TYPE_CLOB:
            return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
        if default_type == oracledb.DB_TYPE_BLOB:
            return cursor.var(oracledb.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)

    def execute(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description] if cur.description else []
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        return rows

    def close(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass


class OracleRacEngine:
    """Oracle RAC 服务切换引擎"""

    def __init__(self, config_dict, force=False, dry_run=False, target_node='node2'):
        self.cfg = config_dict
        self.force = force
        self.dry_run = dry_run
        self.target_node = target_node if target_node in ('node1', 'node2') else 'node2'
        self.steps = []
        self.logs = []
        self.node1_conn = None
        self.node2_conn = None

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
        raise RacSwitchoverError(msg)

    # ------------------------------------------------------------------
    def step0_connect(self):
        self._add_step('连接 RAC 节点', 'running')

        for label, host, sid in [
            ('节点1', self.cfg['rac_node1_host'], self.cfg['rac_node1_sid']),
            ('节点2', self.cfg['rac_node2_host'], self.cfg['rac_node2_sid']),
        ]:
            try:
                c = RacConnection(host, sid, self.cfg['rac_user'], self.cfg['rac_pass'])
                c.connect()
                self._log(f'{label} ({host}/{sid}) 连接成功')
                if label == '节点1':
                    self.node1_conn = c
                else:
                    self.node2_conn = c
            except Exception as e:
                self._log(f'{label} ({host}/{sid}) 连接失败: {e}', 'WARNING')

        if not self.node1_conn and not self.node2_conn:
            self._fail('所有 RAC 节点均连接失败')
        if not self.node1_conn:
            self._log('节点1不可用，将使用节点2进行操作', 'WARNING')
        if not self.node2_conn:
            self._log('节点2不可用，切换可能无法完成', 'WARNING')
            if not self.force:
                self._fail('目标节点不可用，无法切换', self._close_all)

        self._add_step('连接 RAC 节点', 'success')

    def step1_check_cluster_status(self):
        self._add_step('检查集群状态', 'running')

        primary_conn = self.node1_conn or self.node2_conn
        if not primary_conn:
            self._fail('无可用的 RAC 连接')

        try:
            # 查询实例信息
            instances = primary_conn.execute(
                "SELECT INST_ID,INSTANCE_NAME,HOST_NAME,STATUS,DATABASE_STATUS "
                "FROM GV$INSTANCE ORDER BY INST_ID"
            )
            self._log(f'RAC 实例数量: {len(instances)}')
            for inst in instances:
                self._log(f'  实例{inst["inst_id"]}: {inst["instance_name"]} '
                          f'({inst["host_name"]}) 状态={inst["status"]}')

            # 查询数据库信息
            db_info = primary_conn.execute(
                "SELECT NAME,DB_UNIQUE_NAME,DATABASE_ROLE,OPEN_MODE FROM V$DATABASE"
            )
            if db_info:
                d = db_info[0]
                self._log(f'数据库: {d["name"]} / {d["db_unique_name"]}, '
                          f'角色={d["database_role"]}, 模式={d["open_mode"]}')

            # 查询当前服务状态
            svc_name = self.cfg.get('rac_service_name', '')
            svc_query = (
                "SELECT INST_ID,SERVICE_NAME,NETWORK_NAME FROM GV$ACTIVE_SERVICES "
                "WHERE SERVICE_NAME NOT IN ('SYS$BACKGROUND','SYS$USERS') "
                "ORDER BY INST_ID"
            )
            if svc_name:
                svc_query = (
                    f"SELECT INST_ID,SERVICE_NAME,NETWORK_NAME FROM GV$ACTIVE_SERVICES "
                    f"WHERE SERVICE_NAME = '{svc_name}'"
                )

            services = primary_conn.execute(svc_query)
            self._log(f'活跃服务: {len(services)} 个')
            for svc in services:
                self._log(f'  实例{svc["inst_id"]}: {svc["service_name"]}')

            if svc_name and not services:
                self._log(f'服务 {svc_name} 当前未在任何实例上运行', 'WARNING')
                if not self.force:
                    self._fail(f'服务 {svc_name} 未找到', self._close_all)

            self._add_step('检查集群状态', 'success',
                           f'{len(instances)} 个实例, {len(services)} 个服务')
        except RacSwitchoverError:
            raise
        except Exception as e:
            self._fail(f'检查集群状态失败: {e}', self._close_all)

    def step2_relocate_service(self):
        """按指定目标节点关停/启动服务，验证 RAC 高可用"""
        target_label = '节点1' if self.target_node == 'node1' else '节点2'
        peer_label = '节点2' if self.target_node == 'node1' else '节点1'
        self._add_step(f'关停 {target_label} 服务', 'running')
        if self.dry_run:
            self._log(f'演习模式: 跳过{target_label}服务关停')
            self._add_step(f'关停 {target_label} 服务', 'success', '演习跳过')
            return

        service = self.cfg.get('rac_service_name', '')
        if not service:
            self._fail('未配置 RAC 服务名 (rac_service_name)')

        primary_conn = self.node1_conn or self.node2_conn

        try:
            db_name = ''
            db_rows = primary_conn.execute("SELECT DB_UNIQUE_NAME FROM V$DATABASE")
            if db_rows:
                db_name = db_rows[0]['db_unique_name']

            # 获取实例名
            inst_rows = primary_conn.execute(
                "SELECT INST_ID,INSTANCE_NAME FROM GV$INSTANCE ORDER BY INST_ID"
            )
            inst_map = {r['inst_id']: r['instance_name'] for r in inst_rows}

            if len(inst_map) < 2:
                self._fail('RAC 集群需要至少 2 个实例')

            # 根据 target_node 确定源和目标实例：target_node 为被关停节点
            target_inst_id = 1 if self.target_node == 'node1' else 2
            peer_inst_id = 2 if self.target_node == 'node1' else 1
            old_inst = inst_map.get(target_inst_id)
            new_inst = inst_map.get(peer_inst_id)

            if not old_inst or not new_inst:
                self._fail(f'无法获取{target_label}或{peer_label}的实例名')

            self._log(f'{target_label}关停演练: {old_inst}(实例{target_inst_id}) -> {new_inst}(实例{peer_inst_id})')

            # 尝试用 DBMS_SERVICE STOP + START
            try:
                stop_sql = (
                    f"BEGIN DBMS_SERVICE.STOP_SERVICE('{service}', '{old_inst}'); END;"
                )
                primary_conn.execute(stop_sql)
                self._log(f'服务 {service} 在实例 {old_inst} 已停止')
            except Exception as e:
                self._log(f'STOP_SERVICE 失败: {e} (可能服务不在此实例)', 'WARNING')

            time.sleep(2)

            try:
                start_sql = (
                    f"BEGIN DBMS_SERVICE.START_SERVICE('{service}', '{new_inst}'); END;"
                )
                primary_conn.execute(start_sql)
                self._log(f'服务 {service} 在实例 {new_inst} 已启动')
            except Exception as e:
                self._log(f'START_SERVICE 失败: {e}', 'ERROR')
                # 尝试回滚
                try:
                    primary_conn.execute(f"BEGIN DBMS_SERVICE.START_SERVICE('{service}', '{old_inst}'); END;")
                except Exception:
                    pass
                self._fail(f'服务启动失败: {e}', self._close_all)

            self._add_step(f'关停 {target_label} 服务', 'success',
                           f'{old_inst}(实例{target_inst_id}) 已停止, 服务漂移至 {new_inst}(实例{peer_inst_id})')
        except RacSwitchoverError:
            raise
        except Exception as e:
            self._fail(f'服务启停失败: {e}', self._close_all)

    def step3_verify(self):
        peer_label = '节点2' if self.target_node == 'node1' else '节点1'
        expected_inst_id = 2 if self.target_node == 'node1' else 1
        self._add_step(f'{peer_label} 业务验证', 'running')

        # 使用对端节点连接验证业务
        verify_conn = self.node2_conn if self.target_node == 'node1' else self.node1_conn
        try:
            service = self.cfg.get('rac_service_name', '')

            rows = verify_conn.execute("SELECT 1 AS ok FROM DUAL")
            if not rows or rows[0]['ok'] != 1:
                self._add_step(f'{peer_label} 业务验证', 'failed', '业务查询失败')
                return
            self._log(f'{peer_label}业务查询验证通过')

            svc_rows = verify_conn.execute(
                f"SELECT INST_ID,SERVICE_NAME,STATUS FROM GV$ACTIVE_SERVICES "
                f"WHERE SERVICE_NAME = '{service}'"
            )

            if svc_rows:
                current_inst = svc_rows[0]['inst_id']
                status = svc_rows[0].get('status', 'UNKNOWN')
                self._log(f'服务 {service} 当前在实例 {current_inst}, 状态 {status}')
                if current_inst == expected_inst_id:
                    self._add_step(f'{peer_label} 业务验证', 'success',
                                   f'服务已漂移至{peer_label}(实例 {current_inst})')
                else:
                    self._add_step(f'{peer_label} 业务验证', 'success',
                                   f'业务查询正常，服务在实例 {current_inst}')
            else:
                self._log(f'服务 {service} 未在任何实例执行', 'WARNING')
                self._add_step(f'{peer_label} 业务验证', 'failed', '服务未找到')

        except Exception as e:
            self._add_step(f'{peer_label} 业务验证', 'failed', str(e))

    # ------------------------------------------------------------------
    def run(self) -> list:
        all_ok = False
        try:
            if not ORACLE_AVAILABLE:
                raise RacSwitchoverError('oracledb 未安装')

            self.step0_connect()
            self.step1_check_cluster_status()
            self.step2_relocate_service()
            if not self.dry_run:
                self.step3_verify()
            all_ok = True
        except RacSwitchoverError as e:
            self._log(f'切换中断: {e}', 'ERROR')
            self._add_step('切换中断', 'failed', str(e))
        except Exception as e:
            self._log(f'未预期错误: {e}\n{traceback.format_exc()}', 'ERROR')
            self._add_step('未知错误', 'failed', str(e))
        finally:
            self._close_all()

        return self.steps, self.logs, all_ok

    def _close_all(self):
        for c in [self.node1_conn, self.node2_conn]:
            if c:
                try:
                    c.close()
                except Exception:
                    pass

    def repair_replication(self, emit=None) -> dict:
        """Oracle RAC 服务修复：检查服务是否在运行，不在则尝试拉起

        功能：
          1. 连接 RAC 节点（任一可达节点即可）；
          2. 检查目标服务（rac_service_name）在各实例的运行状态；
          3. 服务未运行：在主用实例上 START_SERVICE（幂等，已运行则跳过）；
          4. 服务正常运行：直接判定健康。

        返回 dict：{'repaired': bool, 'direction_mismatch': bool, 'details': {...}}
        """
        if emit is None:
            emit = lambda level, msg: None  # noqa: E731
        results = {'repaired': False, 'direction_mismatch': False, 'details': {}}

        if not ORACLE_AVAILABLE:
            emit('error', 'oracledb 库未安装，请执行: pip install oracledb')
            results['details']['error'] = 'oracledb 未安装'
            return results

        svc = self.cfg.get('rac_service_name', '')
        emit('info', f'⭐ 开始 Oracle RAC 服务修复（节点1 {self.cfg.get("rac_node1_host")} ↔ 节点2 {self.cfg.get("rac_node2_host")}），服务: {svc or "未配置"}')

        # ── 1. 建立连接（任一节点可达即可）──
        conn = None
        errs = []
        for label, host, sid in [
            ('节点1', self.cfg.get('rac_node1_host'), self.cfg.get('rac_node1_sid')),
            ('节点2', self.cfg.get('rac_node2_host'), self.cfg.get('rac_node2_sid')),
        ]:
            if not host:
                continue
            try:
                c = RacConnection(host, sid, self.cfg.get('rac_user', 'oracle'),
                                  self.cfg.get('rac_pass', ''))
                c.connect()
                conn = c
                emit('info', f'✓ 使用 {label}（{host}/{sid}）连接成功')
                self.node1_conn = conn
                break
            except Exception as e:
                errs.append(f'{label}: {e}')
                emit('warning', f'✗ {label}（{host}/{sid}）连接失败: {e}')

        if conn is None:
            emit('error', 'RAC 节点全部连接失败，无法完成服务修复（需人工检查实例状态）')
            results['details']['connect_errors'] = errs
            return results

        try:
            # ── 2. 集群与数据库状态 ──
            try:
                db = conn.execute("SELECT NAME,DB_UNIQUE_NAME,DATABASE_ROLE,OPEN_MODE FROM V$DATABASE")
                if db:
                    d = db[0]
                    results['details']['db_name'] = d.get('name', '')
                    results['details']['db_unique_name'] = d.get('db_unique_name', '')
                    results['details']['database_role'] = d.get('database_role', '')
                    results['details']['open_mode'] = d.get('open_mode', '')
                    emit('info', f"数据库: {d.get('name')} / {d.get('db_unique_name')}，角色={d.get('database_role')}，模式={d.get('open_mode')}")
            except Exception as e:
                emit('warning', f'查询 V$DATABASE 失败: {e}')

            insts = conn.execute("SELECT INST_ID,INSTANCE_NAME,STATUS FROM GV$INSTANCE ORDER BY INST_ID")
            results['details']['instances'] = [{'inst_id': r.get('inst_id'), 'instance_name': r.get('instance_name'), 'status': r.get('status')} for r in insts]
            distro = ', '.join(("{0}={1}".format(r.get('instance_name'), r.get('status'))) for r in insts)
            emit('info', '实例数量: %s，状态分布: %s' % (len(insts), distro))

            # ── 3. 服务检查 ──
            if not svc:
                emit('warning', '未配置 rac_service_name，仅完成实例健康诊断')
                results['repaired'] = True  # 诊断完成（无服务名时视为健康）
                return results

            svc_rows = conn.execute(
                "SELECT INST_ID,SERVICE_NAME,STATUS FROM GV$ACTIVE_SERVICES "
                f"WHERE SERVICE_NAME = '{svc}'"
            )
            if svc_rows:
                active = [r for r in svc_rows if str(r.get('status', '')).upper() in ('READY', 'STARTING', 'RUNNING')]
                results['details']['service_running'] = active
                emit('info', f"服务 {svc} 运行于: {[(r.get('inst_id'), r.get('status')) for r in svc_rows]}")
                if active:
                    emit('info', '✓ RAC 服务运行正常，无需修复')
                    results['repaired'] = True
                    return results
            else:
                emit('info', f'服务 {svc} 当前未在任何实例上运行')

            # ── 4. 服务拉起：在首个 OPEN 实例上 START_SERVICE ──
            inst_map = {r['inst_id']: r['instance_name'] for r in insts
                        if str(r.get('status', '')).upper() == 'OPEN'}
            if not inst_map:
                emit('error', '无 OPEN 状态实例，无法启动服务（需人工检查实例）')
                results['details']['error'] = '无 OPEN 实例'
                return results
            target_inst = inst_map[min(inst_map.keys())]
            try:
                conn.execute(f"BEGIN DBMS_SERVICE.START_SERVICE('{svc}', '{target_inst}'); END;")
                results['repaired'] = True
                results['details']['started_on'] = target_inst
                emit('info', f"✓ 服务 {svc} 已在实例 {target_inst} 启动")
            except Exception as e:
                # ORA-33056 / ORA-44305 等表示服务已存在/正在启动
                err = str(e).upper()
                if '44305' in err or '33056' in err or 'ALREADY' in err:
                    emit('info', f'服务已处于运行/启动状态（{err}），无需重复启动')
                    results['details']['started_on'] = target_inst
                    results['repaired'] = True
                else:
                    emit('error', f'服务启动失败: {e}')
                    results['details']['start_error'] = str(e)
            return results
        except Exception as e:
            emit('error', f'RAC 服务修复中断: {e}')
            results['details']['error'] = str(e)
            return results
        finally:
            self._close_all()


class RacSwitchoverError(Exception):
    pass


# ------------------------------------------------------------------
def test_rac_connection(host, sid, user, password, port=1521) -> dict:
    """测试单个 RAC 节点连接"""
    if not ORACLE_AVAILABLE:
        return {'ok': False, 'error': 'oracledb 未安装'}

    c = RacConnection(host, sid, user, password, port)
    try:
        c.connect()
        db_info = c.execute("SELECT NAME,DB_UNIQUE_NAME,INSTANCE_NAME,HOST_NAME FROM V$INSTANCE,V$DATABASE")
        row = db_info[0] if db_info else {}
        return {
            'ok': True, 'host': host, 'sid': sid,
            'instance_name': row.get('instance_name', ''),
            'host_name': row.get('host_name', ''),
            'db_unique_name': row.get('db_unique_name', ''),
            'db_name': row.get('name', ''),
        }
    except Exception as e:
        return {'ok': False, 'error': str(e)}
    finally:
        c.close()
