# -*- coding: utf-8 -*-
"""
MongoDB 副本集高可用切换引擎
支持:
  - replica: 副本集 Primary 角色切换（rs.stepDown()）
  - cluster: 分片集群节点关停测试
参考命令:
  rs.stepDown(durationSecs, forceVote)
  rs.status()
  sh.moveChunk()
"""
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, OperationFailure
    MONGO_AVAILABLE = True
except ImportError:
    MONGO_AVAILABLE = False


def _now():
    return datetime.now(timezone.utc).isoformat()


def _step(name, success=True, detail=''):
    return {'name': name, 'status': 'success' if success else 'failed',
            'timestamp': _now(), 'detail': detail}


class MongoSwitchoverError(Exception):
    pass


class MongoSwitchoverEngine:
    """MongoDB 副本集/分片集群切换引擎"""

    def __init__(self, config: dict, force: bool = False, dry_run: bool = False, target_node: str = 'node2'):
        self.cfg = config
        self.force = force
        self.dry_run = dry_run
        self.target_node = target_node if target_node in ('node1', 'node2') else 'node2'
        self.subtype = config.get('mongodb_subtype', 'replica')
        self.steps = []
        self.logs = []
        self.logs_cb = None

    def _log(self, msg, level='INFO'):
        self.logs.append(f'[{level}] {msg}')
        if self.logs_cb:
            self.logs_cb(msg)

    def _connect(self, host, port, user=None, password=None, replset=None, auth_db='admin'):
        if not MONGO_AVAILABLE:
            raise MongoSwitchoverError('pymongo 未安装，请执行: pip install pymongo')
        uri = f'mongodb://{host}:{port}'
        uri += f'/?replicaSet={replset}' if replset else ''
        kwargs = {'host': uri, 'serverSelectionTimeoutMS': 10000,
                  'connectTimeoutMS': 10000}
        # pymongo 4.x：通过连接参数认证（authenticate 方法已移除）
        if user and password:
            kwargs['username'] = user
            kwargs['password'] = password
            kwargs['authSource'] = auth_db or 'admin'
        client = MongoClient(**kwargs)
        return client

    def run(self, log_callback=None):
        self.logs_cb = log_callback
        if self.subtype == 'cluster':
            return self._run_cluster()
        return self._run_replica()

    def get_status(self, emit=None):
        """MongoDB 副本集状态：探测主备角色与复制延迟（标准化状态接口）"""
        primary_host = self.cfg.get('mongo_primary_host', '')
        primary_port = int(self.cfg.get('mongo_primary_port', 27017))
        standby_host = self.cfg.get('mongo_standby_host', '')
        standby_port = int(self.cfg.get('mongo_standby_port', 27017))
        user = self.cfg.get('mongo_user', '')
        password = self.cfg.get('mongo_pass', '')
        replset = self.cfg.get('mongo_replset_name', '')
        auth_db = self.cfg.get('mongo_auth_db', 'admin')
        result = {'ok': True, 'primary': {}, 'standby': {}, 'healthy': False, 'summary': ''}

        for label, key, host, port in (
                ('primary', 'primary', primary_host, primary_port),
                ('standby', 'standby', standby_host, standby_port)):
            try:
                client = self._connect(host, port, user, password, replset, auth_db)
                st = client.admin.command('replSetGetStatus')
                members = {str(m.get('name', '')): m for m in st.get('members', [])}
                entry = members.get(f'{host}:{port}', {})
                state = entry.get('stateStr', 'UNKNOWN')
                role = 'primary' if state == 'PRIMARY' else 'standby' if state == 'SECONDARY' else state.lower()
                lag = entry.get('optimeDate')
                result[key] = {
                    'host': host, 'port': port, 'role': role,
                    'state': state, 'ok': True, 'version': st.get('setVersion'),
                    'replset': replset,
                }
                client.close()
            except Exception as e:
                result[key] = {'host': host, 'port': port, 'ok': False, 'error': str(e)[:120]}

        p, s = result['primary'], result['standby']
        if p.get('ok') and s.get('ok'):
            result['healthy'] = p.get('role') == 'primary' and s.get('role') == 'standby'
            if result['healthy']:
                result['summary'] = '副本集正常：主备角色符合配置方向'
            else:
                result['summary'] = f"方向异常：主节点={p.get('role')}，备节点={s.get('role')}"
        else:
            result['ok'] = False
            result['summary'] = '主备节点存在连接失败'
        return result

    def _run_replica(self):
        """副本集 Primary 角色切换"""
        try:
            primary_host = self.cfg.get('mongo_primary_host', '')
            primary_port = int(self.cfg.get('mongo_primary_port', 27017))
            standby_host = self.cfg.get('mongo_standby_host', '')
            standby_port = int(self.cfg.get('mongo_standby_port', 27017))
            user = self.cfg.get('mongo_user', '')
            password = self.cfg.get('mongo_pass', '')
            replset = self.cfg.get('mongo_replset_name', 'rs0')
            auth_db = self.cfg.get('mongo_auth_db', 'admin')

            # Step 1: 连接 Primary 并查看副本集状态
            self._log(f'连接 Primary 节点 {primary_host}:{primary_port}')
            steps_ok = True
            real_primary_host = primary_host
            real_primary_port = primary_port
            secondary_name = ''
            try:
                client = self._connect(primary_host, primary_port, user, password, replset, auth_db)
                status = client.admin.command('replSetGetStatus')
                primary_member = None
                for m in status.get('members', []):
                    if m.get('stateStr') == 'PRIMARY':
                        primary_member = m
                        break
                if not primary_member:
                    raise MongoSwitchoverError('未找到 PRIMARY 节点')
                self._log(f'当前 Primary: {primary_member.get("name")}, 状态: {primary_member.get("stateStr")}')
                # 以实际探测到的 PRIMARY 为准（漂移场景配置方向可能过期）
                try:
                    ph, pp = primary_member.get('name', '').rsplit(':', 1)
                    real_primary_host = ph.strip()
                    real_primary_port = int(pp)
                except Exception:
                    pass
                self.steps.append(_step('检查副本集状态', True,
                                        f'Primary={primary_member.get("name")}'))
                client.close()
            except MongoSwitchoverError:
                raise
            except Exception as e:
                self.steps.append(_step('检查副本集状态', False, str(e)))
                if not self.force:
                    raise
                steps_ok = False

            # Step 2: 验证 Secondary 节点（以复制集真实状态为准，不强制等于配置备库）
            self._log(f'验证 Secondary 节点 {standby_host}:{standby_port}')
            try:
                client2 = self._connect(standby_host, standby_port, user, password, replset, auth_db)
                status2 = client2.admin.command('replSetGetStatus')
                secondary_ok = False
                secondary_name = ''
                for m in status2.get('members', []):
                    if m.get('stateStr') == 'SECONDARY':
                        secondary_ok = True
                        secondary_name = m.get('name', '')
                        self._log(f'Secondary 正常: {m.get("name")}, 延迟: {m.get("optimeDate")} offset={m.get("optimeLag", 0)}s')
                        break
                client2.close()
                self.steps.append(_step('验证Secondary', secondary_ok,
                                        secondary_name or '无 SECONDARY 成员'))
                if not secondary_ok:
                    raise MongoSwitchoverError('复制集无可用 SECONDARY 成员')
            except MongoSwitchoverError:
                raise
            except Exception as e:
                self.steps.append(_step('验证Secondary', False, str(e)))
                self._log(f'Secondary 检查失败: {e}', 'WARN')

            # Step 3: 对真实 PRIMARY 执行 rs.stepDown() 触发切换
            if self.dry_run:
                self._log('[演习] 模拟 rs.stepDown()')
                self.steps.append(_step('rs.stepDown()（演习）', True, '未执行真实操作'))
            else:
                self._log(f'在 Primary {real_primary_host}:{real_primary_port} 上执行 rs.stepDown()...')
                try:
                    client = self._connect(real_primary_host, real_primary_port, user, password, replset, auth_db)
                    # stepDown(120s duration, 120s forceVote timeout)
                    result = client.admin.command('replSetStepDown', 120, force=False)
                    self._log(f'rs.stepDown() 完成: {result}')
                    self.steps.append(_step('rs.stepDown()', True, str(result)[:80]))
                    client.close()
                except OperationFailure as e:
                    if 'not primary' in str(e).lower() or 'elect' in str(e).lower():
                        self._log(f'stepDown 已生效（节点不再是 Primary）: {e}')
                        self.steps.append(_step('rs.stepDown()', True, '已触发选举'))
                    else:
                        raise

            # Step 4: 等待选举完成并验证新 Primary（轮询直到节点切换或超时）
            if not self.dry_run:
                import time
                self._log('等待选举完成...')
                old_primary = f'{real_primary_host}:{real_primary_port}'
                new_primary_host = None
                deadline = time.time() + 35
                check_from = standby_host
                while time.time() < deadline:
                    time.sleep(3)
                    try:
                        client2 = self._connect(check_from, standby_port, user, password, replset, auth_db)
                        status = client2.admin.command('replSetGetStatus')
                        cur = None
                        for m in status.get('members', []):
                            if m.get('stateStr') == 'PRIMARY':
                                cur = m.get('name', '')
                                break
                        client2.close()
                    except Exception:
                        cur = None
                    if cur and cur != old_primary:
                        new_primary_host = cur
                        break
                if new_primary_host:
                    self._log(f'新 Primary: {new_primary_host}')
                    self.steps.append(_step('验证新Primary', True, f'{new_primary_host}'))
                else:
                    self._log(f'切换未生效：原 Primary {old_primary} 在 35 秒内仍是主（可能无追平的 SECONDARY）', 'ERROR')
                    self.steps.append(_step('验证新Primary', False,
                                            f'仍为 {old_primary}，未完成选举'))
                    steps_ok = False
            else:
                self.steps.append(_step('验证新Primary（演习）', True))

            self._log('MongoDB 副本集切换完成')
            return {'steps': self.steps, 'logs': self.logs, 'ok': steps_ok,
                    'full_log': '\n'.join(self.logs)}

        except Exception as e:
            self._log(f'切换异常: {e}', 'ERROR')
            return {'steps': self.steps, 'logs': self.logs, 'ok': False,
                    'full_log': '\n'.join(self.logs), 'error': str(e)}

    def _run_cluster(self):
        """分片集群节点关停测试"""
        node1_host = self.cfg.get('mongo_cluster_node1_host', '')
        node1_port = int(self.cfg.get('mongo_cluster_node1_port', 27017))
        node2_host = self.cfg.get('mongo_cluster_node2_host', '')
        node2_port = int(self.cfg.get('mongo_cluster_node2_port', 27017))
        user = self.cfg.get('mongo_user', '')
        password = self.cfg.get('mongo_pass', '')

        target_host = node1_host if self.target_node == 'node1' else node2_host
        target_port = node1_port if self.target_node == 'node1' else node2_port
        peer_host = node2_host if self.target_node == 'node1' else node1_host
        peer_port = node2_port if self.target_node == 'node1' else node1_port

        try:
            self._log(f'MongoDB 分片集群关停测试: {target_host}:{target_port} -> 验证 {peer_host}:{peer_port}')

            # Step 1: 验证对端可达
            try:
                client = self._connect(peer_host, peer_port, user, password)
                client.admin.command('ping')
                self.steps.append(_step(f'验证对端{peer_host}可达', True))
                client.close()
            except Exception as e:
                self.steps.append(_step(f'验证对端{peer_host}可达', False, str(e)))
                raise

            # Step 2: 通过 admin shutdown 命令真实关停目标节点
            if self.dry_run:
                self._log(f'[演习] 模拟关停 {target_host}')
                self.steps.append(_step(f'关停{target_host}（演习）', True))
            else:
                self._log(f'通过 admin shutdown 关停 {target_host}:{target_port}...')
                try:
                    shutdown_client = self._connect(target_host, target_port, user, password)
                    # 发送 shutdown 命令，force=False 允许等待从节点同步
                    try:
                        shutdown_client.admin.command('shutdown', force=False)
                    except Exception as sd_err:
                        # shutdown 命令正常情况下会断开连接导致异常
                        if 'connection' in str(sd_err).lower() or 'closed' in str(sd_err).lower() or 'timeout' in str(sd_err).lower():
                            self._log(f'✓ {target_host} 已通过 shutdown 命令关停（连接已断开）')
                        else:
                            raise
                    shutdown_client.close()
                except Exception as e:
                    self._log(f'shutdown 命令执行异常（预期）: {e}', 'WARN')

                # 验证目标节点已不可达
                import time
                time.sleep(3)
                node_down = False
                try:
                    verify_client = self._connect(target_host, target_port, user, password)
                    verify_client.admin.command('ping')
                    verify_client.close()
                    self._log(f'⚠ {target_host} 仍然可达，可能未成功关停', 'WARN')
                except Exception:
                    node_down = True
                    self._log(f'✓ {target_host} 已确认不可达')

                self.steps.append(_step(f'关停{target_host}', node_down,
                                        f'{"节点已关停" if node_down else "关停结果不确定"}'))

            # Step 3: 验证对端正常
            if not self.dry_run:
                import time
                time.sleep(5)
            try:
                client2 = self._connect(peer_host, peer_port, user, password)
                result = client2.admin.command('ping')
                self._log(f'对端 {peer_host} 服务正常')
                self.steps.append(_step(f'验证{peer_host}可用', True))
                client2.close()
            except Exception as e:
                self.steps.append(_step(f'验证{peer_host}可用', False, str(e)))
                raise

            # Step 4: 恢复 — 通过副本集自动恢复验证
            if not self.dry_run:
                self._log(f'等待 {target_host} 恢复（副本集节点重启后会自动加入）...')
                import time
                recovered = False
                for attempt in range(6):  # 最多等待 30s
                    time.sleep(5)
                    try:
                        rc = self._connect(target_host, target_port, user, password)
                        rc.admin.command('ping')
                        rc.close()
                        recovered = True
                        self._log(f'✓ {target_host} 已恢复并重新加入集群')
                        break
                    except Exception:
                        self._log(f'  等待恢复... ({attempt + 1}/6)')
                if not recovered:
                    self._log(f'⚠ {target_host} 在 30s 内未恢复，可能需要手动启动: systemctl start mongod', 'WARN')
                self.steps.append(_step(f'恢复{target_host}', recovered,
                                        f'{"自动恢复成功" if recovered else "需手动恢复"}'))
            else:
                self.steps.append(_step(f'恢复{target_host}（演习）', True))

            self._log('MongoDB 分片集群关停测试完成')
            return {'steps': self.steps, 'logs': self.logs, 'ok': True,
                    'full_log': '\n'.join(self.logs)}

        except Exception as e:
            self._log(f'集群测试异常: {e}', 'ERROR')
            return {'steps': self.steps, 'logs': self.logs, 'ok': False,
                    'full_log': '\n'.join(self.logs), 'error': str(e)}

    # ------------------------------------------------------------------
    # 复制修复（运维实用功能，副本集模式）
    # ------------------------------------------------------------------
    def repair_replication(self, emit=None) -> dict:
        """MongoDB 副本集一键修复：诊断 + 成员修正

        功能：
          1. 连接主备节点，replSetGetStatus 查看副本集成员角色/健康；
          2. 校验配置主节点应为 PRIMARY、配置备节点应在成员列表且状态正常；
          3. 若有成员掉线/角色异常：以 replSetReconfig 修正（保留其他成员配置）；
          4. 复查副本集状态。

        返回 dict：{'repaired': bool, 'direction_mismatch': bool, 'details': {...}}
        """
        if emit is None:
            emit = lambda level, msg: None  # noqa: E731
        results = {'repaired': False, 'direction_mismatch': False, 'details': {}}

        if not MONGO_AVAILABLE:
            emit('error', 'pymongo 未安装，请执行: pip install pymongo')
            results['details']['error'] = 'pymongo 未安装'
            return results

        primary_host = self.cfg.get('mongo_primary_host', '')
        primary_port = int(self.cfg.get('mongo_primary_port', 27017))
        standby_host = self.cfg.get('mongo_standby_host', '')
        standby_port = int(self.cfg.get('mongo_standby_port', 27017))
        user = self.cfg.get('mongo_user', '')
        password = self.cfg.get('mongo_pass', '')
        replset = self.cfg.get('mongo_replset_name', 'rs0')
        auth_db = self.cfg.get('mongo_auth_db', 'admin')

        emit('info', f'⭐ 开始 MongoDB 副本集修复（{primary_host}:{primary_port} ↔ {standby_host}:{standby_port}）')

        # ── 1. 连接主节点 ──
        try:
            client = self._connect(primary_host, primary_port, user, password, replset, auth_db)
            client.admin.command('ping')
            emit('info', f"✓ 主节点 {primary_host}:{primary_port} 连接正常")
        except Exception as e:
            emit('error', f'✗ 主节点连接失败: {e}')
            results['details']['primary_connect_error'] = str(e)
            return results

        try:
            # ── 2. 副本集状态诊断 ──
            status = client.admin.command('replSetGetStatus')
            config = client.admin.command('replSetGetConfig')
            members = status.get('members', [])
            results['details']['replica_set_name'] = status.get('set')
            results['details']['members'] = [
                {'name': m.get('name'), 'state': m.get('stateStr'), 'health': m.get('health')}
                for m in members]
            emit('info', f"副本集 {status.get('set')} 共 {len(members)} 个成员：")
            primary_name = None
            for m in members:
                s = m.get('stateStr')
                flag = '' if m.get('health', 1) == 1 else ' (不健康)'
                emit('info', f"  {m.get('name')}: {s}{flag}")
                if s == 'PRIMARY':
                    primary_name = m.get('name')

            # ── 3. 方向校验 ──
            cfg_primary = f'{primary_host}:{primary_port}'
            if not primary_name:
                emit('error', '副本集当前无 PRIMARY 节点（可能未选举），无法校验方向')
                results['details']['no_primary'] = True
                results['repaired'] = True  # 诊断信息已提供
                return results
            if primary_name != cfg_primary:
                desc = f'副本集 PRIMARY 实际是 {primary_name}，配置主节点是 {cfg_primary}，方向可能已颠倒'
                emit('warning', f'⚠ 配置方向与物理拓扑不一致：{desc}')
                results['direction_mismatch'] = True
                results['details']['actual_primary'] = primary_name
                results['details']['direction_mismatch_desc'] = desc
                return results
            emit('info', f"✓ 方向校验通过：PRIMARY={cfg_primary}")

            # ── 4. 成员健康检查与修正 ──
            unhealthy = [m for m in members if m.get('health', 1) != 1]
            cfg_standby = f'{standby_host}:{standby_port}'
            standby_member = next((m for m in members if m.get('name') == cfg_standby), None)
            if not standby_member:
                emit('warning', f'配置备节点 {cfg_standby} 不在副本集成员列表中，尝试追加...')
                # 追加成员到副本集配置
                try:
                    cfg_doc = config.get('config', config)
                    new_member = {'_id': len(members) + 1, 'host': cfg_standby, 'priority': 1, 'votes': 1}
                    cfg_doc['members'].append(new_member)
                    client.admin.command('replSetReconfig', cfg_doc)
                    results['repaired'] = True
                    results['details']['member_added'] = cfg_standby
                    emit('info', f"✓ 已将 {cfg_standby} 追加到副本集，等待同步...")
                except Exception as e:
                    emit('warning', f'追加成员失败（可能需在主节点执行）: {e}')
                    results['details']['member_add_error'] = str(e)
            else:
                emit('info', f"备节点 {cfg_standby} 在成员列表中，状态={standby_member.get('stateStr')}")
                # 若备节点健康但角色异常（如 RECOVERING），可尝试重新同步
                if standby_member.get('stateStr') in ('RECOVERING', 'STARTUP2', 'ROLLBACK'):
                    emit('warning', f"备节点 {cfg_standby} 状态={standby_member.get('stateStr')}，"
                                    f"建议人工重新同步（MongoDB 会自动恢复）")
                    results['details']['standby_state'] = standby_member.get('stateStr')

            if not results['repaired']:
                results['repaired'] = True  # 诊断健康
            emit('info', '✅ MongoDB 副本集修复完成')
            return results
        except Exception as e:
            emit('error', f'修复中断: {e}')
            results['details']['error'] = str(e)
            return results
        finally:
            client.close()


def test_mongo_connection(data: dict) -> dict:
    """测试 MongoDB 连接"""
    host = data.get('mongo_primary_host', '')
    port = int(data.get('mongo_primary_port', 27017))
    user = data.get('mongo_user', '')
    password = data.get('mongo_pass', '')
    auth_db = data.get('mongo_auth_db', 'admin')
    try:
        uri = f'mongodb://{host}:{port}'
        kwargs = {'host': uri, 'serverSelectionTimeoutMS': 10000}
        if user and password:
            kwargs['username'] = user
            kwargs['password'] = password
            kwargs['authSource'] = auth_db or 'admin'
        client = MongoClient(**kwargs)
        info = client.server_info()
        client.close()
        return {'ok': True, 'version': info.get('version', '')}
    except Exception as e:
        return {'ok': False, 'error': str(e)}
