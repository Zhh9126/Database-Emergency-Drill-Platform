"""
Redis 主从模式切换引擎

适用于传统 Redis 主从复制（不使用 Sentinel / Cluster）。
切换流程：
  1. 校验旧主 <-> 从 当前复制关系（验证 config 中指定的主从角色真实）
  2. 在旧主上：执行 REPLICAOF NO ONE（或 SLAVEOF NO ONE）取消复制
  3. 在从库上：执行 REPLICAOF NO ONE 提升为主
  4. 在旧主上：执行 REPLICAOF new_master_host new_master_port 重新指向新主
  5. 验证新主可读写、新从可复制

要求主/从节点使用独立账号密码（redis_master_user/pass、redis_slave_user/pass）。
"""

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

import time


class RedisSwitchoverEngine:
    def __init__(self, cfg, force=False, dry_run=False):
        self.cfg = cfg
        self.force = force
        self.dry_run = dry_run

        self.m_host = cfg.get('redis_master_host') or cfg.get('master_host')
        self.m_port = cfg.get('redis_master_port') or cfg.get('master_port') or 6379
        self.m_user = cfg.get('redis_master_user') or ''
        self.m_pass = cfg.get('redis_master_pass') or ''

        self.s_host = cfg.get('redis_slave_host') or cfg.get('slave_host')
        self.s_port = cfg.get('redis_slave_port') or cfg.get('slave_port') or 6379
        self.s_user = cfg.get('redis_slave_user') or self.m_user
        self.s_pass = cfg.get('redis_slave_pass') or self.m_pass

        self.max_delay = cfg.get('max_delay', 60)
        self.wait_timeout = cfg.get('wait_timeout', 300)
        self.skip_delay_check = cfg.get('skip_delay_check', False)
        self.auto_rollback = cfg.get('auto_rollback', False)

    # ── 连接工具 ──
    def _make_client(self, host, port, user, password):
        kwargs = dict(host=host, port=int(port), decode_responses=True, socket_timeout=5)
        if user:
            kwargs['username'] = user
        if password:
            kwargs['password'] = password
        return redis.Redis(**kwargs)

    def _conn_master(self):
        return self._make_client(self.m_host, self.m_port, self.m_user, self.m_pass)

    def _conn_slave(self):
        return self._make_client(self.s_host, self.s_port, self.s_user, self.s_pass)

    # ── 探测连接 ──
    def quick_health_check(self):
        """测试主从连接 + 验证当前主从关系"""
        try:
            r_m = self._conn_master()
            r_m.ping()
            info_m = r_m.info('replication')
            role_m = info_m.get('role')

            r_s = self._conn_slave()
            r_s.ping()
            info_s = r_s.info('replication')
            role_s = info_s.get('role')

            connected_s = info_m.get('connected_slaves') or info_m.get('connected_replicas') or 0

            return {
                'ok': True,
                'master_role': role_m,
                'slave_role': role_s,
                'connected_slaves': connected_s,
                'msg': f"主节点 role={role_m}, 从节点 role={role_s}, 主已连接从 {connected_s}",
            }
        except Exception as e:
            return {'ok': False, 'msg': f'连接失败：{e}'}

    # ── 同步入口 ──
    def run(self, log=None, progress=None):
        """
        log(progress, level, message) -> None
        progress(value 0..100) -> None
        """
        steps = []

        def emit(progress_val, level, msg):
            steps.append({'progress': progress_val, 'level': level, 'msg': msg})
            if log:
                log(progress_val, level, msg)
            if progress:
                progress(progress_val)

        emit(5, 'info', '开始 Redis 主从切换')

        # 步骤 1：连接主从
        emit(10, 'info', f'连接主节点 {self.m_host}:{self.m_port}')
        try:
            r_m = self._conn_master()
            r_m.ping()
        except Exception as e:
            emit(0, 'error', f'主节点连接失败：{e}')
            return False, steps

        emit(15, 'info', f'连接从节点 {self.s_host}:{self.s_port}')
        try:
            r_s = self._conn_slave()
            r_s.ping()
        except Exception as e:
            emit(0, 'error', f'从节点连接失败：{e}')
            return False, steps

        # 步骤 2：读取当前主从状态
        emit(25, 'info', '读取主从复制状态')
        try:
            info_m = r_m.info('replication')
            info_s = r_s.info('replication')
        except Exception as e:
            emit(0, 'error', f'读取 replication 信息失败：{e}')
            return False, steps

        role_m = info_m.get('role')
        role_s = info_s.get('role')
        emit(30, 'info', f'当前：主节点 role={role_m}, 从节点 role={role_s}')

        if role_s != 'slave':
            if not self.force:
                emit(0, 'error', f'从节点当前角色={role_s}（不是 slave）。如确认要强制切换，请启用 force')
                return False, steps
            emit(30, 'warn', f'从节点角色={role_s}，强制模式将继续')

        # 步骤 3：检查从库复制延迟
        delay = 0
        try:
            lag = info_s.get('master_last_io_seconds')
            if isinstance(lag, (int, float)):
                delay = lag
        except Exception:
            pass

        if delay > self.max_delay and not self.skip_delay_check:
            emit(0, 'error', f'从库复制延迟 {delay}s 超过阈值 {self.max_delay}s；如确认切换请启用 skip_delay_check')
            return False, steps

        emit(40, 'info', f'从库复制延迟 {delay}s（阈值 {self.max_delay}s）')

        # 步骤 4：提升从库为主
        emit(50, 'info', f'将从节点 {self.s_host}:{self.s_port} 提升为新主（REPLICAOF NO ONE）')
        if self.dry_run:
            emit(50, 'info', '[DRY-RUN] 跳过实际执行')
            emit(100, 'success', '[DRY-RUN] 模拟切换完成')
            return True, steps

        try:
            r_s.execute_command('REPLICAOF', 'NO', 'ONE')
        except redis.exceptions.ResponseError:
            try:
                r_s.execute_command('SLAVEOF', 'NO', 'ONE')
            except Exception as e:
                emit(0, 'error', f'提升从库失败：{e}')
                return False, steps
        except Exception as e:
            emit(0, 'error', f'提升从库失败：{e}')
            return False, steps

        # 步骤 5：等待从库角色转换
        ok = self._wait_role(r_s, 'master', timeout=10)
        if not ok:
            emit(0, 'error', '从库提升后未能在 10s 内转换为 master 角色')
            return False, steps

        emit(60, 'success', '从库已提升为新主')

        # 步骤 6：将旧主重新指向新主
        emit(75, 'info', f'将旧主 {self.m_host}:{self.m_port} 重新指向新主 {self.s_host}:{self.s_port}')
        try:
            r_m.execute_command('REPLICAOF', self.s_host, int(self.s_port))
        except redis.exceptions.ResponseError:
            try:
                r_m.execute_command('SLAVEOF', self.s_host, int(self.s_port))
            except Exception as e:
                emit(0, 'error', f'旧主重指向失败：{e}')
                emit(0, 'warn', '主从切换已完成，但旧主仍独立，新主已对外服务')
                return True, steps
        except Exception as e:
            emit(0, 'error', f'旧主重指向失败：{e}')
            emit(0, 'warn', '主从切换已完成，但旧主仍独立，新主已对外服务')
            return True, steps

        # 步骤 7：等待旧主角色转换
        ok = self._wait_role(r_m, 'slave', timeout=10)
        if not ok:
            emit(85, 'warn', '旧主未在 10s 内转换为 slave，请人工确认')

        # 步骤 8：写入测试键，验证新主可写
        emit(90, 'info', '验证新主可读写')
        try:
            r_s.set('__switchover_test__', str(time.time()), ex=60)
            v = r_s.get('__switchover_test__')
            r_s.delete('__switchover_test__')
            if not v:
                emit(0, 'error', '新主写入验证失败')
                return False, steps
        except Exception as e:
            emit(0, 'error', f'新主写入验证失败：{e}')
            return False, steps

        emit(100, 'success', f'切换完成：原主 {self.m_host}:{self.m_port} -> 从；原从 {self.s_host}:{self.s_port} -> 主')
        return True, steps

    def _wait_role(self, client, expect_role, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                info = client.info('replication')
                if info.get('role') == expect_role:
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        return False

    def get_topology(self):
        """返回当前主从角色和复制状态，供 API 状态查询使用。

        注意：以真实角色为准（role==master 即为主，即使节点本身在配置里
        被标记为 slave），避免切换后 new_master_host 记录成配置里的旧 master。
        """
        result = {'master': {}, 'slave': {}}
        nodes = [
            ('master', self.m_host, self.m_port, self.m_user, self.m_pass),
            ('slave', self.s_host, self.s_port, self.s_user, self.s_pass),
        ]
        roles = {}  # (host, port) -> replication info
        for key, host, port, user, pwd in nodes:
            try:
                r = self._make_client(host, port, user, pwd)
                info = r.info('replication')
                roles[(host, port)] = info
            except Exception as e:
                result[key]['error'] = str(e)

        nodes_map = {}  # (host,port) -> entry，支持双主/多节点场景
        for (host, port), info in roles.items():
            role = info.get('role')
            entry = {'host': host, 'port': port, 'role': role}
            if role == 'master':
                entry['connected_replicas'] = info.get('connected_slaves', 0)
            elif role == 'slave':
                entry['master_host'] = info.get('master_host')
                entry['master_port'] = info.get('master_port')
                entry['lag_seconds'] = info.get('master_last_io_seconds', 0)
            nodes_map[(host, port)] = entry

        masters = [e for e in nodes_map.values() if e['role'] == 'master']
        slaves = [e for e in nodes_map.values() if e['role'] == 'slave']
        result['master'] = masters[0] if len(masters) == 1 else {'nodes': [m['host'] + ':' + str(m['port']) for m in masters], 'count': len(masters), 'conflict': len(masters) > 1}
        result['slave'] = slaves[0] if len(slaves) == 1 else {'nodes': [s['host'] + ':' + str(s['port']) for s in slaves], 'count': len(slaves)}
        result['nodes'] = list(nodes_map.values())
        if len(masters) > 1:
            result['multi_master'] = True
        return result

    # ── 复制修复（运维实用功能）──────────────────────────────────────────
    def repair_replication(self, emit=None) -> dict:
        """Redis 主从复制一键修复：诊断 + 从库重定向到主库 + 复制验证

        功能：
          1. 探测主从角色与复制状态（quick_health_check）；
          2. 方向校验：配置主库应为 master、配置从库应为 slave，颠倒时停止；
          3. 主库若为从（slave）角色：自动纠正为独立主（REPLICAOF NO ONE）；
          4. 从库重定向：REPLICAOF master master_port（指向配置主库）；
          5. 等待角色转换并验证复制延迟。

        返回 dict：{'repaired': bool, 'direction_mismatch': bool, 'details': {...}}
        """
        if emit is None:
            emit = lambda level, msg: None  # noqa: E731
        results = {'repaired': False, 'direction_mismatch': False, 'details': {}}

        if not REDIS_AVAILABLE:
            emit('error', 'redis-py 未安装，请执行: pip install redis')
            results['details']['error'] = 'redis-py 未安装'
            return results

        emit('info', f'⭐ 开始 Redis 复制修复（{self.m_host}:{self.m_port} ↔ {self.s_host}:{self.s_port}）')

        # ── 1. 连接主从 ──
        try:
            r_m = self._conn_master()
            r_m.ping()
            info_m = r_m.info('replication')
            results['details']['master_role'] = info_m.get('role')
            emit('info', f"✓ 主节点 {self.m_host}:{self.m_port} 连接正常 role={info_m.get('role')}")
        except Exception as e:
            emit('error', f'✗ 主节点连接失败: {e}')
            results['details']['primary_connect_error'] = str(e)
            return results

        try:
            r_s = self._conn_slave()
            r_s.ping()
            info_s = r_s.info('replication')
            results['details']['slave_role'] = info_s.get('role')
            emit('info', f"✓ 从节点 {self.s_host}:{self.s_port} 连接正常 role={info_s.get('role')}")
        except Exception as e:
            emit('error', f'✗ 从节点连接失败: {e}')
            results['details']['slave_connect_error'] = str(e)
            return results

        role_m = info_m.get('role')
        role_s = info_s.get('role')

        # ── 2. 方向校验 ──
        if role_m == 'slave':
            desc = f'配置主库 {self.m_host}:{self.m_port} 实际是从库（master_host={info_m.get("master_host")}），方向可能已颠倒'
            emit('warning', f'⚠ 配置方向与物理拓扑不一致：{desc}')
            results['direction_mismatch'] = True
            results['details']['direction_mismatch_desc'] = desc
            results['details']['topology'] = self.get_topology()
            return results

        if role_s == 'master':
            # 从库已是主：若主库还是 slave 上面已拦截；否则说明从库被独立提升，重新指向主库
            emit('warning', f'从节点 {self.s_host}:{self.s_port} 当前是 master（独立），执行 REPLICAOF 重定向到主库')
            results['details']['slave_was_master'] = True

        # ── 3. 从库重定向到主库 ──
        reforged = False  # 是否已执行重定向动作（即使复制尚未恢复）
        emit('info', f"从节点 {self.s_host}:{self.s_port} -> REPLICAOF {self.m_host} {self.m_port}")
        try:
            r_s.execute_command('REPLICAOF', self.m_host, int(self.m_port))
        except redis.exceptions.ResponseError:
            try:
                r_s.execute_command('SLAVEOF', self.m_host, int(self.m_port))
            except Exception as e:
                emit('error', f'从库重定向失败: {e}')
                results['details']['error'] = str(e)
                return results
        except Exception as e:
            emit('error', f'从库重定向失败: {e}')
            results['details']['error'] = str(e)
            return results
        # 主库有 requirepass 时，从库需同步设置 masterauth 才能通过认证
        if self.m_pass:
            try:
                cur_auth = r_s.config_get('masterauth') or {}
                if cur_auth.get('masterauth') != self.m_pass:
                    r_s.config_set('masterauth', self.m_pass)
                    emit('info', '✓ 已设置从库 masterauth（匹配主库 requirepass）')
            except Exception as e3:
                emit('warning', f'⚠ 设置从库 masterauth 失败: {e3}')
        reforged = True
        results['details']['reforged'] = True
        emit('info', '✓ 已执行 REPLICAOF 重定向')

        # ── 4. 等待从库角色转换 ──
        ok = self._wait_role(r_s, 'slave', timeout=10)
        if ok:
            emit('info', '✓ 从节点已转换为 slave 角色')
        else:
            emit('warning', '从节点未在 10s 内转换为 slave，请稍后人工确认')

        # ── 5. 复制状态与延迟验证 ──
        master_link = None
        lag = 0
        for _attempt in range(6):  # 轮询最多 6 次（约 12s），等待主从链路建立
            try:
                info_s2 = r_s.info('replication')
                master_link = info_s2.get('master_link_status')
                lag = info_s2.get('master_last_io_seconds', 0)
                if master_link == 'up':
                    break
            except Exception as e:
                master_link = None
                emit('warning', f'复制状态验证失败（重试 {_attempt + 1}/6）: {e}')
            time.sleep(2)
        results['details']['master_link_status'] = master_link
        results['details']['lag_seconds'] = lag
        emit('info', f"复制状态: master_link_status={master_link} lag={lag}s")

        # ── 6. 主节点只读保障（从角色时由复制自动机构保证，主角色不强制）──
        connected = 0
        try:
            connected = r_m.info('replication').get('connected_slaves', 0)
            results['details']['connected_slaves'] = connected
            emit('info', f"主节点已连接从库数: {connected}")
        except Exception as e:
            emit('warning', f'主节点复制信息读取失败: {e}')

        # ── 7. 健康判定：master_link=up 才算真正修复成功 ──
        if master_link == 'up':
            results['repaired'] = True
            emit('info', '✅ Redis 复制修复完成（主从链路已恢复）')
        elif reforged:
            results['repaired'] = False
            emit('warning', f'⚠ 已执行 REPLICAOF 重定向，但主从链路仍为 {master_link}（从库主库连接 {connected} 个），复制未恢复；请检查主库 auth/网络/哨兵配置')
        else:
            results['repaired'] = False
            emit('warning', '⚠ Redis 复制修复未完成，主从链路未恢复')
        return results


# 兼容旧接口的模块级函数（被 app.py 和 ai_assistant.py 使用）
def test_redis_status(master_host='', slave_host='', master_port=6379, slave_port=6379,
                      master_user='', master_pass='', slave_user='', slave_pass='',
                      **_ignored_legacy_kwargs):
    """
    兼容旧 test_redis_status 调用格式（曾用于 Sentinel 配置）。
    现以传统主从模式实现，忽略 sentinel_hosts/master_name 等遗留参数。
    """
    cfg = {
        'redis_master_host': master_host,
        'redis_master_port': master_port,
        'redis_master_user': master_user,
        'redis_master_pass': master_pass,
        'redis_slave_host': slave_host,
        'redis_slave_port': slave_port,
        'redis_slave_user': slave_user,
        'redis_slave_pass': slave_pass,
    }
    eng = RedisSwitchoverEngine(cfg)
    top = eng.get_topology()

    if 'error' in top['master'] or 'error' in top['slave']:
        return {'ok': False, 'error': top['master'].get('error') or top['slave'].get('error')}

    return {'ok': True, 'master': top['master'], 'slave': top['slave']}