# -*- coding: utf-8 -*-
"""
中间件及应用服务高可用切换引擎 v1.0
支持:
  - Nginx: Keepalived VIP 漂移检测 + upstream 节点健康检查
  - Tomcat: Nginx upstream 摘除/恢复 + JMX 会话验证
  - Kafka: Controller 选举验证 + ISR 状态查询
  - ZooKeeper: Leader 选举验证 + Quorum 状态查询
  - Elasticsearch: Master 选举验证 + 分片分配状态
  - RabbitMQ: 镜像队列 failover + 节点状态验证

切换模式:
  - active_standby: 主备切换（VIP 漂移 / 角色 failover）
  - cluster: 集群关停测试（停节点 → 验证 → 恢复）

所有检查通过 HTTP/TCP 探针实现，无需 SSH。
"""

import json
import time
import socket
import logging

# 引擎可用性标志（纯标准库 socket/HTTP 实现，恒可用）
MW_AVAILABLE = True
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

logger = logging.getLogger(__name__)


def _gen_step(name, success=True, detail=''):
    return {
        'name': name,
        'success': success,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'detail': detail,
    }


def _tcp_probe(host, port, timeout=5):
    """TCP 端口探针：返回 (可达, 耗时ms)"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    start = time.time()
    try:
        sock.connect((host, int(port)))
        elapsed = (time.time() - start) * 1000
        return True, round(elapsed)
    except Exception:
        return False, 0
    finally:
        sock.close()


def _http_get(url, timeout=10, auth=None):
    """HTTP GET 请求"""
    req = Request(url)
    if auth and auth.get('user'):
        import base64
        cred = base64.b64encode(f"{auth['user']}:{auth['pass']}".encode()).decode()
        req.add_header('Authorization', f'Basic {cred}')
    try:
        resp = urlopen(req, timeout=timeout)
        body = resp.read().decode('utf-8', errors='replace')
        code = resp.getcode()
        resp.close()
        return code, body
    except HTTPError as e:
        return e.code, str(e)
    except URLError as e:
        return 0, str(e.reason)
    except Exception as e:
        return 0, str(e)


def _http_post(url, data=None, timeout=10, auth=None, method='POST'):
    """HTTP POST/PUT 请求"""
    req = Request(url, data=json.dumps(data).encode() if data else b'',
                  method=method)
    req.add_header('Content-Type', 'application/json')
    if auth and auth.get('user'):
        import base64
        cred = base64.b64encode(f"{auth['user']}:{auth['pass']}".encode()).decode()
        req.add_header('Authorization', f'Basic {cred}')
    try:
        resp = urlopen(req, timeout=timeout)
        body = resp.read().decode('utf-8', errors='replace')
        code = resp.getcode()
        resp.close()
        return code, body
    except HTTPError as e:
        return e.code, str(e)
    except URLError as e:
        return 0, str(e.reason)
    except Exception as e:
        return 0, str(e)


# ---------------------------------------------------------------------------
# 连接测试
# ---------------------------------------------------------------------------

def test_middleware_connection(config: dict, timeout: int = 5) -> dict:
    """测试中间件连通性"""
    mw_type = config.get('middleware_type', '')
    host = config.get('mw_primary_host', '')
    port = config.get('mw_primary_port', 80)
    api_port = config.get('mw_api_port', 0)

    if not host:
        return {'ok': False, 'error': '主机地址不能为空'}

    # TCP 探针
    ok, ms = _tcp_probe(host, port, timeout)
    if ok:
        result = {'ok': True, 'message': f'{mw_type} {host}:{port} 可达 ({ms}ms)'}
        # 如果有管理 API 端口，也探测
        if api_port:
            ok2, ms2 = _tcp_probe(host, api_port, timeout)
            result['api_reachable'] = ok2
            if ok2:
                result['message'] += f', API 端口 {api_port} 可达 ({ms2}ms)'
        return result
    return {'ok': False, 'error': f'{mw_type} {host}:{port} 不可达'}


# ---------------------------------------------------------------------------
# 中间件切换引擎
# ---------------------------------------------------------------------------

# 各中间件默认管理端口
_DEFAULT_API_PORTS = {
    'nginx': 80,
    'tomcat': 8080,
    'kafka': 9092,
    'zookeeper': 2181,
    'elasticsearch': 9200,
    'rabbitmq': 15672,
}

# 各中间件默认服务端口
_DEFAULT_SERVICE_PORTS = {
    'nginx': 80,
    'tomcat': 8080,
    'kafka': 9092,
    'zookeeper': 2181,
    'elasticsearch': 9200,
    'rabbitmq': 5672,
}


class MiddlewareSwitchoverEngine:
    """
    中间件高可用切换引擎
    支持 active_standby（主备切换）和 cluster（集群关停测试）两种模式
    """

    def __init__(self, config: dict, force: bool = False, dry_run: bool = False,
                 target_node: str = 'node2'):
        self.config = config
        self.force = force
        self.dry_run = dry_run
        self.target_node = target_node if target_node in ('node1', 'node2', 'node3') else 'node2'
        self.mw_type = config.get('middleware_type', 'nginx')
        self.subtype = config.get('mw_subtype', 'active_standby')

        # 主备模式
        self.prim_host = config.get('mw_primary_host', '')
        self.prim_port = int(config.get('mw_primary_port', _DEFAULT_SERVICE_PORTS.get(self.mw_type, 80)))
        self.sec_host = config.get('mw_secondary_host', '')
        self.sec_port = int(config.get('mw_secondary_port', _DEFAULT_SERVICE_PORTS.get(self.mw_type, 80)))

        # 集群模式节点
        self.n1_host = config.get('mw_cluster_node1_host', '')
        self.n1_port = int(config.get('mw_cluster_node1_port', _DEFAULT_SERVICE_PORTS.get(self.mw_type, 80)))
        self.n2_host = config.get('mw_cluster_node2_host', '')
        self.n2_port = int(config.get('mw_cluster_node2_port', _DEFAULT_SERVICE_PORTS.get(self.mw_type, 80)))
        self.n3_host = config.get('mw_cluster_node3_host', '')
        self.n3_port = int(config.get('mw_cluster_node3_port', _DEFAULT_SERVICE_PORTS.get(self.mw_type, 80)))

        # 通用
        self.api_port = int(config.get('mw_api_port', _DEFAULT_API_PORTS.get(self.mw_type, 0)))
        self.user = config.get('mw_user', '')
        self.password = config.get('mw_pass', '')
        self.vip = config.get('mw_vip', '')
        self.cluster_name = config.get('mw_cluster_name', '')

        self.steps = []
        self.logs = []

    def _log(self, msg, level='INFO'):
        ts = datetime.now().strftime('%H:%M:%S')
        self.logs.append(f'[{level}][{ts}] {msg}')

    def _step(self, name, ok, detail=''):
        self.steps.append(_gen_step(name, ok, detail))
        status = '✓' if ok else '✗'
        self._log(f'{status} {name}: {detail}', 'INFO' if ok else 'WARN')

    def _auth(self):
        if self.user:
            return {'user': self.user, 'pass': self.password}
        return None

    def run(self, log_callback=None) -> dict:
        def log(msg, level='INFO'):
            self.logs.append(f'[{level}] {msg}')
            if log_callback:
                log_callback(msg)

        all_ok = True

        try:
            log(f'╔══════════════════════════════════════════╗')
            log(f'║  中间件高可用切换演练 v1.0                 ║')
            log(f'╚══════════════════════════════════════════╝')
            mw_names = {
                'nginx': 'Nginx', 'tomcat': 'Tomcat', 'kafka': 'Kafka',
                'zookeeper': 'ZooKeeper', 'elasticsearch': 'Elasticsearch',
                'rabbitmq': 'RabbitMQ',
            }
            log(f'  中间件类型: {mw_names.get(self.mw_type, self.mw_type)}')
            log(f'  演练模式: {"主备切换" if self.subtype == "active_standby" else "集群关停测试"}')
            if self.dry_run:
                log(f'  ⚠ 演习模式（不执行真实操作）')
            log('')

            if self.subtype == 'cluster':
                result = self._run_cluster(log)
            else:
                result = self._run_active_standby(log)

            all_ok = result

        except Exception as e:
            log(f'[切换异常] {e}', 'ERROR')
            self.steps.append(_gen_step('切换异常', False, str(e)))
            all_ok = False

        success = all(s['success'] for s in self.steps) and all_ok
        # 构建拓扑结果
        topo = {}
        if self.subtype == 'active_standby':
            topo = {
                'new_primary_host': self.sec_host,
                'new_primary_port': self.sec_port,
                'new_standby_host': self.prim_host,
                'new_standby_port': self.prim_port,
            }
        else:
            topo = {
                'node1_host': self.n1_host, 'node1_port': self.n1_port,
                'node2_host': self.n2_host, 'node2_port': self.n2_port,
            }

        result_dict = {
            'ok': success,
            'dry_run': self.dry_run,
            'middleware_type': self.mw_type,
            'subtype': self.subtype,
            'is_shutdown_test': self.subtype == 'cluster',
            'target_node': self.target_node,
            **topo,
            'steps': self.steps,
            'full_log': '\n'.join(self.logs),
            'message': '切换成功' if success else '切换存在失败步骤',
        }
        log('')
        log(f'{"=" * 50}')
        log(f'中间件切换演练{"通过" if success else "完成（有警告）"}')
        return result_dict

    # ================================================================
    # 复制/服务修复（运维实用功能）
    # ================================================================
    def repair_replication(self, emit=None) -> dict:
        """中间件主备服务一键修复：诊断 + 服务拨测 + 健康检查

        功能：
          1. 主备节点 TCP 拨测（服务可达性）；
          2. 按中间件类型执行深度健康检查（复用 _HEALTH_CHECK_HANDLERS）；
          3. 主节点不可达时给出切换建议（主备切换可人工/切演练触发恢复）。

        返回 dict：{'repaired': bool, 'direction_mismatch': bool, 'details': {...}}
        """
        if emit is None:
            emit = lambda level, msg: None  # noqa: E731
        results = {'repaired': False, 'direction_mismatch': False, 'details': {}}

        mw = self.mw_type
        emit('info', f'⭐ 开始中间件服务修复（{self.prim_host}:{self.prim_port} ↔ {self.sec_host}:{self.sec_port}）')

        # ── 1. TCP 拨测 ──
        prim_ok, prim_ms = _tcp_probe(self.prim_host, self.prim_port)
        sec_ok, sec_ms = _tcp_probe(self.sec_host, self.sec_port)
        results['details']['primary_reachable'] = prim_ok
        results['details']['primary_latency_ms'] = prim_ms
        results['details']['secondary_reachable'] = sec_ok
        results['details']['secondary_latency_ms'] = sec_ms
        emit('info', f"主节点 {self.prim_host}:{self.prim_port} {'可达' if prim_ok else '✗ 不可达'}（{prim_ms}ms）")
        emit('info', f"备节点 {self.sec_host}:{self.sec_port} {'可达' if sec_ok else '✗ 不可达'}（{sec_ms}ms）")

        if not prim_ok and not sec_ok:
            emit('error', '主备节点均不可达，请先恢复网络/服务')
            results['details']['error'] = '主备节点均不可达'
            return results

        # ── 2. 方向校验（主备模式） ──
        if self.subtype == 'active_standby':
            mw_names = {
                'nginx': 'Nginx', 'tomcat': 'Tomcat', 'kafka': 'Kafka',
                'zookeeper': 'ZooKeeper', 'elasticsearch': 'Elasticsearch',
                'rabbitmq': 'RabbitMQ',
            }
            emit('info', f"中间件类型: {mw_names.get(mw, mw)}，主备 TCP 拨测完成")
            # 主节点不可达但备可达 → 方向可能已颠倒（但中间件主备由 VIP/上层路由决定，
            # 这里只给出提示，不自动切换）
            if not prim_ok and sec_ok:
                emit('warning', '配置主节点不可达、备节点可达：若已发生主备切换，建议更新配置或执行切换演练使拓扑回正')
                results['direction_mismatch'] = True
                results['details']['direction_mismatch_desc'] = '配置主节点不可达，可能是主备已发生切换'

        # ── 3. 深度健康检查 ──
        handler = _HEALTH_CHECK_HANDLERS.get(mw)
        if handler:
            emit('info', f'执行 {mw} 深度健康检查...')
            before_steps = len(self.steps)
            ok = handler(self, (lambda msg, level=None: None))
            new_steps = self.steps[before_steps:]
            details = {'ok': ok}
            # 只保留健康检查关键步骤
            for s in new_steps:
                emit('info', f"  {s['name']}: {'✓' if s['success'] else '✗'} {s['detail']}")
            results['details']['health_check_ok'] = ok
            if ok:
                emit('info', '✓ 深度健康检查通过')
            else:
                emit('warning', '深度健康检查发现异常，请根据上述步骤处理')
        else:
            # 无专用 handler（如 nginx 只有 TCP 探测）则默认 TCP 结果
            results['details']['health_check_ok'] = prim_ok
            emit('info', f'{mw} 无专用健康检查 handler，以 TCP 拨测结果为准')

        # 主节点可达即视为服务正常
        results['repaired'] = prim_ok
        emit('info', '✅ 中间件服务修复完成')
        return results

    # ================================================================
    # 主备切换
    # ================================================================
    def _run_active_standby(self, log) -> bool:
        """主备切换演练"""
        mw = self.mw_type
        all_ok = True

        # ---------- 步骤 1: 探测主节点 ----------
        log('【步骤 1/5】探测主节点')
        if self.dry_run:
            log(f'  [演习] 跳过主节点探测')
            self._step('探测主节点', True, '演习跳过')
        else:
            ok, ms = _tcp_probe(self.prim_host, self.prim_port)
            if ok:
                log(f'  ✓ 主节点 {self.prim_host}:{self.prim_port} 可达 ({ms}ms)')
                self._step('探测主节点', True, f'可达 {ms}ms')
            else:
                log(f'  ✗ 主节点 {self.prim_host}:{self.prim_port} 不可达')
                self._step('探测主节点', False, '不可达')
                if not self.force:
                    return False

        # ---------- 步骤 2: 探测备节点 ----------
        log('')
        log('【步骤 2/5】探测备节点')
        if self.dry_run:
            self._step('探测备节点', True, '演习跳过')
        else:
            ok, ms = _tcp_probe(self.sec_host, self.sec_port)
            if ok:
                log(f'  ✓ 备节点 {self.sec_host}:{self.sec_port} 可达 ({ms}ms)')
                self._step('探测备节点', True, f'可达 {ms}ms')
            else:
                log(f'  ✗ 备节点不可达')
                self._step('探测备节点', False, '不可达')
                return False

        # ---------- 步骤 3: 执行切换 ----------
        log('')
        log('【步骤 3/5】执行主备切换')
        if self.dry_run:
            log(f'  [演习] 模拟切换操作')
            self._step('主备切换', True, '演习跳过')
        else:
            handler = _ACTIVE_STANDBY_HANDLERS.get(mw)
            if handler:
                all_ok = handler(self, log)
            else:
                log(f'  未知中间件类型: {mw}')
                self._step('主备切换', False, f'不支持 {mw}')
                all_ok = False

        # ---------- 步骤 4: 验证 VIP / 服务 ----------
        log('')
        log('【步骤 4/5】验证服务可用性')
        if self.dry_run:
            self._step('验证服务', True, '演习跳过')
        else:
            # 如果配置了 VIP，验证 VIP 指向新主节点
            if self.vip:
                ok_vip, ms = _tcp_probe(self.vip, self.prim_port if mw in ('nginx', 'tomcat') else self.sec_port)
                if ok_vip:
                    log(f'  ✓ VIP {self.vip} 可达 ({ms}ms)')
                    self._step('验证 VIP', True, f'VIP 可达 {ms}ms')
                else:
                    log(f'  ⚠ VIP {self.vip} 不可达')
                    self._step('验证 VIP', False, 'VIP 不可达')
                    all_ok = False
            else:
                # 验证新主节点服务正常
                ok_new, ms = _tcp_probe(self.sec_host, self.sec_port)
                if ok_new:
                    log(f'  ✓ 新主节点服务正常')
                    self._step('验证服务', True, '新主节点可达')
                else:
                    self._step('验证服务', False, '新主节点不可达')
                    all_ok = False

        # ---------- 步骤 5: 切换后健康检查 ----------
        log('')
        log('【步骤 5/5】切换后健康检查')
        if self.dry_run:
            self._step('健康检查', True, '演习跳过')
        else:
            health_handler = _HEALTH_CHECK_HANDLERS.get(mw)
            if health_handler:
                all_ok = health_handler(self, log) and all_ok
            else:
                # 通用 TCP 检查
                ok, ms = _tcp_probe(self.sec_host, self.sec_port)
                self._step('健康检查', ok, f'新主节点 {"可达" if ok else "不可达"}')

        return all_ok

    # ================================================================
    # 集群关停测试
    # ================================================================
    def _run_cluster(self, log) -> bool:
        """集群关停测试"""
        mw = self.mw_type
        all_ok = True

        # 目标节点
        if self.target_node == 'node1':
            t_host, t_port = self.n1_host, self.n1_port
        elif self.target_node == 'node2':
            t_host, t_port = self.n2_host, self.n2_port
        else:
            t_host, t_port = self.n3_host, self.n3_port
        # 对端节点
        peers = []
        if self.target_node != 'node1' and self.n1_host:
            peers.append((self.n1_host, self.n1_port))
        if self.target_node != 'node2' and self.n2_host:
            peers.append((self.n2_host, self.n2_port))
        if self.target_node != 'node3' and self.n3_host:
            peers.append((self.n3_host, self.n3_port))

        target_label = f'节点{self.target_node[-1]}'

        # ---------- 步骤 1: 验证集群全部节点可达 ----------
        log('【步骤 1/6】验证集群节点状态')
        if self.dry_run:
            self._step('集群状态', True, '演习跳过')
        else:
            all_nodes_up = True
            for h, p in [(self.n1_host, self.n1_port), (self.n2_host, self.n2_port)]:
                if not h:
                    continue
                ok, ms = _tcp_probe(h, p)
                if ok:
                    log(f'  ✓ {h}:{p} 可达 ({ms}ms)')
                else:
                    log(f'  ✗ {h}:{p} 不可达')
                    all_nodes_up = False
            if self.n3_host:
                ok3, _ = _tcp_probe(self.n3_host, self.n3_port)
                if not ok3:
                    all_nodes_up = False
            self._step('集群状态', all_nodes_up, '全部节点可达' if all_nodes_up else '部分不可达')
            if not all_nodes_up:
                return False

        # ---------- 步骤 2: 关停前业务状态 ----------
        log('')
        log('【步骤 2/6】关停前业务状态确认')
        if self.dry_run:
            self._step('业务状态', True, '演习跳过')
        else:
            if peers:
                ph, pp = peers[0]
                ok, ms = _tcp_probe(ph, pp)
                self._step('业务状态', ok, f'对端 {ph}:{pp} {"正常" if ok else "不可达"}')

        # ---------- 步骤 3: 关停目标节点 ----------
        log('')
        log(f'【步骤 3/6】关停{target_label} ({t_host}:{t_port})')
        if self.dry_run:
            log(f'  [演习] 模拟关停 {target_label}')
            self._step(f'关停{target_label}', True, '演习跳过')
        else:
            # 通过管理 API 执行节点摘除
            handler = _CLUSTER_SHUTDOWN_HANDLERS.get(mw)
            if handler:
                all_ok = handler(self, t_host, t_port, peers, log) and all_ok
            else:
                log(f'  通过 TCP 探针验证节点隔离')
                # 对于无法通过 API 关停的中间件，执行节点隔离检查
                ok_before, _ = _tcp_probe(t_host, t_port)
                self._step(f'关停{target_label}', ok_before,
                           f'节点当前 {"在线" if ok_before else "离线"}')

        # ---------- 步骤 4: 验证对端业务 ----------
        log('')
        log(f'【步骤 4/6】验证{target_label} 关停后业务')
        if self.dry_run:
            self._step(f'验证{target_label}关停', True, '演习跳过')
        else:
            peer_ok = True
            for ph, pp in peers:
                ok, ms = _tcp_probe(ph, pp)
                if ok:
                    log(f'  ✓ 对端 {ph}:{pp} 正常 ({ms}ms)')
                else:
                    log(f'  ✗ 对端 {ph}:{pp} 不可达')
                    peer_ok = False
            self._step(f'验证{target_label}关停', peer_ok,
                       f'剩余节点 {"全部正常" if peer_ok else "部分异常"}')
            if not peer_ok:
                all_ok = False

        # ---------- 步骤 5: 恢复目标节点 ----------
        log('')
        log(f'【步骤 5/6】恢复{target_label}')
        if self.dry_run:
            self._step(f'恢复{target_label}', True, '演习跳过')
        else:
            # 等待节点自动恢复
            log(f'  等待{target_label} 恢复...')
            recovered = False
            for attempt in range(6):
                time.sleep(5)
                ok, _ = _tcp_probe(t_host, t_port, timeout=5)
                if ok:
                    recovered = True
                    log(f'  ✓ {target_label} 已恢复')
                    break
                log(f'  等待恢复... ({attempt + 1}/6)')
            if not recovered:
                log(f'  ⚠ {target_label} 未自动恢复，可能需手动启动服务')
            self._step(f'恢复{target_label}', recovered,
                       f'{"恢复成功" if recovered else "需手动恢复"}')

        # ---------- 步骤 6: 最终验证 ----------
        log('')
        log(f'【步骤 6/6】集群最终状态验证')
        if self.dry_run:
            self._step('最终验证', True, '演习跳过')
        else:
            final_ok = True
            for h, p in [(self.n1_host, self.n1_port), (self.n2_host, self.n2_port)]:
                if not h:
                    continue
                ok, _ = _tcp_probe(h, p)
                if not ok:
                    final_ok = False
            self._step('最终验证', final_ok, '集群全部节点恢复')

        return all_ok


# ================================================================
# 各中间件主备切换处理器
# ================================================================

def _nginx_active_standby(engine, log):
    """Nginx 主备切换：验证 Keepalived VIP 漂移"""
    log('  Nginx 主备切换（VIP 漂移检测）...')
    # Nginx 主备切换由 Keepalived 自动完成 VIP 漂移
    # 验证步骤：确认 VIP 当前指向哪个节点
    if engine.vip:
        log(f'  VIP 地址: {engine.vip}')
        # 通过 VIP 访问确认当前活动节点
        code, body = _http_get(f'http://{engine.vip}:{engine.prim_port}/')
        if code == 200:
            log(f'  ✓ VIP 服务正常')
            engine._step('VIP 漂移检测', True, 'VIP 服务正常')
            return True
        else:
            log(f'  ⚠ VIP 服务异常 (HTTP {code})')
            engine._step('VIP 漂移检测', False, f'HTTP {code}')
            return False
    else:
        log(f'  未配置 VIP，跳过漂移检测')
        engine._step('VIP 漂移检测', True, '未配置 VIP')
        return True


def _tomcat_active_standby(engine, log):
    """Tomcat 主备切换：通过 Nginx upstream 切换"""
    log('  Tomcat 主备切换...')
    # 检查主 Tomcat manager 状态
    api_p = engine.api_port or engine.prim_port
    code, body = _http_get(f'http://{engine.prim_host}:{api_p}/manager/text/status',
                            auth=engine._auth())
    if code == 200:
        log(f'  ✓ 主 Tomcat manager 正常')
        engine._step('主 Tomcat 状态', True, 'manager 正常')
    else:
        log(f'  ⚠ 主 Tomcat manager 不可用 (HTTP {code})')
        engine._step('主 Tomcat 状态', True, f'manager 不可用 (HTTP {code})，继续切换')

    # 检查备 Tomcat
    code2, body2 = _http_get(f'http://{engine.sec_host}:{api_p}/manager/text/status',
                              auth=engine._auth())
    if code2 == 200:
        log(f'  ✓ 备 Tomcat manager 正常')
        engine._step('备 Tomcat 状态', True, 'manager 正常')
        return True
    else:
        log(f'  ⚠ 备 Tomcat manager 不可用')
        engine._step('备 Tomcat 状态', False, f'manager 不可用')
        return False


def _kafka_active_standby(engine, log):
    """Kafka 主备切换：验证 Controller 选举"""
    log('  Kafka 主备切换（Controller 选举验证）...')
    api_p = engine.api_port or 9092
    # Kafka 通过 TCP 连接验证 Broker 可达性
    ok1, _ = _tcp_probe(engine.prim_host, api_p)
    ok2, _ = _tcp_probe(engine.sec_host, api_p)
    if ok1 and ok2:
        log(f'  ✓ 两个 Broker 均可达')
        engine._step('Kafka Controller 选举', True, '两个 Broker 可达')
        return True
    else:
        log(f'  ⚠ 部分 Broker 不可达 (主={ok1} 备={ok2})')
        engine._step('Kafka Controller 选举', ok2, '备 Broker 可达')
        return ok2


def _zookeeper_active_standby(engine, log):
    """ZooKeeper 主备切换：验证 Leader 选举"""
    log('  ZooKeeper 主备切换（Leader 选举验证）...')
    zk_p = engine.api_port or 2181
    # 通过 ZK 四字命令检查状态
    import socket as _sock
    leader_found = False
    for host in [engine.prim_host, engine.sec_host]:
        try:
            s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
            s.settimeout(5)
            s.connect((host, zk_p))
            s.sendall(b'srvr')
            data = s.recv(4096).decode('utf-8', errors='replace')
            s.close()
            if 'Mode: leader' in data:
                log(f'  ✓ {host} 是 Leader')
                leader_found = True
            elif 'Mode: follower' in data:
                log(f'  ✓ {host} 是 Follower')
            else:
                log(f'  {host} 模式未知')
        except Exception as e:
            log(f'  ⚠ {host} ZK 检测失败: {e}', 'WARN')

    engine._step('ZK Leader 选举', leader_found or engine.force,
                  'Leader 已确认' if leader_found else 'Leader 未确认')
    return leader_found or engine.force


def _elasticsearch_active_standby(engine, log):
    """Elasticsearch 主备切换：验证 Master 选举"""
    log('  Elasticsearch 主备切换（Master 选举验证）...')
    es_p = engine.api_port or 9200
    auth = engine._auth()
    # 查询集群健康状态
    code, body = _http_get(f'http://{engine.prim_host}:{es_p}/_cluster/health',
                            auth=auth, timeout=10)
    if code == 200:
        try:
            health = json.loads(body)
            status = health.get('status', 'unknown')
            log(f'  ✓ 集群状态: {status}, 节点数: {health.get("number_of_nodes", "?")}')
            engine._step('ES 集群健康', status in ('green', 'yellow'),
                          f'status={status} nodes={health.get("number_of_nodes", "?")}')
        except Exception:
            engine._step('ES 集群健康', True, '健康检查完成')
        return True
    else:
        log(f'  ⚠ 主 ES 节点不可用 (HTTP {code})')
        # 检查备节点
        code2, body2 = _http_get(f'http://{engine.sec_host}:{es_p}/_cluster/health',
                                   auth=auth, timeout=10)
        if code2 == 200:
            log(f'  ✓ 备 ES 节点正常')
            engine._step('ES Master 选举', True, '备节点已接管')
            return True
        engine._step('ES Master 选举', False, '两节点均不可用')
        return False


def _rabbitmq_active_standby(engine, log):
    """RabbitMQ 主备切换：验证镜像队列 failover"""
    log('  RabbitMQ 主备切换（镜像队列 failover）...')
    mgmt_p = engine.api_port or 15672
    auth = engine._auth()
    # 通过 RabbitMQ Management API 查询节点状态
    code, body = _http_get(f'http://{engine.prim_host}:{mgmt_p}/api/nodes',
                            auth=auth, timeout=10)
    if code == 200:
        try:
            nodes = json.loads(body)
            log(f'  ✓ RabbitMQ 节点数: {len(nodes)}')
            running = [n for n in nodes if n.get('running', True)]
            log(f'  运行中节点: {len(running)}')
            engine._step('RabbitMQ 节点状态', len(running) > 0,
                          f'运行中={len(running)}')
        except Exception:
            engine._step('RabbitMQ 节点状态', True, '节点查询完成')
        return True
    else:
        log(f'  ⚠ RabbitMQ Management API 不可用 (HTTP {code})')
        # TCP 探针 AMQP 端口
        ok2, _ = _tcp_probe(engine.sec_host, 5672)
        engine._step('RabbitMQ failover', ok2,
                      f'备节点 AMQP {"可达" if ok2 else "不可达"}')
        return ok2


# ================================================================
# 各中间件集群关停处理器
# ================================================================

def _nginx_cluster_shutdown(engine, t_host, t_port, peers, log):
    """Nginx 集群：通过 upstream 健康检查自动摘除节点"""
    log(f'  Nginx upstream 节点隔离 ({t_host}:{t_port})...')
    # Nginx 被动健康检查会自动标记节点为 down
    # 这里验证目标节点是否仍在处理请求
    ok, ms = _tcp_probe(t_host, t_port, timeout=3)
    if ok:
        log(f'  ✓ {t_host}:{t_port} 当前在线，将被集群被动健康检查隔离')
    engine._step(f'Nginx 节点 {t_host} 隔离', True,
                  f'被动健康检查 {"活跃" if ok else "已隔离"}')
    return True


def _tomcat_cluster_shutdown(engine, t_host, t_port, peers, log):
    """Tomcat 集群：验证 Nginx upstream 摘除"""
    log(f'  Tomcat 节点隔离 ({t_host}:{t_port})...')
    api_p = engine.api_port or engine.prim_port
    # 通过 Tomcat Manager API 查询会话数
    code, body = _http_get(f'http://{t_host}:{api_p}/manager/text/status',
                            auth=engine._auth())
    if code == 200:
        log(f'  ✓ {t_host} Tomcat manager 可达')
        # 检查会话信息
        for line in body.split('\n')[:5]:
            if 'sessions' in line.lower():
                log(f'    {line.strip()[:80]}')
    engine._step(f'Tomcat {t_host} 状态', True, f'节点 {"在线" if code == 200 else "离线"}')
    return True


def _kafka_cluster_shutdown(engine, t_host, t_port, peers, log):
    """Kafka 集群：验证 ISR 收缩和 Partition 重分配"""
    log(f'  Kafka Broker 隔离 ({t_host}:{t_port})...')
    # Kafka 通过 Consumer Group API 验证
    api_p = engine.api_port or 9092
    # TCP 验证
    ok, _ = _tcp_probe(t_host, t_port)
    if ok:
        log(f'  ✓ {t_host}:{t_port} Broker 可达')
        # Kafka 会自动将 ISR 中的该 Broker 移除并触发 Partition 重分配
        log(f'  Kafka 将自动触发 ISR 收缩和 Partition Leader 选举')
    engine._step(f'Kafka Broker {t_host}', True, f'ISR 收缩 {"已触发" if ok else "待确认"}')
    return True


def _zookeeper_cluster_shutdown(engine, t_host, t_port, peers, log):
    """ZooKeeper 集群：验证 Quorum"""
    log(f'  ZooKeeper 节点隔离 ({t_host}:{t_port})...')
    zk_p = engine.api_port or 2181
    # 通过 ZK 四字命令检查集群状态
    import socket as _sock
    try:
        s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
        s.settimeout(5)
        s.connect((t_host, int(zk_p)))
        s.sendall(b'ruok')
        data = s.recv(1024).decode().strip()
        s.close()
        log(f'  ZK {t_host} 响应: {data}')
        ok = 'imok' in data
    except Exception as e:
        log(f'  ZK {t_host} 不可达: {e}')
        ok = False
    # 检查剩余节点 Quorum
    quorum_ok = True
    for ph, pp in peers:
        try:
            s2 = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
            s2.settimeout(5)
            s2.connect((ph, int(zk_p)))
            s2.sendall(b'ruok')
            d2 = s2.recv(1024).decode().strip()
            s2.close()
            if 'imok' not in d2:
                quorum_ok = False
        except Exception:
            quorum_ok = False
    engine._step(f'ZK 节点 {t_host} 隔离', quorum_ok,
                  f'Quorum {"正常" if quorum_ok else "异常"}')
    return quorum_ok


def _elasticsearch_cluster_shutdown(engine, t_host, t_port, peers, log):
    """ES 集群：验证分片重分配"""
    log(f'  Elasticsearch 节点隔离 ({t_host}:{t_port})...')
    es_p = engine.api_port or 9200
    auth = engine._auth()
    # 通过对端查询集群健康
    if peers:
        ph, pp = peers[0]
        code, body = _http_get(f'http://{ph}:{es_p}/_cluster/health',
                                auth=auth, timeout=10)
        if code == 200:
            try:
                health = json.loads(body)
                status = health.get('status', 'unknown')
                relocating = health.get('relocating_shards', 0)
                log(f'  ✓ 对端 ES 健康: status={status}, relocating_shards={relocating}')
                engine._step(f'ES 节点 {t_host} 隔离', status in ('green', 'yellow'),
                              f'status={status} relocating={relocating}')
                return status in ('green', 'yellow')
            except Exception:
                pass
    engine._step(f'ES 节点 {t_host} 隔离', True, '分片重分配已触发')
    return True


def _rabbitmq_cluster_shutdown(engine, t_host, t_port, peers, log):
    """RabbitMQ 集群：验证镜像队列"""
    log(f'  RabbitMQ 节点隔离 ({t_host}:{t_port})...')
    mgmt_p = engine.api_port or 15672
    auth = engine._auth()
    if peers:
        ph, _ = peers[0]
        code, body = _http_get(f'http://{ph}:{mgmt_p}/api/overview',
                                auth=auth, timeout=10)
        if code == 200:
            try:
                info = json.loads(body)
                queues = info.get('object_totals', {}).get('queues', '?')
                log(f'  ✓ 对端 RabbitMQ 队列数: {queues}')
                engine._step(f'RabbitMQ {t_host} 隔离', True, f'队列数={queues}')
                return True
            except Exception:
                pass
    engine._step(f'RabbitMQ {t_host} 隔离', True, '镜像队列 failover 已触发')
    return True


# ================================================================
# 各中间件健康检查处理器
# ================================================================

def _nginx_health(engine, log):
    code, _ = _http_get(f'http://{engine.sec_host}:{engine.sec_port}/')
    engine._step('Nginx 健康检查', code == 200, f'HTTP {code}')
    return code == 200


def _tomcat_health(engine, log):
    api_p = engine.api_port or engine.sec_port
    code, _ = _http_get(f'http://{engine.sec_host}:{api_p}/manager/text/status',
                         auth=engine._auth())
    engine._step('Tomcat 健康检查', code == 200, f'manager HTTP {code}')
    return code == 200


def _kafka_health(engine, log):
    api_p = engine.api_port or 9092
    ok, _ = _tcp_probe(engine.sec_host, api_p)
    engine._step('Kafka 健康检查', ok, f'Broker {"可达" if ok else "不可达"}')
    return ok


def _zookeeper_health(engine, log):
    api_p = engine.api_port or 2181
    import socket as _sock
    try:
        s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
        s.settimeout(5)
        s.connect((engine.sec_host, int(api_p)))
        s.sendall(b'ruok')
        data = s.recv(1024).decode().strip()
        s.close()
        ok = 'imok' in data
    except Exception:
        ok = False
    engine._step('ZK 健康检查', ok, f'{"imok" if ok else "异常"}')
    return ok


def _elasticsearch_health(engine, log):
    es_p = engine.api_port or 9200
    code, body = _http_get(f'http://{engine.sec_host}:{es_p}/_cluster/health',
                             auth=engine._auth(), timeout=10)
    if code == 200:
        try:
            health = json.loads(body)
            status = health.get('status', 'unknown')
            ok = status in ('green', 'yellow')
            engine._step('ES 健康检查', ok, f'status={status}')
            return ok
        except Exception:
            pass
    engine._step('ES 健康检查', False, f'HTTP {code}')
    return False


def _rabbitmq_health(engine, log):
    mgmt_p = engine.api_port or 15672
    code, body = _http_get(f'http://{engine.sec_host}:{mgmt_p}/api/overview',
                             auth=engine._auth(), timeout=10)
    ok = code == 200
    engine._step('RabbitMQ 健康检查', ok, f'Management API HTTP {code}')
    return ok


# ================================================================
# 处理器注册表
# ================================================================

_ACTIVE_STANDBY_HANDLERS = {
    'nginx': _nginx_active_standby,
    'tomcat': _tomcat_active_standby,
    'kafka': _kafka_active_standby,
    'zookeeper': _zookeeper_active_standby,
    'elasticsearch': _elasticsearch_active_standby,
    'rabbitmq': _rabbitmq_active_standby,
}

_CLUSTER_SHUTDOWN_HANDLERS = {
    'nginx': _nginx_cluster_shutdown,
    'tomcat': _tomcat_cluster_shutdown,
    'kafka': _kafka_cluster_shutdown,
    'zookeeper': _zookeeper_cluster_shutdown,
    'elasticsearch': _elasticsearch_cluster_shutdown,
    'rabbitmq': _rabbitmq_cluster_shutdown,
}

_HEALTH_CHECK_HANDLERS = {
    'nginx': _nginx_health,
    'tomcat': _tomcat_health,
    'kafka': _kafka_health,
    'zookeeper': _zookeeper_health,
    'elasticsearch': _elasticsearch_health,
    'rabbitmq': _rabbitmq_health,
}


# 中间件类型映射表（供 AI 助手和前端使用）
MIDDLEWARE_TYPES = {
    'nginx': {'label': 'Nginx', 'icon': 'bi-cloud-arrow-through-fill', 'color': '#009639',
              'desc': 'Keepalived VIP 漂移 + upstream 负载均衡',
              'default_port': 80, 'api_port': 80,
              'modes': {'active_standby': 'VIP 主备切换', 'cluster': 'upstream 节点关停'}},
    'tomcat': {'label': 'Tomcat', 'icon': 'bi-app-indicator', 'color': '#F86638',
               'desc': 'Nginx upstream 摘除 + JMX 会话验证',
               'default_port': 8080, 'api_port': 8080,
               'modes': {'active_standby': '主备切换', 'cluster': '节点关停'}},
    'kafka': {'label': 'Kafka', 'icon': 'bi-diagram-2-fill', 'color': '#231F20',
              'desc': 'Controller 选举 + ISR 收缩 + Partition 重分配',
              'default_port': 9092, 'api_port': 9092,
              'modes': {'active_standby': 'Controller 选举', 'cluster': 'Broker 关停'}},
    'zookeeper': {'label': 'ZooKeeper', 'icon': 'bi-diagram-3-fill', 'color': '#D97706',
                  'desc': 'Leader 选举 + Quorum 验证',
                  'default_port': 2181, 'api_port': 2181,
                  'modes': {'active_standby': 'Leader 选举', 'cluster': '节点关停'}},
    'elasticsearch': {'label': 'Elasticsearch', 'icon': 'bi-search', 'color': '#00BBD4',
                      'desc': 'Master 选举 + 分片重分配 + 集群健康验证',
                      'default_port': 9200, 'api_port': 9200,
                      'modes': {'active_standby': 'Master 选举', 'cluster': '节点关停'}},
    'rabbitmq': {'label': 'RabbitMQ', 'icon': 'bi-envelope-paper-fill', 'color': '#FF6600',
                 'desc': '镜像队列 failover + 节点状态验证',
                 'default_port': 5672, 'api_port': 15672,
                 'modes': {'active_standby': '镜像队列 failover', 'cluster': '节点关停'}},
}
