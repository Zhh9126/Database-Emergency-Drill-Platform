# -*- coding: utf-8 -*-
"""
MySQL Group Replication (MGR) 切换演练引擎
- mgr_single: 单主模式，执行 group_replication_set_as_primary 切换 Primary
- mgr_multi:  多主模式，执行"节点关停测试"验证集群高可用
"""
import json
import time
import uuid
import random
import logging
from datetime import datetime, timezone

import pymysql

logger = logging.getLogger(__name__)


def _now():
    return datetime.now(timezone.utc).isoformat()


def _step(name, success=True, detail=''):
    return {'name': name, 'success': success, 'timestamp': _now(), 'detail': detail}


class MgrSwitchoverError(Exception):
    pass


class MgrSwitchoverEngine:
    """MySQL MGR 切换/演练引擎"""

    def __init__(self, config: dict, force: bool = False, dry_run: bool = False, target_node: str = 'node2'):
        self.cfg = config
        self.force = force
        self.dry_run = dry_run
        self.target_node = target_node if target_node in ('node1', 'node2') else 'node2'
        self.mode = config.get('mysql_subtype', 'mgr_single')  # mgr_single | mgr_multi
        self.logs = []
        self.steps = []
        self.primary_host = config.get('master_host', '')
        self.primary_port = int(config.get('master_port', 3306))
        self.secondary_host = config.get('slave_host', '')
        self.secondary_port = int(config.get('slave_port', 3306))
        self.user = config.get('mysql_user', 'root')
        self.password = config.get('mysql_pass', '')
        self.connect_timeout = int(config.get('connect_timeout', 5))
        # 多主模式下支持选择关停节点
        if self.target_node == 'node1':
            self.target_host = self.primary_host
            self.target_port = self.primary_port
            self.peer_host = self.secondary_host
            self.peer_port = self.secondary_port
        else:
            self.target_host = self.secondary_host
            self.target_port = self.secondary_port
            self.peer_host = self.primary_host
            self.peer_port = self.primary_port

    def _log(self, msg, level='INFO'):
        self.logs.append(f'[{level}] {msg}')

    def _connect(self, host, port):
        return pymysql.connect(
            host=host, port=port, user=self.user, password=self.password,
            connect_timeout=self.connect_timeout,
            read_timeout=30, write_timeout=30,
            charset='utf8mb4', autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def _get_one(self, conn, sql, params=()):
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return row

    def _get_all(self, conn, sql, params=()):
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    # ------------------------------------------------------------------
    # 单主模式：Primary 角色切换
    # ------------------------------------------------------------------
    def _run_single_primary(self):
        conn = None
        try:
            self._log(f'连接当前 Primary: {self.primary_host}:{self.primary_port}')
            conn = self._connect(self.primary_host, self.primary_port)
            self.steps.append(_step('连接当前 Primary', True, f'{self.primary_host}:{self.primary_port}'))

            # 1) 确认组复制状态
            row = self._get_one(conn, "SELECT * FROM performance_schema.replication_group_members WHERE MEMBER_ID = (SELECT @@server_uuid)")
            if not row:
                raise MgrSwitchoverError('无法获取当前节点 MGR 成员信息')
            if row.get('MEMBER_STATE') != 'ONLINE':
                raise MgrSwitchoverError(f'当前节点状态异常: {row.get("MEMBER_STATE")}')
            self._log(f'当前节点状态: ONLINE, 角色: {row.get("MEMBER_ROLE")}')
            self.steps.append(_step('检查本机 MGR 状态', True, f'状态=ONLINE 角色={row.get("MEMBER_ROLE")}'))

            # 2) 查找目标 Secondary 的 member_id
            members = self._get_all(
                conn,
                "SELECT MEMBER_ID, MEMBER_HOST, MEMBER_PORT, MEMBER_ROLE, MEMBER_STATE "
                "FROM performance_schema.replication_group_members WHERE MEMBER_STATE='ONLINE'"
            )
            self._log(f'组内 ONLINE 成员数: {len(members)}')
            target_member = None
            target_host = None
            target_port = None
            for m in members:
                host = m.get('MEMBER_HOST', '')
                port = int(m.get('MEMBER_PORT', 3306))
                # 优先匹配配置中的 secondary_host
                if host == self.secondary_host or str(port) == str(self.secondary_port):
                    target_member = m.get('MEMBER_ID')
                    target_host = host
                    target_port = port
                    break
            if not target_member:
                # 退而求其次：找任意 SECONDARY
                for m in members:
                    if m.get('MEMBER_ROLE') == 'SECONDARY':
                        target_member = m.get('MEMBER_ID')
                        target_host = m.get('MEMBER_HOST')
                        target_port = int(m.get('MEMBER_PORT', 3306))
                        break
            if not target_member:
                raise MgrSwitchoverError('未找到可用的 SECONDARY 成员')
            self._log(f'目标 Secondary: {target_host}:{target_port} ({target_member[:8]}...)')
            self.steps.append(_step('定位目标 Secondary', True, f'{target_host}:{target_port}'))

            # 3) 执行切换 Primary
            if self.dry_run:
                self._log('[演习] 模拟执行 group_replication_set_as_primary')
                self.steps.append(_step('切换 Primary（演习跳过）', True, '未执行真实 SQL'))
            else:
                self._log('执行 SELECT group_replication_set_as_primary(...)')
                self._get_one(conn, "SELECT group_replication_set_as_primary(%s)", (target_member,))
                self.steps.append(_step('切换 Primary', True, f'新 Primary UUID={target_member[:8]}...'))

            # 4) 验证新 Primary
            if self.dry_run:
                self.steps.append(_step('验证新 Primary（演习跳过）', True))
            else:
                time.sleep(1)
                verified = False
                for attempt in range(3):
                    try:
                        # 连接到原 secondary 验证它现在是 PRIMARY
                        verify_conn = self._connect(target_host, target_port)
                        role_row = self._get_one(verify_conn, "SELECT MEMBER_ROLE FROM performance_schema.replication_group_members WHERE MEMBER_ID=(SELECT @@server_uuid)")
                        verify_conn.close()
                        role = role_row.get('MEMBER_ROLE') if role_row else 'UNKNOWN'
                        self._log(f'验证新 Primary 角色: {role}')
                        if role == 'PRIMARY':
                            verified = True
                            self.steps.append(_step('验证新 Primary', True, f'{target_host}:{target_port} 角色=PRIMARY'))
                            break
                        time.sleep(1)
                    except Exception as e:
                        self._log(f'验证尝试 {attempt+1} 失败: {e}', 'WARNING')
                        time.sleep(1)
                if not verified:
                    raise MgrSwitchoverError('切换后未能在目标节点确认 PRIMARY 角色')

            return {
                'success': True,
                'new_master_host': target_host or self.secondary_host,
                'new_master_port': target_port or self.secondary_port,
                'new_slave_host': self.primary_host,
                'new_slave_port': self.primary_port,
            }
        except Exception as e:
            self._log(str(e), 'ERROR')
            self.steps.append(_step('切换执行', False, str(e)))
            return {'success': False, 'error': str(e)}
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # 多主模式：节点关停测试
    # ------------------------------------------------------------------
    def _run_multi_primary(self):
        """多主模式下进行指定节点关停测试，验证对端节点可继续提供服务"""
        target_label = '节点1' if self.target_node == 'node1' else '节点2'
        peer_label = '节点2' if self.target_node == 'node1' else '节点1'
        try:
            self._log(f'多主模式集群关停测试: 节点1={self.primary_host}:{self.primary_port}, 节点2={self.secondary_host}:{self.secondary_port}, 目标={self.target_node}')
            self.steps.append(_step(f'连接{peer_label}', True, f'{self.peer_host}:{self.peer_port}'))

            if self.dry_run:
                self._log(f'[演习] 模拟{target_label} 服务停止')
                self.steps.append(_step(f'停止{target_label} 服务（演习）', True, '未执行真实操作'))
                self.steps.append(_step(f'验证{peer_label} 业务可用（演习）', True, '模拟验证通过'))
                self.steps.append(_step(f'启动{target_label} 服务（演习）', True, '模拟恢复完成'))
                self.steps.append(_step('验证集群恢复（演习）', True, '模拟集群恢复'))
            else:
                # 通过连接目标节点执行 STOP GROUP_REPLICATION 实现真实关停，
                # 然后通过对端节点查询集群成员状态验证切换效果。
                try:
                    conn_peer = self._connect(self.peer_host, self.peer_port)
                    members = self._get_all(conn_peer, "SELECT MEMBER_HOST, MEMBER_PORT, MEMBER_ROLE, MEMBER_STATE FROM performance_schema.replication_group_members")
                    self._log(f'集群成员数: {len(members)}')
                    for m in members:
                        self._log(f"  {m.get('MEMBER_HOST')}:{m.get('MEMBER_PORT')} {m.get('MEMBER_ROLE')}/{m.get('MEMBER_STATE')}")
                    conn_peer.close()
                    self.steps.append(_step('查询集群成员状态', True, f'成员数={len(members)}'))
                except Exception as e:
                    self._log(f'查询集群状态失败: {e}', 'WARNING')
                    self.steps.append(_step('查询集群成员状态', False, str(e)))

                self._log(f'停止{target_label} group_replication 服务（需确保{target_label}可访问）')
                try:
                    conn_target = self._connect(self.target_host, self.target_port)
                    self._get_one(conn_target, "STOP GROUP_REPLICATION")
                    conn_target.close()
                    self._log(f'{target_label} 已停止组复制')
                    self.steps.append(_step(f'停止{target_label} 服务', True, f'{self.target_host}:{self.target_port}'))
                except Exception as e:
                    self._log(f'停止{target_label} 失败: {e}', 'WARNING')
                    self.steps.append(_step(f'停止{target_label} 服务', False, str(e)))
                    # 多主关停测试允许失败继续，但标记最终状态

                time.sleep(1)
                self._log(f'验证{peer_label} 业务可用')
                try:
                    conn_peer = self._connect(self.peer_host, self.peer_port)
                    row = self._get_one(conn_peer, "SELECT 1 AS ok")
                    conn_peer.close()
                    self._log(f'{peer_label} 业务验证通过')
                    self.steps.append(_step(f'验证{peer_label} 业务可用', True, 'SELECT 1 成功'))
                except Exception as e:
                    self._log(f'{peer_label} 验证失败: {e}', 'ERROR')
                    self.steps.append(_step(f'验证{peer_label} 业务可用', False, str(e)))

                self._log(f'恢复{target_label} group_replication 服务')
                try:
                    conn_target = self._connect(self.target_host, self.target_port)
                    self._get_one(conn_target, "START GROUP_REPLICATION")
                    conn_target.close()
                    self._log(f'{target_label} 已恢复组复制')
                    self.steps.append(_step(f'启动{target_label} 服务', True, f'{self.target_host}:{self.target_port}'))
                except Exception as e:
                    self._log(f'恢复{target_label} 失败: {e}', 'ERROR')
                    self.steps.append(_step(f'启动{target_label} 服务', False, str(e)))

                self._log('验证集群完全恢复')
                try:
                    conn_peer = self._connect(self.peer_host, self.peer_port)
                    members = self._get_all(conn_peer, "SELECT MEMBER_HOST, MEMBER_PORT, MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_STATE='ONLINE'")
                    conn_peer.close()
                    self._log(f'ONLINE 成员数: {len(members)}')
                    self.steps.append(_step('验证集群恢复', True, f'ONLINE 成员数={len(members)}'))
                except Exception as e:
                    self._log(f'集群恢复验证失败: {e}', 'ERROR')
                    self.steps.append(_step('验证集群恢复', False, str(e)))

            # 多主模式拓扑不改变
            return {
                'success': True,
                'new_master_host': self.primary_host,
                'new_master_port': self.primary_port,
                'new_slave_host': self.secondary_host,
                'new_slave_port': self.secondary_port,
            }
        except Exception as e:
            self._log(str(e), 'ERROR')
            return {'success': False, 'error': str(e)}

    # ------------------------------------------------------------------
    def run(self, log_callback=None):
        self.logs = []
        self.steps = []
        if log_callback:
            self.log_callback = log_callback
        else:
            self.log_callback = lambda x: None

        self._log(f'[MGR] 模式={self.mode}, 演习={self.dry_run}, 强制={self.force}')
        if self.mode == 'mgr_multi':
            result = self._run_multi_primary()
        else:
            result = self._run_single_primary()

        for line in self.logs:
            self.log_callback(line)

        return {
            'success': result.get('success', False),
            'steps': self.steps,
            'logs': self.logs,
            'is_shutdown_test': self.mode == 'mgr_multi',
            'target_node': self.target_node,
            'new_master_host': result.get('new_master_host', self.primary_host),
            'new_master_port': result.get('new_master_port', self.primary_port),
            'new_slave_host': result.get('new_slave_host', self.secondary_host),
            'new_slave_port': result.get('new_slave_port', self.secondary_port),
        }


# 兼容性别名
MgrEngine = MgrSwitchoverEngine
