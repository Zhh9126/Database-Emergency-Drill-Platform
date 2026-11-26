"""
应急演练平台 - AI 助手模块
支持多厂商大模型 API 配置
可通过 AI 驱动切换演练（工具调用）
"""
import json
import time
import uuid
import threading
import logging
from urllib.request import Request, urlopen
from urllib.error import URLError

logger = logging.getLogger(__name__)


# ======================== 系统提示词 ========================

SYSTEM_PROMPT = """你是应急演练平台的 AI 运维助手，专注于数据库高可用架构的主从切换演练。

平台支持的数据库与集群模式（已对齐分类）：
- MySQL：传统主从复制（基于 binlog 主备互换）；MGR 单主（复制型主备切换，Primary 角色切换）；MGR 多主（集群型主备切换，节点关停验证 HA）
- Oracle：ADG（Active DataGuard，复制型主备切换）与 RAC（Real Application Clusters，集群型主备切换）
- PostgreSQL：流复制（Streaming Replication，复制型主备切换）；Patroni/PG-Pool 等集群（集群型主备切换，底层仍依赖流复制）
- Redis：主从复制（复制型主从切换）；Sentinel/Redis Cluster（集群型主备切换）
- 达梦 DM8：主备模式（ALTER DATABASE SWITCHOVER，复制型）和 DSC 集群（集群型主备切换）
- 金仓 KingbaseES：主备模式（sys_rewind）和 KDC 集群（集群型主备切换）

切换演练的两种基本语义（核心分类）：
A. 复制型主备切换（Replica-based）：主备/主从角色互换，适用于基于数据复制的架构
   - MySQL 主从、MySQL MGR 单主、Oracle ADG、PG 流复制、Redis 主从、达梦主备、金仓主备
B. 集群型主备切换（Cluster-based）：多节点共享存储/共享状态集群的 HA 验证
   - Oracle RAC、达梦 DSC、金仓 KDC、MySQL MGR 多主、Redis Sentinel/Cluster、PG Patroni
   - 流程：关停一个非主业务节点的服务 → 验证业务在剩余节点正常 → 重启关停节点 → 验证集群完全恢复
   - 目的：验证集群的高可用（HA）能力，不做数据层面的主备切换

每种数据库可能有多种集群模式（例如：Oracle 同时支持 ADG 和 RAC，达梦同时支持主备和 DSC），切换类型由 db_type + subtype 共同决定。

你的职责：
1. 帮助用户了解各类数据库主从切换的原理和流程
2. 引导用户配置和执行数据库主从切换演练
3. 诊断切换过程中的问题并给出建议
4. 通过工具调用来执行健康检查、切换演练等实际操作

当前支持的操作：
- health_check: 对配置的数据库实例进行健康检查
- dry_run_switchover: 执行切换演习（Dry-Run），不实际切换
- execute_switchover: 执行正式的主从切换
- list_configs: 列出所有已保存的切换配置

回答要求：
- 简洁专业，使用中文
- 涉及数据库操作时务必谨慎，先确认再执行
- 在切换前必须确保用户确认
- 当用户要求直接操作时，先进行健康检查作为前置条件
- 当配置属于集群模式（Oracle RAC、达梦 DSC、金仓 KDC 等）时，明确告知用户这是「集群关停测试」而不是主备互换"""


# ======================== 工具定义 (OpenAI Function Calling 格式) ========================

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "health_check",
            "description": "对数据库实例进行快速健康检查，返回连接状态、拓扑信息等。自动识别数据库类型（MySQL/Oracle/PostgreSQL/Redis/达梦/金仓）",
            "parameters": {
                "type": "object",
                "properties": {
                    "config_id": {
                        "type": "integer",
                        "description": "配置ID，从 list_configs 获取"
                    }
                },
                "required": ["config_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "dry_run_switchover",
            "description": "执行切换演习（Dry-Run模式），检查所有前置条件但不实际切换，用于验证切换可行性。支持所有数据库类型",
            "parameters": {
                "type": "object",
                "properties": {
                    "config_id": {
                        "type": "integer",
                        "description": "配置ID"
                    }
                },
                "required": ["config_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "execute_switchover",
            "description": "正式执行数据库主从切换，从库提升为新主库。支持 MySQL/Oracle/PostgreSQL/Redis/达梦/金仓。⚠️ 此操作不可逆，执行前务必确保用户已确认",
            "parameters": {
                "type": "object",
                "properties": {
                    "config_id": {
                        "type": "integer",
                        "description": "配置ID"
                    },
                    "confirmed": {
                        "type": "boolean",
                        "description": "用户是否已确认执行切换"
                    }
                },
                "required": ["config_id", "confirmed"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_configs",
            "description": "列出所有已保存的切换配置，包含数据库类型和连接信息",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    }
]


# ======================== AI 助手类 ========================

class AIAssistant:
    """AI 助手，支持多厂商大模型"""

    PROVIDERS = {
        'openai': {
            'name': 'OpenAI',
            'default_base': 'https://api.openai.com/v1',
            'models': ['gpt-4o', 'gpt-4o-mini', 'gpt-4-turbo', 'gpt-3.5-turbo'],
        },
        'deepseek': {
            'name': 'DeepSeek',
            'default_base': 'https://api.deepseek.com/v1',
            'models': ['deepseek-chat', 'deepseek-reasoner'],
        },
        'qwen': {
            'name': '通义千问',
            'default_base': 'https://dashscope.aliyuncs.com/compatible-mode/v1',
            'models': ['qwen-plus', 'qwen-max', 'qwen-turbo'],
        },
        'ollama': {
            'name': 'Ollama (本地)',
            'default_base': 'http://localhost:11434/v1',
            'models': ['llama3', 'qwen2.5', 'mistral', 'deepseek-r1'],
        },
        'custom': {
            'name': '自定义',
            'default_base': 'https://your-api-host/v1',
            'models': [],
        },
    }

    def __init__(self, tool_executor=None):
        self._tool_executor = tool_executor  # 工具执行回调
        self._conversations: dict[str, list[dict]] = {}  # session_id -> messages
        self._config_cache: dict = {}

    def get_provider_info(self, provider_id: str) -> dict | None:
        """获取厂商信息"""
        return self.PROVIDERS.get(provider_id)

    def get_all_providers(self) -> dict:
        """获取所有厂商"""
        return self.PROVIDERS

    # ─── 对话管理 ───

    def start_conversation(self, session_id: str):
        """开始新对话"""
        self._conversations[session_id] = [
            {"role": "system", "content": SYSTEM_PROMPT}
        ]

    # 对话历史最大消息数（防止内存泄漏和 API token 超限）
    MAX_HISTORY = 50

    def add_message(self, session_id: str, role: str, content: str):
        """添加消息，超出上限时自动截断（保留系统提示词）"""
        if session_id not in self._conversations:
            self.start_conversation(session_id)
        self._conversations[session_id].append({"role": role, "content": content})
        # 截断过长的对话历史
        conv = self._conversations[session_id]
        if len(conv) > self.MAX_HISTORY:
            # 保留第一条系统提示 + 最近 MAX_HISTORY-1 条
            system_msg = conv[0] if conv[0]['role'] == 'system' else None
            keep = conv[-(self.MAX_HISTORY - 1):] if system_msg else conv[-self.MAX_HISTORY:]
            if system_msg:
                self._conversations[session_id] = [system_msg] + keep
            else:
                self._conversations[session_id] = keep

    def get_history(self, session_id: str) -> list[dict]:
        """获取对话历史"""
        return self._conversations.get(session_id, [])

    # ─── LLM 调用 ───

    def _call_llm(self, config: dict, messages: list[dict],
                  tools: list[dict] = None) -> dict:
        """调用大模型 API（兼容 OpenAI 格式）"""
        api_base = config['api_base'].rstrip('/')
        api_key = config['api_key']
        model = config.get('model_name', 'gpt-4o-mini')
        temperature = config.get('temperature', 0.3)
        max_tokens = config.get('max_tokens', 4096)

        # 兼容用户填写完整 chat/completions 端点或仅填写 base URL 的情况
        if api_base.endswith('/chat/completions'):
            url = api_base
        else:
            url = f"{api_base}/chat/completions"

        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = "auto"

        body = json.dumps(payload).encode('utf-8')

        req = Request(url, data=body, method='POST')
        req.add_header('Content-Type', 'application/json')
        req.add_header('Authorization', f'Bearer {api_key}')

        # 追加自定义请求头
        extra_headers = config.get('extra_headers', '')
        if extra_headers:
            try:
                for k, v in json.loads(extra_headers).items():
                    req.add_header(str(k), str(v))
            except Exception:
                pass

        try:
            with urlopen(req, timeout=120) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except URLError as e:
            raise Exception(f"API请求失败: {e}")

    def _execute_tool(self, tool_name: str, arguments: dict) -> str:
        """执行工具调用"""
        if self._tool_executor:
            return self._tool_executor(tool_name, arguments)
        return json.dumps({"error": "工具执行器未配置"})

    # ─── 主要聊天方法 ───

    def chat(self, session_id: str, user_message: str,
             llm_config: dict) -> dict:
        """
        用户发消息，返回 AI 回复
        支持多轮对话和工具调用
        """
        self.add_message(session_id, "user", user_message)
        messages = self.get_history(session_id)

        response = self._call_llm(llm_config, messages, TOOLS)
        if 'choices' not in response or not response['choices']:
            error_msg = response.get('error', {}).get('message', 'API返回格式异常') if isinstance(response.get('error'), dict) else str(response)
            raise Exception(f"LLM API返回异常: {error_msg}")
        choice = response['choices'][0]

        # 处理工具调用
        finish_reason = choice.get('finish_reason', 'stop')
        if finish_reason == 'tool_calls':
            tool_calls = choice['message'].get('tool_calls', [])
            self.add_message(session_id, "assistant",
                           json.dumps(choice['message'], ensure_ascii=False))

            # 执行工具调用
            tool_results = []
            for tc in tool_calls:
                fn_name = tc['function']['name']
                fn_args = json.loads(tc['function']['arguments'])
                result = self._execute_tool(fn_name, fn_args)
                tool_results.append({
                    'tool_call_id': tc['id'],
                    'role': 'tool',
                    'content': result,
                })

            # 追加工具结果到消息列表
            for tr in tool_results:
                self._conversations[session_id].append(tr)

            # 再次调用 LLM 获取最终回复
            messages = self.get_history(session_id)
            response2 = self._call_llm(llm_config, messages)
            if 'choices' not in response2 or not response2['choices']:
                raise Exception("LLM API第二次调用返回异常")
            assistant_msg = response2['choices'][0]['message'].get('content', '操作已完成')
            self.add_message(session_id, "assistant", assistant_msg)

            return {
                'reply': assistant_msg,
                'tool_calls': [{
                    'name': tc['function']['name'],
                    'arguments': json.loads(tc['function']['arguments']),
                    'result': json.loads(tr['content']) if tr['content'] else None,
                } for tc, tr in zip(tool_calls, tool_results)],
            }

        assistant_msg = choice['message'].get('content', '')
        if not assistant_msg:
            assistant_msg = '（AI未返回文本内容）'
        self.add_message(session_id, "assistant", assistant_msg)
        return {'reply': assistant_msg, 'tool_calls': []}

    # ─── 连接测试 ───

    def test_connection(self, config: dict) -> dict:
        """测试 LLM API 连接"""
        api_base = config['api_base'].rstrip('/')
        api_key = config['api_key']
        model = config.get('model_name', 'gpt-4o-mini')

        # 兼容用户填写完整 chat/completions 端点或仅填写 base URL 的情况
        if api_base.endswith('/chat/completions'):
            url = api_base
        else:
            url = f"{api_base}/chat/completions"
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": "Hi, reply with 'OK' only."}],
            "max_tokens": 10,
        }
        body = json.dumps(payload).encode('utf-8')

        req = Request(url, data=body, method='POST')
        req.add_header('Content-Type', 'application/json')
        req.add_header('Authorization', f'Bearer {api_key}')

        # 追加自定义请求头
        extra_headers = config.get('extra_headers', '')
        if extra_headers:
            try:
                for k, v in json.loads(extra_headers).items():
                    req.add_header(str(k), str(v))
            except Exception:
                pass

        start = time.time()
        try:
            with urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                latency = round((time.time() - start) * 1000)
                content = data['choices'][0]['message']['content']
                return {
                    'success': True,
                    'latency_ms': latency,
                    'model': data.get('model', model),
                    'response': content,
                }
        except Exception as e:
            latency = round((time.time() - start) * 1000)
            return {
                'success': False,
                'latency_ms': latency,
                'error': str(e),
            }


# ======================== 工具执行器工厂 ========================

def create_tool_executor(app):
    """创建工具执行器，绑定 Flask 应用上下文，支持所有数据库类型"""
    from models import db, SwitchoverConfig, SwitchoverHistory
    from datetime import datetime, timezone

    def _get_config(config_id):
        """安全获取配置"""
        config = db.session.get(SwitchoverConfig, config_id)
        if not config:
            raise ValueError(f'配置 {config_id} 不存在')
        return config

    def _build_engine_config(config):
        """
        从 model 对象构建包含真实密码的引擎配置字典。
        注意：不能使用 to_dict()，因为该方法会将密码脱敏为 '***'。
        """
        cfg = {}
        for col in config.__table__.columns:
            cfg[col.name] = getattr(config, col.name)
        return cfg

    def _do_health_check(config):
        """根据数据库类型执行健康检查"""
        if config.db_type == 'mysql':
            mysql_subtype = config.mysql_subtype or 'replica'
            if mysql_subtype == 'replica':
                from switchover_engine import SwitchoverEngine
                eng = SwitchoverEngine()
                return eng.quick_health_check(
                    config.master_host, config.master_port,
                    config.slave_host, config.slave_port,
                    config.mysql_user, config.mysql_pass,
                )
            from mysql_mgr_engine import MgrSwitchoverEngine
            eng = MgrSwitchoverEngine(config.to_dict())
            # MGR 健康检查：简单尝试连接节点1
            try:
                import pymysql
                conn = pymysql.connect(
                    host=config.master_host, port=config.master_port or 3306,
                    user=config.mysql_user, password=config.mysql_pass or '',
                    connect_timeout=5, cursorclass=pymysql.cursors.DictCursor,
                )
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 AS ok")
                    row = cur.fetchone()
                conn.close()
                return {'ok': bool(row), 'node1': config.master_host, 'node2': config.slave_host}
            except Exception as e:
                return {'ok': False, 'error': str(e)}
        elif config.db_type == 'oracle':
            oracle_subtype = config.oracle_subtype or 'adg'
            if oracle_subtype == 'rac':
                from oracle_rac_engine import OracleRacEngine, test_rac_connection
                primary = test_rac_connection(
                    host=config.rac_node1_host, sid=config.rac_service_name,
                    user=config.rac_user or config.oracle_user, password=config.rac_pass or config.oracle_pass,
                    port=config.rac_node1_port if hasattr(config, 'rac_node1_port') else 1521)
                standby = None
                if config.rac_node2_host:
                    standby = test_rac_connection(
                        host=config.rac_node2_host, sid=config.rac_service_name,
                        user=config.rac_node2_user or config.rac_user or config.oracle_user,
                        password=config.rac_node2_pass or config.rac_pass or config.oracle_pass,
                        port=1521)
                return {'ok': primary['ok'] and (standby is None or standby['ok']),
                        'primary': primary, 'standby': standby}
            else:
                from oracle_switchover_engine import test_oracle_connection
                primary = test_oracle_connection(
                    host=config.oracle_primary_host, port=config.oracle_primary_port or 1521,
                    sid=config.oracle_primary_sid,
                    user=config.oracle_user, password=config.oracle_pass)
                standby = None
                if config.oracle_standby_host:
                    standby = test_oracle_connection(
                        host=config.oracle_standby_host, port=config.oracle_standby_port or 1521,
                        sid=config.oracle_standby_sid,
                        user=config.oracle_standby_user or config.oracle_user,
                        password=config.oracle_standby_pass or config.oracle_pass)
                return {'ok': primary['ok'] and (standby is None or standby['ok']),
                        'primary': primary, 'standby': standby}
        elif config.db_type == 'postgresql':
            from postgresql_switchover_engine import test_pg_connection
            primary = test_pg_connection(
                host=config.pg_primary_host, port=config.pg_primary_port or 5432,
                user=config.pg_user or '', password=config.pg_pass or '')
            standby = None
            if config.pg_standby_host:
                standby = test_pg_connection(
                    host=config.pg_standby_host, port=config.pg_standby_port or 5432,
                    user=config.pg_standby_user or config.pg_user or '',
                    password=config.pg_standby_pass or config.pg_pass or '')
            return {'ok': primary['ok'] and (standby is None or standby['ok']),
                    'primary': primary, 'standby': standby}
        elif config.db_type == 'redis':
            from redis_switchover_engine import test_redis_status
            return test_redis_status(
                master_host=config.redis_master_host or '',
                slave_host=config.redis_slave_host or '',
                master_port=config.redis_master_port or 6379,
                slave_port=config.redis_slave_port or 6379,
                master_user=config.redis_master_user or '',
                master_pass=config.redis_master_pass or '',
                slave_user=config.redis_slave_user or config.redis_master_user or '',
                slave_pass=config.redis_slave_pass or config.redis_master_pass or '',
            )
        elif config.db_type == 'dameng':
            dm_subtype = config.dameng_subtype or 'standby'
            if dm_subtype == 'dsc':
                from dameng_switchover_engine import test_dameng_dsc_connection
                return test_dameng_dsc_connection({
                    'dm_dsc_node1_host': config.dm_dsc_node1_host or '',
                    'dm_dsc_node1_port': config.dm_dsc_node1_port or 5236,
                })
            from dameng_switchover_engine import test_dameng_connection
            return test_dameng_connection({
                'dm_primary_host': config.dm_primary_host or '',
                'dm_primary_port': config.dm_primary_port or 5236,
            })
        elif config.db_type == 'kingbase':
            kb_subtype = config.kingbase_subtype or 'standby'
            if kb_subtype == 'kdc':
                from kingbase_switchover_engine import test_kingbase_kdc_connection
                return test_kingbase_kdc_connection({
                    'kb_kdc_node1_host': config.kb_kdc_node1_host or '',
                    'kb_kdc_node1_port': config.kb_kdc_node1_port or 54321,
                })
            from kingbase_switchover_engine import test_kingbase_connection
            return test_kingbase_connection({
                'kb_primary_host': config.kb_primary_host or '',
                'kb_primary_port': config.kb_primary_port or 54321,
            })
        elif config.db_type == 'opengauss':
            from opengauss_switchover_engine import test_opengauss_connection
            return test_opengauss_connection({
                'og_user': config.og_user or 'gaussdb',
                'og_pass': config.og_pass or '',
                'og_primary_host': config.og_primary_host or '',
                'og_primary_port': config.og_primary_port or 5432,
                'og_db_name': config.og_db_name or 'postgres',
            })
        elif config.db_type == 'oceanbase':
            from oceanbase_switchover_engine import test_oceanbase_connection
            return test_oceanbase_connection({
                'ob_user': config.ob_user or 'root',
                'ob_pass': config.ob_pass or '',
                'ob_primary_host': config.ob_primary_host or '',
                'ob_primary_port': config.ob_primary_port or 2881,
            })
        elif config.db_type == 'tidb':
            from tidb_switchover_engine import test_tidb_connection
            return test_tidb_connection({
                'tidb_user': config.tidb_user or 'root',
                'tidb_pass': config.tidb_pass or '',
                'tidb_primary_host': config.tidb_primary_host or '',
                'tidb_primary_port': config.tidb_primary_port or 4000,
            })
        elif config.db_type == 'mongodb':
            from mongodb_switchover_engine import test_mongo_connection
            return test_mongo_connection({
                'mongo_user': config.mongo_user or '',
                'mongo_pass': config.mongo_pass or '',
                'mongo_primary_host': config.mongo_primary_host or '',
                'mongo_primary_port': config.mongo_primary_port or 27017,
            })
        elif config.db_type == 'highgo':
            from highgo_switchover_engine import test_highgo_connection
            return test_highgo_connection({
                'hg_user': config.hg_user or 'highgo',
                'hg_pass': config.hg_pass or '',
                'hg_primary_host': config.hg_primary_host or '',
                'hg_primary_port': config.hg_primary_port or 5866,
            })
        elif config.db_type == 'middleware':
            from middleware_switchover_engine import test_middleware_connection
            return test_middleware_connection({
                'middleware_type': config.middleware_type or 'nginx',
                'mw_primary_host': config.mw_primary_host or '',
                'mw_primary_port': config.mw_primary_port or 80,
                'mw_api_port': config.mw_api_port or 0,
            })
        else:
            raise ValueError(f'不支持的数据库类型: {config.db_type}')

    def _do_switchover(config, dry_run=False):
        """根据数据库类型执行切换，返回 (steps, logs, ok, message)"""
        # BUG修复：不能使用 to_dict()，因为密码字段会被脱敏为 '***' 导致引擎认证失败
        # 直接从 model 对象构建包含真实密码的配置字典
        cfg = _build_engine_config(config)
        cfg['dry_run'] = dry_run

        if config.db_type == 'mysql':
            mysql_subtype = config.mysql_subtype or 'replica'
            if mysql_subtype == 'replica':
                from switchover_engine import SwitchoverEngine
                eng = SwitchoverEngine()
                task_id = eng.start_switchover(cfg)
                for _ in range(300):
                    time.sleep(1)
                    status = eng.get_task_status(task_id)
                    if status and status['status'] == 'completed':
                        result = status.get('result', {})
                        ok = result.get('status') == 'success'
                        msg = result.get('message', 'MySQL切换完成')
                        return result.get('steps', []), status['logs'], ok, msg
                return [], ['切换超时'], False, 'MySQL切换超时'
            from mysql_mgr_engine import MgrSwitchoverEngine
            eng = MgrSwitchoverEngine(cfg, force=cfg.get('force_switchover', False), dry_run=dry_run)
            result = eng.run()
            ok = result.get('success', False)
            steps = result.get('steps', [])
            logs = result.get('logs', [])
            msg = 'MySQL MGR 切换完成' if ok else (result.get('error') or 'MySQL MGR 切换失败')
            return steps, logs, ok, msg

        elif config.db_type == 'oracle':
            oracle_subtype = config.oracle_subtype or 'adg'
            if oracle_subtype == 'rac':
                from oracle_rac_engine import OracleRacEngine
                eng = OracleRacEngine(cfg, force=cfg.get('force_switchover', False),
                                       dry_run=dry_run)
                steps, logs, ok = eng.run()
            else:
                from oracle_switchover_engine import OracleSwitchoverEngine
                cfg['force_switchover'] = cfg.get('force_switchover', False)
                cfg['dry_run'] = dry_run
                eng = OracleSwitchoverEngine(cfg)
                result = eng.run()
                ok = result.get('success', False)
                steps = result.get('steps', [])
                logs = result.get('full_log', '')
            msg = 'Oracle切换完成' if ok else 'Oracle切换失败'
            return steps, logs, ok, msg

        elif config.db_type == 'postgresql':
            from postgresql_switchover_engine import PgSwitchoverEngine
            eng = PgSwitchoverEngine(cfg, force=cfg.get('force_switchover', False),
                                     dry_run=dry_run)
            steps, logs, ok = eng.run()
            msg = 'PostgreSQL切换完成' if ok else 'PostgreSQL切换失败'
            return steps, logs, ok, msg

        elif config.db_type == 'redis':
            from redis_switchover_engine import RedisSwitchoverEngine
            eng = RedisSwitchoverEngine(cfg, force=cfg.get('force_switchover', False),
                                         dry_run=dry_run)
            ok, steps = eng.run()
            logs = [s.get('detail', s.get('name', '')) for s in steps]
            msg = 'Redis切换完成' if ok else 'Redis切换失败'
            return steps, logs, ok, msg

        elif config.db_type == 'dameng':
            dm_subtype = cfg.get('dameng_subtype', cfg.get('dm_subtype', 'standby'))
            if dm_subtype == 'dsc':
                from dameng_switchover_engine import DamengDscEngine
                eng = DamengDscEngine(cfg, force=cfg.get('force_switchover', False), dry_run=dry_run)
                result = eng.run()
                steps, logs, ok = result.get('steps', []), result.get('full_log', ''), result.get('ok', False)
                msg = 'DSC服务关停测试通过' if ok else 'DSC测试存在警告'
            else:
                from dameng_switchover_engine import DamengStandbyEngine
                eng = DamengStandbyEngine(cfg, force=cfg.get('force_switchover', False), dry_run=dry_run)
                result = eng.run()
                steps, logs, ok = result.get('steps', []), result.get('full_log', ''), result.get('ok', False)
                msg = '达梦主备切换完成' if ok else '达梦切换存在警告'
            return steps, logs, ok, msg

        elif config.db_type == 'kingbase':
            kb_subtype = cfg.get('kingbase_subtype', cfg.get('kb_subtype', 'standby'))
            if kb_subtype == 'kdc':
                from kingbase_switchover_engine import KingbaseKdcEngine
                eng = KingbaseKdcEngine(cfg, force=cfg.get('force_switchover', False), dry_run=dry_run)
                result = eng.run()
                steps, logs, ok = result.get('steps', []), result.get('full_log', ''), result.get('ok', False)
                msg = 'KDC服务关停测试通过' if ok else 'KDC测试存在警告'
            else:
                from kingbase_switchover_engine import KingbaseStandbyEngine
                eng = KingbaseStandbyEngine(cfg, force=cfg.get('force_switchover', False), dry_run=dry_run)
                result = eng.run()
                steps, logs, ok = result.get('steps', []), result.get('full_log', ''), result.get('ok', False)
                msg = '金仓主备切换完成' if ok else '金仓切换存在警告'
            return steps, logs, ok, msg

        elif config.db_type == 'opengauss':
            from opengauss_switchover_engine import OpenGaussSwitchoverEngine
            target_node = cfg.get('target_node', 'node2')
            eng = OpenGaussSwitchoverEngine(cfg, force=cfg.get('force_switchover', False),
                                            dry_run=dry_run, target_node=target_node)
            result = eng.run()
            steps = result.get('steps', [])
            logs = result.get('full_log', '\n'.join(result.get('logs', [])))
            ok = result.get('ok', False)
            msg = 'openGauss/GaussDB 切换完成' if ok else result.get('error', '切换失败')
            return steps, logs, ok, msg

        elif config.db_type == 'oceanbase':
            from oceanbase_switchover_engine import OceanBaseSwitchoverEngine
            target_node = cfg.get('target_node', 'node2')
            eng = OceanBaseSwitchoverEngine(cfg, force=cfg.get('force_switchover', False),
                                            dry_run=dry_run, target_node=target_node)
            result = eng.run()
            steps = result.get('steps', [])
            logs = result.get('full_log', '\n'.join(result.get('logs', [])))
            ok = result.get('ok', False)
            msg = 'OceanBase 切换完成' if ok else result.get('error', '切换失败')
            return steps, logs, ok, msg

        elif config.db_type == 'tidb':
            from tidb_switchover_engine import TiDBSwitchoverEngine
            target_node = cfg.get('target_node', 'node2')
            eng = TiDBSwitchoverEngine(cfg, force=cfg.get('force_switchover', False),
                                       dry_run=dry_run, target_node=target_node)
            result = eng.run()
            steps = result.get('steps', [])
            logs = result.get('full_log', '\n'.join(result.get('logs', [])))
            ok = result.get('ok', False)
            msg = 'TiDB 切换演练完成' if ok else result.get('error', '切换失败')
            return steps, logs, ok, msg

        elif config.db_type == 'mongodb':
            from mongodb_switchover_engine import MongoSwitchoverEngine
            target_node = cfg.get('target_node', 'node2')
            eng = MongoSwitchoverEngine(cfg, force=cfg.get('force_switchover', False),
                                        dry_run=dry_run, target_node=target_node)
            result = eng.run()
            steps = result.get('steps', [])
            logs = result.get('full_log', '\n'.join(result.get('logs', [])))
            ok = result.get('ok', False)
            msg = 'MongoDB 切换完成' if ok else result.get('error', '切换失败')
            return steps, logs, ok, msg

        elif config.db_type == 'highgo':
            from highgo_switchover_engine import HighGoSwitchoverEngine
            eng = HighGoSwitchoverEngine(cfg, force=cfg.get('force_switchover', False), dry_run=dry_run)
            result = eng.run()
            steps = result.get('steps', [])
            logs = result.get('full_log', '\n'.join(result.get('logs', [])))
            ok = result.get('ok', False)
            msg = '瀚高数据库切换完成' if ok else result.get('error', '切换失败')
            return steps, logs, ok, msg

        elif config.db_type == 'middleware':
            from middleware_switchover_engine import MiddlewareSwitchoverEngine
            target_node = cfg.get('target_node', 'node2')
            eng = MiddlewareSwitchoverEngine(cfg, force=cfg.get('force_switchover', False),
                                             dry_run=dry_run, target_node=target_node)
            result = eng.run()
            steps = result.get('steps', [])
            logs = result.get('full_log', '')
            ok = result.get('ok', False)
            msg = result.get('message', '中间件切换完成' if ok else '切换失败')
            return steps, logs, ok, msg

        else:
            raise ValueError(f'不支持的数据库类型: {config.db_type}')

    def _get_topology_from_config(config):
        """从配置中提取主从/主备拓扑信息"""
        if config.db_type == 'mysql':
            mysql_subtype = config.mysql_subtype or 'replica'
            if mysql_subtype == 'mgr_single':
                return config.master_host, config.master_port, config.slave_host, config.slave_port
            elif mysql_subtype == 'mgr_multi':
                return config.master_host, config.master_port, config.slave_host, config.slave_port
            return config.master_host, config.master_port, config.slave_host, config.slave_port
        elif config.db_type == 'oracle':
            oracle_subtype = config.oracle_subtype or 'adg'
            if oracle_subtype == 'rac':
                return config.rac_node1_host, 1521, \
                       config.rac_node2_host, 1521
            else:
                return config.oracle_primary_host, config.oracle_primary_port, \
                       config.oracle_standby_host, config.oracle_standby_port
        elif config.db_type == 'postgresql':
            return config.pg_primary_host, config.pg_primary_port, \
                   config.pg_standby_host, config.pg_standby_port
        elif config.db_type == 'redis':
            return None, None, None, None  # Redis topology is dynamic
        elif config.db_type == 'dameng':
            dm_subtype = config.dameng_subtype or 'standby'
            if dm_subtype == 'dsc':
                return config.dm_dsc_node1_host, config.dm_dsc_node1_port, \
                       config.dm_dsc_node2_host, config.dm_dsc_node2_port
            return config.dm_primary_host, config.dm_primary_port, \
                   config.dm_standby_host, config.dm_standby_port
        elif config.db_type == 'kingbase':
            kb_subtype = config.kingbase_subtype or 'standby'
            if kb_subtype == 'kdc':
                return config.kb_kdc_node1_host, config.kb_kdc_node1_port, \
                       config.kb_kdc_node2_host, config.kb_kdc_node2_port
            return config.kb_primary_host, config.kb_primary_port, \
                   config.kb_standby_host, config.kb_standby_port
        elif config.db_type == 'opengauss':
            og_subtype = config.opengauss_subtype or 'standby'
            if og_subtype == 'cluster':
                return config.og_cluster_node1_host, config.og_cluster_node1_port, \
                       config.og_cluster_node2_host, config.og_cluster_node2_port
            return config.og_primary_host, config.og_primary_port, \
                   config.og_standby_host, config.og_standby_port
        elif config.db_type == 'oceanbase':
            ob_subtype = config.oceanbase_subtype or 'standby'
            if ob_subtype == 'cluster':
                return config.ob_cluster_node1_host, config.ob_cluster_node1_port, \
                       config.ob_cluster_node2_host, config.ob_cluster_node2_port
            return config.ob_primary_host, config.ob_primary_port, \
                   config.ob_standby_host, config.ob_standby_port
        elif config.db_type == 'tidb':
            td_subtype = config.tidb_subtype or 'standby'
            if td_subtype == 'cluster':
                return config.tidb_cluster_node1_host, config.tidb_cluster_node1_port, \
                       config.tidb_cluster_node2_host, config.tidb_cluster_node2_port
            return config.tidb_primary_host, config.tidb_primary_port, \
                   config.tidb_standby_host, config.tidb_standby_port
        elif config.db_type == 'mongodb':
            mg_subtype = config.mongodb_subtype or 'replica'
            if mg_subtype == 'cluster':
                return config.mongo_cluster_node1_host, config.mongo_cluster_node1_port, \
                       config.mongo_cluster_node2_host, config.mongo_cluster_node2_port
            return config.mongo_primary_host, config.mongo_primary_port, \
                   config.mongo_standby_host, config.mongo_standby_port
        elif config.db_type == 'highgo':
            return config.hg_primary_host, config.hg_primary_port, \
                   config.hg_standby_host, config.hg_standby_port
        elif config.db_type == 'middleware':
            mw_sub = config.mw_subtype or 'active_standby'
            if mw_sub == 'cluster':
                return config.mw_cluster_node1_host, config.mw_cluster_node1_port, \
                       config.mw_cluster_node2_host, config.mw_cluster_node2_port
            return config.mw_primary_host, config.mw_primary_port, \
                   config.mw_secondary_host, config.mw_secondary_port
        return None, None, None, None

    # ── 主执行器 ──

    def executor(tool_name: str, arguments: dict) -> str:
        with app.app_context():
            try:
                if tool_name == 'list_configs':
                    configs = SwitchoverConfig.query.all()
                    items = []
                    for c in configs:
                        # 计算 cluster_type: 复制型 vs 集群型
                        sub = ''
                        if c.db_type == 'mysql':    sub = c.mysql_subtype or 'replica'
                        elif c.db_type == 'oracle': sub = c.oracle_subtype or 'adg'
                        elif c.db_type == 'dameng': sub = c.dameng_subtype or 'standby'
                        elif c.db_type == 'kingbase': sub = c.kingbase_subtype or 'standby'
                        is_shutdown = sub in ('rac','dsc','kdc','mgr_multi')
                        items.append({
                            'id': c.id, 'name': c.name, 'description': c.description,
                            'db_type': c.db_type,
                            'mysql_subtype': c.mysql_subtype if c.db_type == 'mysql' else '',
                            'oracle_subtype': c.oracle_subtype if c.db_type == 'oracle' else '',
                            'dameng_subtype': c.dameng_subtype if c.db_type == 'dameng' else '',
                            'kingbase_subtype': c.kingbase_subtype if c.db_type == 'kingbase' else '',
                            'cluster_type': 'shutdown' if is_shutdown else 'replica',
                            'switch_semantics': '集群型主备切换' if is_shutdown else '复制型主备切换',
                            'master': f'{c.master_host}:{c.master_port}' if c.master_host else '',
                            'slave': f'{c.slave_host}:{c.slave_port}' if c.slave_host else '',
                            'pg_primary': f'{c.pg_primary_host}:{c.pg_primary_port}' if c.pg_primary_host else '',
                            'pg_standby': f'{c.pg_standby_host}:{c.pg_standby_port}' if c.pg_standby_host else '',
                            'redis_master': f'{c.redis_master_host}:{c.redis_master_port}' if c.redis_master_host else '',
                            'redis_slave': f'{c.redis_slave_host}:{c.redis_slave_port}' if c.redis_slave_host else '',
                            'dm_primary': f'{c.dm_primary_host}:{c.dm_primary_port}' if c.dm_primary_host else '',
                            'dm_standby': f'{c.dm_standby_host}:{c.dm_standby_port}' if c.dm_standby_host else '',
                            'dm_dsc_node1': f'{c.dm_dsc_node1_host}:{c.dm_dsc_node1_port}' if c.dm_dsc_node1_host else '',
                            'dm_dsc_node2': f'{c.dm_dsc_node2_host}:{c.dm_dsc_node2_port}' if c.dm_dsc_node2_host else '',
                            'kb_primary': f'{c.kb_primary_host}:{c.kb_primary_port}' if c.kb_primary_host else '',
                            'kb_standby': f'{c.kb_standby_host}:{c.kb_standby_port}' if c.kb_standby_host else '',
                            'kb_kdc_node1': f'{c.kb_kdc_node1_host}:{c.kb_kdc_node1_port}' if c.kb_kdc_node1_host else '',
                            'kb_kdc_node2': f'{c.kb_kdc_node2_host}:{c.kb_kdc_node2_port}' if c.kb_kdc_node2_host else '',
                            'force_switchover': c.force_switchover,
                        })
                    return json.dumps({'configs': items}, ensure_ascii=False)

                elif tool_name == 'health_check':
                    config = _get_config(arguments.get('config_id'))
                    result = _do_health_check(config)
                    return json.dumps(result, ensure_ascii=False, default=str)

                elif tool_name == 'dry_run_switchover':
                    config = _get_config(arguments.get('config_id'))
                    steps, logs, ok, msg = _do_switchover(config, dry_run=True)
                    return json.dumps({
                        'db_type': config.db_type,
                        'status': 'success' if ok else 'failed',
                        'message': msg,
                        'steps_count': len(steps),
                    }, ensure_ascii=False)

                elif tool_name == 'execute_switchover':
                    config_id = arguments.get('config_id')
                    confirmed = arguments.get('confirmed', False)

                    if not confirmed:
                        return json.dumps({
                            'error': '切换操作需要用户确认',
                            'hint': '请用户确认后再执行，此操作将改变数据库拓扑结构'
                        })

                    config = _get_config(config_id)
                    cfg = config.to_dict()
                    m_host, m_port, s_host, s_port = _get_topology_from_config(config)

                    task_id = str(uuid.uuid4())[:8]

                    # 创建历史记录
                    history = SwitchoverHistory(
                        task_id=task_id,
                        db_type=config.db_type,
                        config_id=config.id,
                        config_name=config.name or '',
                        status='running',
                        triggered_by='ai',
                        master_host=m_host, master_port=m_port,
                        slave_host=s_host, slave_port=s_port,
                        start_time=datetime.now(timezone.utc),
                    )
                    db.session.add(history)
                    db.session.commit()

                    steps, logs, ok, msg = _do_switchover(config, dry_run=False)

                    # 更新历史记录
                    h = db.session.get(SwitchoverHistory, history.id)
                    if h:
                        h.status = 'success' if ok else 'failed'
                        h.end_time = datetime.now(timezone.utc)
                        h.duration_seconds = 0
                        h.error_message = '' if ok else msg
                        h.steps = [{'name': s['name'], 'status': s['status'],
                                     'detail': s.get('detail', '')} for s in steps]
                        h.full_log = '\n'.join(logs)
                        db.session.commit()

                    return json.dumps({
                        'task_id': task_id,
                        'db_type': config.db_type,
                        'status': 'success' if ok else 'failed',
                        'message': msg,
                        'steps_count': len(steps),
                        'logs_count': len(logs),
                    }, ensure_ascii=False)

                else:
                    return json.dumps({'error': f'未知工具: {tool_name}'})

            except ValueError as e:
                return json.dumps({'error': str(e)})
            except Exception as e:
                logger.exception(f'工具执行异常 [{tool_name}]')
                return json.dumps({'error': str(e)})

    return executor


# 全局 AI 助手单例
ai_assistant = AIAssistant()
