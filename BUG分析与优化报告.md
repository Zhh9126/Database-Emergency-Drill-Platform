# 应急演练平台 - BUG 分析与优化报告

> 分析日期：2026-10-28  
> 分析范围：全系统（app.py、switchover_engine.py、ai_assistant.py、models.py、report_generator.py 及全部数据库引擎模块）  
> 修复状态：**全部已修复**

---

## 一、系统架构概览

| 模块 | 文件 | 功能 |
|------|------|------|
| Web 应用 | `app.py` (1889行) | Flask 主应用，路由、认证、API、SSE 流 |
| 数据模型 | `models.py` (331行) | 用户、切换配置、切换历史、LLM 配置 |
| MySQL 引擎 | `switchover_engine.py` (844行) | MySQL 主从 GTID 切换 |
| MGR 引擎 | `mysql_mgr_engine.py` (309行) | MySQL Group Replication 切换 |
| Oracle ADG 引擎 | `oracle_switchover_engine.py` | Oracle DataGuard 切换 |
| Oracle RAC 引擎 | `oracle_rac_engine.py` | Oracle RAC 集群关停测试 |
| PG 引擎 | `postgresql_switchover_engine.py` | PostgreSQL 流复制切换 |
| Redis 引擎 | `redis_switchover_engine.py` | Redis 主从切换 |
| 达梦引擎 | `dameng_switchover_engine.py` | 达梦主备/DSC 集群 |
| 金仓引擎 | `kingbase_switchover_engine.py` | 金仓主备/KDC 集群 |
| AI 助手 | `ai_assistant.py` (745行) | 多厂商大模型 API + 工具调用 |
| 报告生成 | `report_generator.py` (358行) | PDF/Word 演练报告 |

---

## 二、BUG 详情与修复

### 🔴 严重级别（P0）— 安全漏洞 / 运行时崩溃

#### BUG-01: SQL 注入漏洞 — MySQL 复制配置
- **文件**: `switchover_engine.py` 第 370-418 行
- **问题**: `_setup_replication()` 将数据库密码、主机地址直接拼入 SQL 字符串（`CHANGE REPLICATION SOURCE TO SOURCE_PASSWORD='{password}'`），若密码包含单引号等特殊字符可导致 SQL 注入
- **修复**: 改用参数化查询 `cur.execute(sql, params)`，所有用户可控值通过 `%s` 占位符传递
- **影响**: 攻击者可通过精心构造的密码字段执行任意 SQL

#### BUG-02: SQL 注入漏洞 — GTID 深度检查
- **文件**: `switchover_engine.py` 第 194-197 行
- **问题**: `_deep_gtid_check()` 将 GTID 字符串直接拼入 `GTID_SUBTRACT` 函数调用
- **修复**: 改用参数化查询

#### BUG-03: SQL 注入漏洞 — GTID_PURGED 设置
- **文件**: `switchover_engine.py` 第 552 行
- **问题**: `SET GLOBAL gtid_purged='{new_gtid}'` 字符串拼接
- **修复**: 改用参数化查询

#### BUG-04: AI 助手密码脱敏后传递给引擎（致命）
- **文件**: `ai_assistant.py` `_do_switchover()` 第 503 行
- **问题**: `config.to_dict()` 将密码字段脱敏为 `'***'`，随后用此 dict 创建引擎实例，导致所有通过 AI 发起的数据库切换操作认证失败
- **修复**: 新增 `_build_engine_config()` 直接从 model 对象提取原始字段值（含真实密码）

#### BUG-05: AI 助手引用不存在的模型属性（AttributeError）
- **文件**: `ai_assistant.py` `_do_health_check()` 和 `_get_topology_from_config()`
- **问题**: 
  - Oracle ADG 健康检查使用 `config.master_host` / `config.slave_host` / `config.oracle_service_name`（均不存在，实际应为 `oracle_primary_host` / `oracle_standby_host` / `oracle_primary_sid`）
  - Oracle RAC 使用 `config.rac_primary_host` / `config.rac_standby_host`（不存在，实际应为 `rac_node1_host` / `rac_node2_host`）
  - `_get_topology_from_config()` 同样引用上述不存在属性
- **修复**: 全部替换为 models.py 中实际定义的属性名

#### BUG-06: AI 助手引用不存在的引擎类（ImportError）
- **文件**: `ai_assistant.py` `_do_switchover()` 第 521、533 行
- **问题**: 
  - 引用 `OracleRACSwitchoverEngine`（实际类名 `OracleRacEngine`）
  - 引用 `PostgreSQLSwitchoverEngine`（实际类名 `PgSwitchoverEngine`）
- **修复**: 更正为正确类名

#### BUG-07: Redis 引擎返回值解包不匹配（ValueError）
- **文件**: `ai_assistant.py` 第 544 行
- **问题**: Redis 引擎 `run()` 返回 `(bool, list)` 即 `(ok, steps)`，但代码按 `(steps, logs, ok)` 三值解包
- **修复**: 正确解包 `(ok, steps)` 并从 steps 中提取日志

#### BUG-08: Oracle ADG 引擎调用签名不匹配
- **文件**: `ai_assistant.py` 第 526 行
- **问题**: `OracleSwitchoverEngine(cfg, force=..., dry_run=...)` — 实际 `__init__` 签名为 `(config, log_callback=None)`，不接受 `force` / `dry_run` 关键字参数。且 `run()` 返回 dict 而非 tuple
- **修复**: 将 `force` / `dry_run` 写入 config dict 后调用引擎，并正确处理 dict 返回值

#### BUG-09: 生产环境暴露 Debug 调试器
- **文件**: `app.py` 第 1889 行
- **问题**: `app.run(debug=True)` 在生产环境暴露 Werkzeug 调试器，攻击者可通过调试器 PIN 执行任意代码
- **修复**: 改为通过环境变量 `FLASK_DEBUG` 控制，默认关闭

#### BUG-10: SECRET_KEY 硬编码
- **文件**: `app.py` 第 93 行
- **问题**: `SECRET_KEY` 默认值为固定字符串 `'drill-platform-secret-key-2024'`，攻击者可伪造 session cookie
- **修复**: 默认使用 `os.urandom(32)` 生成随机密钥

---

### 🟠 中等级别（P1）— 逻辑错误 / 数据不一致

#### BUG-11: GTID 事务计数逻辑错误
- **文件**: `switchover_engine.py` 第 215-233 行
- **问题**: `for/else` 语法导致 `count += 1` 被错误放置在 `for r in ranges.split(':')` 的 `else` 分支中，每当内层循环正常结束就额外计数一次，导致结果偏大
- **修复**: 将单值计数的 `else` 分支移至正确位置（与 `if '-' in r` 同级）

#### BUG-12: 配置 API 缺少权限控制
- **文件**: `app.py` 第 344、461、522 行
- **问题**: 配置的创建/修改/删除路由仅有 `@login_required`，普通用户可修改/删除任意切换配置
- **修复**: 增加 `session.get('role') != 'admin'` 管理员权限检查

#### BUG-13: DB 提交失败被静默吞没
- **文件**: `app.py` `_finish_history()` 第 1098 行
- **问题**: `except Exception: pass` 吞掉所有异常，切换历史记录可能丢失且无任何日志
- **修复**: 添加 rollback + 重试机制 + 错误日志记录

#### BUG-14: 开放重定向漏洞
- **文件**: `app.py` 第 194 行
- **问题**: `next_url.startswith('/')` 检查不足以防止 `//evil.com` 形式的协议相对 URL 重定向
- **修复**: 增加 `not next_url.startswith('//')` 检查

#### BUG-15: AI 对话历史无限增长（内存泄漏）
- **文件**: `ai_assistant.py` 第 186 行
- **问题**: 对话历史列表没有长度限制，长时间运行导致内存泄漏和 LLM API token 超限
- **修复**: 添加 `MAX_HISTORY = 50` 上限，超出时自动截断（保留系统提示词）

#### BUG-16: AI 对话缺少 API 异常处理
- **文件**: `ai_assistant.py` `chat()` 第 274 行
- **问题**: 直接 `response['choices'][0]` 访问，若 API 返回错误格式则抛出 KeyError
- **修复**: 添加格式校验和安全访问

#### BUG-17: 达梦 DSC 状态查询缺少认证信息和端口
- **文件**: `app.py` 第 1443 行
- **问题**: DSC 节点状态查询未传递端口和用户认证信息给测试连接函数
- **修复**: 补充端口、用户名、密码参数

---

### 🟡 低级别（P2）— 健壮性改进

#### BUG-18: MySQL 连接测试端口未转整数
- **文件**: `app.py` 第 683 行
- **问题**: 前端传入的端口可能是字符串，`pymysql.connect()` 需要整数
- **修复**: 显式 `int()` 转换 + 类型异常处理

#### BUG-19: 连接测试资源泄漏
- **文件**: `app.py` `api_mysql_test()` / `api_mysql_repl_status()`
- **问题**: 若查询过程中抛异常，`conn.close()` 不会被执行
- **修复**: 添加 `finally` 块确保连接关闭

#### BUG-20: 修改密码 API 缺少确认密码校验
- **文件**: `app.py` `api_change_password()` 第 664 行
- **问题**: 未校验 `confirm_password` 字段
- **修复**: 增加确认密码匹配检查和空值检查

---

## 三、修复汇总

| 级别 | 数量 | 状态 |
|------|------|------|
| P0 严重 | 10 | ✅ 全部修复 |
| P1 中等 | 7 | ✅ 全部修复 |
| P2 低级 | 3 | ✅ 全部修复 |
| **合计** | **20** | **100%** |

### 修改的文件清单

| 文件 | 修改点 |
|------|--------|
| `app.py` | SECRET_KEY、debug 模式、权限控制、异常处理、端口类型、资源泄漏、开放重定向、密码校验、达梦状态查询参数 |
| `switchover_engine.py` | SQL 注入修复（3处）、GTID 计数逻辑修正 |
| `ai_assistant.py` | 属性名修正（6处）、类名修正（2处）、Redis 返回值解包、Oracle 引擎签名、密码脱敏传递、对话历史限制、API 异常处理 |

---

## 四、后续建议

1. **密码加密存储**: 当前数据库切换配置中的数据库密码以明文存储在 SQLite 中，建议使用对称加密（如 `cryptography.fernet`）加密后存储
2. **审计日志**: 增加操作审计日志，记录谁在什么时候执行了什么切换操作
3. **CSRF 防护**: 添加 Flask-WTF 或自定义 CSRF Token 验证
4. **速率限制**: 对登录 API 和切换执行 API 添加速率限制防止暴力破解和误操作
5. **生产部署**: 使用 Gunicorn/waitress 等 WSGI 服务器替代 Flask 内置服务器
6. **自动化测试**: 为核心切换流程编写单元测试和集成测试
