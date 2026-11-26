# 数据库主备切换应急演练平台

> 一个面向 DBA 与运维团队的开源 Web 平台，用于**集中管理、可视化演练、安全执行**多种主流数据库（MySQL / Oracle ADG / Oracle RAC / PostgreSQL / Redis 主从 / 达梦 / MongoDB）的主备（主从）切换，并支持 AI 大模型驱动的智能运维助手。

---

## 一、平台概览

平台以「**配置管理 → 健康检查 → 切换演练（Dry-Run） → 正式切换 → 实时进度 → 演练报告**」为主线，把原本高风险、高门槛、强依赖个人经验的线下切换操作，变成可审计、可回滚、可复盘的线上标准化流程。

### 核心亮点

- **全栈数据库覆盖**：原生支持 7 类数据库 / 集群形态。
- **主备独立账号**：主库（Primary）与备库（Standby）/ 从节点可使用**完全不同的账号与密码**，满足生产环境最小权限与安全隔离要求。
- **双模式切换**：支持 `Dry-Run（演习）` 与 `正式切换` 两种模式，演习零风险、可随时验证流程正确性。
- **流式实时进度**：切换过程通过 SSE（Server-Sent Events）实时推送每步日志，断点可见。
- **自动回滚**：检测切换失败时自动执行回滚步骤，降低事故影响面。
- **一键演练报告**：自动生成结构化演练报告，支持在线预览与下载。
- **AI 运维助手**：内置大模型（LLM）驱动的智能助手，可对话式执行健康检查、切换演习、正式切换与故障诊断。

---

## 二、功能详解

### 1. 配置管理

集中维护所有切换目标的环境信息，按数据库类型分 Tab 录入：

| 数据库类型 | 录入字段 | 说明 |
|---|---|---|
| **MySQL** | 模式（普通主从 / MGR 单主 / MGR 多主）+ 主库账号密码、主从主机端口、从库独立账号密码 | 支持传统主从与 MySQL Group Replication |
| **Oracle ADG** | 主备独立账号密码 + 主备主机/端口/SID、备库独立账号密码 | DataGuard 物理备库切换 |
| **Oracle RAC** | 节点 1/2 独立账号密码 + 节点主机/SID、RAC 服务名 | 基于 `srvctl` 的服务漂移（relocate）切换 |
| **PostgreSQL** | 主备独立账号密码 + 主备主机/端口、备库独立账号密码 | 流复制主备切换 |
| **Redis 主从** | 主节点 / 从节点 独立账号密码 + 主机端口 | **直接主从切换（非 Sentinel）**，通过 `REPLICAOF` 完成角色互换 |
| **达梦 DM** | 主备模式 / DSC 集群模式，主备独立账号密码 | 主备切换或服务关停高可用验证 |
| **MongoDB** | 复制集地址 + 账号密码 | 复制集主节点切换 |

> ⚠️ **安全设计**：所有密码在 API 返回与前端展示中均以 `***` 脱敏；页面可单独「测试连接」逐一验证主/备/节点连通性与账号权限。

### 2. 健康检查

进入切换前，一键对目标环境执行健康检查，覆盖：

- 主备复制状态、复制延迟（可配置 `max_delay` 阈值）
- 节点可达性、账号权限
- 集群角色（谁是当前主 / Primary / Master）

健康检查不修改任何数据，是切换前的「安全闸门」。

### 3. 切换执行（Dry-Run / 正式）

对每条配置可发起两类操作：

- **切换演习（Dry-Run）**：完整跑通切换逻辑与步骤序列，**不实际变更数据库角色**，用于流程验证与人员培训。
- **正式切换（Switchover）**：在人工确认后真实执行主备角色互换。

执行过程中支持：

- **实时流式日志**：通过 `/api/<db>/switchover/<id>/stream` 端点以 SSE 持续推送每步进展。
- **自动回滚**：当某关键步骤失败且开启 `auto_rollback` 时，平台自动执行回滚动作，恢复到切换前状态。
- **延迟跳过开关**：可在演习或特殊情况下跳过复制延迟校验（`skip_delay_check`）。

### 4. 演练报告

切换（含演习）完成后自动生成演练报告，包含：

- 环境信息摘要
- 执行步骤与每步耗时
- 成功 / 失败标记与异常堆栈（如有）
- 结果结论

报告支持在线预览与一键下载，便于归档与审计。

### 5. AI 运维助手

平台内置 **AI 大模型驱动**的智能运维助手，支持两种形态：

- **全屏页面**（`/ai-assistant`）：侧边栏菜单进入，聊天区域平铺全页。
- **右侧悬浮窗**：右下角悬浮按钮，任意页面一键呼出（快捷键 `Ctrl+K`），不影响当前操作。

AI 助手能力：

- 执行**健康检查**
- 发起**切换演习（Dry-Run）**
- 执行**正式主从切换**
- **诊断数据库问题**

接入方式：在「AI 模型配置」页录入多家厂商（OpenAI / DeepSeek / 通义千问 / Ollama / 自定义 / 九天等）的 `API Base`、`API Key`、`Model`，平台通过标准 OpenAI 兼容协议调用，并支持自定义请求头（如 `moma` 类网关鉴权）。可设置「默认启用」的模型，助手自动选用。

---

## 三、支持的数据库与切换形态一览

| 类型 | 切换方式 | 主备独立账号 | 引擎文件 |
|---|---|---|---|
| MySQL 主从 / MGR | `REPLICAOF` / 复制拓扑切换 | ✅ | `switchover_engine.py` |
| Oracle ADG | DataGuard 角色切换 | ✅ | `oracle_switchover_engine.py` |
| Oracle RAC | `srvctl relocate service` | ✅（节点级） | `oracle_rac_engine.py` |
| PostgreSQL | 流复制主备切换 | ✅ | `postgresql_switchover_engine.py` |
| Redis 主从 | `REPLICAOF NO ONE` + `REPLICAOF` | ✅ | `redis_switchover_engine.py` |
| 达梦 DM | 主备切换 / DSC 服务关停 | ✅ | `dameng_engine.py` |
| MongoDB | 复制集主节点切换 | ✅ | `mongodb_switchover_engine.py` |

---

## 四、系统架构

```
浏览器 (Jinja2 模板 + 原生 JS)
        │  HTTP / SSE
        ▼
Flask 应用 (app.py)
  ├─ 认证与权限 (User 模型, login_required / admin_required)
  ├─ 配置 CRUD API
  ├─ 健康检查 / 切换执行 (SSE 流式)
  ├─ AI 助手 API (LLM 调用 + 工具执行)
  └─ 报告生成 (report_generator.py)
        │
        ▼
切换引擎层 (engine/*.py)  ── SSH / 驱动 ──►  目标数据库集群
        │
        ▼
存储层 (SQLAlchemy + SQLite, instance/*.db)
```

### 技术栈

- **后端**：Python 3 + Flask + SQLAlchemy
- **前端**：Jinja2 模板 + 原生 JavaScript + Bootstrap Icons（无重型前端框架，部署极简）
- **存储**：SQLite（零外部依赖，开箱即用）
- **数据库驱动**：`pymysql`、`oracledb`、`psycopg2`、`redis`、`pymongo`、`dmPython` 等按需安装
- **AI**：标准 OpenAI Chat Completions 兼容协议，可选工具调用（function calling）

---

## 五、快速开始

### 1. 环境准备

```bash
cd 应急演练平台
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. 启动

```bash
python app.py
```

默认访问 `http://127.0.0.1:5000`。

### 3. 初始账号

首次启动自动创建管理员账户：

- 用户名：`admin`
- 密码：`admin123`

> 登录后请前往「修改密码」修改默认口令，并通过「用户管理」（管理员可见）创建运维角色账号。

### 4. 配置第一个切换目标

1. 进入「配置管理」→ 选择数据库类型 Tab
2. 填写主库 / 备库（或从节点）的**独立账号密码**与连接信息
3. 点击「测试连接」逐一验证
4. 保存配置

### 5. 执行演练

1. 点击「健康检查」确认环境就绪
2. 点击「切换演习（Dry-Run）」零风险验证流程
3. 确认无误后，点击「正式切换」并在确认框中输入 `CONFIRM`

---

## 六、权限模型

| 角色 | 权限范围 |
|---|---|
| `admin`（管理员） | 全部功能：配置增删改、用户管理、切换执行、AI 模型配置 |
| `user`（普通用户） | 查看配置、执行健康检查与切换（依页面授权） |

所有页面与 API 均受 `login_required` 保护；用户管理、配置写入等敏感操作额外受 `admin_required` 保护。

---

## 七、AI 模型配置说明

在「AI 模型配置」页录入：

- **厂商 / Provider**：OpenAI、DeepSeek、通义千问、Ollama、自定义、九天等
- **API Base**：兼容 OpenAI 的 `.../chat/completions` 端点（也兼容已包含 `/chat/completions` 的完整地址）
- **API Key**：模型服务鉴权密钥
- **Model**：模型名（如 `gpt-4o-mini`、`deepseek-chat`、`qwen-plus`）
- **自定义请求头**：JSON 格式，用于网关 / 私有化部署的特殊鉴权
- **温度 / Max Tokens**：模型推理参数（温度限制 0–2）
- **默认启用**：标记助手默认选用的模型

> 提示：仅当未配置任何模型时，AI 助手页面会显示「请先配置大模型」的引导。

---

## 八、目录结构

```
应急演练平台/
├── app.py                      # Flask 主应用：路由、API、切换执行、AI 助手
├── models.py                   # 数据模型（User / SwitchoverConfig / LLMConfig / DrillReport 等）
├── ai_assistant.py             # AI 助手核心：LLM 调用、工具执行器、会话管理
├── switchover_engine.py        # MySQL 切换引擎
├── oracle_switchover_engine.py # Oracle ADG 切换引擎
├── oracle_rac_engine.py        # Oracle RAC 切换引擎
├── postgresql_switchover_engine.py # PostgreSQL 切换引擎
├── redis_switchover_engine.py  # Redis 主从切换引擎（非 Sentinel）
├── dameng_engine.py            # 达梦切换引擎
├── mongodb_switchover_engine.py # MongoDB 复制集切换引擎
├── report_generator.py         # 演练报告生成
├── templates/                  # 页面模板（index / configure / users / profile / ai_settings / ai_assistant / ai_panel 等）
├── static/css/style.css        # 全局样式
├── instance/                   # SQLite 数据库文件
└── requirements.txt            # 依赖清单
```

---

## 九、典型使用场景

- **定期容灾演练**：每月对核心数据库执行 Dry-Run，验证切换流程与团队响应。
- **真实故障切换**：主库异常时，经审批后一键执行正式切换并全程留痕。
- **新人培训**：通过演习模式与安全脱敏的界面，让新人熟悉切换步骤。
- **AI 辅助运维**：用自然语言让 AI 助手执行健康检查、发起演习、解读复制延迟与报错。

---

## 十、安全与合规

- 密码全程脱敏，不落明文日志。
- 所有写操作与切换执行需登录授权，敏感操作限管理员。
- 正式切换设二次确认（输入 `CONFIRM`），防误触。
- 切换过程自动回滚与延迟校验，降低业务影响。

---

> 本项目面向**应急演练与容灾能力建设**，建议在测试环境充分验证后再用于生产切换。
