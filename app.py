# -*- coding: utf-8 -*-
"""应急演练平台 - Flask Web 应用"""
import io
import json
import os
import re
import logging
import threading
import time
from functools import wraps
from datetime import datetime, timezone
from flask import (Flask, render_template, request, jsonify,
                   Response, stream_with_context, session, redirect, url_for,
                   send_file)
from sqlalchemy import text

from models import db, SwitchoverConfig, SwitchoverHistory, LLMConfig, gen_task_id, User

# 报告生成器
from report_generator import make_report, report_filename

# MySQL 引擎
from switchover_engine import SwitchoverEngine
from mysql_mgr_engine import MgrSwitchoverEngine
import pymysql

# Oracle 模块（可选）
try:
    from oracle_switchover_engine import (
        OracleSwitchoverEngine, test_oracle_connection, get_oracle_archived_logs
    )
    ORACLE_AVAILABLE = True
except ImportError:
    ORACLE_AVAILABLE = False

# PostgreSQL 模块（可选）
try:
    from postgresql_switchover_engine import (
        PgSwitchoverEngine, test_pg_connection, PG_AVAILABLE
    )
    PG_ENABLED = True
except ImportError:
    PG_ENABLED = False

# Redis 模块（可选）
try:
    from redis_switchover_engine import (
        RedisSwitchoverEngine, test_redis_status, REDIS_AVAILABLE
    )
    REDIS_ENABLED = True
except ImportError:
    REDIS_ENABLED = False

# Oracle RAC 模块（可选）
try:
    from oracle_rac_engine import OracleRacEngine, test_rac_connection
    RAC_AVAILABLE = True
except ImportError:
    RAC_AVAILABLE = False

# 达梦数据库模块（可选）
try:
    from dameng_switchover_engine import (
        DamengStandbyEngine, DamengDscEngine,
        test_dameng_connection, test_dameng_dsc_connection,
    )
    DM_AVAILABLE = True
except ImportError:
    DM_AVAILABLE = False

# 金仓数据库模块（可选）
try:
    from kingbase_switchover_engine import (
        KingbaseStandbyEngine, KingbaseKdcEngine,
        test_kingbase_connection, test_kingbase_kdc_connection,
    )
    KB_AVAILABLE = True
except ImportError:
    KB_AVAILABLE = False

# openGauss / GaussDB 模块（可选）
try:
    from opengauss_switchover_engine import OpenGaussSwitchoverEngine, test_opengauss_connection
    OG_AVAILABLE = True
except ImportError:
    OG_AVAILABLE = False

# OceanBase 模块（可选）
try:
    from oceanbase_switchover_engine import OceanBaseSwitchoverEngine, test_oceanbase_connection
    OB_AVAILABLE = True
except ImportError:
    OB_AVAILABLE = False

# TiDB 模块（可选）
try:
    from tidb_switchover_engine import TiDBSwitchoverEngine, test_tidb_connection
    TIDB_AVAILABLE = True
except ImportError:
    TIDB_AVAILABLE = False

# MongoDB 模块（可选）
try:
    from mongodb_switchover_engine import MongoSwitchoverEngine, test_mongo_connection
    MONGO_AVAILABLE = True
except ImportError:
    MONGO_AVAILABLE = False

# 瀚高(HighGo)模块（可选）
try:
    from highgo_switchover_engine import HighGoSwitchoverEngine, test_highgo_connection
    HG_AVAILABLE = True
except ImportError:
    HG_AVAILABLE = False

# 中间件模块
try:
    from middleware_switchover_engine import (
        MiddlewareSwitchoverEngine, test_middleware_connection,
        MIDDLEWARE_TYPES,
    )
    MW_AVAILABLE = True
except ImportError as _mw_err:
    MW_AVAILABLE = False
    MiddlewareSwitchoverEngine = None
    test_middleware_connection = None
    MIDDLEWARE_TYPES = {}
    print(f'[WARN] 中间件模块加载失败: {_mw_err}')

# AI 模块（可选）
try:
    from ai_assistant import AIAssistant, ai_assistant, create_tool_executor
    AI_AVAILABLE = True
except ImportError as _ai_import_err:
    AI_AVAILABLE = False
    AIAssistant = None
    ai_assistant = None
    create_tool_executor = None
    print(f'[WARN] AI 模块加载失败: {_ai_import_err}')

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///drill.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY') or os.urandom(32)
db.init_app(app)
logger = logging.getLogger(__name__)

# 为全局 AI 助手实例注入工具执行器
if AI_AVAILABLE and ai_assistant and create_tool_executor:
    ai_assistant._tool_executor = create_tool_executor(app)

# 全局 MySQL 引擎实例
mysql_engine = SwitchoverEngine()


# ========== 认证 ==========

def login_required(f):
    """登录检查装饰器"""
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login_page', next=request.url))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    """管理员权限装饰器"""
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login_page', next=request.url))
        if session.get('role') != 'admin':
            return jsonify({'ok': False, 'error': '需要管理员权限'}), 403
        return f(*args, **kwargs)
    return decorated


# ========== 初始化 ==========

def init_db():
    with app.app_context():
        db.create_all()
        # 兼容旧库：为 switchover_config 增加 mysql_subtype 字段
        with db.engine.connect() as conn:
            try:
                conn.execute(text("SELECT mysql_subtype FROM switchover_config LIMIT 1"))
            except Exception:
                conn.execute(text("ALTER TABLE switchover_config ADD COLUMN mysql_subtype VARCHAR(20) DEFAULT 'replica'"))
                conn.commit()
        # 兼容旧库：为 switchover_config 增加 MySQL 复制账号字段（可配置化）
        with db.engine.connect() as conn:
            try:
                conn.execute(text("SELECT mysql_repl_user FROM switchover_config LIMIT 1"))
            except Exception:
                conn.execute(text("ALTER TABLE switchover_config ADD COLUMN mysql_repl_user VARCHAR(120) DEFAULT 'repl'"))
                conn.commit()
        with db.engine.connect() as conn:
            try:
                conn.execute(text("SELECT mysql_repl_pass FROM switchover_config LIMIT 1"))
            except Exception:
                conn.execute(text("ALTER TABLE switchover_config ADD COLUMN mysql_repl_pass VARCHAR(255) DEFAULT ''"))
                conn.commit()
        # 兼容旧库：为 switchover_history 增加 target_node 字段
        with db.engine.connect() as conn:
            try:
                conn.execute(text("SELECT target_node FROM switchover_history LIMIT 1"))
            except Exception:
                conn.execute(text("ALTER TABLE switchover_history ADD COLUMN target_node VARCHAR(10) DEFAULT ''"))
                conn.commit()
        # 创建默认管理员账号
        if not User.query.filter_by(username='admin').first():
            admin = User(username='admin', role='admin')
            admin.set_password('admin123')
            db.session.add(admin)
            db.session.commit()
            print('[INIT] 默认管理员账号已创建: admin / admin123')


# 上下文注入
@app.context_processor
def inject_globals():
    try:
        active_ai = LLMConfig.query.filter_by(is_active=True).first()
    except Exception:
        active_ai = None
    current_user = None
    if 'user_id' in session:
        try:
            current_user = db.session.get(User, session['user_id'])
        except Exception:
            pass
    return {
        'active_ai': active_ai,
        'current_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'oracle_available': ORACLE_AVAILABLE,
        'dm_available': DM_AVAILABLE,
        'kb_available': KB_AVAILABLE,
        'og_available': OG_AVAILABLE,
        'ob_available': OB_AVAILABLE,
        'tidb_available': TIDB_AVAILABLE,
        'mongo_available': MONGO_AVAILABLE,
        'hg_available': HG_AVAILABLE,
        'current_user': current_user,
    }


# ========== 页面路由 ==========

@app.route('/login', methods=['GET', 'POST'])
def login_page():
    error = None
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        user = User.query.filter_by(username=username, is_active=True).first()
        if user and user.check_password(password):
            session['user_id'] = user.id
            session['username'] = user.username
            session['role'] = user.role
            next_url = request.args.get('next')
            # 安全修复：防止开放重定向（拒绝以 // 开头的协议相对URL）
            if next_url and next_url.startswith('/') and not next_url.startswith('//') and 'login' not in next_url:
                return redirect(next_url)
            return redirect(url_for('index'))
        error = '用户名或密码错误'
    return render_template('login.html', error=error)


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login_page'))


@app.route('/profile', methods=['GET', 'POST'])
@login_required
def profile_page():
    user = db.session.get(User, session['user_id'])
    if not user:
        session.clear()
        return redirect(url_for('login_page'))
    error = None
    success = None
    if request.method == 'POST':
        old_password = request.form.get('old_password', '')
        new_password = request.form.get('new_password', '')
        confirm = request.form.get('confirm_password', '')
        if not user.check_password(old_password):
            error = '原密码错误'
        elif len(new_password) < 6:
            error = '新密码长度不能少于6位'
        elif new_password != confirm:
            error = '两次输入的新密码不一致'
        else:
            user.set_password(new_password)
            db.session.commit()
            success = '密码修改成功'
    return render_template('profile.html', user=user, error=error, success=success)


@app.route('/admin/users')
@login_required
def user_management():
    if session.get('role') != 'admin':
        return redirect(url_for('index'))
    users = User.query.order_by(User.created_at.desc()).all()
    return render_template('users.html', users=users, current_user_id=session['user_id'])


@app.route('/')
@login_required
def index():
    try:
        total = SwitchoverHistory.query.count()
        success = SwitchoverHistory.query.filter_by(status='success').count()
        failed = SwitchoverHistory.query.filter_by(status='failed').count()
        mysql_total = SwitchoverHistory.query.filter_by(db_type='mysql').count()
        oracle_total = SwitchoverHistory.query.filter(
            SwitchoverHistory.db_type.in_(['oracle', 'oracle_rac'])).count()
        pg_total = SwitchoverHistory.query.filter_by(db_type='postgresql').count()
        redis_total = SwitchoverHistory.query.filter_by(db_type='redis').count()
        dm_total = SwitchoverHistory.query.filter_by(db_type='dameng').count()
        kb_total = SwitchoverHistory.query.filter_by(db_type='kingbase').count()
        og_total = SwitchoverHistory.query.filter_by(db_type='opengauss').count()
        ob_total = SwitchoverHistory.query.filter_by(db_type='oceanbase').count()
        tidb_total = SwitchoverHistory.query.filter_by(db_type='tidb').count()
        mongo_total = SwitchoverHistory.query.filter_by(db_type='mongodb').count()
        hg_total = SwitchoverHistory.query.filter_by(db_type='highgo').count()
        # 中间件统计（db_type 以 mw_ 开头）
        mw_counts = {}
        for mw_t in ('nginx', 'tomcat', 'kafka', 'zookeeper', 'elasticsearch', 'rabbitmq'):
            mw_counts[mw_t] = SwitchoverHistory.query.filter(
                SwitchoverHistory.db_type == f'mw_{mw_t}').count()
        recent = SwitchoverHistory.query.order_by(
            SwitchoverHistory.start_time.desc()).limit(10).all()
        configs = SwitchoverConfig.query.order_by(
            SwitchoverConfig.updated_at.desc()).all()
    except Exception:
        total, success, failed = 0, 0, 0
        mysql_total, oracle_total, pg_total, redis_total = 0, 0, 0, 0
        dm_total, kb_total = 0, 0
        og_total, ob_total, tidb_total, mongo_total, hg_total = 0, 0, 0, 0, 0
        mw_counts = {}
        recent, configs = [], []
    return render_template('index.html',
                           total=total, success=success, failed=failed,
                           mysql_total=mysql_total, oracle_total=oracle_total,
                           pg_total=pg_total, redis_total=redis_total,
                           dm_total=dm_total, kb_total=kb_total,
                           og_total=og_total, ob_total=ob_total,
                           tidb_total=tidb_total, mongo_total=mongo_total, hg_total=hg_total,
                           mw_counts=mw_counts,
                           recent=recent, configs=configs)


@app.route('/configure')
@login_required
def configure():
    configs = SwitchoverConfig.query.order_by(
        SwitchoverConfig.updated_at.desc()).all()
    return render_template('configure.html', configs=configs)


@app.route('/switchover')
@login_required
def switchover_page():
    config_id = request.args.get('config_id')
    config = None
    if config_id:
        config = db.session.get(SwitchoverConfig, int(config_id))
    configs = [c.to_dict() for c in SwitchoverConfig.query.order_by(SwitchoverConfig.name).all()]
    config_dict = config.to_dict() if config else None
    return render_template('switchover.html', config=config_dict, configs=configs)


@app.route('/history')
@login_required
def history():
    page = request.args.get('page', 1, type=int)
    status = request.args.get('status', '')
    db_type = request.args.get('db_type', '')
    per_page = 15
    query = SwitchoverHistory.query.order_by(SwitchoverHistory.start_time.desc())
    if status:
        query = query.filter_by(status=status)
    if db_type:
        query = query.filter_by(db_type=db_type)
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    return render_template('history.html', tasks=pagination.items,
                           pagination=pagination, current_status=status,
                           current_db_type=db_type)


@app.route('/logs/<task_id>')
@login_required
def logs(task_id):
    task = SwitchoverHistory.query.filter_by(task_id=task_id).first_or_404()
    return render_template('logs.html', task=task)


@app.route('/ai-settings')
@login_required
def ai_settings():
    configs = LLMConfig.query.order_by(LLMConfig.created_at.desc()).all()
    providers = AIAssistant.PROVIDERS if AIAssistant else {}
    return render_template('ai_settings.html', llm_configs=configs, providers=providers)


@app.route('/ai-assistant')
@login_required
def ai_assistant_page():
    return render_template('ai_assistant.html')


# ========== 通用配置 API ==========

@app.route('/api/configs', methods=['GET'])
@login_required
def api_list_configs():
    db_type = request.args.get('db_type', '')
    query = SwitchoverConfig.query
    if db_type:
        query = query.filter_by(db_type=db_type)
    configs = query.order_by(SwitchoverConfig.updated_at.desc()).all()
    return jsonify([c.to_dict() for c in configs])


@app.route('/api/configs/<int:cfg_id>/topology')
@login_required
def api_config_topology(cfg_id):
    """返回指定配置的结构化拓扑数据（节点 + 连接 + 参数明细），供前端拓扑图与下钻使用"""
    cfg = db.session.get(SwitchoverConfig, cfg_id)
    if not cfg:
        return jsonify({'ok': False, 'error': '配置不存在'}), 404
    d = cfg.to_dict()
    db_type = d.get('db_type', '')
    nodes, links = [], []
    # 通用节点信息提取
    def node(rid, role, label, host, port, extra=None):
        n = {'id': rid, 'role': role, 'label': label, 'host': host or '-', 'port': port or 0,
             'params': extra or {}}
        nodes.append(n)
        return rid
    def link(src, dst, kind='replication', label=''):
        links.append({'src': src, 'dst': dst, 'kind': kind, 'label': label})

    # ── 按类型构建拓扑 ──
    if db_type == 'mysql':
        sub = d.get('mysql_subtype') or 'replica'
        if sub in ('mgr_single', 'mgr_multi'):
            node('n1', 'primary', 'MGR 节点1', d.get('master_host'), d.get('master_port'),
                 {'用户': d.get('mysql_user', '-'), '认证': d.get('mysql_slave_user', '-'), '模式': 'MGR ' + sub})
            node('n2', 'primary', 'MGR 节点2', d.get('slave_host'), d.get('slave_port'),
                 {'用户': d.get('mysql_slave_user', '-'), '复制用户': d.get('mysql_slave_user', '-')})
            link('n1', 'n2', 'cluster', 'MGR 组复制')
        else:
            node('n1', 'primary', 'MySQL 主库', d.get('master_host'), d.get('master_port'),
                 {'用户': d.get('mysql_user', '-'), 'GTID': 'ON', '复制账号': d.get('mysql_slave_user', '-')})
            node('n2', 'secondary', 'MySQL 从库', d.get('slave_host'), d.get('slave_port'),
                 {'用户': d.get('mysql_slave_user', '-'), '复制源': d.get('master_host', '-')})
            link('n1', 'n2', 'replication', 'GTID 复制')
    elif db_type == 'postgresql':
        node('n1', 'primary', 'PG 主库', d.get('pg_primary_host'), d.get('pg_primary_port'),
             {'用户': d.get('pg_user', '-'), '数据库': 'postgres', 'WAL 级别': 'replica'})
        node('n2', 'secondary', 'PG 备库', d.get('pg_standby_host'), d.get('pg_standby_port'),
             {'用户': d.get('pg_standby_user', '-'), '恢复模式': 'standby'})
        link('n1', 'n2', 'replication', '流复制')
    elif db_type == 'oracle':
        sub = d.get('oracle_subtype') or 'adg'
        if sub == 'rac':
            node('n1', 'primary', 'RAC 节点1', d.get('rac_node1_host'), 1521,
                 {'SID': d.get('rac_node1_sid', '-'), '服务名': d.get('rac_service_name', '-'), '用户': d.get('rac_user', '-')})
            node('n2', 'primary', 'RAC 节点2', d.get('rac_node2_host'), 1521,
                 {'SID': d.get('rac_node2_sid', '-'), '用户': d.get('rac_node2_user', '-')})
            link('n1', 'n2', 'cluster', 'RAC 集群')
        else:
            node('n1', 'primary', 'Oracle 主库', d.get('oracle_primary_host'), d.get('oracle_primary_port'),
                 {'SID': d.get('oracle_primary_sid', '-'), '用户': d.get('oracle_user', '-'), '模式': d.get('oracle_connect_mode', '-')})
            node('n2', 'secondary', 'Oracle 备库', d.get('oracle_standby_host'), d.get('oracle_standby_port'),
                 {'SID': d.get('oracle_standby_sid', '-'), '用户': d.get('oracle_standby_user', '-')})
            link('n1', 'n2', 'replication', 'ADG 同步')
    elif db_type == 'redis':
        node('n1', 'primary', 'Redis 主节点', d.get('redis_master_host'), d.get('redis_master_port'),
             {'用户': d.get('redis_master_user', '-')})
        node('n2', 'secondary', 'Redis 从节点', d.get('redis_slave_host'), d.get('redis_slave_port'),
             {'用户': d.get('redis_slave_user', '-')})
        link('n1', 'n2', 'replication', '主从复制')
    elif db_type == 'dameng':
        sub = d.get('dameng_subtype') or 'standby'
        if sub == 'dsc':
            node('n1', 'primary', 'DSC 节点1', d.get('dm_dsc_node1_host'), d.get('dm_dsc_node1_port'),
                 {'服务名': d.get('dm_dsc_service_name', '-'), '用户': d.get('dm_dsc_user', '-')})
            node('n2', 'primary', 'DSC 节点2', d.get('dm_dsc_node2_host'), d.get('dm_dsc_node2_port'),
                 {'用户': d.get('dm_dsc_node2_user', '-')})
            link('n1', 'n2', 'cluster', 'DSC 共享集群')
        else:
            node('n1', 'primary', '达梦主库', d.get('dm_primary_host'), d.get('dm_primary_port'),
                 {'SID': d.get('dm_primary_sid', '-'), '用户': d.get('dm_user', '-')})
            node('n2', 'secondary', '达梦备库', d.get('dm_standby_host'), d.get('dm_standby_port'),
                 {'SID': d.get('dm_standby_sid', '-'), '用户': d.get('dm_standby_user', '-')})
            link('n1', 'n2', 'replication', 'DA MAL 同步')
    elif db_type == 'kingbase':
        sub = d.get('kingbase_subtype') or 'standby'
        if sub == 'kdc':
            node('n1', 'primary', 'KDC 节点1', d.get('kb_kdc_node1_host'), d.get('kb_kdc_node1_port'),
                 {'服务名': d.get('kb_kdc_service_name', '-'), '用户': d.get('kb_kdc_user', '-')})
            node('n2', 'primary', 'KDC 节点2', d.get('kb_kdc_node2_host'), d.get('kb_kdc_node2_port'),
                 {'用户': d.get('kb_kdc_node2_user', '-')})
            link('n1', 'n2', 'cluster', 'KDC 共享集群')
        else:
            node('n1', 'primary', '金仓主库', d.get('kb_primary_host'), d.get('kb_primary_port'),
                 {'用户': d.get('kb_user', '-')})
            node('n2', 'secondary', '金仓备库', d.get('kb_standby_host'), d.get('kb_standby_port'),
                 {'用户': d.get('kb_standby_user', '-')})
            link('n1', 'n2', 'replication', '流复制')
    elif db_type == 'opengauss':
        sub = d.get('opengauss_subtype') or 'standby'
        if sub == 'cluster':
            node('n1', 'primary', 'openGauss 节点1', d.get('og_cluster_node1_host'), d.get('og_cluster_node1_port'),
                 {'库名': d.get('og_db_name', '-'), '用户': d.get('og_user', '-')})
            node('n2', 'primary', 'openGauss 节点2', d.get('og_cluster_node2_host'), d.get('og_cluster_node2_port'),
                 {'用户': d.get('og_user', '-')})
            link('n1', 'n2', 'cluster', '集群互备')
        else:
            node('n1', 'primary', 'openGauss 主库', d.get('og_primary_host'), d.get('og_primary_port'),
                 {'库名': d.get('og_db_name', '-'), '用户': d.get('og_user', '-')})
            node('n2', 'secondary', 'openGauss 备库', d.get('og_standby_host'), d.get('og_standby_port'),
                 {'用户': d.get('og_standby_user', '-')})
            link('n1', 'n2', 'replication', '流复制')
    elif db_type == 'oceanbase':
        sub = d.get('oceanbase_subtype') or 'standby'
        if sub == 'cluster':
            node('n1', 'primary', 'OB 节点1', d.get('ob_cluster_node1_host'), d.get('ob_cluster_node1_port'),
                 {'租户': d.get('ob_tenant_name', '-'), '用户': d.get('ob_user', '-')})
            node('n2', 'primary', 'OB 节点2', d.get('ob_cluster_node2_host'), d.get('ob_cluster_node2_port'),
                 {'用户': d.get('ob_user', '-')})
            link('n1', 'n2', 'cluster', 'OB 集群')
        else:
            node('n1', 'primary', 'OB 主库', d.get('ob_primary_host'), d.get('ob_primary_port'),
                 {'租户': d.get('ob_tenant_name', '-'), '用户': d.get('ob_user', '-')})
            node('n2', 'secondary', 'OB 备库', d.get('ob_standby_host'), d.get('ob_standby_port'),
                 {'用户': d.get('ob_standby_user', '-')})
            link('n1', 'n2', 'replication', '日志同步')
    elif db_type == 'tidb':
        sub = d.get('tidb_subtype') or 'standby'
        if sub == 'cluster':
            node('n1', 'primary', 'TiDB 节点1', d.get('tidb_cluster_node1_host'), d.get('tidb_cluster_node1_port'),
                 {'库名': d.get('tidb_db_name', '-'), 'PD 端口': d.get('tidb_pd_port', '-'), '用户': d.get('tidb_user', '-')})
            node('n2', 'primary', 'TiDB 节点2', d.get('tidb_cluster_node2_host'), d.get('tidb_cluster_node2_port'),
                 {'用户': d.get('tidb_user', '-')})
            link('n1', 'n2', 'cluster', 'PD 调度')
        else:
            node('n1', 'primary', 'TiDB 主库', d.get('tidb_primary_host'), d.get('tidb_primary_port'),
                 {'库名': d.get('tidb_db_name', '-'), '用户': d.get('tidb_user', '-')})
            node('n2', 'secondary', 'TiDB 备库', d.get('tidb_standby_host'), d.get('tidb_standby_port'),
                 {'用户': d.get('tidb_standby_user', '-')})
            link('n1', 'n2', 'replication', 'Binlog 复制')
    elif db_type == 'mongodb':
        sub = d.get('mongodb_subtype') or 'replica'
        if sub == 'cluster':
            node('n1', 'primary', 'Mongo 节点1', d.get('mongo_cluster_node1_host'), d.get('mongo_cluster_node1_port'),
                 {'副本集': d.get('mongo_replset_name', '-'), '认证库': d.get('mongo_auth_db', '-'), '用户': d.get('mongo_user', '-')})
            node('n2', 'primary', 'Mongo 节点2', d.get('mongo_cluster_node2_host'), d.get('mongo_cluster_node2_port'),
                 {'用户': d.get('mongo_user', '-')})
            link('n1', 'n2', 'cluster', '分片集群')
        else:
            node('n1', 'primary', 'Mongo 主节点', d.get('mongo_primary_host'), d.get('mongo_primary_port'),
                 {'副本集': d.get('mongo_replset_name', '-'), '认证库': d.get('mongo_auth_db', '-'), '用户': d.get('mongo_user', '-')})
            node('n2', 'secondary', 'Mongo 从节点', d.get('mongo_standby_host'), d.get('mongo_standby_port'),
                 {'用户': d.get('mongo_user', '-')})
            link('n1', 'n2', 'replication', '副本集同步')
    elif db_type == 'highgo':
        node('n1', 'primary', '瀚高主库', d.get('hg_primary_host'), d.get('hg_primary_port'),
             {'库名': d.get('hg_db_name', '-'), '用户': d.get('hg_user', '-')})
        node('n2', 'secondary', '瀚高备库', d.get('hg_standby_host'), d.get('hg_standby_port'),
             {'用户': d.get('hg_standby_user', '-')})
        link('n1', 'n2', 'replication', '流复制')
    elif db_type == 'middleware':
        mw = d.get('middleware_type') or 'nginx'
        mw_label = mw.upper()
        sub = d.get('mw_subtype') or 'active_standby'
        if sub == 'cluster':
            node('n1', 'primary', f'{mw_label} 节点1', d.get('mw_cluster_node1_host'), d.get('mw_cluster_node1_port'),
                 {'API 端口': d.get('mw_api_port', '-'), '集群名': d.get('mw_cluster_name', '-'), '用户': d.get('mw_user', '-')})
            if d.get('mw_cluster_node2_host'):
                node('n2', 'primary', f'{mw_label} 节点2', d.get('mw_cluster_node2_host'), d.get('mw_cluster_node2_port'),
                     {'用户': d.get('mw_user', '-')})
                link('n1', 'n2', 'cluster', '集群互联')
            if d.get('mw_cluster_node3_host'):
                node('n3', 'primary', f'{mw_label} 节点3', d.get('mw_cluster_node3_host'), d.get('mw_cluster_node3_port'),
                     {'用户': d.get('mw_user', '-')})
                link('n1', 'n3', 'cluster', '集群互联')
                link('n2', 'n3', 'cluster', '集群互联')
        else:
            node('n1', 'primary', f'{mw_label} 主节点', d.get('mw_primary_host'), d.get('mw_primary_port'),
                 {'API 端口': d.get('mw_api_port', '-'), 'VIP': d.get('mw_vip', '-'), '用户': d.get('mw_user', '-')})
            node('n2', 'secondary', f'{mw_label} 备节点', d.get('mw_secondary_host'), d.get('mw_secondary_port'),
                 {'用户': d.get('mw_user', '-')})
            link('n1', 'n2', 'replication', f'{mw_label} 主备心跳')
    else:  # 兜底：mysql 风格
        node('n1', 'primary', '主库', d.get('master_host'), d.get('master_port'),
             {'用户': d.get('mysql_user', '-')})
        node('n2', 'secondary', '备库', d.get('slave_host'), d.get('slave_port'),
             {'用户': d.get('mysql_slave_user', '-')})
        link('n1', 'n2', 'replication', '复制')

    # 通用切换参数
    global_params = {
        '最大延迟阈值': f"{d.get('max_delay', 60)}s",
        '等待超时': f"{d.get('wait_timeout', 300)}s",
        '跳过延迟检查': '是' if d.get('skip_delay_check') else '否',
        '跳过一致性检查': '是' if d.get('skip_consistency_check') else '否',
        '强制切换': '是' if d.get('force_switchover') else '否',
        '自动回滚': '是' if d.get('auto_rollback') else '否',
        '描述': d.get('description', '') or '-',
    }
    return jsonify({'ok': True, 'config': {'id': cfg.id, 'name': d.get('name', ''), 'db_type': db_type,
                                            'description': d.get('description', '')},
                    'params': global_params, 'nodes': nodes, 'links': links})


VALID_DB_TYPES = ['mysql', 'oracle', 'postgresql', 'redis', 'dameng', 'kingbase',
                  'opengauss', 'oceanbase', 'tidb', 'mongodb', 'highgo', 'middleware']
VALID_SUBTYPES = {
    'mysql': ['replica', 'mgr', 'cluster'],
    'oracle': ['adg', 'rac'],
    'dameng': ['standby', 'dsc'],
    'kingbase': ['standby', 'kdc'],
    'opengauss': ['standby', 'cluster'],
    'oceanbase': ['standby', 'cluster'],
    'tidb': ['standby', 'cluster'],
    'mongodb': ['replica', 'cluster'],
    'highgo': ['standby', 'cluster'],
    'middleware': ['active_standby', 'cluster'],
}


def _validate_config_input(data):
    """配置创建的通用输入校验，返回 None 或错误信息"""
    db_type = data.get('db_type', 'mysql')
    if db_type not in VALID_DB_TYPES:
        return f'非法数据库类型: {db_type}'
    if not str(data.get('name', '')).strip():
        return '配置名称不能为空'
    if len(str(data.get('name', ''))) > 100:
        return '配置名称过长（最多 100 字符）'
    for k, v in data.items():
        if k.endswith('_port') and v is not None and v != '':
            try:
                iv = int(v)
                if iv < 0 or iv > 65535:
                    return f'端口字段 {k} 非法: {v}（须为 0-65535，0 表示未启用）'
            except (ValueError, TypeError):
                return f'端口字段 {k} 非法: {v}（须为整数）'
    # 主库端口必填（避免缺省静默降级到默认值）
    primary_port_keys = {
        'mysql': 'master_port',
        'oracle': 'oracle_primary_port',
        'postgresql': 'pg_primary_port',
        'redis': 'redis_master_port',
        'dameng': 'dm_primary_port',
        'kingbase': 'kb_primary_port',
        'opengauss': 'og_primary_port',
        'oceanbase': 'ob_primary_port',
        'tidb': 'tidb_primary_port',
        'mongodb': 'mongo_primary_port',
        'highgo': 'hg_primary_port',
        'middleware': 'mw_primary_port',
    }
    pk = primary_port_keys.get(db_type)
    if pk and (data.get(pk) in (None, '')):
        return f'字段 {pk} 必填（主库端口）'
    # 主库账号必填（避免缺省静默回退到内置默认账号）
    primary_user_keys = {
        'mysql': 'mysql_user',
        'oracle': 'oracle_user',
        'postgresql': 'pg_user',
        'redis': 'redis_master_user',
        'dameng': 'dm_user',
        'kingbase': 'kb_user',
        'opengauss': 'og_user',
        'oceanbase': 'ob_user',
        'tidb': 'tidb_user',
        'mongodb': 'mongo_user',
        'highgo': 'hg_user',
        # middleware（nginx 等）可能无账号，不强制
    }
    uk = primary_user_keys.get(db_type)
    if uk and (data.get(uk) in (None, '')):
        return f'字段 {uk} 必填（主库账号）'
    subtype_keys = {
        'mysql': ('mysql_subtype',),
        'oracle': ('oracle_subtype',),
        'dameng': ('dameng_subtype',),
        'kingbase': ('kingbase_subtype',),
        'opengauss': ('opengauss_subtype',),
        'oceanbase': ('oceanbase_subtype',),
        'tidb': ('tidb_subtype',),
        'mongodb': ('mongodb_subtype',),
        'highgo': ('highgo_subtype',),
        'middleware': ('mw_subtype',),
    }
    valid = VALID_SUBTYPES.get(db_type, [])
    for key in subtype_keys.get(db_type, ()):
        if data.get(key) and data[key] not in valid:
            return f'字段 {key} 非法: {data[key]}（合法值: {", ".join(valid)}）'
    return None


@app.route('/api/configs', methods=['POST'])
@login_required
def api_create_config():
    # 安全修复：配置创建需要管理员权限
    if session.get('role') != 'admin':
        return jsonify({'ok': False, 'error': '需要管理员权限才能创建配置'}), 403
    data = request.get_json() or {}
    _err = _validate_config_input(data)
    if _err:
        return jsonify({'ok': False, 'error': _err}), 400
    db_type = data.get('db_type', 'mysql')
    cfg = SwitchoverConfig(
        db_type=db_type,
        name=data.get('name', ''),
        description=data.get('description', ''),
        max_delay=data.get('max_delay', 60),
        wait_timeout=data.get('wait_timeout', 300),
        skip_delay_check=data.get('skip_delay_check', False),
        skip_consistency_check=data.get('skip_consistency_check', False),
        force_switchover=data.get('force_switchover', False),
        auto_rollback=data.get('auto_rollback', True),
    )
    if db_type == 'mysql':
        cfg.mysql_subtype = data.get('mysql_subtype', 'replica')
        cfg.mysql_user = data.get('mysql_user', 'root')
        cfg.mysql_pass = data.get('mysql_pass', '')
        cfg.master_host = data.get('master_host', '')
        cfg.master_port = data.get('master_port', 3306)
        cfg.slave_host = data.get('slave_host', '')
        cfg.slave_port = data.get('slave_port', 3306)
        cfg.mysql_slave_user = data.get('mysql_slave_user', '')
        cfg.mysql_slave_pass = data.get('mysql_slave_pass', '')
        cfg.mysql_repl_user = data.get('mysql_repl_user', 'repl')
        cfg.mysql_repl_pass = data.get('mysql_repl_pass', '')
    elif db_type == 'oracle':
        cfg.oracle_user = data.get('oracle_user', 'sys')
        cfg.oracle_pass = data.get('oracle_pass', '')
        cfg.oracle_subtype = data.get('oracle_subtype', 'adg')
        cfg.oracle_primary_host = data.get('oracle_primary_host', '')
        cfg.oracle_primary_port = data.get('oracle_primary_port', 1521)
        cfg.oracle_primary_sid = data.get('oracle_primary_sid', '')
        cfg.oracle_standby_host = data.get('oracle_standby_host', '')
        cfg.oracle_standby_port = data.get('oracle_standby_port', 1521)
        cfg.oracle_standby_sid = data.get('oracle_standby_sid', '')
        cfg.oracle_connect_mode = data.get('oracle_connect_mode', 'SYSDBA')
        cfg.oracle_standby_user = data.get('oracle_standby_user', '')
        cfg.oracle_standby_pass = data.get('oracle_standby_pass', '')
        if cfg.oracle_subtype == 'rac':
            cfg.rac_node1_host = data.get('rac_node1_host', '')
            cfg.rac_node1_sid = data.get('rac_node1_sid', '')
            cfg.rac_node2_host = data.get('rac_node2_host', '')
            cfg.rac_node2_sid = data.get('rac_node2_sid', '')
            cfg.rac_service_name = data.get('rac_service_name', '')
            cfg.rac_node2_user = data.get('rac_node2_user', '')
            cfg.rac_node2_pass = data.get('rac_node2_pass', '')
    elif db_type == 'postgresql':
        cfg.pg_user = data.get('pg_user', 'postgres')
        cfg.pg_pass = data.get('pg_pass', '')
        cfg.pg_primary_host = data.get('pg_primary_host', '')
        cfg.pg_primary_port = data.get('pg_primary_port', 5432)
        cfg.pg_standby_host = data.get('pg_standby_host', '')
        cfg.pg_standby_port = data.get('pg_standby_port', 5432)
        cfg.pg_standby_user = data.get('pg_standby_user', '')
        cfg.pg_standby_pass = data.get('pg_standby_pass', '')
    elif db_type == 'redis':
        cfg.redis_master_host = data.get('redis_master_host', '')
        cfg.redis_master_port = data.get('redis_master_port', 6379)
        cfg.redis_master_user = data.get('redis_master_user', '')
        cfg.redis_master_pass = data.get('redis_master_pass', '')
        cfg.redis_slave_host = data.get('redis_slave_host', '')
        cfg.redis_slave_port = data.get('redis_slave_port', 6379)
        cfg.redis_slave_user = data.get('redis_slave_user', '')
        cfg.redis_slave_pass = data.get('redis_slave_pass', '')
    elif db_type == 'dameng':
        cfg.dameng_subtype = data.get('dameng_subtype', 'standby')
        if cfg.dameng_subtype == 'dsc':
            cfg.dm_dsc_user = data.get('dm_dsc_user', 'SYSDBA')
            cfg.dm_dsc_pass = data.get('dm_dsc_pass', '')
            cfg.dm_dsc_node1_host = data.get('dm_dsc_node1_host', '')
            cfg.dm_dsc_node1_port = data.get('dm_dsc_node1_port', 5236)
            cfg.dm_dsc_node2_host = data.get('dm_dsc_node2_host', '')
            cfg.dm_dsc_node2_port = data.get('dm_dsc_node2_port', 5236)
            cfg.dm_dsc_service_name = data.get('dm_dsc_service_name', 'DSC_SERVICE')
            cfg.dm_dsc_node2_user = data.get('dm_dsc_node2_user', '')
            cfg.dm_dsc_node2_pass = data.get('dm_dsc_node2_pass', '')
        else:
            cfg.dm_user = data.get('dm_user', 'SYSDBA')
            cfg.dm_pass = data.get('dm_pass', '')
            cfg.dm_primary_host = data.get('dm_primary_host', '')
            cfg.dm_primary_port = data.get('dm_primary_port', 5236)
            cfg.dm_primary_sid = data.get('dm_primary_sid', '')
            cfg.dm_standby_host = data.get('dm_standby_host', '')
            cfg.dm_standby_port = data.get('dm_standby_port', 5236)
            cfg.dm_standby_sid = data.get('dm_standby_sid', '')
            cfg.dm_standby_user = data.get('dm_standby_user', '')
            cfg.dm_standby_pass = data.get('dm_standby_pass', '')
    elif db_type == 'kingbase':
        cfg.kingbase_subtype = data.get('kingbase_subtype', 'standby')
        if cfg.kingbase_subtype == 'kdc':
            cfg.kb_kdc_user = data.get('kb_kdc_user', 'SYSTEM')
            cfg.kb_kdc_pass = data.get('kb_kdc_pass', '')
            cfg.kb_kdc_node1_host = data.get('kb_kdc_node1_host', '')
            cfg.kb_kdc_node1_port = data.get('kb_kdc_node1_port', 54321)
            cfg.kb_kdc_node2_host = data.get('kb_kdc_node2_host', '')
            cfg.kb_kdc_node2_port = data.get('kb_kdc_node2_port', 54321)
            cfg.kb_kdc_service_name = data.get('kb_kdc_service_name', 'KDC_SERVICE')
            cfg.kb_kdc_node2_user = data.get('kb_kdc_node2_user', '')
            cfg.kb_kdc_node2_pass = data.get('kb_kdc_node2_pass', '')
        else:
            cfg.kb_user = data.get('kb_user', 'SYSTEM')
            cfg.kb_pass = data.get('kb_pass', '')
            cfg.kb_primary_host = data.get('kb_primary_host', '')
            cfg.kb_primary_port = data.get('kb_primary_port', 54321)
            cfg.kb_standby_host = data.get('kb_standby_host', '')
            cfg.kb_standby_port = data.get('kb_standby_port', 54321)
            cfg.kb_standby_user = data.get('kb_standby_user', '')
            cfg.kb_standby_pass = data.get('kb_standby_pass', '')
    elif db_type == 'opengauss':
        cfg.opengauss_subtype = data.get('opengauss_subtype', 'standby')
        cfg.og_user = data.get('og_user', 'gaussdb')
        cfg.og_pass = data.get('og_pass', '')
        cfg.og_primary_host = data.get('og_primary_host', '')
        cfg.og_primary_port = data.get('og_primary_port', 5432)
        cfg.og_standby_host = data.get('og_standby_host', '')
        cfg.og_standby_port = data.get('og_standby_port', 5432)
        cfg.og_standby_user = data.get('og_standby_user', '')
        cfg.og_standby_pass = data.get('og_standby_pass', '')
        cfg.og_db_name = data.get('og_db_name', 'postgres')
        if cfg.opengauss_subtype == 'cluster':
            cfg.og_cluster_node1_host = data.get('og_cluster_node1_host', '')
            cfg.og_cluster_node1_port = data.get('og_cluster_node1_port', 5432)
            cfg.og_cluster_node2_host = data.get('og_cluster_node2_host', '')
            cfg.og_cluster_node2_port = data.get('og_cluster_node2_port', 5432)
    elif db_type == 'oceanbase':
        cfg.oceanbase_subtype = data.get('oceanbase_subtype', 'standby')
        cfg.ob_user = data.get('ob_user', 'root')
        cfg.ob_pass = data.get('ob_pass', '')
        cfg.ob_primary_host = data.get('ob_primary_host', '')
        cfg.ob_primary_port = data.get('ob_primary_port', 2881)
        cfg.ob_standby_host = data.get('ob_standby_host', '')
        cfg.ob_standby_port = data.get('ob_standby_port', 2881)
        cfg.ob_standby_user = data.get('ob_standby_user', '')
        cfg.ob_standby_pass = data.get('ob_standby_pass', '')
        cfg.ob_tenant_name = data.get('ob_tenant_name', '')
        if cfg.oceanbase_subtype == 'cluster':
            cfg.ob_cluster_node1_host = data.get('ob_cluster_node1_host', '')
            cfg.ob_cluster_node1_port = data.get('ob_cluster_node1_port', 2881)
            cfg.ob_cluster_node2_host = data.get('ob_cluster_node2_host', '')
            cfg.ob_cluster_node2_port = data.get('ob_cluster_node2_port', 2881)
    elif db_type == 'tidb':
        cfg.tidb_subtype = data.get('tidb_subtype', 'standby')
        cfg.tidb_user = data.get('tidb_user', 'root')
        cfg.tidb_pass = data.get('tidb_pass', '')
        cfg.tidb_primary_host = data.get('tidb_primary_host', '')
        cfg.tidb_primary_port = data.get('tidb_primary_port', 4000)
        cfg.tidb_standby_host = data.get('tidb_standby_host', '')
        cfg.tidb_standby_port = data.get('tidb_standby_port', 4000)
        cfg.tidb_standby_user = data.get('tidb_standby_user', '')
        cfg.tidb_standby_pass = data.get('tidb_standby_pass', '')
        cfg.tidb_db_name = data.get('tidb_db_name', 'test')
        cfg.tidb_pd_port = data.get('tidb_pd_port', 2379)
        if cfg.tidb_subtype == 'cluster':
            cfg.tidb_cluster_node1_host = data.get('tidb_cluster_node1_host', '')
            cfg.tidb_cluster_node1_port = data.get('tidb_cluster_node1_port', 4000)
            cfg.tidb_cluster_node2_host = data.get('tidb_cluster_node2_host', '')
            cfg.tidb_cluster_node2_port = data.get('tidb_cluster_node2_port', 4000)
    elif db_type == 'mongodb':
        cfg.mongodb_subtype = data.get('mongodb_subtype', 'replica')
        cfg.mongo_user = data.get('mongo_user', '')
        cfg.mongo_pass = data.get('mongo_pass', '')
        cfg.mongo_primary_host = data.get('mongo_primary_host', '')
        cfg.mongo_primary_port = data.get('mongo_primary_port', 27017)
        cfg.mongo_standby_host = data.get('mongo_standby_host', '')
        cfg.mongo_standby_port = data.get('mongo_standby_port', 27017)
        cfg.mongo_replset_name = data.get('mongo_replset_name', 'rs0')
        cfg.mongo_auth_db = data.get('mongo_auth_db', 'admin')
        if cfg.mongodb_subtype == 'cluster':
            cfg.mongo_cluster_node1_host = data.get('mongo_cluster_node1_host', '')
            cfg.mongo_cluster_node1_port = data.get('mongo_cluster_node1_port', 27017)
            cfg.mongo_cluster_node2_host = data.get('mongo_cluster_node2_host', '')
            cfg.mongo_cluster_node2_port = data.get('mongo_cluster_node2_port', 27017)
    elif db_type == 'highgo':
        cfg.highgo_subtype = 'standby'
        cfg.hg_user = data.get('hg_user', 'highgo')
        cfg.hg_pass = data.get('hg_pass', '')
        cfg.hg_primary_host = data.get('hg_primary_host', '')
        cfg.hg_primary_port = data.get('hg_primary_port', 5866)
        cfg.hg_standby_host = data.get('hg_standby_host', '')
        cfg.hg_standby_port = data.get('hg_standby_port', 5866)
        cfg.hg_standby_user = data.get('hg_standby_user', '')
        cfg.hg_standby_pass = data.get('hg_standby_pass', '')
        cfg.hg_db_name = data.get('hg_db_name', 'highgo')
    elif db_type == 'middleware':
        cfg.middleware_type = data.get('middleware_type', 'nginx')
        cfg.mw_subtype = data.get('mw_subtype', 'active_standby')
        cfg.mw_primary_host = data.get('mw_primary_host', '')
        cfg.mw_primary_port = data.get('mw_primary_port', 80)
        cfg.mw_secondary_host = data.get('mw_secondary_host', '')
        cfg.mw_secondary_port = data.get('mw_secondary_port', 80)
        cfg.mw_cluster_node1_host = data.get('mw_cluster_node1_host', '')
        cfg.mw_cluster_node1_port = data.get('mw_cluster_node1_port', 80)
        cfg.mw_cluster_node2_host = data.get('mw_cluster_node2_host', '')
        cfg.mw_cluster_node2_port = data.get('mw_cluster_node2_port', 80)
        cfg.mw_cluster_node3_host = data.get('mw_cluster_node3_host', '')
        cfg.mw_cluster_node3_port = data.get('mw_cluster_node3_port', 80)
        cfg.mw_user = data.get('mw_user', '')
        cfg.mw_pass = data.get('mw_pass', '')
        cfg.mw_vip = data.get('mw_vip', '')
        cfg.mw_api_port = data.get('mw_api_port', 0)
        cfg.mw_cluster_name = data.get('mw_cluster_name', '')
        cfg.mw_extra = data.get('mw_extra', '')
    db.session.add(cfg)
    db.session.commit()
    return jsonify({'ok': True, 'id': cfg.id, 'config': cfg.to_dict()})


@app.route('/api/configs/<int:cfg_id>', methods=['PUT'])
@login_required
def api_update_config(cfg_id):
    # 安全修复：配置修改需要管理员权限
    if session.get('role') != 'admin':
        return jsonify({'ok': False, 'error': '需要管理员权限才能修改配置'}), 403
    cfg = db.session.get(SwitchoverConfig, cfg_id)
    if not cfg:
        return jsonify({'ok': False, 'error': '配置不存在'}), 404
    data = request.get_json() or {}
    # 更新校验（db_type 若改变则按新类型校验，否则按原类型）
    _target_type = data.get('db_type', cfg.db_type)
    # 合并现有配置值补齐缺失字段：更新非端口字段时不必重复提交主端口
    _merged = dict(data)
    for _f, _v in cfg.__dict__.items():
        if not _f.startswith('_') and _f not in _merged:
            _merged[_f] = _v
    _err = _validate_config_input({**_merged, 'db_type': _target_type, 'name': data.get('name', cfg.name)})
    if _err:
        return jsonify({'ok': False, 'error': _err}), 400
    PASSWORD_FIELDS = {'mysql_pass', 'mysql_slave_pass', 'mysql_repl_pass', 'oracle_pass', 'oracle_standby_pass',
                      'rac_pass', 'rac_node2_pass', 'pg_pass', 'pg_standby_pass',
                      'redis_master_pass', 'redis_slave_pass',
                      'dm_pass', 'dm_standby_pass', 'dm_dsc_pass', 'dm_dsc_node2_pass',
                      'kb_pass', 'kb_standby_pass', 'kb_kdc_pass', 'kb_kdc_node2_pass',
                      'mw_pass', 'og_pass', 'og_standby_pass', 'ob_pass', 'ob_standby_pass',
                      'tidb_pass', 'tidb_standby_pass', 'mongo_pass',
                      'hg_pass', 'hg_standby_pass'}
    for field in ['name', 'description', 'db_type', 'max_delay', 'wait_timeout',
                  'skip_delay_check', 'skip_consistency_check',
                  'force_switchover', 'auto_rollback',
                  'mysql_subtype',
                  'mysql_user', 'mysql_pass', 'master_host', 'master_port',
                  'slave_host', 'slave_port',
                  'mysql_slave_user', 'mysql_slave_pass',
                  'mysql_repl_user', 'mysql_repl_pass',
                  'oracle_user', 'oracle_pass', 'oracle_primary_host', 'oracle_primary_port',
                  'oracle_primary_sid', 'oracle_standby_host',
                  'oracle_standby_port', 'oracle_standby_sid',
                  'oracle_standby_user', 'oracle_standby_pass',
                  'oracle_connect_mode',
                  'oracle_subtype',
                  'pg_user', 'pg_pass', 'pg_primary_host', 'pg_primary_port',
                  'pg_standby_host', 'pg_standby_port',
                  'pg_standby_user', 'pg_standby_pass',
                  'redis_master_host', 'redis_master_port',
                  'redis_master_user', 'redis_master_pass',
                  'redis_slave_host', 'redis_slave_port',
                  'redis_slave_user', 'redis_slave_pass',
                  'rac_node1_host', 'rac_node1_sid',
                  'rac_node2_host', 'rac_node2_sid', 'rac_service_name',
                  'rac_node2_user', 'rac_node2_pass',
                  'dameng_subtype',
                  'dm_user', 'dm_pass', 'dm_primary_host', 'dm_primary_port',
                  'dm_primary_sid', 'dm_standby_host', 'dm_standby_port',
                  'dm_standby_sid', 'dm_standby_user', 'dm_standby_pass',
                  'dm_dsc_user', 'dm_dsc_pass', 'dm_dsc_node1_host', 'dm_dsc_node1_port',
                  'dm_dsc_node2_host', 'dm_dsc_node2_port', 'dm_dsc_service_name',
                  'dm_dsc_node2_user', 'dm_dsc_node2_pass',
                  'kingbase_subtype',
                  'kb_user', 'kb_pass', 'kb_primary_host', 'kb_primary_port',
                  'kb_standby_host', 'kb_standby_port',
                  'kb_standby_user', 'kb_standby_pass',
                  'kb_kdc_user', 'kb_kdc_pass', 'kb_kdc_node1_host', 'kb_kdc_node1_port',
                  'kb_kdc_node2_host', 'kb_kdc_node2_port', 'kb_kdc_service_name',
                  'kb_kdc_node2_user', 'kb_kdc_node2_pass',
                  'opengauss_subtype',
                  'og_user', 'og_pass', 'og_primary_host', 'og_primary_port',
                  'og_standby_host', 'og_standby_port', 'og_standby_user', 'og_standby_pass',
                  'og_db_name',
                  'og_cluster_node1_host', 'og_cluster_node1_port',
                  'og_cluster_node2_host', 'og_cluster_node2_port',
                  'oceanbase_subtype',
                  'ob_user', 'ob_pass', 'ob_primary_host', 'ob_primary_port',
                  'ob_standby_host', 'ob_standby_port', 'ob_standby_user', 'ob_standby_pass',
                  'ob_tenant_name',
                  'ob_cluster_node1_host', 'ob_cluster_node1_port',
                  'ob_cluster_node2_host', 'ob_cluster_node2_port',
                  'tidb_subtype',
                  'tidb_user', 'tidb_pass', 'tidb_primary_host', 'tidb_primary_port',
                  'tidb_standby_host', 'tidb_standby_port', 'tidb_standby_user', 'tidb_standby_pass',
                  'tidb_db_name', 'tidb_pd_port',
                  'tidb_cluster_node1_host', 'tidb_cluster_node1_port',
                  'tidb_cluster_node2_host', 'tidb_cluster_node2_port',
                  'mongodb_subtype',
                  'mongo_user', 'mongo_pass', 'mongo_primary_host', 'mongo_primary_port',
                  'mongo_standby_host', 'mongo_standby_port',
                  'mongo_replset_name', 'mongo_auth_db',
                  'mongo_cluster_node1_host', 'mongo_cluster_node1_port',
                  'mongo_cluster_node2_host', 'mongo_cluster_node2_port',
                  'highgo_subtype',
                  'hg_user', 'hg_pass', 'hg_primary_host', 'hg_primary_port',
                  'hg_standby_host', 'hg_standby_port', 'hg_standby_user', 'hg_standby_pass',
                  'hg_db_name',
                  'middleware_type', 'mw_subtype',
                  'mw_primary_host', 'mw_primary_port',
                  'mw_secondary_host', 'mw_secondary_port',
                  'mw_cluster_node1_host', 'mw_cluster_node1_port',
                  'mw_cluster_node2_host', 'mw_cluster_node2_port',
                  'mw_cluster_node3_host', 'mw_cluster_node3_port',
                  'mw_user', 'mw_pass', 'mw_vip', 'mw_api_port',
                  'mw_cluster_name', 'mw_extra']:
        if field in data:
            if field in PASSWORD_FIELDS and data[field] == '***':
                continue
            setattr(cfg, field, data[field])
    cfg.updated_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({'ok': True, 'config': cfg.to_dict()})


@app.route('/api/configs/<int:cfg_id>', methods=['DELETE'])
@login_required
def api_delete_config(cfg_id):
    # 安全修复：配置删除需要管理员权限
    if session.get('role') != 'admin':
        return jsonify({'ok': False, 'error': '需要管理员权限才能删除配置'}), 403
    cfg = db.session.get(SwitchoverConfig, cfg_id)
    if not cfg:
        return jsonify({'ok': False, 'error': '配置不存在'}), 404
    db.session.delete(cfg)
    db.session.commit()
    return jsonify({'ok': True})


@app.route('/api/history/<task_id>')
@login_required
def api_history_detail(task_id):
    task = SwitchoverHistory.query.filter_by(task_id=task_id).first_or_404()
    return jsonify({'ok': True, 'history': task.to_dict()})


@app.route('/api/history/<task_id>/report.pdf')
@login_required
def api_history_report_pdf(task_id):
    """下载 PDF 演练报告"""
    task = SwitchoverHistory.query.filter_by(task_id=task_id).first_or_404()
    cfg = db.session.get(SwitchoverConfig, task.config_id)
    config_dict = cfg.to_dict() if cfg else None
    data = task.to_dict()
    data['full_log'] = task.full_log or ''
    pdf_bytes = make_report(data, config_dict, fmt='pdf')
    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype='application/pdf',
        as_attachment=True,
        download_name=report_filename(task_id, 'pdf')
    )


@app.route('/api/history/<task_id>/report.docx')
@login_required
def api_history_report_word(task_id):
    """下载 Word 演练报告"""
    task = SwitchoverHistory.query.filter_by(task_id=task_id).first_or_404()
    cfg = db.session.get(SwitchoverConfig, task.config_id)
    config_dict = cfg.to_dict() if cfg else None
    data = task.to_dict()
    data['full_log'] = task.full_log or ''
    docx_bytes = make_report(data, config_dict, fmt='word')
    return send_file(
        io.BytesIO(docx_bytes),
        mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        as_attachment=True,
        download_name=report_filename(task_id, 'docx')
    )


# ========== 用户管理 API ==========

@app.route('/api/users', methods=['GET'])
@login_required
def api_list_users():
    if session.get('role') != 'admin':
        return jsonify({'ok': False, 'error': '需要管理员权限'}), 403
    users = User.query.order_by(User.created_at.desc()).all()
    return jsonify([u.to_dict() for u in users])


@app.route('/api/users', methods=['POST'])
@login_required
def api_create_user():
    if session.get('role') != 'admin':
        return jsonify({'ok': False, 'error': '需要管理员权限'}), 403
    data = request.get_json()
    username = (data.get('username') or '').strip()
    password = data.get('password') or ''
    role = data.get('role', 'user')
    if not username or len(username) < 2:
        return jsonify({'ok': False, 'error': '用户名至少2个字符'}), 400
    if len(password) < 6:
        return jsonify({'ok': False, 'error': '密码长度不能少于6位'}), 400
    if role not in ('admin', 'user'):
        return jsonify({'ok': False, 'error': '角色无效'}), 400
    if User.query.filter_by(username=username).first():
        return jsonify({'ok': False, 'error': '用户名已存在'}), 409
    user = User(username=username, role=role)
    user.set_password(password)
    db.session.add(user)
    db.session.commit()
    return jsonify({'ok': True, 'user': user.to_dict()})


@app.route('/api/users/<int:user_id>', methods=['PUT'])
@login_required
def api_update_user(user_id):
    if session.get('role') != 'admin':
        return jsonify({'ok': False, 'error': '需要管理员权限'}), 403
    user = db.session.get(User, user_id)
    if not user:
        return jsonify({'ok': False, 'error': '用户不存在'}), 404
    if user.id == session['user_id']:
        return jsonify({'ok': False, 'error': '不能修改自己的角色'}), 400
    data = request.get_json()
    if 'role' in data:
        if data['role'] not in ('admin', 'user'):
            return jsonify({'ok': False, 'error': '角色无效'}), 400
        user.role = data['role']
    if 'password' in data and data['password']:
        if len(data['password']) < 6:
            return jsonify({'ok': False, 'error': '密码长度不能少于6位'}), 400
        user.set_password(data['password'])
    if 'is_active' in data:
        user.is_active = data['is_active']
    db.session.commit()
    return jsonify({'ok': True, 'user': user.to_dict()})


@app.route('/api/users/<int:user_id>', methods=['DELETE'])
@login_required
def api_delete_user(user_id):
    if session.get('role') != 'admin':
        return jsonify({'ok': False, 'error': '需要管理员权限'}), 403
    if user_id == session['user_id']:
        return jsonify({'ok': False, 'error': '不能删除自己'}), 400
    user = db.session.get(User, user_id)
    if not user:
        return jsonify({'ok': False, 'error': '用户不存在'}), 404
    db.session.delete(user)
    db.session.commit()
    return jsonify({'ok': True})


@app.route('/api/change-password', methods=['POST'])
@login_required
def api_change_password():
    user = db.session.get(User, session['user_id'])
    if not user:
        return jsonify({'ok': False, 'error': '用户不存在'}), 404
    data = request.get_json()
    old_pw = data.get('old_password', '')
    new_pw = data.get('new_password', '')
    confirm_pw = data.get('confirm_password', '')
    if not user.check_password(old_pw):
        return jsonify({'ok': False, 'error': '原密码错误'}), 400
    if not new_pw:
        return jsonify({'ok': False, 'error': '新密码不能为空'}), 400
    if len(new_pw) < 6:
        return jsonify({'ok': False, 'error': '新密码长度不能少于6位'}), 400
    if confirm_pw and new_pw != confirm_pw:
        return jsonify({'ok': False, 'error': '两次输入的新密码不一致'}), 400
    user.set_password(new_pw)
    db.session.commit()
    return jsonify({'ok': True})


def _log_entry(level, msg):
    return json.dumps({'level': level, 'msg': msg,
                       'time': datetime.now(timezone.utc).isoformat()})


def _normalize_role(node_role, entry):
    """将各引擎角色字段归一化为 primary/standby/unknown"""
    if not isinstance(entry, dict):
        return 'unknown'
    # 有明确错误标记 → unknown
    if entry.get('error') and not entry.get('ok', True):
        return 'unknown'
    r = (node_role or '').lower()
    if r in ('primary', 'master', 'primary_node', 'leader'):
        return 'primary'
    if r in ('standby', 'slave', 'secondary', 'replica', 'fc_standby'):
        return 'standby'
    # 兼容探测结果自带 role 字段
    er = str(entry.get('role', '')).lower()
    if er in ('primary', 'master'):
        return 'primary'
    if er in ('standby', 'slave', 'secondary', 'replica'):
        return 'standby'
    if isinstance(entry.get('is_replica'), bool):
        return 'standby' if entry['is_replica'] else 'primary'
    dr = str(entry.get('database_role', '')).upper()
    if 'PRIMARY' in dr:
        return 'primary'
    if 'STANDBY' in dr or 'SECONDARY' in dr:
        return 'standby'
    return 'unknown'


def _standard_status_pack(result, primary_key='primary', standby_key='standby',
                          primary_role=None, standby_role=None, db_label=''):
    """把各类状态探测结果包装为统一契约 {ok, primary, standby, healthy, summary}
    保留原字段（向后兼容），同时写入标准 primary/standby 对象与 healthy/summary。
    节点有效数据但无 ok 字段时（如 Redis）视为可达。
    """
    if not isinstance(result, dict):
        return result
    out = dict(result)
    entries = {}
    for std_key, src_key, role in ((primary_key, primary_key, primary_role),
                                   (standby_key, standby_key, standby_role)):
        entry = out.get(src_key)
        if isinstance(entry, dict):
            e = dict(entry)
            # 始终归一化角色（master→primary, slave→standby），保留原始角色于 raw_role
            normalized = _normalize_role(role, entry)
            if normalized != 'unknown':
                e['raw_role'] = e.get('role', '')
                e['role'] = normalized
            elif not e.get('role'):
                e['role'] = 'unknown'
            # 无 ok 字段但无 error 且有 host → 视为可达
            if 'ok' not in e and not e.get('error') and e.get('host'):
                e['ok'] = True
            entries[std_key] = e
        else:
            entries[std_key] = {'ok': False, 'role': 'unknown', 'error': '节点无探测结果'}
    out['primary'] = entries[primary_key]
    out['standby'] = entries[standby_key]
    # 整体状态
    p_ok = bool(entries[primary_key].get('ok'))
    s_ok = bool(entries[standby_key].get('ok'))
    out['ok'] = bool(out.get('ok', p_ok and s_ok)) if out.get('ok') is not None else (p_ok and s_ok)
    p_role, s_role = entries[primary_key].get('role'), entries[standby_key].get('role')
    # healthy 判定：角色明确时验方向，角色 unknown 时只验可达性
    if p_role == 'unknown' or s_role == 'unknown':
        out['healthy'] = p_ok and s_ok
    else:
        out['healthy'] = p_ok and s_ok and (p_role == 'primary') and (s_role == 'standby')
    if p_ok and s_ok:
        if out['healthy']:
            if p_role == 'unknown' or s_role == 'unknown':
                out['summary'] = f'{db_label}主备均连接正常' if db_label else '主备均连接正常'
            else:
                out['summary'] = f'{db_label}主备正常：方向正确' if db_label else '主备正常：方向正确'
        else:
            out['summary'] = f'方向异常：主={p_role}，备={s_role}'
    else:
        out['summary'] = '主备节点存在连接失败'
    return out


def _standard_status_pack_mapped(result, primary_src, standby_src, db_label='', p_role=None, s_role=None):
    """用于源键非 primary/standby（如 Redis 的 master/slave）的标准包装"""
    if not isinstance(result, dict):
        return result
    out = _standard_status_pack(
        dict(result, **{primary_src: result.get(primary_src), standby_src: result.get(standby_src)}),
        primary_key=primary_src, standby_key=standby_src,
        primary_role=p_role, standby_role=s_role, db_label=db_label)
    return out


# =====================================================================
# 统一能力分发（能力通用化：status/test/switchover/repair 四能力统一注册）
# 每个 db_type 一行，描述该类型的主备字段映射与子类型引擎映射。
# 新增数据库类型只需注册一行，路由层无需再写任何 per-type 代码。
# =====================================================================
_ALIAS_TO_TYPE = {
    'pg': 'postgresql', 'kb': 'kingbase', 'dm': 'dameng',
    'og': 'opengauss', 'ob': 'oceanbase', 'mongo': 'mongodb', 'hg': 'highgo',
    'rac': 'oracle', 'mid': 'middleware', 'mw': 'middleware',
}

# 状态探测：每类型的主备 host 字段、user/pass 字段与是否子类型路由
_DB_STATUS_SPEC = {
    'mysql': {
        'primary_key': 'master', 'standby_key': 'slave',
        'primary_host': 'master_host', 'primary_port': 'master_port',
        'standby_host': 'slave_host', 'standby_port': 'slave_port',
        'user': 'mysql_user', 'pass': 'mysql_pass',
        'standby_user': 'mysql_slave_user', 'standby_pass': 'mysql_slave_pass',
        'label': 'MySQL', 'style': 'mysql',
    },
    'postgresql': {
        'primary_key': 'pg_primary_host', 'standby_key': 'pg_standby_host',
        'primary_host': 'pg_primary_host', 'primary_port': 'pg_primary_port',
        'standby_host': 'pg_standby_host', 'standby_port': 'pg_standby_port',
        'user': 'pg_user', 'pass': 'pg_pass',
        'standby_user': 'pg_standby_user', 'standby_pass': 'pg_standby_pass',
        'label': 'PostgreSQL', 'style': 'pg',
    },
    'redis': {
        'primary_key': 'redis_master_host', 'standby_key': 'redis_slave_host',
        'primary_host': 'redis_master_host', 'primary_port': 'redis_master_port',
        'standby_host': 'redis_slave_host', 'standby_port': 'redis_slave_port',
        'user': 'redis_master_user', 'pass': 'redis_master_pass',
        'standby_user': 'redis_slave_user', 'standby_pass': 'redis_slave_pass',
        'label': 'Redis', 'style': 'redis',
    },
    'oracle': {
        'primary_key': 'oracle_primary_host', 'standby_key': 'oracle_standby_host',
        'primary_host': 'oracle_primary_host', 'primary_port': 'oracle_primary_port',
        'standby_host': 'oracle_standby_host', 'standby_port': 'oracle_standby_port',
        'user': 'oracle_user', 'pass': 'oracle_pass',
        'standby_user': 'oracle_standby_user', 'standby_pass': 'oracle_standby_pass',
        'label': 'Oracle', 'style': 'oracle', 'subtype_field': 'oracle_subtype',
        'subtypes': {'rac': 'rac'},
    },
    'dameng': {
        'primary_key': 'dm_primary_host', 'standby_key': 'dm_standby_host',
        'primary_host': 'dm_primary_host', 'primary_port': 'dm_primary_port',
        'standby_host': 'dm_standby_host', 'standby_port': 'dm_standby_port',
        'user': 'dm_user', 'pass': 'dm_pass',
        'standby_user': 'dm_standby_user', 'standby_pass': 'dm_standby_pass',
        'label': '达梦', 'style': 'dm', 'subtype_field': 'dameng_subtype',
        'subtypes': {'dsc': 'dsc'},
    },
    'kingbase': {
        'primary_key': 'kb_primary_host', 'standby_key': 'kb_standby_host',
        'primary_host': 'kb_primary_host', 'primary_port': 'kb_primary_port',
        'standby_host': 'kb_standby_host', 'standby_port': 'kb_standby_port',
        'user': 'kb_user', 'pass': 'kb_pass',
        'standby_user': 'kb_standby_user', 'standby_pass': 'kb_standby_pass',
        'label': '金仓', 'style': 'kb', 'subtype_field': 'kingbase_subtype',
        'subtypes': {'kdc': 'kdc'},
    },
    'opengauss': {
        'primary_key': 'og_primary_host', 'standby_key': 'og_standby_host',
        'primary_host': 'og_primary_host', 'primary_port': 'og_primary_port',
        'standby_host': 'og_standby_host', 'standby_port': 'og_standby_port',
        'user': 'og_user', 'pass': 'og_pass',
        'standby_user': 'og_standby_user', 'standby_pass': 'og_standby_pass',
        'label': 'openGauss', 'style': 'generictest',
    },
    'oceanbase': {
        'primary_key': 'ob_primary_host', 'standby_key': 'ob_standby_host',
        'primary_host': 'ob_primary_host', 'primary_port': 'ob_primary_port',
        'standby_host': 'ob_standby_host', 'standby_port': 'ob_standby_port',
        'user': 'ob_user', 'pass': 'ob_pass',
        'standby_user': 'ob_standby_user', 'standby_pass': 'ob_standby_pass',
        'label': 'OceanBase', 'style': 'generictest',
    },
    'tidb': {
        'primary_key': 'tidb_primary_host', 'standby_key': 'tidb_standby_host',
        'primary_host': 'tidb_primary_host', 'primary_port': 'tidb_primary_port',
        'standby_host': 'tidb_standby_host', 'standby_port': 'tidb_standby_port',
        'user': 'tidb_user', 'pass': 'tidb_pass',
        'standby_user': 'tidb_standby_user', 'standby_pass': 'tidb_standby_pass',
        'label': 'TiDB', 'style': 'generictest',
    },
    'mongodb': {
        'primary_key': 'mongo_primary_host', 'standby_key': 'mongo_standby_host',
        'primary_host': 'mongo_primary_host', 'primary_port': 'mongo_primary_port',
        'standby_host': 'mongo_standby_host', 'standby_port': 'mongo_standby_port',
        'user': 'mongo_user', 'pass': 'mongo_pass',
        'standby_user': None, 'standby_pass': None,
        'label': 'MongoDB', 'style': 'generictest',
    },
    'highgo': {
        'primary_key': 'hg_primary_host', 'standby_key': 'hg_standby_host',
        'primary_host': 'hg_primary_host', 'primary_port': 'hg_primary_port',
        'standby_host': 'hg_standby_host', 'standby_port': 'hg_standby_port',
        'user': 'hg_user', 'pass': 'hg_pass',
        'standby_user': 'hg_standby_user', 'standby_pass': 'hg_standby_pass',
        'label': '瀚高', 'style': 'generictest',
    },
    'middleware': {
        'primary_host': 'mw_primary_host', 'primary_port': 'mw_primary_port',
        'standby_host': 'mw_secondary_host', 'standby_port': 'mw_secondary_port',
        'user': 'mw_user', 'pass': 'mw_pass',
        'label': '中间件', 'style': 'mw',
    },
}

_DB_TEST_FN = {}


def _resolve_db_type(db_type):
    """路由参数 db_type 兼容简称/别名 → 规范类型名"""
    return _ALIAS_TO_TYPE.get(db_type, db_type)


def _ensure_status_spec(db_type):
    """惰性初始化状态探测函数注册（避免模块加载时依赖未就绪的引擎导入）"""
    if db_type in _DB_TEST_FN:
        return
    spec = _DB_STATUS_SPEC.get(db_type)
    if spec is None:
        return
    style = spec.get('style')
    if style == 'pg':
        _DB_TEST_FN[db_type] = test_pg_connection
    elif style == 'redis':
        _DB_TEST_FN[db_type] = test_redis_status
    elif style == 'oracle':
        _DB_TEST_FN[db_type] = test_oracle_connection
    elif style == 'dm':
        _DB_TEST_FN[db_type] = test_dameng_connection
    elif style == 'kb':
        _DB_TEST_FN[db_type] = test_kingbase_connection
    elif style == 'generictest':
        _DB_TEST_FN[db_type] = {
            'opengauss': test_opengauss_connection,
            'oceanbase': test_oceanbase_connection,
            'tidb': test_tidb_connection,
            'mongodb': test_mongo_connection,
            'highgo': test_highgo_connection,
        }.get(db_type)


def _db_status(cfg, db_type):
    """统一状态探测：按注册表分发到各引擎测试函数，输出标准契约。
    返回 dict 或 (dict, http_code)。"""
    spec = _DB_STATUS_SPEC.get(db_type)
    if spec is None:
        return {'ok': False, 'error': f'暂不支持 {db_type} 状态查询'}
    _ensure_status_spec(db_type)
    # 子类型（RAC/DSC/KDC）走集群节点探测
    subtype_field = spec.get('subtype_field')
    if subtype_field:
        subtype = getattr(cfg, subtype_field, None) or 'standby'
        if spec.get('subtypes', {}).get(subtype) is not None:
            return _db_cluster_status(cfg, db_type, subtype, spec)
    style = spec.get('style')
    label = spec.get('label', db_type)
    if style == 'redis':
        result = test_redis_status(
            master_host=getattr(cfg, spec['primary_host'], ''),
            slave_host=getattr(cfg, spec['standby_host'], ''),
            master_port=getattr(cfg, spec['primary_port'], '') or 6379,
            slave_port=getattr(cfg, spec['standby_port'], '') or 6379,
            master_user=getattr(cfg, spec['user'], '') or '',
            master_pass=getattr(cfg, spec['pass'], '') or '',
            slave_user=(getattr(cfg, spec['standby_user'], '') or '' if spec.get('standby_user') else '') or '',
            slave_pass=(getattr(cfg, spec['standby_pass'], '') or '' if spec.get('standby_pass') else '') or '',
        )
        return _standard_status_pack(result, primary_key='master', standby_key='slave',
                                     primary_role='master', standby_role='slave', db_label=label)
    if style == 'pg':
        result = {'primary': None, 'standby': None}
        for k, h, p in [('primary', spec['primary_host'], spec['primary_port']),
                        ('standby', spec['standby_host'], spec['standby_port'])]:
            try:
                result[k] = test_pg_connection(getattr(cfg, h, ''), getattr(cfg, p, ''), getattr(cfg, spec['user'], ''), getattr(cfg, spec['pass'], ''))
            except Exception as e:
                result[k] = {'ok': False, 'error': str(e)}
        return _standard_status_pack(result, primary_role='primary', standby_role='standby', db_label=label)
    if style == 'oracle':
        result = {'primary': None, 'standby': None}
        for k, h, p in [('primary', spec['primary_host'], spec['primary_port']),
                        ('standby', spec['standby_host'], spec['standby_port'])]:
            try:
                result[k] = test_oracle_connection(
                    host=getattr(cfg, h, ''), port=getattr(cfg, p, '') or 1521,
                    sid=(getattr(cfg, 'oracle_primary_sid', '') if k == 'primary' else getattr(cfg, 'oracle_standby_sid', '')),
                    user=getattr(cfg, spec['user'], ''), password=getattr(cfg, spec['pass'], ''),
                    mode=getattr(cfg, 'oracle_connect_mode', '') or 'SYSDBA')
            except Exception as e:
                result[k] = {'ok': False, 'error': str(e)}
        return _standard_status_pack(result, primary_role='primary', standby_role='standby', db_label=label)
    if style in ('dm', 'kb'):
        fn = test_dameng_connection if style == 'dm' else test_kingbase_connection
        result = {'primary': None, 'standby': None}
        for k, h, p in [('primary', spec['primary_host'], spec['primary_port']),
                        ('standby', spec['standby_host'], spec['standby_port'])]:
            cfg2 = {
                ('dm_primary_host' if style == 'dm' else 'kb_primary_host'): getattr(cfg, h, ''),
                ('dm_primary_port' if style == 'dm' else 'kb_primary_port'): getattr(cfg, p, ''),
                ('dm_user' if style == 'dm' else 'kb_user'): getattr(cfg, spec['user'], '') or 'SYSDBA',
                ('dm_pass' if style == 'dm' else 'kb_pass'): getattr(cfg, spec['pass'], '') or '',
            }
            result[k] = fn(cfg2)
        return _standard_status_pack(result, primary_role='unknown', standby_role='unknown', db_label=label)
    if style == 'mw':
        result = test_middleware_connection({
            'mw_primary_host': getattr(cfg, 'mw_primary_host', ''),
            'mw_primary_port': getattr(cfg, 'mw_primary_port', ''),
            'mw_secondary_host': getattr(cfg, 'mw_secondary_host', ''),
            'mw_secondary_port': getattr(cfg, 'mw_secondary_port', ''),
            'mw_user': getattr(cfg, 'mw_user', ''), 'mw_pass': getattr(cfg, 'mw_pass', ''),
            'mw_type': getattr(cfg, 'middleware_type', ''), 'mw_subtype': getattr(cfg, 'mw_subtype', ''),
        })
        return _standard_status_pack(result, primary_key='primary', standby_key='secondary',
                                     primary_role='primary', standby_role='standby', db_label=label)
    if style == 'mysql':
        return _db_mysql_status(cfg)
    # generictest：优先引擎 get_status，其次 test_fn
    test_fn = _DB_TEST_FN.get(db_type)
    eng_cfg = _build_generic_engine_cfg(cfg, db_type)
    EngineClass = _GENERIC_ENGINE_MAP.get(db_type, (None, None, None))[1]
    if EngineClass is not None:
        try:
            eng = EngineClass(eng_cfg)
            if hasattr(eng, 'get_status') and callable(eng.get_status):
                st = eng.get_status()
                if isinstance(st, dict) and st.get('ok') is not None:
                    return st
        except Exception:
            pass
    # 回退连通性测试
    result = {}
    for k, h, p in [('primary', spec['primary_host'], spec['primary_port']),
                    ('standby', spec['standby_host'], spec['standby_port'])]:
        host, port = getattr(cfg, h, ''), getattr(cfg, p, '')
        try:
            result[k] = test_fn({**{h: host, p: port, spec['user']: getattr(cfg, spec['user'], ''), spec['pass']: getattr(cfg, spec['pass'], '')}})
        except Exception as e:
            result[k] = {'ok': False, 'error': str(e)}
    return _standard_status_pack(result, primary_role='unknown', standby_role='unknown', db_label=label)


def _db_mysql_status(cfg):
    """MySQL 状态（主从/复制）"""
    return _mysql_status_impl(cfg)


def _db_cluster_status(cfg, db_type, subtype, spec):
    """集群子类型（Oracle RAC / 达梦 DSC / 金仓 KDC）状态：探测各节点"""
    if db_type == 'oracle' and subtype == 'rac':
        result = {'node1': None, 'node2': None}
        for label, host, sid, u, pw in [
            ('node1', cfg.rac_node1_host, cfg.rac_node1_sid, cfg.rac_user or cfg.oracle_user, cfg.rac_pass or cfg.oracle_pass),
            ('node2', cfg.rac_node2_host, cfg.rac_node2_sid, cfg.rac_node2_user or cfg.rac_user or cfg.oracle_user, cfg.rac_node2_pass or cfg.rac_pass or cfg.oracle_pass),
        ]:
            try:
                result[label] = test_rac_connection(host, sid, u, pw)
            except Exception as e:
                result[label] = {'ok': False, 'error': str(e)}
        return result
    if db_type == 'dameng' and subtype == 'dsc':
        return {
            'node1': (test_dameng_dsc_connection({
                'dm_dsc_node1_host': cfg.dm_dsc_node1_host, 'dm_dsc_node1_port': cfg.dm_dsc_node1_port,
                'dm_dsc_user': cfg.dm_dsc_user, 'dm_dsc_pass': cfg.dm_dsc_pass})),
            'node2': (test_dameng_dsc_connection({
                'dm_dsc_node1_host': cfg.dm_dsc_node2_host, 'dm_dsc_node1_port': cfg.dm_dsc_node2_port,
                'dm_dsc_user': cfg.dm_dsc_node2_user or cfg.dm_dsc_user, 'dm_dsc_pass': cfg.dm_dsc_node2_pass or cfg.dm_dsc_pass})),
        }
    if db_type == 'kingbase' and subtype == 'kdc':
        return {
            'node1': (test_kingbase_kdc_connection({
                'kb_kdc_node1_host': cfg.kb_kdc_node1_host, 'kb_kdc_node1_port': cfg.kb_kdc_node1_port,
                'kb_kdc_user': cfg.kb_kdc_user or 'SYSTEM', 'kb_kdc_pass': cfg.kb_kdc_pass or ''})),
            'node2': (test_kingbase_kdc_connection({
                'kb_kdc_node1_host': cfg.kb_kdc_node2_host, 'kb_kdc_node1_port': cfg.kb_kdc_node2_port,
                'kb_kdc_user': cfg.kb_kdc_node2_user or cfg.kb_kdc_user or 'SYSTEM', 'kb_kdc_pass': cfg.kb_kdc_node2_pass or cfg.kb_kdc_pass or ''})),
        }
    return {'ok': False, 'error': f'未知集群子类型 {db_type}/{subtype}'}


def _mysql_status_impl(cfg):
    """MySQL 统一状态实现（主库角色+从库复制状态）"""
    m_host, m_port = cfg.master_host, cfg.master_port or 3306
    s_host, s_port = cfg.slave_host, cfg.slave_port or 3306
    m_user, m_pass = cfg.mysql_user or 'root', cfg.mysql_pass or ''
    s_user = cfg.mysql_slave_user or cfg.mysql_user or 'root'
    s_pass = cfg.mysql_slave_pass or cfg.mysql_pass or ''
    primary, standby = {}, {}
    conn = None
    try:
        conn = pymysql.connect(host=m_host, port=m_port, user=m_user, password=m_pass,
                               connect_timeout=5, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        with conn.cursor() as cur:
            cur.execute("SELECT @@global.read_only AS ro, @@global.gtid_executed AS gtid")
            row = cur.fetchone()
            cur.execute("SELECT VERSION() AS v")
            ver = cur.fetchone()
        conn.close()
        primary = {'host': m_host, 'port': m_port, 'ok': True,
                   'role': 'standby' if row and row.get('ro') else 'primary',
                   'version': ver.get('v', '') if ver else '', 'gtid': row.get('gtid', '') if row else ''}
    except Exception as e:
        primary = {'host': m_host, 'port': m_port, 'ok': False, 'error': str(e)[:120]}
    finally:
        if conn:
            try: conn.close()
            except Exception: pass
    conn2 = None
    try:
        conn2 = pymysql.connect(host=s_host, port=s_port, user=s_user, password=s_pass,
                                connect_timeout=5, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        with conn2.cursor() as cur:
            try: cur.execute("SHOW REPLICA STATUS")
            except Exception: cur.execute("SHOW SLAVE STATUS")
            row = cur.fetchone()
        conn2.close()
        if row:
            io = row.get('Replica_IO_Running') or row.get('Slave_IO_Running', 'No')
            sql = row.get('Replica_SQL_Running') or row.get('Slave_SQL_Running', 'No')
            delay = row.get('Seconds_Behind_Source') or row.get('Seconds_Behind_Master') or 0
            standby = {'host': s_host, 'port': s_port, 'ok': True, 'role': 'standby',
                       'io_running': io, 'sql_running': sql, 'lag_seconds': delay}
        else:
            standby = {'host': s_host, 'port': s_port, 'ok': False, 'error': '未找到复制状态（可能非从库）'}
    except Exception as e:
        standby = {'host': s_host, 'port': s_port, 'ok': False, 'error': str(e)[:120]}
    finally:
        if conn2:
            try: conn2.close()
            except Exception: pass
    p_ok, s_ok = bool(primary.get('ok')), bool(standby.get('ok'))
    p_role = primary.get('role', 'unknown')
    s_role = standby.get('role', 'standby' if s_ok else 'unknown')
    # 复制线程健康：IO/SQL 线程必须处于 Yes 状态（Connecting/No 视为复制不可用）
    io_ok = str(standby.get('io_running', '')).strip().lower() == 'yes'
    sql_ok = str(standby.get('sql_running', '')).strip().lower() == 'yes'
    repl_ok = (not s_ok) or (io_ok and sql_ok)  # 从库不可达时不强求线程状态
    healthy = p_ok and s_ok and repl_ok and p_role == 'primary' and s_role == 'standby'
    return {
        'ok': p_ok and s_ok, 'primary': primary, 'standby': standby,
        'healthy': healthy,
        'summary': 'MySQL 主从正常：方向正确' if healthy else
                   (f'复制线程异常：IO={standby.get("io_running")}，SQL={standby.get("sql_running")}' if p_ok and s_ok and not repl_ok else
                    (f'方向异常：主={p_role}，备={s_role}' if p_ok and s_ok else '主从节点存在连接失败')),
    }


# ---------------------------------------------------------------
# 通用复制修复路由（按 db_type 分发）
# ---------------------------------------------------------------
def _build_repair_engine_cfg(cfg, db_type, force=False):
    """根据配置构造各引擎 repair_replication 所需的 eng_cfg"""
    if db_type == 'mysql':
        return {
            'master_host': cfg.master_host, 'master_port': cfg.master_port,
            'slave_host': cfg.slave_host, 'slave_port': cfg.slave_port,
            'mysql_user': cfg.mysql_user or 'root',
            'mysql_pass': cfg.mysql_pass or '',
            'mysql_slave_user': cfg.mysql_slave_user or cfg.mysql_user or 'root',
            'mysql_slave_pass': cfg.mysql_slave_pass or cfg.mysql_pass or '',
            'repl_user': cfg.mysql_repl_user or 'repl',
            'repl_pass': cfg.mysql_repl_pass or '',
        }
    if db_type == 'postgresql':
        return {
            'pg_user': cfg.pg_user, 'pg_pass': cfg.pg_pass,
            'pg_primary_host': cfg.pg_primary_host,
            'pg_primary_port': cfg.pg_primary_port,
            'pg_standby_host': cfg.pg_standby_host,
            'pg_standby_port': cfg.pg_standby_port,
            'pg_standby_user': cfg.pg_standby_user or cfg.pg_user,
            'pg_standby_pass': cfg.pg_standby_pass or cfg.pg_pass,
            'max_delay': cfg.max_delay,
        }
    if db_type == 'redis':
        return {
            'redis_master_host': cfg.redis_master_host or cfg.master_host,
            'redis_master_port': cfg.redis_master_port or cfg.master_port or 6379,
            'redis_master_user': cfg.redis_master_user or '',
            'redis_master_pass': cfg.redis_master_pass or cfg.redis_pass or '',
            'redis_slave_host': cfg.redis_slave_host or cfg.slave_host,
            'redis_slave_port': cfg.redis_slave_port or cfg.slave_port or 6379,
            'redis_slave_user': cfg.redis_slave_user or '',
            'redis_slave_pass': cfg.redis_slave_pass or '',
        }
    if db_type == 'oracle':
        base = {
            'oracle_primary_host': cfg.oracle_primary_host,
            'oracle_primary_port': cfg.oracle_primary_port or 1521,
            'oracle_primary_sid': cfg.oracle_primary_sid or '',
            'oracle_standby_host': cfg.oracle_standby_host,
            'oracle_standby_port': cfg.oracle_standby_port or 1521,
            'oracle_standby_sid': cfg.oracle_standby_sid or '',
            'oracle_user': cfg.oracle_user or 'sys',
            'oracle_pass': cfg.oracle_pass or '',
            'oracle_connect_mode': cfg.oracle_connect_mode or 'SYSDBA',
            'force_switchover': cfg.force_switchover,
            'dry_run': False,
            'auto_rollback': cfg.auto_rollback,
            'oracle_subtype': cfg.oracle_subtype or 'adg',
            # RAC 字段（rac 子类型用）
            'rac_user': cfg.rac_user or cfg.oracle_user or 'sys',
            'rac_pass': cfg.rac_pass or cfg.oracle_pass or '',
            'rac_node1_host': cfg.rac_node1_host or '',
            'rac_node1_sid': cfg.rac_node1_sid or '',
            'rac_node2_host': cfg.rac_node2_host or '',
            'rac_node2_sid': cfg.rac_node2_sid or '',
            'rac_service_name': cfg.rac_service_name or '',
            'rac_node2_user': cfg.rac_node2_user or cfg.rac_user or cfg.oracle_user or '',
            'rac_node2_pass': cfg.rac_node2_pass or cfg.rac_pass or cfg.oracle_pass or '',
        }
        return base
    if db_type == 'dameng':
        return {
            'dm_primary_host': cfg.dm_primary_host,
            'dm_primary_port': cfg.dm_primary_port or 5236,
            'dm_standby_host': cfg.dm_standby_host,
            'dm_standby_port': cfg.dm_standby_port or 5236,
            'dm_primary_sid': cfg.dm_primary_sid or '',
            'dm_standby_sid': cfg.dm_standby_sid or '',
            'dm_user': cfg.dm_user or 'SYSDBA',
            'dm_pass': cfg.dm_pass or 'SYSDBA',
            'dm_subtype': cfg.dameng_subtype or 'standby',
            # DSC 字段（dsc 子类型用）
            'dm_dsc_node1_host': cfg.dm_dsc_node1_host or '',
            'dm_dsc_node1_port': cfg.dm_dsc_node1_port or 5236,
            'dm_dsc_node2_host': cfg.dm_dsc_node2_host or '',
            'dm_dsc_node2_port': cfg.dm_dsc_node2_port or 5236,
            'dm_dsc_service_name': cfg.dm_dsc_service_name or '',
        }
    if db_type == 'kingbase':
        return {
            'kb_primary_host': cfg.kb_primary_host,
            'kb_primary_port': cfg.kb_primary_port or 54321,
            'kb_standby_host': cfg.kb_standby_host,
            'kb_standby_port': cfg.kb_standby_port or 54321,
            'kb_user': cfg.kb_user or 'system',
            'kb_pass': cfg.kb_pass or '',
            'kb_standby_user': cfg.kb_standby_user or '',
            'kb_standby_pass': cfg.kb_standby_pass or '',
            'kb_subtype': cfg.kingbase_subtype or 'standby',
            # KDC 字段（kdc 子类型用）
            'kb_kdc_user': cfg.kb_kdc_user or cfg.kb_user or 'system',
            'kb_kdc_pass': cfg.kb_kdc_pass or cfg.kb_pass or '',
            'kb_kdc_node1_host': cfg.kb_kdc_node1_host or '',
            'kb_kdc_node1_port': cfg.kb_kdc_node1_port or 54321,
            'kb_kdc_node2_host': cfg.kb_kdc_node2_host or '',
            'kb_kdc_node2_port': cfg.kb_kdc_node2_port or 54321,
            'kb_kdc_service_name': cfg.kb_kdc_service_name or '',
            'kb_kdc_node2_user': cfg.kb_kdc_node2_user or cfg.kb_kdc_user or '',
            'kb_kdc_node2_pass': cfg.kb_kdc_node2_pass or cfg.kb_kdc_pass or '',
        }
    if db_type == 'opengauss':
        return {
            'og_primary_host': cfg.og_primary_host,
            'og_primary_port': cfg.og_primary_port or 5432,
            'og_standby_host': cfg.og_standby_host,
            'og_standby_port': cfg.og_standby_port or 5432,
            'og_user': cfg.og_user or 'gaussdb',
            'og_pass': cfg.og_pass or '',
            'og_standby_user': cfg.og_standby_user or '',
            'og_standby_pass': cfg.og_standby_pass or '',
            'og_db_name': cfg.og_db_name or 'postgres',
            'opengauss_subtype': cfg.opengauss_subtype or 'standby',
            # cluster 子类型字段
            'og_cluster_node1_host': cfg.og_cluster_node1_host or '',
            'og_cluster_node1_port': cfg.og_cluster_node1_port or 5432,
            'og_cluster_node2_host': cfg.og_cluster_node2_host or '',
            'og_cluster_node2_port': cfg.og_cluster_node2_port or 5432,
        }
    if db_type == 'oceanbase':
        return {
            'ob_primary_host': cfg.ob_primary_host,
            'ob_primary_port': cfg.ob_primary_port or 2881,
            'ob_standby_host': cfg.ob_standby_host,
            'ob_standby_port': cfg.ob_standby_port or 2881,
            'ob_user': cfg.ob_user or 'root',
            'ob_pass': cfg.ob_pass or '',
            'ob_standby_user': cfg.ob_standby_user or '',
            'ob_standby_pass': cfg.ob_standby_pass or '',
            'ob_tenant_name': cfg.ob_tenant_name or '',
            'oceanbase_subtype': cfg.oceanbase_subtype or 'standby',
            # cluster 子类型字段
            'ob_cluster_node1_host': cfg.ob_cluster_node1_host or '',
            'ob_cluster_node1_port': cfg.ob_cluster_node1_port or 2881,
            'ob_cluster_node2_host': cfg.ob_cluster_node2_host or '',
            'ob_cluster_node2_port': cfg.ob_cluster_node2_port or 2881,
        }
    if db_type == 'tidb':
        return {
            'tidb_primary_host': cfg.tidb_primary_host,
            'tidb_primary_port': cfg.tidb_primary_port or 4000,
            'tidb_standby_host': cfg.tidb_standby_host,
            'tidb_standby_port': cfg.tidb_standby_port or 4000,
            'tidb_user': cfg.tidb_user or 'root',
            'tidb_pass': cfg.tidb_pass or '',
            'tidb_pd_host': cfg.tidb_pd_host or '',
            'tidb_pd_port': cfg.tidb_pd_port or 2379,
            'tidb_db_name': cfg.tidb_db_name or 'test',
            'tidb_subtype': cfg.tidb_subtype or 'standby',
            # cluster 子类型字段
            'tidb_cluster_node1_host': cfg.tidb_cluster_node1_host or '',
            'tidb_cluster_node1_port': cfg.tidb_cluster_node1_port or 4000,
            'tidb_cluster_node2_host': cfg.tidb_cluster_node2_host or '',
            'tidb_cluster_node2_port': cfg.tidb_cluster_node2_port or 4000,
        }
    if db_type == 'mongodb':
        return {
            'mongo_primary_host': cfg.mongo_primary_host,
            'mongo_primary_port': cfg.mongo_primary_port or 27017,
            'mongo_standby_host': cfg.mongo_standby_host,
            'mongo_standby_port': cfg.mongo_standby_port or 27017,
            'mongo_user': cfg.mongo_user or '',
            'mongo_pass': cfg.mongo_pass or '',
            'mongo_replset_name': cfg.mongo_replset_name or 'rs0',
            'mongo_auth_db': cfg.mongo_auth_db or 'admin',
            'mongodb_subtype': cfg.mongodb_subtype or 'replica',
            # cluster 子类型字段
            'mongo_cluster_node1_host': cfg.mongo_cluster_node1_host or '',
            'mongo_cluster_node1_port': cfg.mongo_cluster_node1_port or 27017,
            'mongo_cluster_node2_host': cfg.mongo_cluster_node2_host or '',
            'mongo_cluster_node2_port': cfg.mongo_cluster_node2_port or 27017,
        }
    if db_type == 'highgo':
        return {
            'hg_primary_host': cfg.hg_primary_host,
            'hg_primary_port': cfg.hg_primary_port or 5866,
            'hg_standby_host': cfg.hg_standby_host,
            'hg_standby_port': cfg.hg_standby_port or 5866,
            'hg_user': cfg.hg_user or 'highgo',
            'hg_pass': cfg.hg_pass or '',
            'hg_standby_user': cfg.hg_standby_user or '',
            'hg_standby_pass': cfg.hg_standby_pass or '',
            'hg_db_name': cfg.hg_db_name or 'highgo',
        }
    if db_type == 'middleware':
        return {
            'middleware_type': cfg.middleware_type or 'nginx',
            'mw_subtype': cfg.mw_subtype or 'active_standby',
            'mw_primary_host': cfg.mw_primary_host,
            'mw_primary_port': cfg.mw_primary_port or 80,
            'mw_secondary_host': cfg.mw_secondary_host,
            'mw_secondary_port': cfg.mw_secondary_port or 80,
            'mw_api_port': cfg.mw_api_port or 0,
            'mw_user': cfg.mw_user or '',
            'mw_pass': cfg.mw_pass or '',
            'mw_vip': cfg.mw_vip or '',
            'mw_cluster_name': cfg.mw_cluster_name or '',
        }
    return None


def _instantiate_repair_engine(eng_cfg, db_type, force=False):
    """构造修复引擎实例；返回 (engine, method) 或 (None, None)"""
    if db_type == 'mysql':
        eng = SwitchoverEngine()
        return eng, eng.repair_replication
    if db_type == 'postgresql':
        eng = PgSwitchoverEngine(eng_cfg, force=force)
        return eng, eng.repair_replication
    if db_type == 'redis':
        eng = RedisSwitchoverEngine(eng_cfg)
        return eng, eng.repair_replication
    if db_type == 'oracle':
        # 按子类型分发：rac 用 OracleRacEngine（服务漂移修复），其余用 ADG 引擎
        if eng_cfg.get('oracle_subtype') == 'rac' and RAC_AVAILABLE:
            rac_cfg = {k: eng_cfg.get(k) for k in (
                'rac_node1_host', 'rac_node1_sid', 'rac_node2_host', 'rac_node2_sid',
                'rac_service_name', 'rac_user', 'rac_pass', 'rac_node2_user', 'rac_node2_pass')}
            rac_cfg['rac_user'] = rac_cfg.get('rac_user') or eng_cfg.get('oracle_user') or 'sys'
            rac_cfg['rac_pass'] = rac_cfg.get('rac_pass') or eng_cfg.get('oracle_pass') or ''
            eng = OracleRacEngine(rac_cfg, force=force)
            return eng, eng.repair_replication
        eng = OracleSwitchoverEngine(eng_cfg)
        return eng, eng.repair_replication
    if db_type == 'dameng':
        # 按子类型分发：dsc 用 DamengDscEngine，主备用 DamengStandbyEngine
        if eng_cfg.get('dm_subtype') == 'dsc':
            dsc_cfg = {k: eng_cfg.get(k) for k in (
                'dm_dsc_node1_host', 'dm_dsc_node1_port', 'dm_dsc_node2_host', 'dm_dsc_node2_port',
                'dm_dsc_service_name', 'dm_user', 'dm_pass')}
            dsc_cfg['dm_dsc_node1_port'] = dsc_cfg.get('dm_dsc_node1_port') or 5236
            dsc_cfg['dm_dsc_node2_port'] = dsc_cfg.get('dm_dsc_node2_port') or 5236
            eng = DamengDscEngine(dsc_cfg, force=force)
            return eng, eng.repair_replication
        eng = DamengStandbyEngine(eng_cfg, force=force)
        return eng, eng.repair_replication
    if db_type == 'kingbase':
        # 按子类型分发：kdc 用 KingbaseKdcEngine，主备用 KingbaseStandbyEngine
        if eng_cfg.get('kb_subtype') == 'kdc':
            kdc_cfg = {k: eng_cfg.get(k) for k in (
                'kb_kdc_node1_host', 'kb_kdc_node1_port', 'kb_kdc_node2_host', 'kb_kdc_node2_port',
                'kb_kdc_service_name', 'kb_user', 'kb_pass')}
            kdc_cfg['kb_kdc_node1_port'] = kdc_cfg.get('kb_kdc_node1_port') or 54321
            kdc_cfg['kb_kdc_node2_port'] = kdc_cfg.get('kb_kdc_node2_port') or 54321
            eng = KingbaseKdcEngine(kdc_cfg, force=force)
            return eng, eng.repair_replication
        eng = KingbaseStandbyEngine(eng_cfg, force=force)
        return eng, eng.repair_replication
    if db_type == 'opengauss':
        eng = OpenGaussSwitchoverEngine(eng_cfg, force=force)
        return eng, eng.repair_replication
    if db_type == 'oceanbase':
        eng = OceanBaseSwitchoverEngine(eng_cfg, force=force)
        return eng, eng.repair_replication
    if db_type == 'tidb':
        eng = TiDBSwitchoverEngine(eng_cfg, force=force)
        return eng, eng.repair_replication
    if db_type == 'mongodb':
        eng = MongoSwitchoverEngine(eng_cfg, force=force)
        return eng, eng.repair_replication
    if db_type == 'highgo':
        eng = HighGoSwitchoverEngine(eng_cfg, force=force)
        return eng, eng.repair_replication
    if db_type == 'middleware':
        eng = MiddlewareSwitchoverEngine(eng_cfg, force=force)
        return eng, eng.repair_replication
    return None, None


@app.route('/api/db/repair/<int:cfg_id>', methods=['POST'])
@login_required
def api_db_repair(cfg_id):
    """统一复制/服务修复（能力通用化）：按配置 db_type 分发，一次 POST 修复主备方向"""
    cfg = db.session.get(SwitchoverConfig, cfg_id)
    if not cfg:
        return jsonify({'ok': False, 'error': '配置不存在'}), 404
    db_type = cfg.db_type
    return _repair_impl(cfg, db_type)


def _repair_impl(cfg, db_type):
    """修复统一实现（供 /api/db/repair/<id> 与旧 per-type 路由共用的核心）"""
    logs = []
    def _emit(level, msg):
        logs.append({'level': level, 'msg': msg})

    eng_cfg = _build_repair_engine_cfg(cfg, db_type)
    if eng_cfg is None:
        return jsonify({'ok': False, 'error': f'暂不支持 {db_type} 的复制修复'}), 400

    eng, method = _instantiate_repair_engine(eng_cfg, db_type)
    if eng is None:
        return jsonify({'ok': False, 'error': f'引擎不可用（{db_type} 模块可能未启用）'}), 400

    try:
        if db_type == 'mysql':
            # MySQL 引擎签名: repair_replication(config, emit=None)
            result = method(eng_cfg, emit=_emit)
        else:
            result = method(emit=_emit)
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e), 'logs': logs})

    return jsonify({
        'ok': True,
        'db_type': db_type,
        'repaired': result.get('repaired', False),
        'direction_mismatch': result.get('direction_mismatch', False),
        'details': result.get('details', {}),
        'logs': logs,
    })


def _duration_seconds(start, end):
    """兼容 offset-naive 与 offset-aware datetime 的耗时计算"""
    if not start or not end:
        return 0
    try:
        return int((end - start).total_seconds())
    except TypeError:
        # 一者为 naive，一者为 aware 时统一按 UTC 处理
        import datetime as _dt
        if start.tzinfo is None and end.tzinfo is not None:
            start = start.replace(tzinfo=_dt.timezone.utc)
        elif end.tzinfo is None and start.tzinfo is not None:
            end = end.replace(tzinfo=_dt.timezone.utc)
        return int((end - start).total_seconds())


def _finish_history(history, log_queue):
    commit_ok = True
    try:
        # 若 history 已脱离当前会话则重新绑定
        if history not in db.session:
            history = db.session.merge(history)
        history.end_time = datetime.now(timezone.utc)
        history.duration_seconds = _duration_seconds(history.start_time, history.end_time)
        db.session.commit()
    except Exception as e:
        commit_ok = False
        db.session.rollback()
        try:
            history = db.session.merge(history)
            history.end_time = datetime.now(timezone.utc)
            history.duration_seconds = _duration_seconds(history.start_time, history.end_time)
            db.session.commit()
        except Exception:
            db.session.rollback()
            logger.error(f'提交历史记录失败: {e}')
    log_queue.append(_log_entry('done', json.dumps({
        'success': history.status == 'success', 'task_id': history.task_id,
        'commit_ok': commit_ok
    })))


# ========== openGauss / OceanBase / TiDB / MongoDB / 瀚高 通用 API ==========

def _build_generic_engine_cfg(cfg, db_type):
    """构建通用引擎配置（包含真实密码）"""
    mapping = {
        'opengauss': {
            'opengauss_subtype': cfg.opengauss_subtype or 'standby',
            'og_user': cfg.og_user, 'og_pass': cfg.og_pass,
            'og_primary_host': cfg.og_primary_host, 'og_primary_port': cfg.og_primary_port,
            'og_standby_host': cfg.og_standby_host, 'og_standby_port': cfg.og_standby_port,
            'og_standby_user': cfg.og_standby_user or cfg.og_user,
            'og_standby_pass': cfg.og_standby_pass or cfg.og_pass,
            'og_db_name': cfg.og_db_name,
            'og_cluster_node1_host': cfg.og_cluster_node1_host, 'og_cluster_node1_port': cfg.og_cluster_node1_port,
            'og_cluster_node2_host': cfg.og_cluster_node2_host, 'og_cluster_node2_port': cfg.og_cluster_node2_port,
        },
        'oceanbase': {
            'oceanbase_subtype': cfg.oceanbase_subtype or 'standby',
            'ob_user': cfg.ob_user, 'ob_pass': cfg.ob_pass,
            'ob_primary_host': cfg.ob_primary_host, 'ob_primary_port': cfg.ob_primary_port,
            'ob_standby_host': cfg.ob_standby_host, 'ob_standby_port': cfg.ob_standby_port,
            'ob_standby_user': cfg.ob_standby_user or cfg.ob_user,
            'ob_standby_pass': cfg.ob_standby_pass or cfg.ob_pass,
            'ob_tenant_name': cfg.ob_tenant_name,
            'ob_cluster_node1_host': cfg.ob_cluster_node1_host, 'ob_cluster_node1_port': cfg.ob_cluster_node1_port,
            'ob_cluster_node2_host': cfg.ob_cluster_node2_host, 'ob_cluster_node2_port': cfg.ob_cluster_node2_port,
        },
        'tidb': {
            'tidb_subtype': cfg.tidb_subtype or 'standby',
            'tidb_user': cfg.tidb_user, 'tidb_pass': cfg.tidb_pass,
            'tidb_primary_host': cfg.tidb_primary_host, 'tidb_primary_port': cfg.tidb_primary_port,
            'tidb_standby_host': cfg.tidb_standby_host, 'tidb_standby_port': cfg.tidb_standby_port,
            'tidb_standby_user': cfg.tidb_standby_user or cfg.tidb_user,
            'tidb_standby_pass': cfg.tidb_standby_pass or cfg.tidb_pass,
            'tidb_db_name': cfg.tidb_db_name, 'tidb_pd_port': cfg.tidb_pd_port,
            'tidb_cluster_node1_host': cfg.tidb_cluster_node1_host, 'tidb_cluster_node1_port': cfg.tidb_cluster_node1_port,
            'tidb_cluster_node2_host': cfg.tidb_cluster_node2_host, 'tidb_cluster_node2_port': cfg.tidb_cluster_node2_port,
        },
        'mongodb': {
            'mongodb_subtype': cfg.mongodb_subtype or 'replica',
            'mongo_user': cfg.mongo_user, 'mongo_pass': cfg.mongo_pass,
            'mongo_primary_host': cfg.mongo_primary_host, 'mongo_primary_port': cfg.mongo_primary_port,
            'mongo_standby_host': cfg.mongo_standby_host, 'mongo_standby_port': cfg.mongo_standby_port,
            'mongo_replset_name': cfg.mongo_replset_name, 'mongo_auth_db': cfg.mongo_auth_db,
            'mongo_cluster_node1_host': cfg.mongo_cluster_node1_host, 'mongo_cluster_node1_port': cfg.mongo_cluster_node1_port,
            'mongo_cluster_node2_host': cfg.mongo_cluster_node2_host, 'mongo_cluster_node2_port': cfg.mongo_cluster_node2_port,
        },
        'highgo': {
            'hg_user': cfg.hg_user, 'hg_pass': cfg.hg_pass,
            'hg_primary_host': cfg.hg_primary_host, 'hg_primary_port': cfg.hg_primary_port,
            'hg_standby_host': cfg.hg_standby_host, 'hg_standby_port': cfg.hg_standby_port,
            'hg_standby_user': cfg.hg_standby_user or cfg.hg_user,
            'hg_standby_pass': cfg.hg_standby_pass or cfg.hg_pass,
            'hg_db_name': cfg.hg_db_name,
        },
    }
    return mapping.get(db_type, {})


# 各数据库的引擎类和测试函数映射
_GENERIC_ENGINE_MAP = {
    'opengauss': (OG_AVAILABLE, OpenGaussSwitchoverEngine if OG_AVAILABLE else None, test_opengauss_connection if OG_AVAILABLE else None),
    'oceanbase': (OB_AVAILABLE, OceanBaseSwitchoverEngine if OB_AVAILABLE else None, test_oceanbase_connection if OB_AVAILABLE else None),
    'tidb':      (TIDB_AVAILABLE, TiDBSwitchoverEngine if TIDB_AVAILABLE else None, test_tidb_connection if TIDB_AVAILABLE else None),
    'mongodb':   (MONGO_AVAILABLE, MongoSwitchoverEngine if MONGO_AVAILABLE else None, test_mongo_connection if MONGO_AVAILABLE else None),
    'highgo':    (HG_AVAILABLE, HighGoSwitchoverEngine if HG_AVAILABLE else None, test_highgo_connection if HG_AVAILABLE else None),
}


@app.route('/api/db/status/<int:cfg_id>', methods=['GET'])
@login_required
def api_db_status(cfg_id):
    """统一状态接口：按配置 db_type 分发到统一状态探测器（能力通用化）"""
    cfg = db.session.get(SwitchoverConfig, cfg_id)
    if not cfg:
        return jsonify({'ok': False, 'error': '配置不存在'}), 404
    result = _db_status(cfg, cfg.db_type)
    if isinstance(result, tuple):
        result, code = result
        return jsonify(result), code
    return jsonify(result)


@app.route('/api/db/switchover/<int:cfg_id>/stream')
@login_required
def api_db_switchover_stream(cfg_id):
    """统一切换 SSE 流：按配置 db_type 分发到统一切换器（能力通用化）"""
    cfg = db.session.get(SwitchoverConfig, cfg_id)
    if not cfg:
        return jsonify({'error': '配置不存在'}), 404
    return _dispatch_switchover_stream(cfg)


@app.route('/api/db/switchover/<int:cfg_id>', methods=['POST'])
@login_required
def api_db_switchover(cfg_id):
    """统一切换（非SSE，JSON返回）：按配置 db_type 分发"""
    cfg = db.session.get(SwitchoverConfig, cfg_id)
    if not cfg:
        return jsonify({'error': '配置不存在'}), 404
    force = request.args.get('force', '0') == '1'
    dry = request.args.get('dry_run', '0') == '1'
    from functools import wraps
    body = request.get_json(silent=True) or {}
    force = body.get('force', force)
    dry = body.get('dry_run', dry)
    result = _run_switchover_sync(cfg, force=force, dry_run=dry)
    return jsonify(result)


@app.route('/api/db/test-connection', methods=['POST'])
@login_required
def api_db_test_connection():
    """统一测试连接：按 db_type 分发（能力通用化）"""
    data = request.get_json(silent=True) or {}
    db_type = _resolve_db_type(data.get('db_type', ''))
    if db_type not in VALID_DB_TYPES:
        return jsonify({'ok': False, 'error': f'未知数据库类型: {db_type}'}), 400
    if db_type == 'mysql':
        # 复用复制状态探测（从库视角）
        return jsonify(_dispatch_mysql_test_conn(data))
    spec = _DB_STATUS_SPEC.get(db_type)
    if spec is None:
        return jsonify({'ok': False, 'error': f'暂不支持 {db_type} 测试连接'})
    _ensure_status_spec(db_type)
    style = spec.get('style')
    test_fn = _DB_TEST_FN.get(db_type)
    if style == 'redis':
        result = test_redis_status(
            master_host=data.get('host') or data.get(spec['primary_host'], ''),
            slave_host=data.get('slave_host') or data.get(spec['standby_host'], ''),
            master_port=data.get('port') or data.get(spec['primary_port'], 6379),
            slave_port=data.get('slave_port') or data.get(spec['standby_port'], 6379),
            master_user=data.get('user') or data.get(spec['user'], ''),
            master_pass=data.get('password') or data.get(spec['pass'], ''),
        )
        return jsonify(result)
    if style == 'pg':
        result = test_pg_connection(
            data.get('host') or data.get(spec['primary_host'], ''),
            data.get('port') or data.get(spec['primary_port'], ''),
            data.get('user') or data.get(spec['user'], ''), data.get('password') or data.get(spec['pass'], ''))
        return jsonify(result)
    if style == 'oracle':
        result = test_oracle_connection(
            host=data.get('host') or data.get(spec['primary_host'], ''),
            port=data.get('port') or data.get(spec['primary_port'], 1521),
            sid=data.get('sid') or data.get('oracle_primary_sid', ''),
            user=data.get('user') or data.get(spec['user'], ''), password=data.get('password') or data.get(spec['pass'], ''),
            mode=data.get('mode') or data.get('oracle_connect_mode', '') or 'SYSDBA')
        return jsonify(result)
    if style == 'dm':
        result = test_dameng_connection({
            'dm_primary_host': data.get('host') or data.get(spec['primary_host'], ''),
            'dm_primary_port': data.get('port') or data.get(spec['primary_port'], ''),
            'dm_user': data.get('user') or data.get(spec['user'], ''), 'dm_pass': data.get('password') or data.get(spec['pass'], '')})
        return jsonify(result)
    if style == 'kb':
        result = test_kingbase_connection({
            'kb_primary_host': data.get('host') or data.get(spec['primary_host'], ''),
            'kb_primary_port': data.get('port') or data.get(spec['primary_port'], ''),
            'kb_user': data.get('user') or data.get(spec['user'], '') or 'SYSTEM', 'kb_pass': data.get('password') or data.get(spec['pass'], '')})
        return jsonify(result)
    if style == 'mw':
        result = test_middleware_connection({
            'mw_primary_host': data.get('host') or data.get('mw_primary_host', ''),
            'mw_primary_port': data.get('port') or data.get('mw_primary_port', ''),
            'mw_secondary_host': data.get('slave_host') or data.get('mw_secondary_host', ''),
            'mw_secondary_port': data.get('slave_port') or data.get('mw_secondary_port', ''),
            'mw_user': data.get('user') or data.get('mw_user', ''), 'mw_pass': data.get('password') or data.get('mw_pass', ''),
            'mw_type': data.get('middleware_type', ''), 'mw_subtype': data.get('mw_subtype', '')})
        return jsonify(result)
    if db_type == 'rac':
        # Oracle RAC 单节点测试
        try:
            result = test_rac_connection(
                host=data.get('host', ''), sid=data.get('sid', ''),
                user=data.get('user', 'oracle'), password=data.get('password', ''),
                port=data.get('port', 1521))
            return jsonify(result)
        except Exception as e:
            return jsonify({'ok': False, 'error': str(e)})
    if test_fn is not None:
        result = test_fn(data)
        return jsonify(result)
    return jsonify({'ok': False, 'error': f'暂不支持 {db_type} 测试连接'})


def _dispatch_mysql_test_conn(data):
    """MySQL 测试连接：真实连接 + 复制状态"""
    host = data.get('host') or data.get('master_host', '')
    port = int(data.get('port') or data.get('master_port', 3306))
    user = data.get('user') or data.get('mysql_user', 'root')
    password = data.get('password') or data.get('mysql_pass', '')
    conn = None
    try:
        conn = pymysql.connect(host=host, port=port, user=user, password=password,
                               connect_timeout=5, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION() AS v")
            row = cur.fetchone()
        conn.close()
        return {'ok': True, 'version': row.get('v', '') if row else '', 'message': f'MySQL {host}:{port} 连接成功'}
    except Exception as e:
        return {'ok': False, 'error': str(e)}
    finally:
        if conn:
            try: conn.close()
            except Exception: pass



class _AttrDict(dict):
    """支持 d.attr 访问的 dict（属性优先取 dict key，兼容 cfg.xxx 风格）"""
    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError(item)


def _cfg_snapshot(cfg):
    """把 SQLAlchemy SwitchoverConfig 对象快照为 plain dict（后台线程无 Session 可用）"""
    columns = [c.name for c in SwitchoverConfig.__table__.columns]
    d = {}
    for col in columns:
        try:
            d[col] = getattr(cfg, col)
        except Exception:
            d[col] = None
    return d


def _snapshot_cfg_dict(cfg):
    """兼容入口：返回可属性访问的快照（_AttrDict）"""
    return _AttrDict(_cfg_snapshot(cfg))


def _dispatch_switchover_stream(cfg):
    """统一切换 SSE 编排：按 db_type 分发（能力通用化）

    与旧 per-type 路由保持相同行为：构造 history → 子类型引擎 → run → 写状态。
    """
    force = request.args.get('force', '0') == '1'
    dry = request.args.get('dry_run', '0') == '1'
    triggered = request.args.get('triggered_by', 'manual')
    target_node = request.args.get('target_node', 'node2')
    db_type = cfg.db_type
    # 关键修复：后台线程脱离请求上下文后 Session 已关闭，
    # 直接把 cfg 完整快照成属性可访问的 dict，所有后续 cfg.xxx 不再触发 lazy 刷新。
    cfg_snap = _snapshot_cfg_dict(cfg)
    cfg = cfg_snap

    # history 主从字段映射（与旧路由一致）
    if db_type == 'mysql':
        m_host, m_port = cfg.master_host, cfg.master_port
        s_host, s_port = cfg.slave_host, cfg.slave_port
    elif db_type == 'oracle':
        m_host, m_port = cfg.oracle_primary_host, cfg.oracle_primary_port
        s_host, s_port = cfg.oracle_standby_host, cfg.oracle_standby_port
    elif db_type == 'postgresql':
        m_host, m_port = cfg.pg_primary_host, cfg.pg_primary_port
        s_host, s_port = cfg.pg_standby_host, cfg.pg_standby_port
    elif db_type == 'redis':
        m_host, m_port = cfg.redis_master_host, cfg.redis_master_port
        s_host, s_port = cfg.redis_slave_host, cfg.redis_slave_port
    elif db_type == 'dameng':
        m_host, m_port = cfg.dm_primary_host, cfg.dm_primary_port
        s_host, s_port = cfg.dm_standby_host, cfg.dm_standby_port
    elif db_type == 'kingbase':
        m_host, m_port = cfg.kb_primary_host, cfg.kb_primary_port
        s_host, s_port = cfg.kb_standby_host, cfg.kb_standby_port
    elif db_type == 'middleware':
        m_host, m_port = cfg.mw_primary_host, cfg.mw_primary_port
        s_host, s_port = cfg.mw_secondary_host, cfg.mw_secondary_port
    else:
        # generic 5 类
        spec = _DB_STATUS_SPEC.get(db_type, {})
        m_host = getattr(cfg, spec.get('primary_host', ''), '')
        m_port = getattr(cfg, spec.get('primary_port', ''), '')
        s_host = getattr(cfg, spec.get('standby_host', ''), '')
        s_port = getattr(cfg, spec.get('standby_port', ''), '')

    history = SwitchoverHistory(
        task_id=gen_task_id(), db_type=db_type, config_id=cfg.id,
        config_name=cfg.name,
        master_host=m_host or '', master_port=m_port or '',
        slave_host=s_host or '', slave_port=s_port or '',
        status='running', triggered_by=triggered,
        target_node=target_node if (db_type == 'mysql' and cfg.mysql_subtype == 'mgr_multi') else '',
        start_time=datetime.now(timezone.utc)
    )
    db.session.add(history)
    db.session.commit()

    log_queue = []
    all_logs = []

    def _emit(level, msg):
        all_logs.append(f"[{level.upper()}] {msg}")
        log_queue.append(_log_entry(level, msg))

    def _progress(name, pct):
        log_queue.append(_log_entry('progress', f'{name} - {pct}%'))

    def _build_eng_cfg():
        # 统一引擎配置构造：优先复用全类型覆盖的 repair cfg，补充开关字段
        base = _build_repair_engine_cfg(cfg, db_type) or {}
        base.update({
            'max_delay': cfg.max_delay, 'wait_timeout': cfg.wait_timeout,
            'skip_delay_check': cfg.skip_delay_check,
            'skip_consistency_check': cfg.skip_consistency_check,
            'force_switchover': force or cfg.force_switchover,
            'auto_rollback': cfg.auto_rollback,
            'dry_run': dry, 'connect_timeout': 5,
        })
        # generic 类型补充子类型与 target_node
        for f in ('mysql_subtype', 'oracle_subtype', 'dameng_subtype', 'kingbase_subtype',
                  'opengauss_subtype', 'oceanbase_subtype', 'tidb_subtype', 'mongodb_subtype', 'highgo_subtype'):
            v = getattr(cfg, f, None)
            if v:
                base[f] = v
        if target_node:
            base['target_node'] = target_node
        return base

    def _run():
        global mysql_engine
        with app.app_context():
            nonlocal history
            history = db.session.merge(history)
            try:
                eng_cfg = _build_eng_cfg()
                eng, steps, logs, ok = _run_db_engine(cfg, db_type, eng_cfg, force, dry, target_node, _emit)
                history.status = 'success' if ok else 'failed'
                history.full_log = '\n'.join(logs)
                if steps:
                    history.steps = [{'name': s.get('name'), 'status': s.get('status'), 'detail': s.get('detail', '')} for s in steps]
                # 真实新方向（引擎返回时已切换）；仅成功写新拓扑
                new_dir = getattr(eng, 'new_direction', None)
                if ok and isinstance(new_dir, dict):
                    history.new_master_host = new_dir.get('master_host')
                    history.new_master_port = new_dir.get('master_port')
                    history.new_slave_host = new_dir.get('slave_host')
                    history.new_slave_port = new_dir.get('slave_port')
                db.session.commit()
            except Exception as e:
                history.status = 'failed'
                history.full_log = (history.full_log or '') + f'\nERROR: {e}'
                _emit('error', str(e))
                db.session.commit()
            finally:
                # 修复（2026-11-05 真实测试）：统一路由必须落库 end_time/duration_seconds，
                # 否则 history.end_time 恒为 NULL（旧 _finish_history 仅在旧路由被调用）。
                try:
                    history.end_time = datetime.now(timezone.utc)
                    history.duration_seconds = _duration_seconds(history.start_time, history.end_time)
                    db.session.commit()
                except Exception as e:
                    db.session.rollback()
                    logger.error(f'提交 end_time 失败: {e}')
                log_queue.append(_log_entry('done', json.dumps({
                    'success': history.status == 'success', 'task_id': history.task_id,
                })))

    def _stream():
        yield f"data: {_log_entry('info', f'统一切换分发: {db_type}')}\n\n"
        yield f"data: {_log_entry('progress', '开始切换 - 0%')}\n\n"
        from threading import Thread
        t = Thread(target=_run, daemon=True)
        t.start()
        sent = 0
        while t.is_alive() or len(log_queue) > sent:
            while len(log_queue) > sent:
                yield f"data: {log_queue[sent]}\n\n"
                sent += 1
            time.sleep(0.3)
        while len(log_queue) > sent:
            yield f"data: {log_queue[sent]}\n\n"
            sent += 1

    return Response(_stream(), mimetype='text/event-stream')


def _run_db_engine(cfg, db_type, eng_cfg, force, dry, target_node, emit):
    """按 db_type + 子类型实例化引擎并执行切换，返回 (engine, steps, logs, ok)。

    与旧 per-type 路由的行为保持一致。
    """
    if not isinstance(cfg, _AttrDict):
        cfg = _snapshot_cfg_dict(cfg)
    if db_type == 'mysql':
        subtype = getattr(cfg, 'mysql_subtype', None) or 'replica'
        if subtype in ('mgr_single', 'mgr_multi'):
            eng = MgrSwitchoverEngine(eng_cfg, force=force, dry_run=dry, target_node=target_node)
            result = eng.run(log_callback=lambda line: emit('info', line))
            steps = result.get('steps', [])
            for s in steps:
                level = 'success' if s.get('success') else 'error'
                emit(level, f"{s.get('name')}: {s.get('detail', '')}")
            return eng, steps, [], bool(result.get('success'))
        global mysql_engine
        mysql_engine = SwitchoverEngine()
        task_id = mysql_engine.start_switchover(eng_cfg)
        return mysql_engine, [], mysql_engine.get_task_logs_since(task_id)[0], True
    if db_type == 'oracle':
        if eng_cfg.get('oracle_subtype') == 'rac' and RAC_AVAILABLE:
            eng = OracleRacEngine(eng_cfg, force=force, dry_run=dry, target_node=target_node)
        else:
            eng = OracleSwitchoverEngine(eng_cfg, log_callback=emit)
        return _run_via_steps(eng, emit)
    if db_type == 'postgresql':
        eng = PgSwitchoverEngine(eng_cfg, force=force, dry_run=dry)
        return _run_via_steps(eng, emit)
    if db_type == 'redis':
        eng = RedisSwitchoverEngine(eng_cfg, force=force, dry_run=dry)
        return _run_via_steps(eng, emit)
    if db_type == 'dameng':
        if eng_cfg.get('dameng_subtype') == 'dsc':
            eng = DamengDscEngine(eng_cfg, force=force, dry_run=dry, target_node=target_node)
        else:
            eng = DamengStandbyEngine(eng_cfg, force=force, dry_run=dry)
        return _run_via_steps(eng, emit)
    if db_type == 'kingbase':
        if eng_cfg.get('kingbase_subtype') == 'kdc':
            eng = KingbaseKdcEngine(eng_cfg, force=force, dry_run=dry, target_node=target_node)
        else:
            eng = KingbaseStandbyEngine(eng_cfg, force=force, dry_run=dry)
        return _run_via_steps(eng, emit)
    if db_type == 'middleware':
        eng = MiddlewareSwitchoverEngine(eng_cfg, force=force, dry_run=dry)
        return _run_via_steps(eng, emit)
    # generic 5 类（opengauss/oceanbase/tidb/mongodb/highgo）
    entry = _GENERIC_ENGINE_MAP.get(db_type)
    if entry and entry[1]:
        eng = entry[1](eng_cfg, force=force, dry_run=dry, target_node=target_node)
        return _run_via_steps(eng, emit)
    return None, [], [f'{db_type} 引擎未加载'], False


def _run_via_steps(eng, emit):
    """执行 run() 返回 steps/logs/ok 的引擎（兼容多元返回结构）"""
    try:
        r = eng.run(log_callback=lambda line: emit('info', line))
    except TypeError:
        r = eng.run()
    if isinstance(r, tuple):
        # 兼容三种 tuple 形态：
        #   (steps, logs, ok)  — PG / Oracle RAC
        #   (ok, steps)        — Redis（步骤列表已内含 level 信息）
        if len(r) == 2:
            ok, steps = r
            logs = []
        else:
            steps, logs, ok = r[:3]
        # tuple 返回的引擎（PG/部分引擎）不走 log_callback，需将引擎日志逐行转发到 SSE。
        # 引擎 logs 行格式多为 "[LEVEL] msg"，剥掉前缀取级别，避免双层 [INFO]。
        for line in (logs or []):
            if isinstance(line, dict):
                emit(line.get('level', 'info'), line.get('msg') or line.get('message') or '')
                continue
            m = re.match(r'^\[(\w+)\]\s*(.*)$', line)
            if m:
                emit(m.group(1).lower(), m.group(2))
            elif line:
                emit('info', line)
        return eng, steps, logs, bool(ok)
    if isinstance(r, dict):
        steps = r.get('steps', [])
        for s in steps:
            level = 'success' if s.get('success') else 'error'
            emit(level, f"{s.get('name')}: {s.get('detail', '')}")
        return eng, steps, [], bool(r.get('success'))
    return eng, [], [str(r)], True


def _run_switchover_sync(cfg, force=False, dry_run=False):
    """同步执行切换（JSON 返回），供统一 POST /api/db/switchover/<id> 使用"""
    db_type = cfg.db_type
    eng_cfg_base = _build_repair_engine_cfg(cfg, db_type) or {}
    eng_cfg_base.update({
        'max_delay': cfg.max_delay, 'wait_timeout': cfg.wait_timeout,
        'skip_delay_check': cfg.skip_delay_check, 'skip_consistency_check': cfg.skip_consistency_check,
        'force_switchover': force or cfg.force_switchover, 'auto_rollback': cfg.auto_rollback,
        'dry_run': dry_run, 'connect_timeout': 5,
    })
    for f in ('mysql_subtype', 'oracle_subtype', 'dameng_subtype', 'kingbase_subtype',
              'opengauss_subtype', 'oceanbase_subtype', 'tidb_subtype', 'mongodb_subtype', 'highgo_subtype'):
        v = getattr(cfg, f, None)
        if v:
            eng_cfg_base[f] = v
    logs = []
    def _emit(level, msg):
        logs.append({'level': level, 'msg': msg})
    try:
        eng, steps, raw_logs, ok = _run_db_engine(cfg, db_type, eng_cfg_base, force, dry_run, 'node2', _emit)
        return {
            'ok': ok, 'db_type': db_type, 'steps': steps or [],
            'logs': logs or [{'level': 'info', 'msg': l} for l in raw_logs],
        }
    except Exception as e:
        return {'ok': False, 'error': str(e), 'logs': logs}


@app.route('/api/llm-configs', methods=['GET'])
@login_required
def api_llm_configs():
    configs = LLMConfig.query.order_by(LLMConfig.created_at.desc()).all()
    return jsonify([c.to_dict() for c in configs])


@app.route('/api/llm-configs/<int:cfg_id>', methods=['GET'])
@login_required
def api_llm_config(cfg_id):
    cfg = db.session.get(LLMConfig, cfg_id)
    if not cfg:
        return jsonify({'ok': False, 'error': '配置不存在'}), 404
    return jsonify({'ok': True, 'config': cfg.to_dict()})


@app.route('/api/llm-configs', methods=['POST'])
@login_required
def api_create_llm():
    data = request.get_json()
    try:
        temperature = max(0.0, min(2.0, float(data.get('temperature', 0.7))))
    except (TypeError, ValueError):
        temperature = 0.7
    try:
        max_tokens = int(data.get('max_tokens', 4096))
    except (TypeError, ValueError):
        max_tokens = 4096
    cfg = LLMConfig(
        name=data.get('name', ''),
        provider=data.get('provider', 'openai'),
        model_name=data.get('model_name', 'gpt-3.5-turbo'),
        api_base=data.get('api_base', ''),
        api_key=data.get('api_key', ''),
        temperature=temperature,
        max_tokens=max_tokens,
        is_active=data.get('is_active', False),
        description=data.get('description', ''),
        extra_headers=data.get('extra_headers', ''),
    )
    if cfg.is_active:
        LLMConfig.query.filter_by(is_active=True).update({'is_active': False})
    db.session.add(cfg)
    db.session.commit()
    return jsonify({'ok': True, 'id': cfg.id, 'config': cfg.to_dict()})


@app.route('/api/llm-configs/<int:cfg_id>', methods=['PUT'])
@login_required
def api_update_llm(cfg_id):
    cfg = db.session.get(LLMConfig, cfg_id)
    if not cfg:
        return jsonify({'ok': False, 'error': '配置不存在'}), 404
    data = request.get_json()
    for field in ['name', 'provider', 'model_name', 'api_base',
                  'is_active', 'description', 'extra_headers']:
        if field in data:
            setattr(cfg, field, data[field])
    if 'temperature' in data:
        try:
            cfg.temperature = max(0.0, min(2.0, float(data['temperature'])))
        except (TypeError, ValueError):
            pass
    if 'max_tokens' in data:
        try:
            cfg.max_tokens = int(data['max_tokens'])
        except (TypeError, ValueError):
            pass
    if data.get('api_key') and data['api_key'] != '***':
        cfg.api_key = data['api_key']
    if data.get('is_active'):
        LLMConfig.query.filter(LLMConfig.id != cfg.id, LLMConfig.is_active == True)\
            .update({'is_active': False})
    db.session.commit()
    return jsonify({'ok': True, 'config': cfg.to_dict()})


@app.route('/api/llm-configs/<int:cfg_id>', methods=['DELETE'])
@login_required
def api_delete_llm(cfg_id):
    cfg = db.session.get(LLMConfig, cfg_id)
    if not cfg:
        return jsonify({'ok': False, 'error': '配置不存在'}), 404
    db.session.delete(cfg)
    db.session.commit()
    return jsonify({'ok': True})


@app.route('/api/llm-configs/<int:cfg_id>/test', methods=['POST'])
@login_required
def api_test_llm(cfg_id):
    cfg = db.session.get(LLMConfig, cfg_id)
    if not cfg:
        return jsonify({'ok': False, 'error': '配置不存在'}), 404
    if not AI_AVAILABLE or not ai_assistant:
        return jsonify({'ok': False, 'error': 'AI 模块未加载'}), 500
    try:
        llm_config = {
            'api_base': cfg.api_base,
            'api_key': cfg.api_key,
            'model_name': cfg.model_name,
            'temperature': cfg.temperature,
            'max_tokens': cfg.max_tokens,
            'extra_headers': cfg.extra_headers,
        }
        start = datetime.now()
        resp = ai_assistant._call_llm(
            llm_config,
            [{'role': 'user', 'content': '回复"OK"'}]
        )
        elapsed = (datetime.now() - start).total_seconds()
        content = resp['choices'][0]['message'].get('content', '') if 'choices' in resp else str(resp)
        return jsonify({'ok': True, 'latency_seconds': round(elapsed, 2), 'reply': content[:100]})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


# ========== AI 对话 API ==========

@app.route('/api/ai/chat', methods=['POST'])
@login_required
def api_ai_chat():
    if not AI_AVAILABLE:
        return jsonify({'ok': False, 'error': 'AI 模块未加载'}), 500
    cfg = LLMConfig.query.filter_by(is_active=True).first()
    if not cfg:
        return jsonify({'ok': False, 'error': '未配置活跃的大模型'}), 500
    data = request.get_json()
    session_id = data.get('session_id', 'default')
    try:
        llm_config = {
            'api_base': cfg.api_base,
            'api_key': cfg.api_key,
            'model_name': cfg.model_name,
            'temperature': cfg.temperature,
            'max_tokens': cfg.max_tokens,
            'extra_headers': cfg.extra_headers,
        }
        result = ai_assistant.chat(session_id, data.get('message', ''), llm_config=llm_config)
        return jsonify({'ok': True, 'reply': result.get('reply', ''), 'session_id': session_id})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@app.route('/api/ai/configs', methods=['GET'])
@login_required
def api_ai_list_configs():
    configs = SwitchoverConfig.query.all()
    result = []
    for c in configs:
        item = {'id': c.id, 'db_type': c.db_type, 'name': c.name}
        if c.db_type == 'oracle':
            if (c.oracle_subtype or 'adg') == 'rac':
                item['master_host'] = c.rac_node1_host
                item['master_port'] = 1521
                item['slave_host'] = c.rac_node2_host
                item['slave_port'] = 1521
            else:
                item['master_host'] = c.oracle_primary_host
                item['master_port'] = c.oracle_primary_port
                item['slave_host'] = c.oracle_standby_host
                item['slave_port'] = c.oracle_standby_port
        elif c.db_type == 'postgresql':
            item['master_host'] = c.pg_primary_host
            item['master_port'] = c.pg_primary_port
            item['slave_host'] = c.pg_standby_host
            item['slave_port'] = c.pg_standby_port
        elif c.db_type == 'redis':
            item['master_host'] = c.redis_master_host or ''
            item['master_port'] = c.redis_master_port or 6379
            item['slave_host'] = c.redis_slave_host or ''
            item['slave_port'] = c.redis_slave_port or 6379
        else:
            item['master_host'] = c.master_host
            item['master_port'] = c.master_port
            item['slave_host'] = c.slave_host
            item['slave_port'] = c.slave_port
        result.append(item)
    return jsonify(result)


# ========== 健康检查 ==========

@app.route('/api/ping')
def api_ping():
    return jsonify({
        'ok': True, 'time': datetime.now().isoformat(),
        'oracle_available': ORACLE_AVAILABLE,
        'pg_available': PG_ENABLED,
        'redis_available': REDIS_ENABLED,
        'rac_available': RAC_AVAILABLE,
        'dm_available': DM_AVAILABLE,
        'kb_available': KB_AVAILABLE,
        'opengauss_available': OG_AVAILABLE,
        'oceanbase_available': OB_AVAILABLE,
        'tidb_available': TIDB_AVAILABLE,
        'mongodb_available': MONGO_AVAILABLE,
        'highgo_available': HG_AVAILABLE,
        'middleware_available': MW_AVAILABLE,
        'ai_available': AI_AVAILABLE,
    })


# ========== 启动入口 ==========

if __name__ == '__main__':
    init_db()
    # 安全修复：生产环境不应开启 debug 模式（会暴露调试器PIN和堆栈信息）
    debug_mode = os.environ.get('FLASK_DEBUG', '0') == '1'
    app.run(debug=debug_mode, host='0.0.0.0', port=5000, threaded=True)
