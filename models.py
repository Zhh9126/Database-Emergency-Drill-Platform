import uuid
from datetime import datetime, timezone
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()


def gen_task_id():
    return uuid.uuid4().hex[:12]


# ========== 用户模型 ==========

class User(db.Model):
    __tablename__ = 'app_user'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(256), nullable=False)
    role = db.Column(db.String(20), nullable=False, default='user')  # 'admin' | 'user'
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    def set_password(self, password: str):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    def to_dict(self):
        return {
            'id': self.id,
            'username': self.username,
            'role': self.role,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }


class SwitchoverConfig(db.Model):
    __tablename__ = 'switchover_config'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    db_type = db.Column(db.String(20), nullable=False, default='mysql')
    # mysql / oracle / postgresql / redis / dameng / kingbase / opengauss / oceanbase / tidb / mongodb / highgo
    mysql_subtype = db.Column(db.String(20), default='replica')  # 'replica' | 'mgr_single' | 'mgr_multi'
    oracle_subtype = db.Column(db.String(10), default='adg')  # 'adg' | 'rac'
    dameng_subtype = db.Column(db.String(10), default='standby')  # 'standby' | 'dsc'
    kingbase_subtype = db.Column(db.String(10), default='standby')  # 'standby' | 'kdc'
    opengauss_subtype = db.Column(db.String(10), default='standby')  # 'standby' | 'cluster'
    oceanbase_subtype = db.Column(db.String(10), default='standby')  # 'standby'(主备租户) | 'cluster'(集群关停)
    tidb_subtype = db.Column(db.String(10), default='standby')  # 'standby'(主备集群) | 'cluster'(调度切换)
    mongodb_subtype = db.Column(db.String(10), default='replica')  # 'replica'(副本集切换) | 'cluster'(分片集群)
    highgo_subtype = db.Column(db.String(10), default='standby')  # 'standby'(主备切换)
    name = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text, default='')

    # ---- MySQL 专用 ----
    mysql_user = db.Column(db.String(120), default='root')      # 主库用户
    mysql_pass = db.Column(db.String(255), default='')         # 主库密码
    master_host = db.Column(db.String(255), default='')
    master_port = db.Column(db.Integer, default=3306)
    slave_host = db.Column(db.String(255), default='')
    slave_port = db.Column(db.Integer, default=3306)
    mysql_slave_user = db.Column(db.String(120), default='')    # 从库独立账号（默认与主库一致）
    mysql_slave_pass = db.Column(db.String(255), default='')    # 从库独立密码
    mysql_repl_user = db.Column(db.String(120), default='repl') # 复制账号（创建时使用，可配置）
    mysql_repl_pass = db.Column(db.String(255), default='')     # 复制账号密码（可配置）

    # ---- Oracle ADG 专用 ----
    oracle_user = db.Column(db.String(120), default='sys')      # 主库用户
    oracle_pass = db.Column(db.String(255), default='')         # 主库密码
    oracle_primary_host = db.Column(db.String(255), default='')
    oracle_primary_port = db.Column(db.Integer, default=1521)
    oracle_primary_sid = db.Column(db.String(120), default='')
    oracle_standby_host = db.Column(db.String(255), default='')
    oracle_standby_port = db.Column(db.Integer, default=1521)
    oracle_standby_sid = db.Column(db.String(120), default='')
    oracle_connect_mode = db.Column(db.String(20), default='SYSDBA')
    oracle_standby_user = db.Column(db.String(120), default='')  # 备库独立账号
    oracle_standby_pass = db.Column(db.String(255), default='')  # 备库独立密码

    # ---- PostgreSQL 专用 ----
    pg_user = db.Column(db.String(120), default='postgres')    # 主库用户
    pg_pass = db.Column(db.String(255), default='')            # 主库密码
    pg_primary_host = db.Column(db.String(255), default='')
    pg_primary_port = db.Column(db.Integer, default=5432)
    pg_standby_host = db.Column(db.String(255), default='')
    pg_standby_port = db.Column(db.Integer, default=5432)
    pg_standby_user = db.Column(db.String(120), default='')   # 备库独立账号
    pg_standby_pass = db.Column(db.String(255), default='')   # 备库独立密码

    # ---- Redis 主从（不使用 Sentinel） ----
    redis_master_host = db.Column(db.String(255), default='')
    redis_master_port = db.Column(db.Integer, default=6379)
    redis_master_user = db.Column(db.String(120), default='')
    redis_master_pass = db.Column(db.String(255), default='')
    redis_slave_host = db.Column(db.String(255), default='')
    redis_slave_port = db.Column(db.Integer, default=6379)
    redis_slave_user = db.Column(db.String(120), default='')
    redis_slave_pass = db.Column(db.String(255), default='')

    # ---- Oracle RAC 专用 ----
    rac_user = db.Column(db.String(120), default='oracle')     # 节点1用户（复用 oracle_user）
    rac_pass = db.Column(db.String(255), default='')           # 节点1密码（复用 oracle_pass）
    rac_node1_host = db.Column(db.String(255), default='')
    rac_node1_sid = db.Column(db.String(120), default='')
    rac_node2_host = db.Column(db.String(255), default='')
    rac_node2_sid = db.Column(db.String(120), default='')
    rac_service_name = db.Column(db.String(128), default='')
    rac_node2_user = db.Column(db.String(120), default='')    # 节点2独立账号
    rac_node2_pass = db.Column(db.String(255), default='')    # 节点2独立密码

    # ---- 达梦数据库 (Dameng) ----
    # 主备模式 (dameng_subtype='standby')
    dm_user = db.Column(db.String(120), default='SYSDBA')    # 主库用户
    dm_pass = db.Column(db.String(255), default='')           # 主库密码
    dm_primary_host = db.Column(db.String(255), default='')
    dm_primary_port = db.Column(db.Integer, default=5236)
    dm_primary_sid = db.Column(db.String(120), default='')
    dm_standby_host = db.Column(db.String(255), default='')
    dm_standby_port = db.Column(db.Integer, default=5236)
    dm_standby_sid = db.Column(db.String(120), default='')
    dm_standby_user = db.Column(db.String(120), default='')   # 备库独立账号
    dm_standby_pass = db.Column(db.String(255), default='')   # 备库独立密码
    # DSC 集群 (dameng_subtype='dsc') — 服务关停测试
    dm_dsc_user = db.Column(db.String(120), default='SYSDBA')  # 节点1账号
    dm_dsc_pass = db.Column(db.String(255), default='')
    dm_dsc_node1_host = db.Column(db.String(255), default='')
    dm_dsc_node1_port = db.Column(db.Integer, default=5236)
    dm_dsc_node2_host = db.Column(db.String(255), default='')
    dm_dsc_node2_port = db.Column(db.Integer, default=5236)
    dm_dsc_service_name = db.Column(db.String(128), default='DSC_SERVICE')
    dm_dsc_node2_user = db.Column(db.String(120), default='')  # 节点2独立账号
    dm_dsc_node2_pass = db.Column(db.String(255), default='')  # 节点2独立密码

    # ---- 金仓数据库 (KingbaseES) ----
    # 主备模式 (kingbase_subtype='standby')
    kb_user = db.Column(db.String(120), default='SYSTEM')    # 主库用户
    kb_pass = db.Column(db.String(255), default='')           # 主库密码
    kb_primary_host = db.Column(db.String(255), default='')
    kb_primary_port = db.Column(db.Integer, default=54321)
    kb_standby_host = db.Column(db.String(255), default='')
    kb_standby_port = db.Column(db.Integer, default=54321)
    kb_standby_user = db.Column(db.String(120), default='')   # 备库独立账号
    kb_standby_pass = db.Column(db.String(255), default='')   # 备库独立密码
    # KDC 集群 (kingbase_subtype='kdc') — 服务关停测试
    kb_kdc_user = db.Column(db.String(120), default='SYSTEM')  # 节点1账号
    kb_kdc_pass = db.Column(db.String(255), default='')
    kb_kdc_node1_host = db.Column(db.String(255), default='')
    kb_kdc_node1_port = db.Column(db.Integer, default=54321)
    kb_kdc_node2_host = db.Column(db.String(255), default='')
    kb_kdc_node2_port = db.Column(db.Integer, default=54321)
    kb_kdc_service_name = db.Column(db.String(128), default='KDC_SERVICE')
    kb_kdc_node2_user = db.Column(db.String(120), default='')  # 节点2独立账号
    kb_kdc_node2_pass = db.Column(db.String(255), default='')  # 节点2独立密码

    # ---- openGauss / GaussDB ----
    og_user = db.Column(db.String(120), default='gaussdb')   # 主库用户
    og_pass = db.Column(db.String(255), default='')
    og_primary_host = db.Column(db.String(255), default='')
    og_primary_port = db.Column(db.Integer, default=5432)
    og_standby_host = db.Column(db.String(255), default='')
    og_standby_port = db.Column(db.Integer, default=5432)
    og_standby_user = db.Column(db.String(120), default='')   # 备库独立账号
    og_standby_pass = db.Column(db.String(255), default='')
    og_db_name = db.Column(db.String(120), default='postgres')
    # 集群模式额外字段
    og_cluster_node1_host = db.Column(db.String(255), default='')
    og_cluster_node1_port = db.Column(db.Integer, default=5432)
    og_cluster_node2_host = db.Column(db.String(255), default='')
    og_cluster_node2_port = db.Column(db.Integer, default=5432)

    # ---- OceanBase ----
    ob_user = db.Column(db.String(120), default='root')
    ob_pass = db.Column(db.String(255), default='')
    ob_primary_host = db.Column(db.String(255), default='')
    ob_primary_port = db.Column(db.Integer, default=2881)
    ob_standby_host = db.Column(db.String(255), default='')
    ob_standby_port = db.Column(db.Integer, default=2881)
    ob_standby_user = db.Column(db.String(120), default='')
    ob_standby_pass = db.Column(db.String(255), default='')
    ob_tenant_name = db.Column(db.String(120), default='')
    # 集群模式额外字段
    ob_cluster_node1_host = db.Column(db.String(255), default='')
    ob_cluster_node1_port = db.Column(db.Integer, default=2881)
    ob_cluster_node2_host = db.Column(db.String(255), default='')
    ob_cluster_node2_port = db.Column(db.Integer, default=2881)

    # ---- TiDB ----
    tidb_user = db.Column(db.String(120), default='root')
    tidb_pass = db.Column(db.String(255), default='')
    tidb_primary_host = db.Column(db.String(255), default='')    # PD / TiDB Server 节点1
    tidb_primary_port = db.Column(db.Integer, default=4000)
    tidb_standby_host = db.Column(db.String(255), default='')    # PD / TiDB Server 节点2
    tidb_standby_port = db.Column(db.Integer, default=4000)
    tidb_standby_user = db.Column(db.String(120), default='')
    tidb_standby_pass = db.Column(db.String(255), default='')
    tidb_db_name = db.Column(db.String(120), default='test')
    tidb_pd_port = db.Column(db.Integer, default=2379)           # PD API 端口
    # 集群模式额外字段
    tidb_cluster_node1_host = db.Column(db.String(255), default='')
    tidb_cluster_node1_port = db.Column(db.Integer, default=4000)
    tidb_cluster_node2_host = db.Column(db.String(255), default='')
    tidb_cluster_node2_port = db.Column(db.Integer, default=4000)

    # ---- MongoDB ----
    mongo_user = db.Column(db.String(120), default='')
    mongo_pass = db.Column(db.String(255), default='')
    mongo_primary_host = db.Column(db.String(255), default='')   # Primary 节点
    mongo_primary_port = db.Column(db.Integer, default=27017)
    mongo_standby_host = db.Column(db.String(255), default='')   # Secondary 节点
    mongo_standby_port = db.Column(db.Integer, default=27017)
    mongo_replset_name = db.Column(db.String(120), default='rs0')  # 副本集名称
    mongo_auth_db = db.Column(db.String(120), default='admin')     # 认证数据库
    # 分片集群模式额外字段
    mongo_cluster_node1_host = db.Column(db.String(255), default='')
    mongo_cluster_node1_port = db.Column(db.Integer, default=27017)
    mongo_cluster_node2_host = db.Column(db.String(255), default='')
    mongo_cluster_node2_port = db.Column(db.Integer, default=27017)

    # ---- 瀚高 (HighGo) ----
    hg_user = db.Column(db.String(120), default='highgo')
    hg_pass = db.Column(db.String(255), default='')
    hg_primary_host = db.Column(db.String(255), default='')
    hg_primary_port = db.Column(db.Integer, default=5866)
    hg_standby_host = db.Column(db.String(255), default='')
    hg_standby_port = db.Column(db.Integer, default=5866)
    hg_standby_user = db.Column(db.String(120), default='')
    hg_standby_pass = db.Column(db.String(255), default='')
    hg_db_name = db.Column(db.String(120), default='highgo')

    # ---- 通用切换参数 ----
    max_delay = db.Column(db.Integer, default=60)
    wait_timeout = db.Column(db.Integer, default=300)
    skip_delay_check = db.Column(db.Boolean, default=False)
    skip_consistency_check = db.Column(db.Boolean, default=False)
    force_switchover = db.Column(db.Boolean, default=False)
    auto_rollback = db.Column(db.Boolean, default=True)

    # ======== 中间件字段（middleware_type + mw_subtype） ========
    middleware_type = db.Column(db.String(30), default='')  # nginx/tomcat/kafka/zookeeper/elasticsearch/rabbitmq
    mw_subtype = db.Column(db.String(20), default='active_standby')  # active_standby / cluster
    mw_primary_host = db.Column(db.String(255), default='')
    mw_primary_port = db.Column(db.Integer, default=80)
    mw_secondary_host = db.Column(db.String(255), default='')
    mw_secondary_port = db.Column(db.Integer, default=80)
    mw_cluster_node1_host = db.Column(db.String(255), default='')
    mw_cluster_node1_port = db.Column(db.Integer, default=80)
    mw_cluster_node2_host = db.Column(db.String(255), default='')
    mw_cluster_node2_port = db.Column(db.Integer, default=80)
    mw_cluster_node3_host = db.Column(db.String(255), default='')  # ES/ZK 常用三节点
    mw_cluster_node3_port = db.Column(db.Integer, default=80)
    # 通用认证
    mw_user = db.Column(db.String(120), default='')
    mw_pass = db.Column(db.String(255), default='')
    # 各中间件特有
    mw_vip = db.Column(db.String(255), default='')           # Nginx 虚拟 IP（Keepalived）
    mw_api_port = db.Column(db.Integer, default=0)           # 管理端口（Kafka=9092, ZK=2181, ES=9200, RabbitMQ=15672）
    mw_cluster_name = db.Column(db.String(255), default='')  # ES cluster.name / RabbitMQ cluster name / Kafka group.id
    mw_extra = db.Column(db.Text, default='')                # 扩展 JSON（自定义参数）

    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))

    def to_dict(self):
        return {
            'id': self.id, 'db_type': self.db_type, 'mysql_subtype': self.mysql_subtype,
            'oracle_subtype': self.oracle_subtype,
            'dameng_subtype': self.dameng_subtype, 'kingbase_subtype': self.kingbase_subtype,
            'name': self.name, 'description': self.description,
            'mysql_user': self.mysql_user, 'mysql_pass': '***' if self.mysql_pass else '',
            'master_host': self.master_host, 'master_port': self.master_port,
            'slave_host': self.slave_host, 'slave_port': self.slave_port,
            'mysql_slave_user': self.mysql_slave_user or self.mysql_user,
            'mysql_slave_pass': '***' if (self.mysql_slave_pass or self.mysql_pass) else '',
            'mysql_repl_user': self.mysql_repl_user,
            'mysql_repl_pass': '***' if self.mysql_repl_pass else '',
            'oracle_user': self.oracle_user, 'oracle_pass': '***' if self.oracle_pass else '',
            'oracle_primary_host': self.oracle_primary_host,
            'oracle_primary_port': self.oracle_primary_port,
            'oracle_primary_sid': self.oracle_primary_sid,
            'oracle_standby_host': self.oracle_standby_host,
            'oracle_standby_port': self.oracle_standby_port,
            'oracle_standby_sid': self.oracle_standby_sid,
            'oracle_standby_user': self.oracle_standby_user or self.oracle_user,
            'oracle_standby_pass': '***' if (self.oracle_standby_pass or self.oracle_pass) else '',
            'oracle_connect_mode': self.oracle_connect_mode,
            'pg_user': self.pg_user, 'pg_pass': '***' if self.pg_pass else '',
            'pg_primary_host': self.pg_primary_host,
            'pg_primary_port': self.pg_primary_port,
            'pg_standby_host': self.pg_standby_host,
            'pg_standby_port': self.pg_standby_port,
            'pg_standby_user': self.pg_standby_user or self.pg_user,
            'pg_standby_pass': '***' if (self.pg_standby_pass or self.pg_pass) else '',
            'redis_master_host': self.redis_master_host,
            'redis_master_port': self.redis_master_port,
            'redis_master_user': self.redis_master_user,
            'redis_master_pass': '***' if self.redis_master_pass else '',
            'redis_slave_host': self.redis_slave_host,
            'redis_slave_port': self.redis_slave_port,
            'redis_slave_user': self.redis_slave_user or self.redis_master_user,
            'redis_slave_pass': '***' if (self.redis_slave_pass or self.redis_master_pass) else '',
            # 达梦主备
            'dm_user': self.dm_user, 'dm_pass': '***' if self.dm_pass else '',
            'dm_primary_host': self.dm_primary_host, 'dm_primary_port': self.dm_primary_port,
            'dm_primary_sid': self.dm_primary_sid,
            'dm_standby_host': self.dm_standby_host, 'dm_standby_port': self.dm_standby_port,
            'dm_standby_sid': self.dm_standby_sid,
            'dm_standby_user': self.dm_standby_user or self.dm_user,
            'dm_standby_pass': '***' if (self.dm_standby_pass or self.dm_pass) else '',
            # 达梦 DSC
            'dm_dsc_user': self.dm_dsc_user, 'dm_dsc_pass': '***' if self.dm_dsc_pass else '',
            'dm_dsc_node1_host': self.dm_dsc_node1_host, 'dm_dsc_node1_port': self.dm_dsc_node1_port,
            'dm_dsc_node2_host': self.dm_dsc_node2_host, 'dm_dsc_node2_port': self.dm_dsc_node2_port,
            'dm_dsc_service_name': self.dm_dsc_service_name,
            'dm_dsc_node2_user': self.dm_dsc_node2_user or self.dm_dsc_user,
            'dm_dsc_node2_pass': '***' if (self.dm_dsc_node2_pass or self.dm_dsc_pass) else '',
            # 金仓主备
            'kb_user': self.kb_user, 'kb_pass': '***' if self.kb_pass else '',
            'kb_primary_host': self.kb_primary_host, 'kb_primary_port': self.kb_primary_port,
            'kb_standby_host': self.kb_standby_host, 'kb_standby_port': self.kb_standby_port,
            'kb_standby_user': self.kb_standby_user or self.kb_user,
            'kb_standby_pass': '***' if (self.kb_standby_pass or self.kb_pass) else '',
            # 金仓 KDC
            'kb_kdc_user': self.kb_kdc_user, 'kb_kdc_pass': '***' if self.kb_kdc_pass else '',
            'kb_kdc_node1_host': self.kb_kdc_node1_host, 'kb_kdc_node1_port': self.kb_kdc_node1_port,
            'kb_kdc_node2_host': self.kb_kdc_node2_host, 'kb_kdc_node2_port': self.kb_kdc_node2_port,
            'kb_kdc_service_name': self.kb_kdc_service_name,
            'kb_kdc_node2_user': self.kb_kdc_node2_user or self.kb_kdc_user,
            'kb_kdc_node2_pass': '***' if (self.kb_kdc_node2_pass or self.kb_kdc_pass) else '',
            # Oracle RAC
            'rac_user': self.rac_user, 'rac_pass': '***' if self.rac_pass else '',
            'rac_node1_host': self.rac_node1_host, 'rac_node1_sid': self.rac_node1_sid,
            'rac_node2_host': self.rac_node2_host, 'rac_node2_sid': self.rac_node2_sid,
            'rac_service_name': self.rac_service_name,
            'rac_node2_user': self.rac_node2_user or self.rac_user,
            'rac_node2_pass': '***' if (self.rac_node2_pass or self.rac_pass) else '',
            # openGauss / GaussDB
            'opengauss_subtype': self.opengauss_subtype,
            'og_user': self.og_user, 'og_pass': '***' if self.og_pass else '',
            'og_primary_host': self.og_primary_host, 'og_primary_port': self.og_primary_port,
            'og_standby_host': self.og_standby_host, 'og_standby_port': self.og_standby_port,
            'og_standby_user': self.og_standby_user or self.og_user,
            'og_standby_pass': '***' if (self.og_standby_pass or self.og_pass) else '',
            'og_db_name': self.og_db_name,
            'og_cluster_node1_host': self.og_cluster_node1_host, 'og_cluster_node1_port': self.og_cluster_node1_port,
            'og_cluster_node2_host': self.og_cluster_node2_host, 'og_cluster_node2_port': self.og_cluster_node2_port,
            # OceanBase
            'oceanbase_subtype': self.oceanbase_subtype,
            'ob_user': self.ob_user, 'ob_pass': '***' if self.ob_pass else '',
            'ob_primary_host': self.ob_primary_host, 'ob_primary_port': self.ob_primary_port,
            'ob_standby_host': self.ob_standby_host, 'ob_standby_port': self.ob_standby_port,
            'ob_standby_user': self.ob_standby_user or self.ob_user,
            'ob_standby_pass': '***' if (self.ob_standby_pass or self.ob_pass) else '',
            'ob_tenant_name': self.ob_tenant_name,
            'ob_cluster_node1_host': self.ob_cluster_node1_host, 'ob_cluster_node1_port': self.ob_cluster_node1_port,
            'ob_cluster_node2_host': self.ob_cluster_node2_host, 'ob_cluster_node2_port': self.ob_cluster_node2_port,
            # TiDB
            'tidb_subtype': self.tidb_subtype,
            'tidb_user': self.tidb_user, 'tidb_pass': '***' if self.tidb_pass else '',
            'tidb_primary_host': self.tidb_primary_host, 'tidb_primary_port': self.tidb_primary_port,
            'tidb_standby_host': self.tidb_standby_host, 'tidb_standby_port': self.tidb_standby_port,
            'tidb_standby_user': self.tidb_standby_user or self.tidb_user,
            'tidb_standby_pass': '***' if (self.tidb_standby_pass or self.tidb_pass) else '',
            'tidb_db_name': self.tidb_db_name, 'tidb_pd_port': self.tidb_pd_port,
            'tidb_cluster_node1_host': self.tidb_cluster_node1_host, 'tidb_cluster_node1_port': self.tidb_cluster_node1_port,
            'tidb_cluster_node2_host': self.tidb_cluster_node2_host, 'tidb_cluster_node2_port': self.tidb_cluster_node2_port,
            # MongoDB
            'mongodb_subtype': self.mongodb_subtype,
            'mongo_user': self.mongo_user, 'mongo_pass': '***' if self.mongo_pass else '',
            'mongo_primary_host': self.mongo_primary_host, 'mongo_primary_port': self.mongo_primary_port,
            'mongo_standby_host': self.mongo_standby_host, 'mongo_standby_port': self.mongo_standby_port,
            'mongo_replset_name': self.mongo_replset_name, 'mongo_auth_db': self.mongo_auth_db,
            'mongo_cluster_node1_host': self.mongo_cluster_node1_host, 'mongo_cluster_node1_port': self.mongo_cluster_node1_port,
            'mongo_cluster_node2_host': self.mongo_cluster_node2_host, 'mongo_cluster_node2_port': self.mongo_cluster_node2_port,
            # 瀚高 (HighGo)
            'highgo_subtype': self.highgo_subtype,
            'hg_user': self.hg_user, 'hg_pass': '***' if self.hg_pass else '',
            'hg_primary_host': self.hg_primary_host, 'hg_primary_port': self.hg_primary_port,
            'hg_standby_host': self.hg_standby_host, 'hg_standby_port': self.hg_standby_port,
            'hg_standby_user': self.hg_standby_user or self.hg_user,
            'hg_standby_pass': '***' if (self.hg_standby_pass or self.hg_pass) else '',
            'hg_db_name': self.hg_db_name,
            'max_delay': self.max_delay, 'wait_timeout': self.wait_timeout,
            'skip_delay_check': self.skip_delay_check,
            'skip_consistency_check': self.skip_consistency_check,
            'force_switchover': self.force_switchover,
            'auto_rollback': self.auto_rollback,
            # 中间件
            'middleware_type': self.middleware_type, 'mw_subtype': self.mw_subtype,
            'mw_primary_host': self.mw_primary_host, 'mw_primary_port': self.mw_primary_port,
            'mw_secondary_host': self.mw_secondary_host, 'mw_secondary_port': self.mw_secondary_port,
            'mw_cluster_node1_host': self.mw_cluster_node1_host, 'mw_cluster_node1_port': self.mw_cluster_node1_port,
            'mw_cluster_node2_host': self.mw_cluster_node2_host, 'mw_cluster_node2_port': self.mw_cluster_node2_port,
            'mw_cluster_node3_host': self.mw_cluster_node3_host, 'mw_cluster_node3_port': self.mw_cluster_node3_port,
            'mw_user': self.mw_user, 'mw_pass': '***' if self.mw_pass else '',
            'mw_vip': self.mw_vip, 'mw_api_port': self.mw_api_port,
            'mw_cluster_name': self.mw_cluster_name, 'mw_extra': self.mw_extra,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }


class SwitchoverHistory(db.Model):
    __tablename__ = 'switchover_history'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    task_id = db.Column(db.String(32), unique=True, nullable=False, default=gen_task_id)
    db_type = db.Column(db.String(20), nullable=False, default='mysql')
    # mysql / oracle / postgresql / redis
    config_id = db.Column(db.Integer, db.ForeignKey('switchover_config.id'), nullable=True)
    config_name = db.Column(db.String(255), default='')

    master_host = db.Column(db.String(255))
    master_port = db.Column(db.Integer)
    slave_host = db.Column(db.String(255))
    slave_port = db.Column(db.Integer)
    new_master_host = db.Column(db.String(255))
    new_master_port = db.Column(db.Integer)
    new_slave_host = db.Column(db.String(255))
    new_slave_port = db.Column(db.Integer)

    status = db.Column(db.String(20), default='pending')
    triggered_by = db.Column(db.String(20), default='manual')

    # 集群关停测试时选择的目标节点（node1 / node2）
    target_node = db.Column(db.String(10), default='')

    oracle_primary_sid = db.Column(db.String(120), default='')
    oracle_standby_sid = db.Column(db.String(120), default='')

    steps = db.Column(db.JSON, default=list)
    full_log = db.Column(db.Text, default='')
    error_message = db.Column(db.Text, default='')

    start_time = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    end_time = db.Column(db.DateTime, nullable=True)
    duration_seconds = db.Column(db.Integer, default=0)

    config = db.relationship('SwitchoverConfig', backref='histories', lazy=True)

    def to_dict(self):
        d = {
            'id': self.id, 'task_id': self.task_id, 'db_type': self.db_type,
            'config_id': self.config_id, 'config_name': self.config_name,
            'master_host': self.master_host, 'master_port': self.master_port,
            'slave_host': self.slave_host, 'slave_port': self.slave_port,
            'new_master_host': self.new_master_host, 'new_master_port': self.new_master_port,
            'new_slave_host': self.new_slave_host, 'new_slave_port': self.new_slave_port,
            'status': self.status, 'triggered_by': self.triggered_by,
            'target_node': self.target_node,
            'oracle_primary_sid': self.oracle_primary_sid, 'oracle_standby_sid': self.oracle_standby_sid,
            'steps': self.steps, 'full_log': self.full_log, 'error_message': self.error_message,
            'duration_seconds': self.duration_seconds,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
        }
        # 通过关联配置补充子类型，便于前端识别 MGR/RAC/DSC/KDC 等
        cfg = self.config
        if cfg:
            d['mysql_subtype'] = cfg.mysql_subtype if cfg.db_type == 'mysql' else None
            d['oracle_subtype'] = cfg.oracle_subtype if cfg.db_type == 'oracle' else None
            d['dameng_subtype'] = cfg.dameng_subtype if cfg.db_type == 'dameng' else None
            d['kingbase_subtype'] = cfg.kingbase_subtype if cfg.db_type == 'kingbase' else None
        return d


class LLMConfig(db.Model):
    __tablename__ = 'llm_config'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(120), nullable=False)
    provider = db.Column(db.String(50), nullable=False)
    model_name = db.Column(db.String(120), nullable=False)
    api_base = db.Column(db.String(500), nullable=False)
    api_key = db.Column(db.String(500), nullable=False)
    temperature = db.Column(db.Float, default=0.7)
    max_tokens = db.Column(db.Integer, default=4096)
    is_active = db.Column(db.Boolean, default=False)
    description = db.Column(db.String(255), default='')
    extra_headers = db.Column(db.Text, default='')
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    def to_dict(self):
        return {
            'id': self.id, 'name': self.name, 'provider': self.provider,
            'model_name': self.model_name, 'api_base': self.api_base,
            'api_key': '***' if self.api_key else '', 'temperature': self.temperature,
            'max_tokens': self.max_tokens, 'is_active': self.is_active,
            'description': self.description, 'extra_headers': self.extra_headers,
        }