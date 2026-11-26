"""端到端测试：MGR 多主选择 node1 关停 + 报告下载"""
from app import app, db
from models import SwitchoverConfig, User, SwitchoverHistory

client = app.test_client()

with app.app_context():
    user = User.query.filter_by(username='admin').first()
    if not user:
        user = User(username='admin', role='admin', is_active=True)
        user.set_password('admin123')
        db.session.add(user)
        db.session.commit()
    uid, uname, urole = user.id, user.username, user.role
    SwitchoverConfig.query.filter(SwitchoverConfig.name.like('E2E-MGR%')).delete()
    db.session.commit()

with client.session_transaction() as sess:
    sess['user_id'] = uid
    sess['username'] = uname
    sess['role'] = urole

cfg_data = {
    'name': 'E2E-MGR多主-node1',
    'description': 'e2e test',
    'db_type': 'mysql',
    'mysql_subtype': 'mgr_multi',
    'mysql_user': 'root',
    'mysql_pass': 'pass',
    'master_host': '10.0.0.1',
    'master_port': 3306,
    'slave_host': '10.0.0.2',
    'slave_port': 3306,
}
resp = client.post('/api/configs', json=cfg_data)
assert resp.status_code == 200, resp.get_json()

with app.app_context():
    cfg = SwitchoverConfig.query.filter_by(name='E2E-MGR多主-node1').first()
    cfg_id = cfg.id

resp = client.get(f'/api/mysql/switchover/{cfg_id}/stream?dry_run=1&target_node=node1')
assert resp.status_code == 200
body = b''.join(resp.response)
text = body.decode('utf-8', errors='replace')
assert 'node1' in text or '节点1' in text, text[:500]

with app.app_context():
    hist = SwitchoverHistory.query.filter_by(config_id=cfg_id).order_by(SwitchoverHistory.id.desc()).first()
    assert hist is not None
    assert hist.target_node == 'node1', hist.target_node
    task_id = hist.task_id

resp = client.get(f'/api/history/{task_id}/report.pdf')
assert resp.status_code == 200 and len(resp.data) > 1000
resp = client.get(f'/api/history/{task_id}/report.docx')
assert resp.status_code == 200 and len(resp.data) > 1000

print('E2E 测试通过: task_id=', task_id, 'target_node=', hist.target_node)
