"""验证 RAC/DSC 配置预选时切换演练页自动切到对应数据库类型与集群方式"""
import urllib.request, urllib.parse, http.cookiejar, re

cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
opener.open('http://127.0.0.1:5000/login',
           data=urllib.parse.urlencode({'username':'admin','password':'admin123'}).encode(),
           timeout=10)

def active_primary_tab(body):
    m = re.search(r'id="swDbTypeTabs"[^>]*>.*?class="db-type-tab[^"]* active"[^>]*data-type="(\w+)"', body, re.S)
    return m.group(1) if m else None

def active_subtype_tab(body):
    m = re.search(r'id="swSubtypeTabs"[^>]*>.*?class="db-type-tab[^"]* active"[^>]*data-subtype="([^"]*)"', body, re.S)
    return m.group(1) if m else None

# 场景 1: 预选 Oracle RAC (id=5)
body = opener.open('http://127.0.0.1:5000/switchover?config_id=5', timeout=10).read().decode('utf-8','ignore')
print('--- 场景 1: config_id=5 (Oracle RAC) ---')
print(f'  一级 tab db-type = {active_primary_tab(body)} (expected: oracle)')
print(f'  二级 tab subtype = {active_subtype_tab(body)} (expected: rac)')
m = re.search(r'<option[^>]+value="5"[^>]+selected', body)
print(f'  option id=5 selected: {bool(m)}')

# 场景 2: 预选 MySQL 主从 (id=1)
body = opener.open('http://127.0.0.1:5000/switchover?config_id=1', timeout=10).read().decode('utf-8','ignore')
print('\n--- 场景 2: config_id=1 (MySQL 主从) ---')
print(f'  一级 tab db-type = {active_primary_tab(body)} (expected: mysql)')
print(f'  二级 tab subtype = {active_subtype_tab(body)} (expected: replica)')

# 场景 3: 预选 达梦 DSC (id=11)
body = opener.open('http://127.0.0.1:5000/switchover?config_id=11', timeout=10).read().decode('utf-8','ignore')
print('\n--- 场景 3: config_id=11 (达梦 DSC) ---')
print(f'  一级 tab db-type = {active_primary_tab(body)} (expected: dameng)')
print(f'  二级 tab subtype = {active_subtype_tab(body)} (expected: dsc)')

print('\n测试完成。')
