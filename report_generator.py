"""
应急演练平台 - 演练报告生成器
支持 PDF (fpdf2) 与 Word (python-docx) 两种格式。
"""
import os
import io
from datetime import datetime
from typing import Dict, Any, Optional

from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_LINE_SPACING
from fpdf import FPDF


# ---------------------------------------------------------------------------
# 通用辅助
# ---------------------------------------------------------------------------

def _safe_str(value: Any) -> str:
    """把任意值转为字符串，空值返回 '-'"""
    if value is None:
        return '-'
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    return str(value)


def _format_duration(start: Optional[datetime], end: Optional[datetime]) -> str:
    """计算并格式化耗时"""
    if not start or not end:
        return '-'
    try:
        # 兼容 ISO 字符串（来自 to_dict 的 isoformat）与 datetime 对象
        if isinstance(start, str):
            start = datetime.fromisoformat(start.replace('Z', '+00:00'))
        if isinstance(end, str):
            end = datetime.fromisoformat(end.replace('Z', '+00:00'))
        delta = end - start
        total_seconds = int(abs(delta.total_seconds()))
        if total_seconds < 60:
            return f'{total_seconds} 秒'
        minutes, seconds = divmod(total_seconds, 60)
        if minutes < 60:
            return f'{minutes} 分 {seconds} 秒'
        hours, minutes = divmod(minutes, 60)
        return f'{hours} 时 {minutes} 分 {seconds} 秒'
    except Exception:
        return '-'


def _step_verdict(step) -> tuple:
    """统一判定单个步骤的状态。

    兼容 4 种步骤格式：
      A: {name, status, detail}     —— MySQL/PG/Oracle RAC/通用引擎
      B: {name, success, detail}    —— 达梦/金仓/中间件（success 布尔）
      C: {progress, level, msg}     —— Redis（SSE 流转存）
      D: {step, name, status, ...}  —— Oracle RAC
    返回 (status_text, is_success, is_warn)：
      status_text ∈ {'成功','失败','警告','执行中','未知'}
    """
    st = step.get('status')
    lv = step.get('level')
    suc = step.get('success')

    is_warn = (st == 'warning' or lv == 'warning')
    # 显式状态优先
    if st == 'failed' or lv in ('error', 'failed', 'fail'):
        return '失败', False, False
    if st == 'success' or lv == 'success':
        return '成功', True, False
    if st == 'warning' or lv == 'warning':
        return '警告', True, True
    if st == 'running' or lv == 'running':
        return '执行中', False, False
    if suc is not None:
        # 布尔 success 字段
        if suc is True:
            return '成功', True, False
        if suc is False:
            return '失败', False, False
    return '成功', True, False


def _build_report_data(history: Dict[str, Any], config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """把 history dict + 可选 config dict 统一整理成报告数据"""
    cfg = config or {}
    db_type = history.get('db_type') or cfg.get('db_type') or '-'
    # 子类型按数据库类型取对应字段，避免把 MySQL 的 'replica' 串到 Redis 等
    _subtype_keys = {
        'mysql': ('mysql_subtype',),
        'oracle': ('oracle_subtype',),
        'dameng': ('dameng_subtype',),
        'kingbase': ('kingbase_subtype',),
        'opengauss': ('opengauss_subtype',),
        'oceanbase': ('oceanbase_subtype',),
        'tidb': ('tidb_subtype',),
        'mongodb': ('mongodb_subtype',),
        'highgo': ('highgo_subtype',),
        'postgresql': ('pg_subtype',),
        'redis': ('redis_subtype',),
    }
    subtype = '-'
    for k in _subtype_keys.get(db_type, ()):
        subtype = history.get(k) or cfg.get(k) or subtype
        if subtype and subtype != '-':
            break
    cluster_mode = history.get('cluster_mode') or cfg.get('cluster_mode') or '-'

    start_time = history.get('start_time')
    end_time = history.get('end_time')

    # 成功判定兼容两种数据来源：优先取 history['success']（若存在），
    # 否则从 status 字段推导（'success' == 成功；'running'/'failed' 视为失败）
    _success = history.get('success')
    if _success is None:
        _success = history.get('status') == 'success'

    # 汇总信息：显式 message/error 优先；为空时从步骤自动推导摘要
    steps = history.get('steps') or []
    message = history.get('message') or history.get('error_message') or history.get('error') or ''
    if not message and steps:
        def _step_msg(s):
            return s.get('msg') or s.get('detail') or s.get('message') or ''
        if _success:
            succ_msgs = [_step_msg(s) for s in steps
                         if _step_verdict(s)[1] and _step_msg(s)]
            message = (succ_msgs[-1] if succ_msgs else
                       (_step_msg(steps[-1]) if _step_msg(steps[-1]) else ''))
        else:
            err_msgs = [_step_msg(s) for s in steps
                        if not _step_verdict(s)[1] and _step_msg(s)]
            message = (err_msgs[0] if err_msgs else
                       (_step_msg(steps[-1]) if _step_msg(steps[-1]) else ''))

    # 主从主机展示：切换成功时优先展示真实的新主从（new_*），
    # 否则回退到配置定义的主从（master_host/slave_host）。
    # 注意：Redis/PG 等引擎在切换成功后通过 get_topology() 把真实角色写入
    # new_master_host/new_slave_host；失败或演练中止时 new_* 可能为空。
    if _success:
        master_disp = history.get('new_master_host') or history.get('master_host')
        slave_disp = history.get('new_slave_host') or history.get('slave_host')
    else:
        master_disp = history.get('master_host') or history.get('new_master_host')
        slave_disp = history.get('slave_host') or history.get('new_slave_host')

    return {
        'title': '数据库切换演练报告',
        'task_id': _safe_str(history.get('task_id')),
        'config_name': _safe_str(history.get('config_name') or cfg.get('name')),
        'db_type': _safe_str(db_type),
        'subtype': _safe_str(subtype),
        'cluster_mode': _safe_str(cluster_mode),
        'operator': _safe_str(history.get('operator') or history.get('username')),
        'start_time': _safe_str(start_time),
        'end_time': _safe_str(end_time),
        'duration': _format_duration(start_time, end_time),
        'success': _success,
        'status': _safe_str(history.get('status')),
        'message': _safe_str(message),
        'master_host': _safe_str(master_disp),
        'slave_host': _safe_str(slave_disp),
        'target_node': _safe_str(history.get('target_node') or '节点2'),
        'steps': history.get('steps') or [],
        'full_log': _safe_str(history.get('full_log') or history.get('logs')),
    }


# ---------------------------------------------------------------------------
# PDF 报告
# ---------------------------------------------------------------------------

class _ReportPDF(FPDF):
    def header(self):
        self.set_font('simhei', 'B', 16)
        self.set_text_color(33, 37, 41)
        self.cell(0, 10, '数据库切换演练报告', ln=True, align='C')
        self.ln(2)
        self.set_draw_color(0, 128, 128)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('simhei', '', 9)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f'第 {self.page_no()} 页', align='C')


def _register_chinese_font(pdf: FPDF):
    """尝试注册中文字体；若系统无中文字体，则 fallback 到内置 Helvetica 并忽略中文"""
    font_paths = [
        'C:/Windows/Fonts/simhei.ttf',
        'C:/Windows/Fonts/msyh.ttc',
        'C:/Windows/Fonts/msyhbd.ttc',
        'C:/Windows/Fonts/simsun.ttc',
        'C:/Windows/Fonts/simkai.ttf',
        '/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc',
        '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
    ]
    for path in font_paths:
        if os.path.exists(path):
            try:
                # 同一字体文件注册 normal / bold 两种风格
                pdf.add_font('simhei', '', path, uni=True)
                pdf.add_font('simhei', 'B', path, uni=True)
                return True
            except Exception:
                continue
    # fallback：使用 fpdf2 内置字体（中文会显示为乱码/方块，但不会抛异常）
    try:
        pdf.set_font('Helvetica', '', 12)
    except Exception:
        pdf.set_font('Arial', '', 12)
    return False


def generate_pdf(history: Dict[str, Any], config: Optional[Dict[str, Any]] = None) -> bytes:
    """生成 PDF 报告字节流"""
    data = _build_report_data(history, config)
    pdf = _ReportPDF()
    _register_chinese_font(pdf)
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)

    # 基本信息
    pdf.set_font('simhei', 'B', 12)
    pdf.set_text_color(0, 128, 128)
    pdf.cell(0, 8, '一、演练基本信息', ln=True)
    pdf.set_font('simhei', '', 11)
    pdf.set_text_color(33, 37, 41)

    info_items = [
        ('演练任务编号', data['task_id']),
        ('配置名称 / 业务系统', data['config_name']),
        ('数据库类型', f"{data['db_type']} / {data['subtype']}"),
        ('集群切换模式', data['cluster_mode']),
        ('执行人', data['operator']),
        ('开始时间', data['start_time']),
        ('结束时间', data['end_time']),
        ('总耗时', data['duration']),
    ]
    if data.get('target_node') and data['target_node'] != '-':
        info_items.append(('目标节点', data['target_node']))
    if data.get('master_host') and data['master_host'] != '-':
        info_items.append(('主库 / 新主库', data['master_host']))
    if data.get('slave_host') and data['slave_host'] != '-':
        info_items.append(('从库 / 新从库', data['slave_host']))

    for label, value in info_items:
        pdf.cell(50, 7, f'{label}：', ln=0)
        pdf.cell(0, 7, value, ln=1)
    pdf.ln(3)

    # 演练结果
    pdf.set_font('simhei', 'B', 12)
    pdf.set_text_color(0, 128, 128)
    pdf.cell(0, 8, '二、演练结果', ln=True)
    pdf.set_font('simhei', '', 11)
    if data['success']:
        pdf.set_text_color(40, 167, 69)
        pdf.cell(0, 7, '演练结果：通过', ln=True)
    else:
        pdf.set_text_color(220, 53, 69)
        pdf.cell(0, 7, '演练结果：失败', ln=True)
    pdf.set_text_color(33, 37, 41)
    pdf.multi_cell(0, 7, f"汇总信息：{data['message']}")
    pdf.ln(3)

    # 执行过程
    pdf.set_font('simhei', 'B', 12)
    pdf.set_text_color(0, 128, 128)
    pdf.cell(0, 8, '三、执行过程', ln=True)
    pdf.set_font('simhei', '', 10)
    pdf.set_text_color(33, 37, 41)

    steps = data['steps']
    if not steps:
        pdf.cell(0, 7, '无步骤记录', ln=True)
    else:
        for idx, step in enumerate(steps, 1):
            name = (step.get('name') or step.get('step')
                    or (step.get('msg') if isinstance(step, dict) else None)
                    or f'步骤 {idx}')
            status_text, ok, is_warn = _step_verdict(step)
            detail = _safe_str(step.get('detail') or step.get('message') or step.get('msg'))
            if is_warn:
                pdf.set_text_color(255, 193, 7)
            else:
                pdf.set_text_color(40, 167, 69) if ok else pdf.set_text_color(220, 53, 69)
            pdf.cell(0, 7, f"{idx}. {name} — {status_text}", ln=True)
            pdf.set_text_color(33, 37, 41)
            if detail and detail != '-':
                pdf.multi_cell(0, 6, f"    {detail}")

    pdf.ln(5)

    # 完整日志
    if data['full_log'] and data['full_log'] != '-':
        pdf.add_page()
        pdf.set_font('simhei', 'B', 12)
        pdf.set_text_color(0, 128, 128)
        pdf.cell(0, 8, '四、完整执行日志', ln=True)
        pdf.set_font('simhei', '', 9)
        pdf.set_text_color(33, 37, 41)
        for line in data['full_log'].splitlines():
            text = line if line.strip() else ' '
            pdf.set_x(pdf.l_margin)
            pdf.multi_cell(w=pdf.w - pdf.r_margin - pdf.l_margin, h=5, text=text)

    out = pdf.output()
    return out.encode('latin1') if isinstance(out, str) else out


# ---------------------------------------------------------------------------
# Word 报告
# ---------------------------------------------------------------------------

def generate_word(history: Dict[str, Any], config: Optional[Dict[str, Any]] = None) -> bytes:
    """生成 Word (.docx) 报告字节流"""
    data = _build_report_data(history, config)
    doc = Document()

    # 页面边距
    sections = doc.sections
    for section in sections:
        section.top_margin = Inches(1)
        section.bottom_margin = Inches(1)
        section.left_margin = Inches(1)
        section.right_margin = Inches(1)

    # 标题
    title = doc.add_heading('数据库切换演练报告', level=0)
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = title.runs[0]
    run.font.size = Pt(22)
    run.font.color.rgb = RGBColor(0, 128, 128)

    doc.add_paragraph()

    # 一、基本信息
    h1 = doc.add_heading('一、演练基本信息', level=1)
    h1.runs[0].font.color.rgb = RGBColor(0, 128, 128)

    table = doc.add_table(rows=1, cols=2)
    table.style = 'Light Grid Accent 1'
    hdr_cells = table.rows[0].cells
    hdr_cells[0].text = '项目'
    hdr_cells[1].text = '内容'

    info_items = [
        ('演练任务编号', data['task_id']),
        ('配置名称 / 业务系统', data['config_name']),
        ('数据库类型', f"{data['db_type']} / {data['subtype']}"),
        ('集群切换模式', data['cluster_mode']),
        ('执行人', data['operator']),
        ('开始时间', data['start_time']),
        ('结束时间', data['end_time']),
        ('总耗时', data['duration']),
    ]
    if data.get('target_node') and data['target_node'] != '-':
        info_items.append(('目标节点', data['target_node']))
    if data.get('master_host') and data['master_host'] != '-':
        info_items.append(('主库 / 新主库', data['master_host']))
    if data.get('slave_host') and data['slave_host'] != '-':
        info_items.append(('从库 / 新从库', data['slave_host']))

    for label, value in info_items:
        row_cells = table.add_row().cells
        row_cells[0].text = label
        row_cells[1].text = value

    doc.add_paragraph()

    # 二、演练结果
    h2 = doc.add_heading('二、演练结果', level=1)
    h2.runs[0].font.color.rgb = RGBColor(0, 128, 128)

    p = doc.add_paragraph()
    run = p.add_run(f"演练结果：{'通过' if data['success'] else '失败'}")
    run.bold = True
    run.font.size = Pt(12)
    run.font.color.rgb = RGBColor(40, 167, 69) if data['success'] else RGBColor(220, 53, 69)

    doc.add_paragraph(f"汇总信息：{data['message']}")

    # 三、执行过程
    h3 = doc.add_heading('三、执行过程', level=1)
    h3.runs[0].font.color.rgb = RGBColor(0, 128, 128)

    steps = data['steps']
    if not steps:
        doc.add_paragraph('无步骤记录')
    else:
        for idx, step in enumerate(steps, 1):
            name = (step.get('name') or step.get('step')
                    or (step.get('msg') if isinstance(step, dict) else None)
                    or f'步骤 {idx}')
            status_text, ok, is_warn = _step_verdict(step)
            detail = _safe_str(step.get('detail') or step.get('message') or step.get('msg'))

            p = doc.add_paragraph(style='List Number')
            run = p.add_run(f"{name} — {status_text}")
            run.bold = True
            run.font.color.rgb = RGBColor(40, 167, 69) if ok else RGBColor(220, 53, 69)

            if detail and detail != '-':
                d = doc.add_paragraph(detail)
                d.paragraph_format.left_indent = Inches(0.3)
                d.paragraph_format.space_after = Pt(6)

    # 四、完整日志
    if data['full_log'] and data['full_log'] != '-':
        doc.add_page_break()
        h4 = doc.add_heading('四、完整执行日志', level=1)
        h4.runs[0].font.color.rgb = RGBColor(0, 128, 128)
        for line in data['full_log'].splitlines():
            p = doc.add_paragraph(line)
            p.paragraph_format.line_spacing_rule = WD_LINE_SPACING.SINGLE
            p.paragraph_format.space_after = Pt(2)

    buffer = io.BytesIO()
    doc.save(buffer)
    buffer.seek(0)
    return buffer.getvalue()


# ---------------------------------------------------------------------------
# Flask 友好入口
# ---------------------------------------------------------------------------

def make_report(history: Dict[str, Any], config: Optional[Dict[str, Any]] = None, fmt: str = 'pdf') -> bytes:
    """统一入口：fmt='pdf' | 'word'"""
    fmt = fmt.lower()
    if fmt == 'pdf':
        return generate_pdf(history, config)
    if fmt in ('word', 'docx'):
        return generate_word(history, config)
    raise ValueError(f"不支持的报告格式: {fmt}")


def report_filename(task_id: str, fmt: str = 'pdf') -> str:
    """生成下载文件名"""
    fmt = fmt.lower()
    ext = 'docx' if fmt in ('word', 'docx') else 'pdf'
    return f"切换演练报告_{task_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}.{ext}"
