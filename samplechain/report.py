"""JSON + Markdown control reports for fridge plans and device commands."""
from __future__ import annotations
import json
from pathlib import Path

from .store import connect


def build_report(db_path):
    con = connect(db_path)
    try:
        data = {}
        for name in ('fridge_case', 'fridge_feed', 'fridge_plan', 'fridge_command',
                     'fridge_period_state', 'fridge_hold'):
            rows = [dict(r) for r in con.execute(f'SELECT * FROM {name} ORDER BY 1')]
            if name == 'fridge_feed':
                for row in rows:
                    row['raw'] = {'bytes': len(row['raw'] or b''),
                                  'sha256': row['sha256']}
            data[name] = rows
        for row in data['fridge_plan']:
            row['basis'] = json.loads(row['basis'])
            row['summary'] = json.loads(row['summary'])
            row['evidence'] = json.loads(row['evidence'])
        return data
    finally:
        con.close()


def _md_plan(plan, commands_by_plan, states_by_plan, feeds):
    s = plan['summary']
    lines = [
        f"## 计划 `{plan['id'][:12]}` v{plan['plan_version']} — {plan['contract']} "
        f"({plan['status']})",
        '',
        f"- 版本/模式：v{plan['plan_version']} / {plan['basis']['mode']}，"
        f"起始时段 {plan['basis']['start_period']}",
        f"- 来源摘要：case SHA `{plan['basis']['case_sha256'][:12]}…`，"
        f"传感器 SHA `{plan['basis']['sensor_sha256'][:12]}…`",
    ]
    if plan['basis'].get('feed_sha256'):
        lines.append(f"- 反馈来源 SHA：`{plan['basis']['feed_sha256'][:12]}…`（迟到反馈重算后缀）")
    if s.get('zone_exposure_c'):
        expo = '，'.join(f"{z}={v}°C·时段" for z, v in sorted(s['zone_exposure_c'].items()))
        lines.append(f"- 各区累计暴露：{expo}；最大 {s['max_zone_exposure_c']}，"
                     f"合计 {s['total_exposure_c']} / 上限 {s['total_exposure_limit']}")
    lines.append(f"- 实际/预测电费：{s['energy_cents']} 分")
    ev = plan['evidence']
    if ev:
        lines.append(f"- 最优性证据：{ev.get('method')}；每层状态数 "
                     f"{ev.get('layer_state_counts')}；容量剪枝 "
                     f"{ev.get('cap_pruned_transitions')}；可行终态 "
                     f"{ev.get('feasible_terminal_states')}；目标序 "
                     f"{ev.get('objectives_lexicographic')}")
        if ev.get('optimal_terminal_key') is not None:
            lines.append(f"  - 最优终态键：{ev['optimal_terminal_key']}；{ev.get('proof', '')}")
    lines.append('')
    lines.append('| 时段 | 压缩机动作 | 区 | 状态 | 电价(分) | 电费(分) |')
    lines.append('| --- | --- | --- | --- | --- | --- |')
    for cmd in commands_by_plan.get(plan['id'], []):
        lines.append(
            f"| {cmd['period']} | {cmd['action']} | {cmd['zone_id'] or '—'} | "
            f"{cmd['status']} | {cmd['tariff_cents']} | {cmd['energy_cents']} |")
    lines.append('')
    lines.append('| 时段 | 区 | 读数 | 期初温度 | 期末温度 | 本期暴露 | 累计暴露 |')
    lines.append('| --- | --- | --- | --- | --- | --- | --- |')
    for st in states_by_plan.get(plan['id'], []):
        reading = '—' if st['reading_c'] is None else f"{st['reading_c']}°C"
        lines.append(
            f"| {st['period']} | {st['zone_id']} | {reading} | {st['temp_before_c']} | "
            f"{st['temp_after_c']} | {st['exposure_increment_c']} | "
            f"{st['cumulative_exposure_c']} |")
    return lines


def write_report(db_path, out_path):
    data = build_report(db_path)
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=False),
                   encoding='utf-8')
    commands_by_plan, states_by_plan = {}, {}
    for cmd in data['fridge_command']:
        commands_by_plan.setdefault(cmd['plan_id'], []).append(cmd)
    for st in data['fridge_period_state']:
        states_by_plan.setdefault(st['plan_id'], []).append(st)
    md = ['# 双区冷藏控制报告', '']
    if not data['fridge_plan']:
        md.append('（尚无控制计划）')
    for plan in data['fridge_plan']:
        md += _md_plan(plan, commands_by_plan, states_by_plan, data['fridge_feed'])
        md.append('')
    if data['fridge_hold']:
        md.append('## 待核记录')
        for hold in data['fridge_hold']:
            md.append(f"- [{hold['reason']}] {hold['detail']} "
                      f"(plan={str(hold['plan_id'])[:12] if hold['plan_id'] else '—'}, "
                      f"cleared={hold['cleared']})")
        md.append('')
    if data['fridge_feed']:
        md.append('## 传感器反馈来源')
        for feed in data['fridge_feed']:
            md.append(f"- {feed['status']} `{feed['sha256'][:12]}…` {feed['path']} "
                      f"— {feed['detail']}")
    out.with_suffix('.md').write_text('\n'.join(md) + '\n', encoding='utf-8')
    return out
