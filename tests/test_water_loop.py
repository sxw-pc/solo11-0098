"""纯水闭环：守恒、两条安全路线、PLC 故障、乱序传感、召回补水的端到端测试。"""
import json

import pytest

from samplechain import water_loop as wl
from samplechain.store import connect
from tests.water_support import (FIX, CASE, armed_db, seeded_db,
                                 run_route, ledger_of, accounts_of, deliveries_of)


# --------------------------------------------------------------------------- #
# 初始高电导滞留液：简单补丁（只看合格 POLISH 探头）必然失败
# --------------------------------------------------------------------------- #
def test_naive_patch_fails_on_initial_holdup(armed_db):
    plc = wl.SimPLC()
    with pytest.raises(wl.WaterError) as exc:
        wl.attempt_naive_direct(armed_db, CASE, 'W1', 3000, 't0', plc)
    assert exc.value.code == 'holdup_unverified'
    detail = json.loads(exc.value.detail)
    # 1L×10.0 + 2L×0.4 的塞流混水 = 10.8M 质量单位 / 3000mL = 3.6 µS/cm，超过 1.0 限值
    assert detail['prospective_conductivity_milli'] == 3600
    assert detail['limit_milli'] == 1000
    con = connect(armed_db)
    try:
        # 已实际流出的 3L 改向排水并计量，但绝不写入交付事实
        assert con.execute('SELECT count(*) FROM water_delivery').fetchone()[0] == 0
        meter = con.execute('SELECT ml,valve FROM water_meter').fetchone()
        assert meter['ml'] == 3000 and meter['valve'] == 'V-DIRECT'
        assert con.execute("SELECT code FROM water_notice").fetchone()[0] == 'holdup_unverified'
        ledger = wl.rebuild_ledger(con, CASE, wl.load_case(con, CASE))
        assert ledger.drain_ml == 3000 and ledger.delivered_ml == 0
    finally:
        con.close()


def test_engine_blocks_direct_delivery_before_flush(armed_db):
    # 未排空滞留液时执行引擎同样拦截，不允许凭单个合格探头放行全部流量
    wl.freeze_plan(armed_db, CASE, 'direct_first')
    plan = wl.build_plan('direct_first')
    plan['steps'] = [s for s in plan['steps'] if s['op'] != 'flush']
    con = connect(armed_db)
    try:
        with pytest.raises(wl.WaterError, match='plan_invalid'):
            wl._validate_plan(wl.load_case(con, CASE), plan)
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# 两条完整路线：同一冻结输入，真实水源/膜寿命/备用库存不同
# --------------------------------------------------------------------------- #
def test_direct_first_route_delivers_both_orders(armed_db):
    run_route(armed_db, 'direct_first')
    ledger = ledger_of(armed_db)
    # t0：1L 冲洗 + 3L 交 W1；t1：2L 交 W2；膜累计 6L，储罐保留 3L
    assert ledger.membrane_used == 6000 and ledger.membrane_remaining == 2000
    assert ledger.tank_ml == 3000 and ledger.drain_ml == 1000
    assert ledger.delivered_ml == 5000
    accounts = accounts_of(armed_db)
    assert accounts == {
        'W1': {'required_ml': 3000, 'delivered_ml': 3000, 'makeup_ml': 0, 'recalled_ml': 0,
               'qualified_ml': 3000, 'pending_ml': 0},
        'W2': {'required_ml': 2000, 'delivered_ml': 2000, 'makeup_ml': 0, 'recalled_ml': 0,
               'qualified_ml': 2000, 'pending_ml': 0}}
    rows = deliveries_of(armed_db)
    assert {r['order_id']: r['source'] for r in rows} == {'W1': 'polished_direct', 'W2': 'polished_direct'}
    # W2 混水按质量守恒核算为 0.5 µS/cm，而不是对 0.4/0.6 取算术平均
    w2 = next(r for r in rows if r['order_id'] == 'W2')
    assert w2['conductivity_milli'] == 500
    assert w2['receipt_json']


def test_reserve_first_route_delivers_both_orders(armed_db):
    run_route(armed_db, 'reserve_first')
    ledger = ledger_of(armed_db)
    # 储罐 3L 交 W1；t1 直供 1L 冲洗后 2L 交 W2；膜累计 3L，储罐余 0L
    assert ledger.membrane_used == 3000 and ledger.membrane_remaining == 5000
    assert ledger.tank_ml == 0 and ledger.drain_ml == 1000
    assert ledger.delivered_ml == 5000
    rows = deliveries_of(armed_db)
    sources = {r['order_id']: r['source'] for r in rows}
    assert sources == {'W1': 'qualified_reserve', 'W2': 'polished_direct'}
    assert all(r['qualified'] for r in rows)
    accounts = accounts_of(armed_db)
    assert all(a['qualified_ml'] == a['required_ml'] and a['pending_ml'] == 0
               for a in accounts.values())


def test_routes_cannot_be_mixed_in_one_case(armed_db):
    wl.freeze_plan(armed_db, CASE, 'direct_first')
    with pytest.raises(wl.WaterError, match='route_already_frozen'):
        wl.freeze_plan(armed_db, CASE, 'reserve_first')


@pytest.mark.parametrize('route,expected_membrane,expected_tank', [
    ('direct_first', 6000, 3000),
    ('reserve_first', 3000, 0),
])
def test_conservation_balanced_for_both_routes(tmp_path, route, expected_membrane, expected_tank):
    # 独立 DB 跑各自路线，避免路线冻结互斥
    from samplechain.__main__ import init
    from samplechain.ingest import ingest
    db = tmp_path / 'w.db'
    init(db, 'batch-w', 'sample-1'); init(db, 'batch-w', 'sample-2')
    ingest(FIX / 'water-events.jsonl', db)
    wl.register(db, CASE, FIX / 'water-contract.json', FIX / 'water-sensors.csv')
    wl.create_snapshot(db, CASE); wl.load_frozen_telemetry(db, CASE)
    run_route(db, route)
    con = connect(db)
    try:
        case = wl.load_case(con, CASE)
        ledger = wl.rebuild_ledger(con, CASE, case)
        balance = wl.conservation(con, CASE, case, ledger)
        assert balance['volume_ml']['balanced'] is True
        assert balance['mass_milli_ml']['balanced'] is True
        assert ledger.membrane_used == expected_membrane
        assert ledger.tank_ml == expected_tank
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# 传感器：校准过期、乱序、重复、身份冲突、旧快照
# --------------------------------------------------------------------------- #
def _row(seq, **over):
    row = {'sensor': 'POLISH-1', 'seq': seq, 'ts': '2026-11-17T08:00:00+08:00',
           'conductivity_us_cm': 0.4, 'flow_l': 4, 'cal_version': 'cal-pol-1', 'window': 't0'}
    row.update(over)
    return row


def test_calibration_expired_row_rejected(tmp_path):
    db = tmp_path / 'w.db'
    from samplechain.__main__ import init
    from samplechain.ingest import ingest
    init(db, 'batch-w', 'sample-1'); init(db, 'batch-w', 'sample-2')
    ingest(FIX / 'water-events.jsonl', db)
    wl.register(db, CASE, FIX / 'water-contract.json', FIX / 'water-sensors.csv')
    cal = dict(wl.DEFAULT_CALIBRATION)
    cal['POLISH-1'] = ('cal-pol-1', '2026-11-01T00:00:00+08:00', '2026-11-10T00:00:00+08:00')
    wl.create_snapshot(db, CASE, calibration=cal)
    result = wl.ingest_telemetry(db, CASE, [_row(1)])
    assert result['rejected'][0]['code'] == 'calibration_expired'
    assert result['accepted'] == 0


def test_out_of_order_duplicate_and_conflict_rows(armed_db):
    wl.ingest_telemetry(armed_db, CASE, [_row(1)])  # 重复内容：静默忽略
    again = wl.ingest_telemetry(armed_db, CASE, [_row(1)])
    assert again == {'accepted': 0, 'rejected': []}
    conflict = wl.ingest_telemetry(armed_db, CASE, [_row(1, conductivity_us_cm=0.9)])
    assert conflict['rejected'][0]['code'] == 'sensor_identity_conflict'
    out_of_order = wl.ingest_telemetry(armed_db, CASE, [_row(0)])
    assert out_of_order['rejected'][0]['code'] == 'sequence_out_of_order'
    con = connect(armed_db)
    try:
        # 原 seq1 证据不被冲突/乱序行覆盖
        kept = con.execute("SELECT conductivity_milli FROM water_telemetry "
                           "WHERE sensor='POLISH-1' AND seq=1 AND status='accepted'").fetchone()
        assert kept['conductivity_milli'] == 400
    finally:
        con.close()


def test_unknown_sensor_and_cal_version_mismatch(armed_db):
    assert wl.ingest_telemetry(armed_db, CASE, [_row(2, sensor='BOGUS-9')])['rejected'][0]['code'] \
        == 'unknown_sensor'
    assert wl.ingest_telemetry(armed_db, CASE, [_row(3, cal_version='cal-old')])['rejected'][0]['code'] \
        == 'calibration_version_mismatch'


def test_late_stale_snapshot_cannot_override_commands(armed_db):
    run_route(armed_db, 'direct_first')
    result = wl.ingest_telemetry(armed_db, CASE, [_row(9, snapshot_id='deadbeef-old')])
    assert result['rejected'][0]['code'] == 'stale_snapshot'
    con = connect(armed_db)
    try:
        # 命令计数保持不变
        assert con.execute('SELECT count(*) FROM water_command').fetchone()[0] > 0
        assert con.execute("SELECT count(*) FROM water_telemetry WHERE status LIKE 'rejected:stale%' "
                           ).fetchone()[0] == 1
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# 跨窗口膜容量与窗容
# --------------------------------------------------------------------------- #
def test_membrane_total_guard_with_synthetic_orders(tmp_path, monkeypatch):
    # 窗容合计 8L 时，膜总容量上限只能在不越窗容的前提下用更小膜容触发
    import json as _json
    contract = _json.loads((FIX / 'water-contract.json').read_text())
    contract['orders'] = [
        {'id': 'W1', 'ts': '2026-11-17T08:05:00+08:00', 'liters': 2,
         'max_conductivity_us_cm': 1.0},
        {'id': 'W2', 'ts': '2026-11-17T08:15:00+08:00', 'liters': 3,
         'max_conductivity_us_cm': 1.0}]
    cpath = tmp_path / 'contract.json'
    cpath.write_text(_json.dumps(contract))
    from samplechain.__main__ import init
    from samplechain.ingest import ingest
    db = tmp_path / 'w.db'
    init(db, 'batch-w', 'sample-1'); init(db, 'batch-w', 'sample-2')
    ingest(FIX / 'water-events.jsonl', db)
    wl.register(db, CASE, cpath, FIX / 'water-sensors.csv')
    con = connect(db)
    try:
        case = wl.load_case(con, CASE)
    finally:
        con.close()
    # 合法覆盖：t0=4L（2 冲 + 2 交）、t1=4L（1 冲 + 3 交），合计 8L
    plan = {'route': 'direct_first', 'steps': [
        {'id': 'a', 'op': 'flush', 'window': 't0', 'ts': '2026-11-17T08:01:00+08:00', 'ml': 2000},
        {'id': 'b', 'op': 'deliver_direct', 'window': 't0', 'ts': '2026-11-17T08:04:00+08:00',
         'order': 'W1', 'ml': 2000},
        {'id': 'c', 'op': 'flush', 'window': 't1', 'ts': '2026-11-17T08:11:00+08:00', 'ml': 1000},
        {'id': 'd', 'op': 'deliver_direct', 'window': 't1', 'ts': '2026-11-17T08:14:00+08:00',
         'order': 'W2', 'ml': 3000}]}
    assert wl._validate_plan(case, plan) is True  # 8L 膜容下通过
    monkeypatch.setattr(wl, 'MEMBRANE_CAPACITY_ML', 7000)
    with pytest.raises(wl.WaterError, match='membrane_capacity_exceeded'):
        wl._validate_plan(case, plan)  # 同一计划在 7L 膜容下被拦截


def test_membrane_and_slot_capacity_guards(armed_db):
    con = connect(armed_db)
    try:
        case = wl.load_case(con, CASE)
    finally:
        con.close()
    over_slot = {'route': 'direct_first', 'steps': [
        {'id': 'a', 'op': 'flush', 'window': 't0', 'ts': '2026-11-17T08:01:00+08:00', 'ml': 2000},
        {'id': 'b', 'op': 'deliver_direct', 'window': 't0', 'ts': '2026-11-17T08:04:00+08:00',
         'order': 'W1', 'ml': 3000},
        {'id': 'c', 'op': 'deliver_direct', 'window': 't1', 'ts': '2026-11-17T08:12:00+08:00',
         'order': 'W2', 'ml': 2000}]}
    with pytest.raises(wl.WaterError, match='slot_capacity_exceeded'):
        wl._validate_plan(case, over_slot)


def test_membrane_consumed_by_both_windows_combined(armed_db):
    # 两窗冲洗与交付合并核算：direct_first 分窗 t0=4000、t1=2000
    run_route(armed_db, 'direct_first')
    ledger = ledger_of(armed_db)
    assert ledger.slot_used == {'t0': 4000, 't1': 2000}
    assert sum(ledger.slot_used.values()) == ledger.membrane_used == 6000


# --------------------------------------------------------------------------- #
# PLC：断联、ACK 丢失、流量计晚到、并发阀、工人抢单、膜失效
# --------------------------------------------------------------------------- #
def test_plc_offline_blocks_commands_and_status(armed_db):
    plc = wl.SimPLC(); plc.connected = False
    with pytest.raises(wl.PLCError, match='plc_offline'):
        plc.query_status()
    with pytest.raises(wl.PLCError, match='plc_offline'):
        plc.command('V-DIRECT', 'open')


def test_ack_lost_reconciled_by_nonce_status_query(armed_db):
    wl.freeze_plan(armed_db, CASE, 'direct_first')
    plc = wl.SimPLC(faults=['drop_ack'])
    result = wl.execute_plan(armed_db, CASE, plc)
    assert not result.get('awaiting_meter')
    con = connect(armed_db)
    try:
        reconciled = con.execute(
            "SELECT ack FROM water_command WHERE ack LIKE 'lost_reconciled%'").fetchall()
        assert reconciled  # ACK 丢失但状态查询确认阀已动作，命令留痕不重发
        # 两单仍实际交付
        accounts = wl.order_accounts(con, CASE, wl.load_case(con, CASE))
        assert all(a['qualified_ml'] == a['required_ml'] for a in accounts.values())
    finally:
        con.close()


def test_delayed_meter_settles_on_retry_without_double_flow(armed_db):
    wl.freeze_plan(armed_db, CASE, 'direct_first')
    plc = wl.SimPLC(faults=['delay_meter'])
    first = wl.execute_plan(armed_db, CASE, plc)
    runs = 1
    while first.get('awaiting_meter'):
        first = wl.execute_plan(armed_db, CASE, plc)
        runs += 1
    assert runs > 1  # 至少一次晚到后重试
    con = connect(armed_db)
    try:
        meters = con.execute('SELECT step_id,ml,status FROM water_meter ORDER BY id').fetchall()
        # 每个步骤仅一条计量记录，重试不会重复放水
        steps = [m['step_id'] for m in meters]
        assert len(steps) == len(set(steps))
        assert all(m['status'] == 'settled' for m in meters)
        assert sum(m['ml'] for m in meters) == 6000
        accounts = wl.order_accounts(con, CASE, wl.load_case(con, CASE))
        assert all(a['pending_ml'] == 0 for a in accounts.values())
    finally:
        con.close()


def test_concurrent_direct_and_reserve_valves_rejected():
    plc = wl.SimPLC()
    plc.command('V-DIRECT', 'open')
    with pytest.raises(wl.PLCInterlock, match='concurrent_valve'):
        plc.command('V-RESERVE', 'open')
    # 互锁触发时 nonce 不递增、第二只阀未开
    status = plc.query_status()
    assert status['open'] == ['V-DIRECT']


def test_two_workers_claim_same_order_only_one_wins(armed_db):
    assert wl.claim_order(armed_db, CASE, 'W1', 'worker-a')['worker'] == 'worker-a'
    with pytest.raises(wl.WaterError, match='order_already_claimed'):
        wl.claim_order(armed_db, CASE, 'W1', 'worker-b')
    # 另一张订单仍可被第二个工人领取
    assert wl.claim_order(armed_db, CASE, 'W2', 'worker-b')['worker'] == 'worker-b'


def test_flush_retest_blocks_delivery_when_holdup_still_dirty(armed_db):
    wl.freeze_plan(armed_db, CASE, 'direct_first')
    plc = wl.SimPLC()
    plc.probe_overrides = {'PIPE-1': 1200}  # 排空后复测仍报 1.2 µS/cm
    with pytest.raises(wl.WaterError, match='holdup_retest_over_limit'):
        wl.execute_plan(armed_db, CASE, plc)
    con = connect(armed_db)
    try:
        # 复测不合格：不产生任何交付，订单保持待核
        assert con.execute('SELECT count(*) FROM water_delivery').fetchone()[0] == 0
        case = wl.load_case(con, CASE)
        assert all(a['qualified_ml'] == 0 for a in wl.order_accounts(con, CASE, case).values())
    finally:
        con.close()


def test_reserve_source_conflict_blocks_delivery(armed_db):
    # 储罐探头与合同预验证水质冲突：只能待核，不能按储罐路径放行
    con = connect(armed_db)
    con.execute("UPDATE water_telemetry SET conductivity_milli=800 WHERE sensor='RESERVE-1'")
    con.commit()
    con.close()
    wl.freeze_plan(armed_db, CASE, 'reserve_first')
    with pytest.raises(wl.WaterError, match='source_conflict'):
        wl.execute_plan(armed_db, CASE, wl.SimPLC())
    con = connect(armed_db)
    try:
        assert con.execute('SELECT count(*) FROM water_delivery').fetchone()[0] == 0
    finally:
        con.close()


def test_over_limit_source_water_is_diverted_never_delivered(armed_db):
    # t1 入膜水 1.5 µS/cm 本身超限：即使与管内 0.4 滞留液混合后整单可能低于限值，
    # 也必须改向排水，绝不写入交付事实
    con = connect(armed_db)
    con.execute("UPDATE water_telemetry SET conductivity_milli=1500 "
                "WHERE sensor='POLISH-1' AND seq=2")
    con.commit()
    con.close()
    wl.freeze_plan(armed_db, CASE, 'direct_first')
    with pytest.raises(wl.WaterError, match='quality_over_limit'):
        wl.execute_plan(armed_db, CASE, wl.SimPLC())
    con = connect(armed_db)
    try:
        rows = con.execute('SELECT order_id FROM water_delivery').fetchall()
        assert [r['order_id'] for r in rows] == ['W1']  # W2 无任何交付行
        assert con.execute("SELECT code FROM water_notice WHERE code='quality_over_limit'"
                           ).fetchone()
        case = wl.load_case(con, CASE)
        ledger = wl.rebuild_ledger(con, CASE, case)
        # 膜容量仍被实际流出的排水消耗，W2 转为待核，体积/质量守恒不破
        assert ledger.membrane_used == 6000 and ledger.drain_ml == 3000
        assert wl.order_accounts(con, CASE, case)['W2']['pending_ml'] == 2000
        assert wl.conservation(con, CASE, case, ledger)['mass_milli_ml']['balanced']
    finally:
        con.close()


def test_membrane_failure_preserves_partial_flow_and_blocks_followup(armed_db):
    wl.freeze_plan(armed_db, CASE, 'direct_first')
    plc = wl.SimPLC()
    # 冲洗 1L + W1 3L 之后，W2 的 2L 中只流出 1L 膜即失效
    plc.membrane_fail_after_ml = 5000
    with pytest.raises(wl.MembraneFailed):
        wl.execute_plan(armed_db, CASE, plc)
    con = connect(armed_db)
    try:
        ledger = wl.rebuild_ledger(con, CASE, wl.load_case(con, CASE))
        assert ledger.membrane_status == 'failed' and ledger.membrane_used == 5000
        # 已实际流出的合格水保留为交付事实，不回滚
        rows = con.execute("SELECT order_id,ml FROM water_delivery ORDER BY id").fetchall()
        assert [tuple(r) for r in rows] == [('W1', 3000), ('W2', 1000)]
        assert con.execute("SELECT code FROM water_notice WHERE code='membrane_failed'").fetchone()
    finally:
        con.close()
    # 膜失效后重跑被拒绝，后续订单必须走补水/换膜流程
    with pytest.raises(wl.MembraneFailed):
        wl.execute_plan(armed_db, CASE, wl.SimPLC())
    accounts = accounts_of(armed_db)
    assert accounts['W2']['qualified_ml'] == 1000 and accounts['W2']['pending_ml'] == 1000


# --------------------------------------------------------------------------- #
# 异常水质补正：重测超限 → 召回单（保留原交付）→ 补水单
# --------------------------------------------------------------------------- #
def test_retest_over_limit_recall_then_reserve_makeup(armed_db):
    run_route(armed_db, 'direct_first')  # 储罐 3L 完好
    con = connect(armed_db)
    try:
        delivery_id = con.execute(
            "SELECT id FROM water_delivery WHERE order_id='W2'").fetchone()[0]
    finally:
        con.close()
    recall = wl.retest_delivery(armed_db, CASE, delivery_id, 1.2, wl.SimPLC())
    assert recall['recall'] and recall['delivery_id'] == delivery_id
    makeup = wl.makeup_delivery(armed_db, CASE, 'W2', 2000, 'qualified_reserve', wl.SimPLC())
    assert makeup['tank_remaining_ml'] == 1000
    con = connect(armed_db)
    try:
        # 原交付行保留，补水为新 kind='makeup' 行，无数据库回滚
        kinds = [r['kind'] for r in con.execute(
            'SELECT kind FROM water_delivery WHERE case_id=? ORDER BY id', (CASE,))]
        assert kinds == ['delivery', 'delivery', 'makeup']
        case = wl.load_case(con, CASE)
        accounts = wl.order_accounts(con, CASE, case)
        assert accounts['W2']['recalled_ml'] == 2000
        assert accounts['W2']['makeup_ml'] == 2000
        assert accounts['W2']['qualified_ml'] == 2000 and accounts['W2']['pending_ml'] == 0
        ledger = wl.rebuild_ledger(con, CASE, case)
        assert wl.conservation(con, CASE, case, ledger)['mass_milli_ml']['balanced']
    finally:
        con.close()


def test_retest_within_limit_keeps_delivery(armed_db):
    run_route(armed_db, 'reserve_first')
    con = connect(armed_db)
    try:
        delivery_id = con.execute(
            "SELECT id FROM water_delivery WHERE order_id='W1'").fetchone()[0]
    finally:
        con.close()
    result = wl.retest_delivery(armed_db, CASE, delivery_id, 0.8, wl.SimPLC())
    assert result['recall'] is None
    con = connect(armed_db)
    try:
        assert con.execute("SELECT count(*) FROM water_notice WHERE code='recall_over_limit'"
                           ).fetchone()[0] == 0
    finally:
        con.close()


def test_makeup_via_direct_when_reserve_empty(armed_db):
    run_route(armed_db, 'reserve_first')  # 储罐已空，膜仍余 5L
    con = connect(armed_db)
    try:
        delivery_id = con.execute(
            "SELECT id FROM water_delivery WHERE order_id='W1'").fetchone()[0]
    finally:
        con.close()
    wl.retest_delivery(armed_db, CASE, delivery_id, 1.4, wl.SimPLC())
    makeup = wl.makeup_delivery(armed_db, CASE, 'W1', 3000, 'polished_direct', wl.SimPLC())
    assert makeup['membrane_remaining_ml'] == 2000
    accounts = accounts_of(armed_db)
    assert accounts['W1']['qualified_ml'] == 3000 and accounts['W1']['pending_ml'] == 0


def test_makeup_refused_when_reserve_insufficient(armed_db):
    run_route(armed_db, 'reserve_first')  # 储罐 0L
    with pytest.raises(wl.WaterError, match='reserve_capacity_exceeded'):
        wl.makeup_delivery(armed_db, CASE, 'W1', 3000, 'qualified_reserve', wl.SimPLC())


# --------------------------------------------------------------------------- #
# 报告与可回放因果链
# --------------------------------------------------------------------------- #
def test_report_contains_replayable_chain_and_evidence(tmp_path, armed_db):
    run_route(armed_db, 'direct_first')
    out = wl.water_report(armed_db, CASE, tmp_path / 'water-report.json')
    data = json.loads(out.read_text(encoding='utf-8'))
    assert out.with_suffix('.md').read_text(encoding='utf-8').startswith('# 纯水实物交付报告')
    assert data['frozen_input']['sensor_sha256'] and len(data['frozen_input']['sensor_sha256']) == 64
    for delivery in data['deliveries']:
        receipt = delivery['receipt']
        assert receipt['chain'] == ['source_sha', 'water_telemetry', 'water_command',
                                    'water_meter', 'water_delivery']
        assert receipt['telemetry_ids'] and receipt['command_ids']
        assert receipt['meter']['ml'] == delivery['ml']
    assert data['snapshot']['order_version'] and data['snapshot']['valve_versions']
    assert data['membrane']['remaining_ml'] == 2000
    assert data['conservation']['volume_ml']['balanced']
    # 拒收/通知也进入报告
    assert isinstance(data['notices'], list)


def test_snapshot_pins_sensor_valve_and_order_versions_together(armed_db):
    run_route(armed_db, 'direct_first')
    con = connect(armed_db)
    try:
        snap = con.execute('SELECT * FROM water_snapshot').fetchone()
        sensor_versions = json.loads(snap['sensor_versions'])
        valve_versions = json.loads(snap['valve_versions'])
        assert sensor_versions['POLISH-1'][0] == 'cal-pol-1'
        assert valve_versions['V-DIRECT'] == 'fw-vd-3'
        assert snap['order_version'] == 'order-schema-1'
        # 每条命令与传感行都绑定同一快照
        assert con.execute('SELECT count(DISTINCT snapshot_id) FROM water_command').fetchone()[0] == 1
    finally:
        con.close()
