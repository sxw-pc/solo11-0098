import json
import sqlite3
from pathlib import Path

import pytest

from samplechain.__main__ import init
from samplechain.pipette import (
    PipetteError, PipetteInterrupted, execute, finalize, load_robot, persist_journal,
    plan, recover, register, report, run_method, send_command, verify_journal,
    _apply_contract_fault,
)
from samplechain.store import connect

CASE = 'control-plate-2026-05-09'
DILUTE = 'dilute_from_high'
DIRECT = 'direct_stock'
FIXTURE = Path(__file__).parents[1] / 'fixtures' / 'pipette-contract.json'


def make_db(tmp_path, contract_path=FIXTURE, name='lab.db'):
    db = tmp_path / name
    init(db, 'batch-1', 'sample-1')
    register(db, contract_path)
    return db


def contract_file(tmp_path, name='contract.json', **changes):
    contract = json.loads(FIXTURE.read_text(encoding='utf-8'))
    contract.update(changes)
    path = tmp_path / name
    path.write_text(json.dumps(contract), encoding='utf-8')
    return path


def wells(db, method, case_id=CASE):
    con = connect(db)
    rows = {r['well_id']: dict(r) for r in con.execute(
        'SELECT * FROM well_state WHERE case_id=? AND method=?', (case_id, method))}
    con.close()
    return rows


def reservoir_balance(db, method, case_id=CASE):
    con = connect(db)
    contract = json.loads(con.execute(
        'SELECT contract_json FROM pipette_case WHERE id=?', (case_id,)).fetchone()[0])
    out = {}
    for r in contract['reservoirs']:
        used = con.execute(
            'SELECT COALESCE(sum(delta_ul),0) FROM reservoir_ledger'
            ' WHERE case_id=? AND method=? AND reservoir_id=?',
            (case_id, method, r['id'])).fetchone()[0]
        out[r['id']] = r['available_ul'] + used
    con.close()
    return out


def mass_book(db, method, case_id=CASE):
    """(初始总溶质, 期末容器溶质+孔位溶质):两者必须相等。"""
    con = connect(db)
    contract = json.loads(con.execute(
        'SELECT contract_json FROM pipette_case WHERE id=?', (case_id,)).fetchone()[0])
    initial = sum(r['available_ul'] * r['concentration'] for r in contract['reservoirs'])
    remaining = 0.0
    for r in contract['reservoirs']:
        used = con.execute(
            'SELECT COALESCE(sum(delta_ul),0) FROM reservoir_ledger'
            ' WHERE case_id=? AND method=? AND reservoir_id=?',
            (case_id, method, r['id'])).fetchone()[0]
        remaining += (r['available_ul'] + used) * r['concentration']
    in_wells = con.execute(
        'SELECT COALESCE(sum(mass),0) FROM well_state WHERE case_id=? AND method=?',
        (case_id, method)).fetchone()[0]
    con.close()
    return initial, remaining + in_wells


def journal_count(db, command_id=None):
    con = connect(db)
    if command_id:
        n = con.execute('SELECT count(*) FROM robot_journal WHERE command_id=?',
                        (command_id,)).fetchone()[0]
    else:
        n = con.execute('SELECT count(*) FROM robot_journal').fetchone()[0]
    con.close()
    return n


def issue_codes(db, method, case_id=CASE):
    con = connect(db)
    codes = [r['code'] for r in con.execute(
        'SELECT code FROM pipette_issue WHERE case_id=? AND method=? ORDER BY id',
        (case_id, method))]
    con.close()
    return codes


# ---------------------------------------------------------------- 计划

def test_plan_direct_stock_command_spec(tmp_path):
    db = make_db(tmp_path)
    assert plan(db, CASE, DIRECT) == {'planned': 8, 'already': False}
    con = connect(db)
    rows = con.execute('SELECT * FROM pipette_command WHERE method=? ORDER BY seq, kind',
                       (DIRECT,)).fetchall()
    legs = {}
    for r in rows:
        # 每条命令明确源容器、体积、一次性枪头、目标孔与预期目标体积
        assert r['reservoir_id'] and r['volume_ul'] > 0
        assert r['tip_id'] and r['well_id'] and r['expect_well_ul'] == 100
        legs.setdefault(r['seq'], {})[r['kind']] = dict(r)
    moves = [(legs[i]['aspirate']['reservoir_id'], legs[i]['aspirate']['well_id'],
              legs[i]['aspirate']['volume_ul']) for i in sorted(legs)]
    assert moves == [('HIGH', 'A1', 100), ('LOW', 'A2', 100),
                     ('LOW', 'B1', 100), ('DILUENT', 'B2', 100)]
    tips = [r['tip_id'] for r in con.execute('SELECT * FROM tip_ledger ORDER BY tip_id')]
    assert tips == ['T1', 'T2', 'T3', 'T4']
    assert [r['well_id'] for r in con.execute('SELECT * FROM well_state ORDER BY well_id')] == \
        ['A1', 'A2', 'B1', 'B2']
    con.close()


def test_plan_dilute_from_high_is_real_two_step_mixing(tmp_path):
    db = make_db(tmp_path)
    assert plan(db, CASE, DILUTE) == {'planned': 12, 'already': False}
    assert plan(db, CASE, DILUTE)['already']  # 幂等,不重复生成
    con = connect(db)
    a2 = con.execute("SELECT * FROM pipette_command WHERE method=? AND well_id='A2'"
                     " AND kind='dispense' ORDER BY seq", (DILUTE,)).fetchall()
    # A2 必须真的下发两步混合指令,而不是只写库存字段
    assert [(r['reservoir_id'], r['volume_ul'], r['expect_well_ul']) for r in a2] == \
        [('HIGH', 10, 10), ('DILUENT', 90, 100)]
    assert con.execute('SELECT count(*) FROM tip_ledger WHERE method=?', (DILUTE,)).fetchone()[0] == 6
    con.close()


def test_plan_infeasible_contracts_rejected(tmp_path):
    big = contract_file(tmp_path, 'big.json', case_id='case-big',
                        wells=[{'id': 'A1', 'target_ul': 150, 'target_concentration': 100}],
                        reservoirs=[{'id': 'HIGH', 'concentration': 100, 'available_ul': 200},
                                    {'id': 'DILUENT', 'concentration': 0, 'available_ul': 300}])
    db = make_db(tmp_path, big, 'big.db')
    with pytest.raises(PipetteError, match='plan_infeasible'):
        plan(db, 'case-big', DIRECT)  # 单腿 150µL 超过 100µL 上限
    contract = json.loads(FIXTURE.read_text(encoding='utf-8'))
    contract['case_id'] = 'case-short'
    contract['reservoirs'][1]['available_ul'] = 150  # direct_stock 需要 LOW 200µL
    short = tmp_path / 'short.json'
    short.write_text(json.dumps(contract), encoding='utf-8')
    db = make_db(tmp_path, short, 'short.db')
    with pytest.raises(PipetteError, match='plan_infeasible'):
        plan(db, 'case-short', DIRECT)
    contract = json.loads(FIXTURE.read_text(encoding='utf-8'))
    contract['case_id'] = 'case-tips'
    contract['robot']['tip_count'] = 3  # direct_stock 需要 4 支
    few = tmp_path / 'few.json'
    few.write_text(json.dumps(contract), encoding='utf-8')
    db = make_db(tmp_path, few, 'few.db')
    with pytest.raises(PipetteError, match='plan_infeasible'):
        plan(db, 'case-tips', DIRECT)


# ---------------------------------------------------------------- 两种制备法:溶质质量与库存账

def test_direct_stock_solute_mass_and_balances(tmp_path):
    db = make_db(tmp_path)
    robot = load_robot(db, CASE)
    result = run_method(db, CASE, DIRECT, robot=robot)
    assert result['summary']['complete']
    w = wells(db, DIRECT)
    for wid, conc in (('A1', 100), ('A2', 10), ('B1', 10), ('B2', 0)):
        assert w[wid]['state'] == 'complete'
        assert w[wid]['volume_ul'] == 100
        assert w[wid]['mass'] == pytest.approx(100 * conc)
        assert w[wid]['mass'] / w[wid]['volume_ul'] == pytest.approx(conc)
    assert reservoir_balance(db, DIRECT) == {'HIGH': 20, 'LOW': 0, 'DILUENT': 200}
    initial, final = mass_book(db, DIRECT)
    assert final == pytest.approx(initial) == 14000
    assert journal_count(db) == 8


def test_dilute_from_high_solute_mass_and_balances(tmp_path):
    db = make_db(tmp_path)
    robot = load_robot(db, CASE)
    result = run_method(db, CASE, DILUTE, robot=robot)
    assert result['summary']['complete']
    w = wells(db, DILUTE)
    for wid, conc in (('A1', 100), ('A2', 10), ('B1', 10), ('B2', 0)):
        assert w[wid]['state'] == 'complete'
        assert w[wid]['mass'] / w[wid]['volume_ul'] == pytest.approx(conc)
    assert reservoir_balance(db, DILUTE) == {'HIGH': 0, 'LOW': 200, 'DILUENT': 20}
    initial, final = mass_book(db, DILUTE)
    assert final == pytest.approx(initial) == 14000
    # 两步混合真实发生:A2 的消耗日志含 HIGH 10 与 DILUENT 90 两条排出
    con = connect(db)
    a2 = con.execute("SELECT reservoir_id, volume_ul FROM robot_journal"
                     " WHERE well_id='A2' AND kind='dispense' ORDER BY seq").fetchall()
    assert [(r['reservoir_id'], r['volume_ul']) for r in a2] == [('HIGH', 10), ('DILUENT', 90)]
    con.close()
    assert journal_count(db) == 12


# ---------------------------------------------------------------- 容量/枪头约束

def test_aspirate_limit_and_reservoir_guards(tmp_path):
    db = make_db(tmp_path)
    robot = load_robot(db, CASE)
    plan(db, CASE, DIRECT)
    # 机器人物理上限
    with pytest.raises(PipetteError, match='aspirate_limit_exceeded'):
        robot.execute({'id': 'X1', 'kind': 'aspirate', 'reservoir_id': 'HIGH',
                       'well_id': 'A1', 'volume_ul': 150.0, 'tip_id': 'T9'})
    # 控制器上限:伪造超量命令同样被拒
    con = connect(db)
    with con:
        con.execute("INSERT INTO tip_ledger VALUES (?,?,'T9','fresh',NULL,NULL)", (CASE, DIRECT))
        con.execute("INSERT INTO pipette_command(id,case_id,method,seq,kind,reservoir_id,well_id,"
                    "volume_ul,tip_id,expect_well_ul,depends_on,retries,state)"
                    f" VALUES ('x:big','{CASE}','{DIRECT}',90,'aspirate','HIGH','A1',150,'T9',150,NULL,0,'planned')")
    con.close()
    with pytest.raises(PipetteError, match='aspirate_limit_exceeded'):
        send_command(db, robot, CASE, DIRECT, 'x:big')
    # 机器人透支拒绝且余额不变
    robot.execute({'id': 'X2', 'kind': 'aspirate', 'reservoir_id': 'HIGH',
                   'well_id': 'A1', 'volume_ul': 100.0, 'tip_id': 'T8'})
    with pytest.raises(PipetteError, match='insufficient_reservoir'):
        robot.execute({'id': 'X3', 'kind': 'aspirate', 'reservoir_id': 'HIGH',
                       'well_id': 'A1', 'volume_ul': 100.0, 'tip_id': 'T9'})
    assert robot.reservoir_level('HIGH') == 20
    # 控制器透支:LOW 被计划用空后再吸 1µL
    db2 = make_db(tmp_path, FIXTURE, 'lab2.db')
    robot2 = load_robot(db2, CASE)
    run_method(db2, CASE, DIRECT, robot=robot2)
    assert reservoir_balance(db2, DIRECT)['LOW'] == 0
    con = connect(db2)
    with con:
        con.execute("INSERT INTO tip_ledger VALUES (?,?,'T9','fresh',NULL,NULL)", (CASE, DIRECT))
        con.execute("INSERT INTO pipette_command(id,case_id,method,seq,kind,reservoir_id,well_id,"
                    "volume_ul,tip_id,expect_well_ul,depends_on,retries,state)"
                    f" VALUES ('x:over','{CASE}','{DIRECT}',91,'aspirate','LOW','A2',1,'T9',101,NULL,0,'planned')")
    con.close()
    with pytest.raises(PipetteError, match='insufficient_reservoir'):
        send_command(db2, robot2, CASE, DIRECT, 'x:over')
    assert reservoir_balance(db2, DIRECT)['LOW'] == 0  # 余额不为负


def test_tip_and_dependency_guards(tmp_path):
    db = make_db(tmp_path)
    robot = load_robot(db, CASE)
    plan(db, CASE, DIRECT)
    asp0 = f'{CASE}:{DIRECT}:00:asp'
    dsp0 = f'{CASE}:{DIRECT}:00:dsp'
    # 先确认吸入才能排出
    with pytest.raises(PipetteError, match='unconfirmed_aspirate'):
        send_command(db, robot, CASE, DIRECT, dsp0)
    send_command(db, robot, CASE, DIRECT, asp0)
    send_command(db, robot, CASE, DIRECT, dsp0)
    con = connect(db)
    with con:
        # 一份液体不能喂两条排出命令
        con.execute("INSERT INTO pipette_command(id,case_id,method,seq,kind,reservoir_id,well_id,"
                    "volume_ul,tip_id,expect_well_ul,depends_on,retries,state)"
                    f" VALUES('x:dup','{CASE}','{DIRECT}',92,'dispense','HIGH','A1',100,'T1',200,'{asp0}',0,'planned')")
        # 高浓度枪头回插空白容器
        con.execute("INSERT INTO pipette_command(id,case_id,method,seq,kind,reservoir_id,well_id,"
                    "volume_ul,tip_id,expect_well_ul,depends_on,retries,state)"
                    f" VALUES('x:dirty','{CASE}','{DIRECT}',93,'aspirate','DILUENT','B2',10,'T1',10,NULL,0,'planned')")
        # 同浓度复用同样禁止
        con.execute("INSERT INTO pipette_command(id,case_id,method,seq,kind,reservoir_id,well_id,"
                    "volume_ul,tip_id,expect_well_ul,depends_on,retries,state)"
                    f" VALUES('x:reuse','{CASE}','{DIRECT}',94,'aspirate','HIGH','A1',10,'T1',110,NULL,0,'planned')")
    con.close()
    with pytest.raises(PipetteError, match='liquid_already_consumed'):
        send_command(db, robot, CASE, DIRECT, 'x:dup')
    with pytest.raises(PipetteError, match='tip_contamination_risk'):
        send_command(db, robot, CASE, DIRECT, 'x:dirty')
    with pytest.raises(PipetteError, match='tip_reuse'):
        send_command(db, robot, CASE, DIRECT, 'x:reuse')
    # 机器人空枪头无法排出
    with pytest.raises(PipetteError, match='tip_empty'):
        robot.execute({'id': 'X4', 'kind': 'dispense', 'reservoir_id': 'LOW',
                       'well_id': 'A2', 'volume_ul': 10.0, 'tip_id': 'T9'})


# ---------------------------------------------------------------- 回执丢失与中断恢复

def test_lost_ack_reconciles_without_replay(tmp_path):
    db = make_db(tmp_path)
    robot = load_robot(db, CASE)
    plan(db, CASE, DILUTE)
    _apply_contract_fault(db, CASE, DILUTE, robot)  # fixture: A2 排液后 ACK 丢失
    stuck = f'{CASE}:{DILUTE}:02:dsp'
    with pytest.raises(PipetteInterrupted):
        execute(db, CASE, DILUTE, robot)
    # 安全层拒绝盲重放
    with pytest.raises(PipetteError, match='command_in_flight'):
        send_command(db, robot, CASE, DILUTE, stuck)
    actions = recover(db, CASE, DILUTE, robot)
    assert {'command': stuck, 'action': 'reconciled'} in actions
    execute(db, CASE, DILUTE, robot)
    summary = finalize(db, CASE, DILUTE, robot)
    assert summary['complete']
    # 没有重放:该命令在消耗日志中恰好一条
    assert journal_count(db, stuck) == 1
    w = wells(db, DILUTE)
    assert w['A2']['volume_ul'] == 100 and w['A2']['mass'] == pytest.approx(1000)
    assert reservoir_balance(db, DILUTE) == {'HIGH': 0, 'LOW': 200, 'DILUENT': 20}


def test_lost_ack_on_aspirate_reconciles(tmp_path):
    db = make_db(tmp_path)
    robot = load_robot(db, CASE)
    plan(db, CASE, DILUTE)
    stuck = f'{CASE}:{DILUTE}:00:asp'
    robot.faults['drop_ack'] = {stuck}
    with pytest.raises(PipetteInterrupted):
        execute(db, CASE, DILUTE, robot)
    actions = recover(db, CASE, DILUTE, robot)
    assert {'command': stuck, 'action': 'reconciled'} in actions
    execute(db, CASE, DILUTE, robot)
    assert finalize(db, CASE, DILUTE, robot)['complete']
    assert journal_count(db, stuck) == 1


def test_proven_resend_after_lost_dispense(tmp_path):
    db = make_db(tmp_path)
    robot = load_robot(db, CASE)
    plan(db, CASE, DILUTE)
    stuck = f'{CASE}:{DILUTE}:02:dsp'
    robot.faults['lose_execute'] = {stuck}  # 断电:排液根本没发生
    with pytest.raises(PipetteInterrupted):
        execute(db, CASE, DILUTE, robot)
    assert robot.wells['A2']['volume'] == 10  # 只有 HIGH 10µL
    robot.faults['lose_execute'] = set()  # 一次性故障,机器人重启后消失
    actions = recover(db, CASE, DILUTE, robot)
    # 日志无记录 + 回读 10µL == 账面 10µL → 证明未执行,允许一次重发
    assert {'command': stuck, 'action': 'proved_not_executed'} in actions
    execute(db, CASE, DILUTE, robot)
    summary = finalize(db, CASE, DILUTE, robot)
    assert summary['complete']
    w = wells(db, DILUTE)
    assert w['A2']['volume_ul'] == 100  # 不欠量,也不过量
    assert journal_count(db, stuck) == 1  # 物理上只执行了一次
    assert reservoir_balance(db, DILUTE) == {'HIGH': 0, 'LOW': 200, 'DILUENT': 20}


def test_unverifiable_dispense_quarantines_well_and_tip(tmp_path):
    db = make_db(tmp_path)
    robot = load_robot(db, CASE)
    plan(db, CASE, DILUTE)
    stuck = f'{CASE}:{DILUTE}:02:dsp'
    robot.faults['drop_ack'] = {stuck}
    robot.faults['readback_override'] = {'A2': 45.0}  # 回读与两种假设都不符
    with pytest.raises(PipetteInterrupted):
        execute(db, CASE, DILUTE, robot)
    actions = recover(db, CASE, DILUTE, robot)
    assert {'command': stuck, 'action': 'quarantined', 'well': 'A2'} in actions
    con = connect(db)
    assert con.execute("SELECT state FROM well_state WHERE well_id='A2'").fetchone()[0] == 'quarantined'
    assert con.execute("SELECT state FROM tip_ledger WHERE tip_id='T3'").fetchone()[0] == 'quarantined'
    assert con.execute("SELECT state FROM pipette_command WHERE id=?", (stuck,)).fetchone()[0] == 'void'
    con.close()
    assert 'unverifiable_dispense' in issue_codes(db, DILUTE)
    execute(db, CASE, DILUTE, robot)
    summary = finalize(db, CASE, DILUTE, robot)
    assert not summary['complete']
    w = wells(db, DILUTE)
    assert w['A2']['state'] == 'quarantined'
    assert w['A2']['volume_ul'] == 10  # 账面只记录已证明的液体
    assert w['B1']['state'] == 'complete' and w['B2']['state'] == 'complete'
    assert journal_count(db, stuck) == 1  # 绝不盲重放
    assert recover(db, CASE, DILUTE, robot) == []  # 恢复幂等


# ---------------------------------------------------------------- 重复命令

def test_duplicate_commands_are_idempotent(tmp_path):
    db = make_db(tmp_path)
    robot = load_robot(db, CASE)
    run_method(db, CASE, DILUTE, robot=robot)
    before = journal_count(db)
    balance_before = reservoir_balance(db, DILUTE)
    execute(db, CASE, DILUTE, robot)  # 全部已确认 → 空转
    assert journal_count(db) == before
    assert reservoir_balance(db, DILUTE) == balance_before
    with pytest.raises(PipetteError, match='duplicate_command'):
        send_command(db, robot, CASE, DILUTE, f'{CASE}:{DILUTE}:00:asp')
    assert plan(db, CASE, DILUTE)['already']


def test_naive_retry_overflow_discards_well_and_corrects_forward(tmp_path):
    db = make_db(tmp_path)
    robot = load_robot(db, CASE)
    plan(db, CASE, DILUTE)
    _apply_contract_fault(db, CASE, DILUTE, robot)
    stuck = f'{CASE}:{DILUTE}:02:dsp'
    with pytest.raises(PipetteInterrupted):
        execute(db, CASE, DILUTE, robot)
    with pytest.raises(PipetteError, match='command_in_flight'):
        send_command(db, robot, CASE, DILUTE, stuck)
    # 模拟绕过安全层的"未见 ACK 就重试":同一支枪头再吸再排 90µL
    robot.journal_sink = lambda entry: persist_journal(db, entry)
    robot.execute({'id': 'NAIVE-1:asp', 'kind': 'aspirate', 'reservoir_id': 'DILUENT',
                   'well_id': 'A2', 'volume_ul': 90.0, 'tip_id': 'T3'})
    robot.execute({'id': 'NAIVE-1:dsp', 'kind': 'dispense', 'reservoir_id': 'DILUENT',
                   'well_id': 'A2', 'volume_ul': 90.0, 'tip_id': 'T3'})
    assert robot.wells['A2']['volume'] == 190  # 100µL 的孔被灌成 190µL
    actions = recover(db, CASE, DILUTE, robot)
    assert any(a.get('action') == 'discarded' and a.get('well') == 'A2' for a in actions)
    # 未入账的消耗前向更正,而不是回滚抹去
    assert any(a.get('action') == 'corrected_to_physical' and a.get('reservoir') == 'DILUENT'
               for a in actions)
    # 库存刚好用尽:补孔因枪头耗尽被如实阻断
    assert any(a.get('action') == 'replan_blocked' for a in actions)
    execute(db, CASE, DILUTE, robot)
    summary = finalize(db, CASE, DILUTE, robot)
    assert not summary['complete']
    w = wells(db, DILUTE)
    assert w['A2']['state'] == 'discarded'
    assert w['A2']['volume_ul'] == 190 and w['A2']['mass'] == pytest.approx(1000)
    assert w['A1']['state'] == 'complete' and w['B1']['state'] == 'complete'
    assert w['B2']['state'] == 'failed'  # DILUENT 不足,如实失败
    balance = reservoir_balance(db, DILUTE)
    assert balance == {'HIGH': 0, 'LOW': 200, 'DILUENT': 30}
    assert all(v >= 0 for v in balance.values())
    initial, final = mass_book(db, DILUTE)
    assert final == pytest.approx(initial)  # 废弃孔的溶质也入账
    codes = issue_codes(db, DILUTE)
    for code in ('well_discarded', 'replan_blocked', 'unlogged_reservoir_consumption',
                 'insufficient_reservoir'):
        assert code in codes
    # 旧记录保留:原命令与废弃孔都还在
    con = connect(db)
    assert con.execute('SELECT state FROM pipette_command WHERE id=?', (stuck,)).fetchone()[0] == 'void'
    assert con.execute("SELECT count(*) FROM robot_journal WHERE command_id LIKE 'NAIVE-1:%'").fetchone()[0] == 2
    con.close()


def test_discard_and_replan_with_spare_inventory(tmp_path):
    contract = json.loads(FIXTURE.read_text(encoding='utf-8'))
    contract['case_id'] = 'control-plate-spare'
    contract['reservoirs'] = [
        {'id': 'HIGH', 'concentration': 100, 'available_ul': 200},
        {'id': 'LOW', 'concentration': 10, 'available_ul': 200},
        {'id': 'DILUENT', 'concentration': 0, 'available_ul': 500}]
    contract['robot']['tip_count'] = 8
    spare = tmp_path / 'spare.json'
    spare.write_text(json.dumps(contract), encoding='utf-8')
    case = 'control-plate-spare'
    db = make_db(tmp_path, spare)
    robot = load_robot(db, case)
    plan(db, case, DILUTE)
    _apply_contract_fault(db, case, DILUTE, robot)
    with pytest.raises(PipetteInterrupted):
        execute(db, case, DILUTE, robot)
    robot.journal_sink = lambda entry: persist_journal(db, entry)
    robot.execute({'id': 'NAIVE-1:asp', 'kind': 'aspirate', 'reservoir_id': 'DILUENT',
                   'well_id': 'A2', 'volume_ul': 90.0, 'tip_id': 'T3'})
    robot.execute({'id': 'NAIVE-1:dsp', 'kind': 'dispense', 'reservoir_id': 'DILUENT',
                   'well_id': 'A2', 'volume_ul': 90.0, 'tip_id': 'T3'})
    actions = recover(db, case, DILUTE, robot)
    assert any(a.get('action') == 'discarded' and a.get('well') == 'A2' for a in actions)
    assert any(a.get('action') == 'replanned' and a.get('replaces') == 'A2' for a in actions)
    execute(db, case, DILUTE, robot)
    summary = finalize(db, case, DILUTE, robot)
    assert summary['complete']  # 四个目标孔都由完成孔覆盖(A2 由替代孔完成)
    w = wells(db, DILUTE, case)
    assert w['A2']['state'] == 'discarded' and w['A2']['volume_ul'] == 190
    assert w['A2R1']['state'] == 'complete' and w['A2R1']['replaces'] == 'A2'
    assert w['A2R1']['volume_ul'] == 100
    assert w['A2R1']['mass'] / w['A2R1']['volume_ul'] == pytest.approx(10)
    assert reservoir_balance(db, DILUTE, case) == {'HIGH': 70, 'LOW': 200, 'DILUENT': 40}
    initial, final = mass_book(db, DILUTE, case)
    assert final == pytest.approx(initial)
    con = connect(db)
    src = con.execute("SELECT reservoir_id, volume_ul, tip_id FROM pipette_command"
                      " WHERE well_id='A2R1' AND kind='dispense' ORDER BY seq").fetchall()
    assert [(r['reservoir_id'], r['volume_ul']) for r in src] == [('HIGH', 10), ('DILUENT', 90)]
    assert con.execute("SELECT state FROM tip_ledger WHERE tip_id='T3'").fetchone()[0] == 'quarantined'
    con.close()


# ---------------------------------------------------------------- 串扰与读回不符

def test_well_crosstalk_detected_by_readback(tmp_path):
    db = make_db(tmp_path)
    robot = load_robot(db, CASE)
    plan(db, CASE, DILUTE)
    dsp2 = f'{CASE}:{DILUTE}:02:dsp'
    robot.faults['misaddress'] = {dsp2: 'B1'}  # 机器人错位:命令写 A2,物理进了 B1
    execute(db, CASE, DILUTE, robot)  # ACK 正常,控制器账面被骗过
    summary = finalize(db, CASE, DILUTE, robot)
    assert not summary['complete']
    w = wells(db, DILUTE)
    assert w['A2']['state'] == 'quarantined'  # 账面 100,回读 10
    assert w['B1']['state'] == 'quarantined'  # 账面 100,回读 190
    assert w['A1']['state'] == 'complete' and w['B2']['state'] == 'complete'
    assert robot.wells['A2']['volume'] == 10
    assert robot.wells['B1']['volume'] == 190
    codes = issue_codes(db, DILUTE)
    assert codes.count('readback_mismatch') == 2
    # 孔位账目互相独立:A2 的记账没有漏进 B1 的账
    assert w['A2']['mass'] == pytest.approx(1000)
    assert w['B1']['mass'] == pytest.approx(1000)


def test_readback_mismatch_blocks_completion(tmp_path):
    db = make_db(tmp_path)
    robot = load_robot(db, CASE)
    robot.faults['readback_override'] = {'B2': 95.0}
    run_method(db, CASE, DILUTE, robot=robot)
    w = wells(db, DILUTE)
    assert w['B2']['state'] == 'quarantined'
    assert w['A1']['state'] == 'complete'
    assert 'readback_mismatch' in issue_codes(db, DILUTE)
    con = connect(db)
    rb = con.execute("SELECT measured_ul FROM well_readback WHERE well_id='B2'"
                     " AND context='finalize'").fetchone()
    assert rb['measured_ul'] == 95.0
    con.close()


# ---------------------------------------------------------------- 日志与余额

def test_robot_journal_immutable_and_chained(tmp_path):
    db = make_db(tmp_path)
    robot = load_robot(db, CASE)
    run_method(db, CASE, DILUTE, robot=robot)
    con = connect(db)
    with pytest.raises(sqlite3.IntegrityError):
        con.execute('UPDATE robot_journal SET volume_ul=0 WHERE seq=1')
    con.rollback()
    with pytest.raises(sqlite3.IntegrityError):
        con.execute('DELETE FROM robot_journal WHERE seq=1')
    con.rollback()
    assert verify_journal(con)
    rows = con.execute('SELECT prev_sha256, sha256 FROM robot_journal ORDER BY seq').fetchall()
    assert rows[0]['prev_sha256'] == '0' * 64
    for prev, cur in zip(rows, rows[1:]):
        assert cur['prev_sha256'] == prev['sha256']
    con.close()


def test_reservoir_ledger_running_balance(tmp_path):
    db = make_db(tmp_path)
    robot = load_robot(db, CASE)
    run_method(db, CASE, DILUTE, robot=robot)
    con = connect(db)
    rows = con.execute('SELECT * FROM reservoir_ledger ORDER BY id').fetchall()
    assert rows
    running = {}
    for r in rows:
        bal = running.get(r['reservoir_id'], {'ul': {'HIGH': 120, 'LOW': 200, 'DILUENT': 300}[r['reservoir_id']],
                                              'mass': {'HIGH': 12000, 'LOW': 2000, 'DILUENT': 0}[r['reservoir_id']]})
        bal['ul'] += r['delta_ul']
        bal['mass'] += r['delta_mass']
        assert r['balance_ul'] == pytest.approx(bal['ul'])
        assert r['balance_mass'] == pytest.approx(bal['mass'])
        assert r['balance_ul'] >= -1e-9 and r['balance_mass'] >= -1e-9
        running[r['reservoir_id']] = bal
    for res_id, bal in running.items():
        assert bal['ul'] == pytest.approx(robot.reservoir_level(res_id))
    con.close()


def test_interrupted_resume_and_idempotent_recovery(tmp_path):
    db = make_db(tmp_path)
    robot = load_robot(db, CASE)
    plan(db, CASE, DILUTE)
    _apply_contract_fault(db, CASE, DILUTE, robot)
    with pytest.raises(PipetteInterrupted):
        execute(db, CASE, DILUTE, robot)
    # 中断后恢复:recover/execute 各自开新连接,等价于控制器重启后继续
    assert recover(db, CASE, DILUTE, robot)
    execute(db, CASE, DILUTE, robot)
    assert finalize(db, CASE, DILUTE, robot)['complete']
    count = journal_count(db)
    assert recover(db, CASE, DILUTE, robot) == []  # 再对账为空转
    execute(db, CASE, DILUTE, robot)  # 再执行为空转
    assert journal_count(db) == count
    assert reservoir_balance(db, DILUTE) == {'HIGH': 0, 'LOW': 200, 'DILUENT': 20}


# ---------------------------------------------------------------- 报告与 CLI

def test_report_json_md_and_deterministic(tmp_path):
    def full_run(folder):
        folder.mkdir()
        db = make_db(folder)
        robot = load_robot(db, CASE)
        result = run_method(db, CASE, DILUTE, robot=robot, inject_fault=True)
        assert result['summary']['complete']
        return report(db, CASE, folder / 'plate')

    first = full_run(tmp_path / 'a')
    data = json.loads(first.read_text(encoding='utf-8'))
    assert data['batch_id'] == 'batch-1'
    assert data['journal']['verified'] and data['journal']['entries'] == 12
    m = data['methods'][DILUTE]
    assert m['reservoirs']['HIGH']['remaining_ul'] == 0
    assert m['reservoirs']['LOW']['remaining_ul'] == 200
    assert m['reservoirs']['DILUENT']['remaining_ul'] == 20
    a2 = m['wells']['A2']
    assert a2['volume_ul'] == 100 and a2['concentration'] == 10
    assert a2['state'] == 'complete' and a2['batch_id'] == 'batch-1'
    assert [(s['reservoir_id'], s['volume_ul']) for s in a2['sources']] == \
        [('HIGH', 10), ('DILUENT', 90)]
    assert all(t['state'] == 'disposed' for t in m['tips'])
    stuck = [c for c in m['commands'] if c['id'].endswith('02:dsp')][0]
    assert stuck['state'] == 'reconciled'  # 恢复动作留痕
    md = first.with_suffix('.md').read_text(encoding='utf-8')
    assert '质控板制备审计' in md and '| A2 |' in md and '试剂库存账' in md and '枪头去向' in md
    second = full_run(tmp_path / 'b')
    assert first.read_bytes() == second.read_bytes()
    assert first.with_suffix('.md').read_bytes() == second.with_suffix('.md').read_bytes()


def test_cli_pipette_flow(tmp_path, monkeypatch, capsys):
    import sys
    from samplechain.__main__ import main

    def cli(*argv):
        monkeypatch.setattr(sys, 'argv', ['samplechain', *argv])
        main()

    db = tmp_path / 'lab.db'
    cli('init', '--db', str(db), '--batch', 'batch-1', '--sample', 'sample-1')
    cli('pipette-register', '--db', str(db), '--contract', str(FIXTURE))
    assert capsys.readouterr().out.strip() == CASE
    cli('pipette-plan', '--db', str(db), '--case', CASE, '--method', DILUTE)
    assert json.loads(capsys.readouterr().out)['planned'] == 12
    cli('pipette-run', '--db', str(db), '--case', CASE, '--method', DILUTE, '--inject-fault')
    assert json.loads(capsys.readouterr().out)['summary']['complete']
    cli('pipette-report', '--db', str(db), '--case', CASE, '--out', str(tmp_path / 'plate'))
    assert (tmp_path / 'plate.json').exists() and (tmp_path / 'plate.md').exists()
