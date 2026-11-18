"""纯水闭环测试公共夹具：真实实验室起点 + 冻结传感器 + 模拟 PLC。"""
import json
from pathlib import Path

import pytest

from samplechain.__main__ import init
from samplechain.ingest import ingest
from samplechain import water_loop as wl
from samplechain.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'
CASE = 'water-1'


@pytest.fixture
def seeded_db(tmp_path):
    """登记真实 sample-1/sample-2 采集来源并冻结 water_case（不含任何控制结果）。"""
    db = tmp_path / 'water.db'
    init(db, 'batch-w', 'sample-1')
    init(db, 'batch-w', 'sample-2')
    assert ingest(FIX / 'water-events.jsonl', db)['imported'] == 2
    wl.register(db, CASE, FIX / 'water-contract.json', FIX / 'water-sensors.csv')
    return db


@pytest.fixture
def armed_db(seeded_db):
    """冻结版本快照并采录 RAW/POLISH/RESERVE 原字节解析出的传感行。"""
    wl.create_snapshot(seeded_db, CASE)
    result = wl.load_frozen_telemetry(seeded_db, CASE)
    assert result['accepted'] == 5 and result['rejected'] == []
    return seeded_db


def frozen_case(db):
    con = connect(db)
    try:
        return wl.load_case(con, CASE)
    finally:
        con.close()


def run_route(db, route, plc=None):
    wl.freeze_plan(db, CASE, route)
    result = wl.execute_plan(db, CASE, plc or wl.SimPLC())
    assert not result.get('awaiting_meter')
    return result


def ledger_of(db):
    con = connect(db)
    try:
        return wl.rebuild_ledger(con, CASE, wl.load_case(con, CASE))
    finally:
        con.close()


def accounts_of(db):
    con = connect(db)
    try:
        return wl.order_accounts(con, CASE, wl.load_case(con, CASE))
    finally:
        con.close()


def deliveries_of(db):
    con = connect(db)
    try:
        return [dict(r) for r in con.execute(
            'SELECT * FROM water_delivery WHERE case_id=? ORDER BY id', (CASE,))]
    finally:
        con.close()
