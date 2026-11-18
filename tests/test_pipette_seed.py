import json
import sqlite3
from hashlib import sha256
from pathlib import Path

import pytest

from samplechain.__main__ import init
from samplechain.pipette import register
from samplechain.store import connect

FIXTURE = Path(__file__).parents[1] / 'fixtures' / 'pipette-contract.json'


def test_pipette_case_is_operational_not_sample_eligibility(tmp_path):
    db = tmp_path / 'lab.db'
    init(db, 'batch-1', 'sample-1')
    fixture = Path(__file__).parents[1] / 'fixtures' / 'pipette-contract.json'
    assert register(db, fixture) == 'control-plate-2026-05-09'
    con = connect(db)
    row = con.execute('SELECT * FROM pipette_case').fetchone()
    assert row['batch_id'] == 'batch-1' and row['contract_sha256']
    assert con.execute('SELECT count(*) FROM decision').fetchone()[0] == 0
    con.close()


def test_register_binds_only_real_batch(tmp_path):
    db = tmp_path / 'lab.db'
    init(db, 'batch-1', 'sample-1')
    contract = json.loads(FIXTURE.read_text(encoding='utf-8'))
    contract['batch_id'] = 'batch-999'
    contract['case_id'] = 'case-foreign'
    fake = tmp_path / 'fake.json'
    fake.write_text(json.dumps(contract), encoding='utf-8')
    with pytest.raises(ValueError, match='unknown_lab_batch'):
        register(db, fake)
    con = connect(db)
    assert con.execute('SELECT count(*) FROM pipette_case').fetchone()[0] == 0
    con.close()


def test_register_freezes_contract_without_commands_or_liquid(tmp_path):
    db = tmp_path / 'lab.db'
    init(db, 'batch-1', 'sample-1')
    raw = FIXTURE.read_bytes()
    register(db, FIXTURE)
    con = connect(db)
    row = con.execute('SELECT * FROM pipette_case').fetchone()
    assert row['contract_sha256'] == sha256(raw).hexdigest()
    assert row['contract_json'] == raw.decode()
    # 注册只冻结合同:没有命令、没有孔位液体、没有库存账、没有机器人日志
    for table in ('pipette_command', 'robot_journal', 'reservoir_ledger', 'well_state',
                  'tip_ledger', 'well_readback', 'pipette_issue'):
        assert con.execute(f'SELECT count(*) FROM {table}').fetchone()[0] == 0, table
    # 与样本可检测性判定互不影响
    assert con.execute('SELECT count(*) FROM decision').fetchone()[0] == 0
    con.close()


def test_register_duplicate_case_rejected(tmp_path):
    db = tmp_path / 'lab.db'
    init(db, 'batch-1', 'sample-1')
    assert register(db, FIXTURE) == 'control-plate-2026-05-09'
    with pytest.raises(sqlite3.IntegrityError):
        register(db, FIXTURE)
