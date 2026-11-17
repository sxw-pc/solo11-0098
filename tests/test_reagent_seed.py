from pathlib import Path
from samplechain.__main__ import init
from samplechain.reagents import register
from samplechain.store import connect


def test_reagent_case_binds_lab_batch_without_decisions(tmp_path):
    db = tmp_path / 'lab.db'
    init(db, 'batch-1', 'sample-1')
    contract = Path(__file__).parents[1] / 'fixtures' / 'reagent-contract.json'
    assert register(db, contract) == 'reagents-2026-05-01'
    con = connect(db)
    assert con.execute('SELECT count(*) FROM decision').fetchone()[0] == 0
    case = con.execute('SELECT * FROM reagent_case').fetchone()
    assert case['batch_id'] == 'batch-1' and case['contract_sha256']
    con.close()
