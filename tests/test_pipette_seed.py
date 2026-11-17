from pathlib import Path
from samplechain.__main__ import init
from samplechain.pipette import register
from samplechain.store import connect


def test_pipette_case_is_operational_not_sample_eligibility(tmp_path):
    db=tmp_path/'lab.db'
    init(db,'batch-1','sample-1')
    fixture=Path(__file__).parents[1]/'fixtures'/'pipette-contract.json'
    assert register(db,fixture)=='control-plate-2026-05-09'
    con=connect(db)
    row=con.execute('SELECT * FROM pipette_case').fetchone()
    assert row['batch_id']=='batch-1' and row['contract_sha256']
    assert con.execute('SELECT count(*) FROM decision').fetchone()[0]==0
    con.close()
