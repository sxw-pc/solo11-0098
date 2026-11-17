from pathlib import Path
from samplechain.__main__ import init
from samplechain.spectra import register
from samplechain.store import connect


def test_spectrum_case_freezes_raw_signals_without_sample_decision(tmp_path):
    db=tmp_path/'lab.db'
    init(db,'batch-1','sample-1')
    fixture=Path(__file__).parents[1]/'fixtures'/'spectral-reader.json'
    assert register(db,fixture)=='control-spectrum-2026-05-08'
    con=connect(db)
    row=con.execute('SELECT * FROM spectrum_case').fetchone()
    assert row['batch_id']=='batch-1' and row['source_sha256']
    assert con.execute('SELECT count(*) FROM decision').fetchone()[0]==0
    con.close()
