from pathlib import Path
from samplechain.__main__ import init
from samplechain.ingest import ingest
from samplechain.mirror import register
from samplechain.store import connect


def test_mirror_case_freezes_conflicting_chunks_not_sample_decision(tmp_path):
    db = tmp_path / 'lab.db'
    init(db, 'batch-1', 'S1')
    fixtures = Path(__file__).parents[1] / 'fixtures'
    ingest(fixtures / 'primary-1.jsonl', db)
    assert register(db, fixtures / 'mirror-manifest.json') == 'scanner-D1-2026-05-06'
    con = connect(db)
    row = con.execute('SELECT * FROM mirror_case').fetchone()
    assert row['device'] == 'D1' and 'raw_base64' in row['frozen_chunks_json']
    assert con.execute('SELECT count(*) FROM decision').fetchone()[0] == 0
    con.close()
