import json
from pathlib import Path
from samplechain.__main__ import init
from samplechain.ingest import ingest
from samplechain.instruments import register
from samplechain.store import connect


def test_instrument_campaign_uses_actual_devices_without_sample_decision(tmp_path):
    db = tmp_path / 'lab.db'
    rows = []
    for device, sample in [('scanner-A', 'S1'), ('scanner-B', 'S2')]:
        init(db, 'batch-1', sample)
        for seq, kind in enumerate(('collect','handoff','receive','fridge_in','fridge_out','test'),1):
            rows.append({'kind':kind,'device':device,'seq':seq,'sample_id':sample,
                         'ts':f'2026-04-01T00:{seq:02d}:00Z','actor':'tech'})
    events = tmp_path / 'scans.jsonl'
    events.write_text('\n'.join(json.dumps(r) for r in rows), encoding='utf-8')
    ingest(events, db)
    contract = Path(__file__).parents[1] / 'fixtures' / 'instrument-contract.json'
    assert register(db, contract) == 'scanner-calibration-2026-04-02'
    con = connect(db)
    assert con.execute('SELECT count(*) FROM decision').fetchone()[0] == 0
    assert con.execute('SELECT device_digest FROM instrument_case').fetchone()[0]
    con.close()
