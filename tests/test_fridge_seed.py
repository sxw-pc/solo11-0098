import json
from pathlib import Path
import pytest
from samplechain.__main__ import init
from samplechain.ingest import ingest
from samplechain.fridge_control import register
from samplechain.store import connect


FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def test_fridge_case_links_real_samples_and_sensor_bytes(tmp_path):
    db_path = tmp_path / 'fridge.db'
    init(db_path, 'batch-f', 'sample-1')
    init(db_path, 'batch-f', 'sample-2')
    assert ingest(FIX / 'fridge-events.jsonl', db_path)['imported'] == 4
    assert register(db_path, 'fridge-01', FIX / 'fridge-case.json', FIX / 'fridge-sensors.csv') == {
        'case_id': 'fridge-01', 'zones': 2}
    db = connect(db_path)
    payload = json.loads(db.execute('SELECT payload FROM fridge_case').fetchone()[0])
    db.close()
    assert [r['sample']['id'] for r in payload['observed']] == ['sample-1', 'sample-2']
    assert len(payload['sensor_sha256']) == 64


def test_fridge_case_requires_real_in_fridge_events(tmp_path):
    db_path = tmp_path / 'fridge.db'
    init(db_path, 'batch-f', 'sample-1')
    init(db_path, 'batch-f', 'sample-2')
    with pytest.raises(ValueError, match='fridge_sample_not_observed'):
        register(db_path, 'fridge-01', FIX / 'fridge-case.json', FIX / 'fridge-sensors.csv')
