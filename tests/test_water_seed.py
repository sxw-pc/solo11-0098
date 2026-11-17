import json
from pathlib import Path
import pytest
from samplechain.__main__ import init
from samplechain.ingest import ingest
from samplechain.water_loop import register
from samplechain.store import connect


FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def test_water_case_freezes_real_lab_context_and_sensor_csv(tmp_path):
    db_path = tmp_path / 'water.db'
    init(db_path, 'batch-w', 'sample-1')
    init(db_path, 'batch-w', 'sample-2')
    assert ingest(FIX / 'water-events.jsonl', db_path)['imported'] == 2
    assert register(db_path, 'water-1', FIX / 'water-contract.json', FIX / 'water-sensors.csv') == {'case_id': 'water-1', 'orders': 2}
    db = connect(db_path)
    payload = json.loads(db.execute('SELECT payload FROM water_case').fetchone()[0])
    assert [o['sample']['id'] for o in payload['observations']] == ['sample-1', 'sample-2']
    assert len(payload['sensor_sha256']) == 64
    assert not db.execute("SELECT name FROM sqlite_master WHERE name='water_delivery'").fetchone()
    db.close()


def test_water_case_requires_real_collect_source(tmp_path):
    db_path = tmp_path / 'water.db'
    init(db_path, 'batch-w', 'sample-1')
    init(db_path, 'batch-w', 'sample-2')
    with pytest.raises(ValueError, match='water_lab_context_missing'):
        register(db_path, 'water-1', FIX / 'water-contract.json', FIX / 'water-sensors.csv')
