"""Freeze real laboratory context and raw water telemetry; no valve controller."""
import hashlib
import json
from pathlib import Path
from .store import connect


def register(db_path, case_id, contract_path, sensor_path):
    contract_raw = Path(contract_path).read_bytes()
    sensor_raw = Path(sensor_path).read_bytes()
    contract = json.loads(contract_raw)
    if not case_id or not contract.get('orders') or not contract.get('valves'):
        raise ValueError('water_contract_invalid')
    db = connect(db_path)
    try:
        observations = []
        for sample_id in contract['samples']:
            sample = db.execute('SELECT id,batch_id FROM sample WHERE id=?', (sample_id,)).fetchone()
            event = db.execute("SELECT e.id,s.sha256 AS source_sha FROM event e JOIN source s ON s.id=e.source_id WHERE e.sample_id=? AND e.kind='collect' ORDER BY e.id LIMIT 1", (sample_id,)).fetchone()
            if sample is None or sample['batch_id'] != contract['batch'] or event is None:
                raise ValueError('water_lab_context_missing')
            observations.append({'sample': dict(sample), 'collect': dict(event)})
        payload = json.dumps({'contract': contract,
                              'contract_sha256': hashlib.sha256(contract_raw).hexdigest(),
                              'sensor_csv': sensor_raw.decode('utf-8-sig'),
                              'sensor_sha256': hashlib.sha256(sensor_raw).hexdigest(),
                              'observations': observations}, sort_keys=True, separators=(',', ':'))
        with db:
            old = db.execute('SELECT payload FROM water_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('water_case_conflict')
            db.execute('INSERT OR IGNORE INTO water_case VALUES (?,?)', (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'orders': len(contract['orders'])}
