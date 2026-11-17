"""Freeze real in-fridge samples and sensor readings; no control planner."""
import csv
import hashlib
import io
import json
from pathlib import Path
from .store import connect


def register(db_path, case_id, fixture, sensors):
    fixture_path = Path(fixture).resolve()
    sensor_path = Path(sensors).resolve()
    case_raw = fixture_path.read_bytes()
    sensor_raw = sensor_path.read_bytes()
    case = json.loads(case_raw)
    readings = list(csv.DictReader(io.StringIO(sensor_raw.decode('utf-8-sig'))))
    if not case_id or not case.get('zones') or not case.get('contracts'):
        raise ValueError('fridge_case_invalid')
    sensor_by_zone = {r['zone']: r for r in readings}
    if len(sensor_by_zone) != len(case['zones']) or len(readings) != len(case['zones']):
        raise ValueError('fridge_sensor_count')
    db = connect(db_path)
    try:
        observed = []
        for zone in case['zones']:
            sid = zone['sample_id']
            reading = sensor_by_zone.get(zone['id'])
            if reading is None or float(reading['temperature_c']) != zone['initial_c']:
                raise ValueError('fridge_sensor_mismatch')
            sample = db.execute('SELECT id,batch_id FROM sample WHERE id=?', (sid,)).fetchone()
            events = [dict(row) for row in db.execute(
                'SELECT e.id,e.ts,e.device,s.sha256 AS source_sha256 '
                'FROM event e JOIN source s ON s.id=e.source_id '
                'WHERE e.sample_id=? AND e.kind=? ORDER BY e.ts,e.id', (sid, 'fridge_in'))]
            if sample is None or not events:
                raise ValueError('fridge_sample_not_observed')
            observed.append({'zone': zone['id'], 'sample': dict(sample),
                             'fridge_events': events, 'sensor': reading})
        payload = json.dumps({'case': case, 'case_sha256': hashlib.sha256(case_raw).hexdigest(),
                              'sensor_sha256': hashlib.sha256(sensor_raw).hexdigest(),
                              'fixture': str(fixture_path), 'sensors': str(sensor_path),
                              'observed': observed}, sort_keys=True, separators=(',', ':'))
        with db:
            old = db.execute('SELECT payload FROM fridge_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('fridge_case_conflict')
            db.execute('INSERT OR IGNORE INTO fridge_case VALUES (?,?)', (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'zones': len(observed)}
