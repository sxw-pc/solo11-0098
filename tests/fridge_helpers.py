"""Shared fixtures/helpers for fridge control tests."""
import json
from pathlib import Path

import pytest

from samplechain.__main__ import init
from samplechain.ingest import ingest
from samplechain.fridge_control import register
from samplechain.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def seed_case(tmp_path, case, sensors, db_name='fridge.db', case_id='fridge-01',
              samples=('sample-1', 'sample-2'), events=None):
    db = tmp_path / db_name
    for sid in samples:
        init(db, 'batch-f', sid)
    events_path = tmp_path / 'events.jsonl'
    if events is None:
        events = [
            {'kind': 'collect', 'device': 'scanner-1', 'seq': 1,
             'sample_id': 'sample-1', 'ts': '2026-01-01T08:00:00Z'},
            {'kind': 'fridge_in', 'device': 'scanner-1', 'seq': 2,
             'sample_id': 'sample-1', 'ts': '2026-01-01T08:05:00Z'},
            {'kind': 'collect', 'device': 'scanner-1', 'seq': 3,
             'sample_id': 'sample-2', 'ts': '2026-01-01T08:10:00Z'},
            {'kind': 'fridge_in', 'device': 'scanner-1', 'seq': 4,
             'sample_id': 'sample-2', 'ts': '2026-01-01T08:15:00Z'}]
    events_path.write_text('\n'.join(json.dumps(e) for e in events), encoding='utf-8')
    ingest(events_path, db)
    case_path = tmp_path / 'case.json'
    case_path.write_text(json.dumps(case), encoding='utf-8')
    sensor_path = tmp_path / 'sensors.csv'
    sensor_path.write_text(sensors, encoding='utf-8')
    register(db, case_id, case_path, sensor_path)
    return db


def default_case(**overrides):
    case = {
        'zones': [
            {'id': 'Z1', 'sample_id': 'sample-1', 'initial_c': 4,
             'guard_c': 6, 'warming_c': [3, 3]},
            {'id': 'Z2', 'sample_id': 'sample-2', 'initial_c': 4,
             'guard_c': 5, 'warming_c': [2, 2]}],
        'cooler': {'max_zones_per_period': 1, 'cooling_c': 2,
                   'tariff_cents': [100, 1000]},
        'total_exposure_limit': 6,
        'contracts': ['min_energy', 'min_max_zone_exposure']}
    case.update(overrides)
    return case


def initial_sensors(case):
    lines = ['zone,at,temperature_c,unit']
    for z in case['zones']:
        lines.append(f"{z['id']},2026-01-01T09:00:00Z,{z['initial_c']},C")
    return '\n'.join(lines) + '\n'


def write_feed(tmp_path, rows, name='feed.csv', at='2026-01-01T10:00:00Z'):
    path = tmp_path / name
    lines = ['zone,at,temperature_c,unit']
    for zone, temp in rows:
        lines.append(f'{zone},{at},{temp},C')
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    return path


def source_hashes(db):
    con = connect(db)
    hashes = {r[0]: r[1] for r in con.execute(
        "SELECT path, sha256 FROM source ORDER BY path")}
    con.close()
    return hashes
