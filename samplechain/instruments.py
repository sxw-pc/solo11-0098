"""Calibration campaign intake for physical scanners, independent of sample decisions."""
import json
from hashlib import sha256
from pathlib import Path
from .store import connect


def register(db_path, contract_path):
    raw = Path(contract_path).read_bytes()
    contract = json.loads(raw)
    con = connect(db_path)
    con.executescript('''CREATE TABLE IF NOT EXISTS instrument_case(
      id TEXT PRIMARY KEY, contract_sha256 TEXT NOT NULL,
      contract_json TEXT NOT NULL, device_digest TEXT NOT NULL
    );''')
    devices = []
    for item in contract['devices']:
        device = item['id']
        rows = [dict(r) for r in con.execute(
            'SELECT id,source_id,device,seq,sha256 FROM event WHERE device=? ORDER BY seq,id',
            (device,))]
        if not rows:
            raise ValueError('unknown_operational_device')
        devices.append({'id': device, 'event_rows': rows})
    digest = sha256(json.dumps(devices, sort_keys=True).encode()).hexdigest()
    with con:
        con.execute('INSERT INTO instrument_case VALUES (?,?,?,?)',
                    (contract['case_id'], sha256(raw).hexdigest(), raw.decode(), digest))
    con.close()
    return contract['case_id']
