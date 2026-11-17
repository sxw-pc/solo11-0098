"""Intake of a synthetic robotic control-plate run; no commands are generated."""
import json
from hashlib import sha256
from pathlib import Path
from .store import connect


def register(db_path, contract_path):
    raw=Path(contract_path).read_bytes()
    contract=json.loads(raw)
    con=connect(db_path)
    con.executescript('''CREATE TABLE IF NOT EXISTS pipette_case(
      id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batch(id),
      contract_sha256 TEXT NOT NULL, contract_json TEXT NOT NULL
    );''')
    if not con.execute('SELECT 1 FROM batch WHERE id=?',(contract['batch_id'],)).fetchone():
        raise ValueError('unknown_lab_batch')
    with con:
        con.execute('INSERT INTO pipette_case VALUES (?,?,?,?)',
                    (contract['case_id'],contract['batch_id'],sha256(raw).hexdigest(),raw.decode()))
    con.close()
    return contract['case_id']
