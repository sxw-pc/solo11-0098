"""Freeze synthetic reader channels and standards; no concentrations calculated."""
import json
from hashlib import sha256
from pathlib import Path
from .store import connect


def register(db_path, fixture_path):
    raw=Path(fixture_path).read_bytes()
    fixture=json.loads(raw)
    con=connect(db_path)
    con.executescript('''CREATE TABLE IF NOT EXISTS spectrum_case(
      id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batch(id),
      source_sha256 TEXT NOT NULL, fixture_json TEXT NOT NULL
    );''')
    if not con.execute('SELECT 1 FROM batch WHERE id=?',(fixture['batch_id'],)).fetchone():
        raise ValueError('unknown_lab_batch')
    with con:
        con.execute('INSERT INTO spectrum_case VALUES (?,?,?,?)',
                    (fixture['case_id'],fixture['batch_id'],sha256(raw).hexdigest(),raw.decode()))
    con.close()
    return fixture['case_id']
