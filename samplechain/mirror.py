"""Freeze signed scanner chunks; canonical reconciliation is not implemented."""
import base64
import json
from hashlib import sha256
from pathlib import Path
from .store import connect


def register(db_path, manifest_path):
    manifest_path = Path(manifest_path)
    raw = manifest_path.read_bytes()
    manifest = json.loads(raw)
    con = connect(db_path)
    con.executescript('''CREATE TABLE IF NOT EXISTS mirror_case(
      id TEXT PRIMARY KEY, device TEXT NOT NULL, manifest_sha256 TEXT NOT NULL,
      manifest_json TEXT NOT NULL, frozen_chunks_json TEXT NOT NULL
    );''')
    if not con.execute('SELECT 1 FROM event WHERE device=?',(manifest['device'],)).fetchone():
        raise ValueError('unknown_scanner')
    chunks = {}
    for item in manifest['chunks']:
        data = (manifest_path.parent / item['file']).read_bytes()
        chunks[item['id']] = {'sha256':sha256(data).hexdigest(),
                              'raw_base64':base64.b64encode(data).decode()}
    with con:
        con.execute('INSERT INTO mirror_case VALUES (?,?,?,?,?)',
                    (manifest['case_id'],manifest['device'],sha256(raw).hexdigest(),
                     raw.decode(),json.dumps(chunks,sort_keys=True)))
    con.close()
    return manifest['case_id']
