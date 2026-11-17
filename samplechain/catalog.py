"""Freeze supplier snapshots for later extraction, matching, and publication."""
import hashlib
import json
from pathlib import Path
from .store import connect


def register(db_path, case_id, contract_path, page_dir):
    contract_path = Path(contract_path).resolve()
    page_dir = Path(page_dir).resolve()
    raw = contract_path.read_bytes()
    contract = json.loads(raw)
    if not case_id or not contract.get('source_pages') or not contract.get('publish_target'):
        raise ValueError('catalog_contract_invalid')
    pages = []
    for name in contract['source_pages']:
        path = (page_dir / name).resolve()
        if path.parent != page_dir or path.suffix != '.html':
            raise ValueError('catalog_page_invalid')
        body = path.read_bytes()
        pages.append({'name': name, 'sha256': hashlib.sha256(body).hexdigest(),
                      'bytes': body.decode('utf-8')})
    db = connect(db_path)
    try:
        samples = [r['id'] for r in db.execute('SELECT id FROM sample ORDER BY id')]
        if not samples:
            raise ValueError('laboratory_not_initialized')
        payload = json.dumps({'contract': contract,
                              'contract_sha256': hashlib.sha256(raw).hexdigest(),
                              'pages': pages, 'sample_inventory_context': samples},
                             sort_keys=True, separators=(',', ':'))
        with db:
            old = db.execute('SELECT payload FROM catalog_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('catalog_case_conflict')
            db.execute('INSERT OR IGNORE INTO catalog_case VALUES (?,?)', (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'pages': len(pages)}
