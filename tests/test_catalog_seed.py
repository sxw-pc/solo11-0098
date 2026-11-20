import hashlib
import json
from pathlib import Path
import pytest
from samplechain.__main__ import init
from samplechain.catalog import register
from samplechain.store import connect


FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def test_catalog_case_freezes_source_pages_without_publishing(tmp_path):
    db_path = tmp_path / 'catalog.db'
    init(db_path, 'batch-c', 'sample-1')
    assert register(db_path, 'cat-1', FIX / 'catalog-contract.json', FIX) == {'case_id': 'cat-1', 'pages': 3}
    db = connect(db_path)
    payload = json.loads(db.execute('SELECT payload FROM catalog_case').fetchone()[0])
    assert [p['name'] for p in payload['pages']] == ['vendor-a.html', 'vendor-a-2.html', 'vendor-b.html']
    assert all(hashlib.sha256(p['bytes'].encode()).hexdigest() == p['sha256'] for p in payload['pages'])
    assert payload['sample_inventory_context'] == ['sample-1']
    assert not db.execute("SELECT name FROM sqlite_master WHERE name='catalog_publication'").fetchone()
    db.close()


def test_catalog_case_rejects_changed_contract(tmp_path):
    db_path = tmp_path / 'catalog.db'
    init(db_path, 'batch-c', 'sample-1')
    register(db_path, 'cat-1', FIX / 'catalog-contract.json', FIX)
    changed = tmp_path / 'contract.json'
    case = json.loads((FIX / 'catalog-contract.json').read_text())
    case['contract_version'] = 'cat-v2'
    changed.write_text(json.dumps(case))
    with pytest.raises(ValueError, match='catalog_case_conflict'):
        register(db_path, 'cat-1', changed, FIX)


def test_seed_freezes_real_pages_and_sources_only(tmp_path):
    db_path = tmp_path / 'catalog.db'
    init(db_path, 'batch-c', 'sample-1')
    register(db_path, 'cat-1', FIX / 'catalog-contract.json', FIX)
    db = connect(db_path)
    payload = json.loads(db.execute('SELECT payload FROM catalog_case').fetchone()[0])
    assert set(payload) == {'contract', 'contract_sha256', 'pages', 'sample_inventory_context'}
    for page in payload['pages']:
        assert page['bytes'].encode() == (FIX / page['name']).read_bytes()
    assert payload['contract']['source_pages'] == [p['name'] for p in payload['pages']]
    for table in ('catalog_offer', 'catalog_issue', 'catalog_version', 'lims_publication'):
        assert db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] == 0
    db.close()


def test_register_requires_initialized_laboratory(tmp_path):
    with pytest.raises(ValueError, match='laboratory_not_initialized'):
        register(tmp_path / 'empty.db', 'cat-1', FIX / 'catalog-contract.json', FIX)
