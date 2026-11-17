import json
from pathlib import Path
import pytest
from samplechain.__main__ import init
from samplechain.ingest import ingest
from samplechain.settlement import register
from samplechain.store import connect


FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def test_settlement_fixture_uses_real_samples_and_bank_source(tmp_path):
    db = tmp_path / 'settlement.db'
    init(db, 'batch-s', 'sample-1')
    init(db, 'batch-s', 'sample-2')
    assert ingest(FIX / 'settlement-events.jsonl', db)['imported'] == 2
    assert register(db, 'settlement-01', FIX / 'settlement-case.json',
                    FIX / 'settlement-bank.jsonl') == {'case_id': 'settlement-01',
                                                      'invoices': 2, 'bank_receipts': 2}
    con = connect(db)
    doc = json.loads(con.execute('SELECT payload FROM settlement_case').fetchone()[0])
    con.close()
    assert [x['sample']['id'] for x in doc['invoices']] == ['sample-1', 'sample-2']
    assert doc['bank_receipts'][0]['candidate_invoices'] == ['INV-1', 'INV-2']
    assert doc['bank_receipts'][1]['candidate_invoices'] == ['INV-1']
    assert len(doc['bank_sha256']) == 64


def test_settlement_requires_actual_collected_sample(tmp_path):
    db = tmp_path / 'settlement.db'
    init(db, 'batch-s', 'sample-1')
    init(db, 'batch-s', 'sample-2')
    with pytest.raises(ValueError, match='settlement_sample_unobserved'):
        register(db, 'settlement-01', FIX / 'settlement-case.json', FIX / 'settlement-bank.jsonl')
