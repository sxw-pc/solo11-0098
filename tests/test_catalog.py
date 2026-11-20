import json
import threading
from pathlib import Path

import pytest

from samplechain.__main__ import init
from samplechain.catalog import (register, extract, build_candidate, publish, recover,
                                 catalog_report, sign_alias)
from samplechain.lims import MockLims
from samplechain.store import connect

FIX = Path(__file__).resolve().parents[1] / 'fixtures'
TARGET = 'mock-lims-catalog'


def load_contract(**over):
    contract = json.loads((FIX / 'catalog-contract.json').read_text())
    contract.update(over)
    return contract


def make_case(tmp_path, case_id='cat-1', contract=None, page_dir=FIX):
    tmp_path = Path(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / 'cat.db'
    init(db_path, 'batch-c', 'sample-1')
    contract_path = tmp_path / f'{case_id}-contract.json'
    contract_path.write_text(json.dumps(contract if contract is not None else load_contract()))
    register(db_path, case_id, contract_path, page_dir)
    return db_path


def variant_pages(tmp_path, **replace):
    pages = Path(tmp_path) / 'pages'
    pages.mkdir(parents=True, exist_ok=True)
    for name in ('vendor-a.html', 'vendor-a-2.html', 'vendor-b.html'):
        (pages / name).write_bytes((FIX / name).read_bytes())
    for name, content in replace.items():
        (pages / name).write_text(content)
    return pages


def published_payload(db_path):
    con = connect(db_path)
    row = con.execute("SELECT payload FROM catalog_version WHERE status='published'").fetchone()
    con.close()
    return json.loads(row['payload'])


def lims_catalog(lims):
    return json.loads(lims.status(TARGET)['catalog'])


# ---------- 抽取与单位归一 ----------

def test_extract_normalizes_cross_format_offers(tmp_path):
    db = make_case(tmp_path)
    result = extract(db, 'cat-1')
    assert result['status'] == 'ok' and result['offers'] == 3
    con = connect(db)
    rows = {(r['supplier'], r['gtin']): dict(r) for r in con.execute('SELECT * FROM catalog_offer')}
    con.close()
    cold_a = rows[('A', '00012345000017')]
    cold_b = rows[('B', '00012345000031')]
    room = rows[('A', '00012345000024')]
    assert cold_a['unit_price_cents'] == 200.0   # 10000 分 / 50 tests
    assert cold_b['unit_price_cents'] == 180.0   # 18000 分 / 100 reactions
    assert room['unit_price_cents'] == 152.0
    assert {r['canonical_unit'] for r in rows.values()} == {'reactions'}
    assert cold_a['page'] == 'vendor-a.html' and cold_b['page'] == 'vendor-b.html'
    assert all(len(r['page_sha256']) == 64 and len(r['offer_sha256']) == 64
               for r in rows.values())


# ---------- 两条身份策略 ----------

def test_strict_gtin_publishes_three_products(tmp_path):
    db = make_case(tmp_path)
    lims = MockLims(tmp_path / 'lims.db')
    cand = build_candidate(db, 'cat-1', 'strict_gtin')
    assert cand['seq'] == 1 and cand['products'] == 3
    out = publish(db, 'cat-1', cand['version_id'], 'nonce-1', lims)
    assert out['status'] == 'published'
    payload = published_payload(db)
    keys = [p['product_key'] for p in payload['products']]
    assert keys == ['gtin:00012345000017', 'gtin:00012345000024', 'gtin:00012345000031']
    cold = next(p for p in payload['products'] if p['product_key'] == 'gtin:00012345000017')
    assert cold['recommended']['supplier'] == 'A'
    assert cold['recommended']['unit_price_cents'] == 200.0
    assert lims.status(TARGET)['seq'] == 1


def test_signed_alias_merge_unifies_cold_packs(tmp_path):
    db = make_case(tmp_path)
    lims = MockLims(tmp_path / 'lims.db')
    cand = build_candidate(db, 'cat-1', 'signed_alias_merge')
    assert cand['products'] == 2
    assert publish(db, 'cat-1', cand['version_id'], 'nonce-1', lims)['status'] == 'published'
    catalog = lims_catalog(lims)
    merged = next(p for p in catalog['products'] if p['product_key'].startswith('alias:'))
    assert merged['product_key'] == 'alias:M-742@2-8C'
    assert merged['gtins'] == ['00012345000017', '00012345000031']
    assert {o['supplier'] for o in merged['offers']} == {'A', 'B'}
    # 贪心反例：按页面顺序会选中 A 的 200；归一后最低价是 B 的 180
    assert merged['recommended']['supplier'] == 'B'
    assert merged['recommended']['unit_price_cents'] == 180.0
    assert merged['identity']['alias']['signer'] == 'MOCK-LAB'


def test_room_temp_similar_name_never_merged(tmp_path):
    for strategy in ('strict_gtin', 'signed_alias_merge'):
        db = make_case(tmp_path / strategy)
        cand = build_candidate(db, 'cat-1', strategy)
        con = connect(db)
        payload = json.loads(con.execute('SELECT payload FROM catalog_version WHERE id=?',
                                         (cand['version_id'],)).fetchone()[0])
        con.close()
        room = next(p for p in payload['products'] if p['gtins'] == ['00012345000024'])
        assert room['product_key'] == 'gtin:00012345000024'
        assert room['storage'] == '15-25C' and len(room['offers']) == 1
        for product in payload['products']:
            assert len({o['storage'] for o in product['offers']}) == 1
        cold = next(p for p in payload['products'] if p['storage'] == '2-8C'
                    and '00012345000017' in p['gtins'])
        assert 'M-742-RT' not in cold['gtins'] + [cold['maker_code']]
        assert cold['maker_code'] == 'M-742'


# ---------- 整批待核的反例 ----------

def test_missing_second_page_pends_whole_batch(tmp_path):
    contract = load_contract(source_pages=['vendor-a.html', 'vendor-b.html'])
    db = make_case(tmp_path, contract=contract)
    result = extract(db, 'cat-1')
    assert result['status'] == 'pending'
    assert 'pagination_incomplete' in {i['code'] for i in result['issues']}
    con = connect(db)
    assert con.execute('SELECT count(*) FROM catalog_offer').fetchone()[0] == 0
    with pytest.raises(ValueError, match='catalog_extraction_pending'):
        build_candidate(db, 'cat-1', 'strict_gtin')
    assert con.execute('SELECT count(*) FROM catalog_version').fetchone()[0] == 0
    con.close()


def test_revision_drift_across_pages_pends(tmp_path):
    drift = ('<html><body><article data-supplier="A" data-page="2">'
             '<div class="offer" data-code="M-742-RT" data-gtin="00012345000024" '
             'data-title="Assay R room 50 tests" data-pack="50 tests" data-price-cents="7600" '
             'data-storage="15-25C" data-revision="3"></div>'
             '<div class="offer" data-code="M-742" data-gtin="00012345000017" '
             'data-title="Assay R cold 50 tests" data-pack="50 tests" data-price-cents="10000" '
             'data-storage="2-8C" data-revision="8"></div>'
             '</article></body></html>')
    pages = variant_pages(tmp_path, **{'vendor-a-2.html': drift})
    db = make_case(tmp_path, page_dir=pages)
    result = extract(db, 'cat-1')
    assert result['status'] == 'pending'
    assert 'revision_drift' in {i['code'] for i in result['issues']}


def test_gtin_conflict_isolated_and_previous_version_kept(tmp_path):
    db = make_case(tmp_path)
    lims = MockLims(tmp_path / 'lims.db')
    cand = build_candidate(db, 'cat-1', 'strict_gtin')
    publish(db, 'cat-1', cand['version_id'], 'n1', lims)
    conflict = ('<html><body><script type="application/ld+json">{"seller":"B","items":['
                '{"maker_code":"M-742","gtin":"00012345000017","name":"Assay R 100-reaction carton",'
                '"quantity":100,"unit":"reactions","amount_cents":18000,"storage":"2-8C",'
                '"revision":12}]}</script></body></html>')
    pages = variant_pages(tmp_path, **{'vendor-b.html': conflict})
    contract_path = tmp_path / 'bad-contract.json'
    contract_path.write_text(json.dumps(load_contract()))
    register(db, 'cat-bad', contract_path, pages)
    result = extract(db, 'cat-bad')
    assert result['status'] == 'pending'
    assert 'gtin_conflict' in {i['code'] for i in result['issues']}
    with pytest.raises(ValueError, match='catalog_extraction_pending'):
        build_candidate(db, 'cat-bad', 'strict_gtin')
    assert lims.status(TARGET)['version_id'] == cand['version_id']
    con = connect(db)
    assert con.execute("SELECT count(*) FROM catalog_version WHERE status='published'").fetchone()[0] == 1
    con.close()


def test_unknown_unit_pends(tmp_path):
    boxes = ('<html><body><script type="application/ld+json">{"seller":"B","items":['
             '{"maker_code":"M-742","gtin":"00012345000031","name":"Assay R carton",'
             '"quantity":10,"unit":"boxes","amount_cents":5000,"storage":"2-8C",'
             '"revision":12}]}</script></body></html>')
    pages = variant_pages(tmp_path, **{'vendor-b.html': boxes})
    db = make_case(tmp_path, page_dir=pages)
    result = extract(db, 'cat-1')
    assert result['status'] == 'pending'
    assert 'unit_unknown' in {i['code'] for i in result['issues']}


def test_register_rejects_pinned_sha_mismatch(tmp_path):
    contract = load_contract(page_sha256={'vendor-a.html': '0' * 64})
    db_path = tmp_path / 'c.db'
    init(db_path, 'b', 's')
    contract_path = tmp_path / 'c.json'
    contract_path.write_text(json.dumps(contract))
    with pytest.raises(ValueError, match='catalog_page_invalid'):
        register(db_path, 'cat-1', contract_path, FIX)


def test_tampered_frozen_page_pends(tmp_path):
    db = make_case(tmp_path)
    con = connect(db)
    payload = json.loads(con.execute('SELECT payload FROM catalog_case').fetchone()[0])
    payload['pages'][0]['bytes'] = payload['pages'][0]['bytes'].replace('10000', '9000')
    with con:
        con.execute('UPDATE catalog_case SET payload=? WHERE id=?', (json.dumps(payload), 'cat-1'))
    con.close()
    result = extract(db, 'cat-1')
    assert result['status'] == 'pending'
    assert 'page_sha_mismatch' in {i['code'] for i in result['issues']}


def test_out_of_order_and_duplicate_pages_pend(tmp_path):
    reordered = load_contract(source_pages=['vendor-a-2.html', 'vendor-a.html', 'vendor-b.html'])
    db = make_case(tmp_path / 'reordered', contract=reordered)
    assert 'page_out_of_order' in {i['code'] for i in extract(db, 'cat-1')['issues']}
    duplicated = load_contract(
        source_pages=['vendor-a.html', 'vendor-a.html', 'vendor-a-2.html', 'vendor-b.html'])
    db = make_case(tmp_path / 'duplicated', contract=duplicated)
    assert 'duplicate_response' in {i['code'] for i in extract(db, 'cat-1')['issues']}


# ---------- 并发、栅栏与恢复 ----------

def test_concurrent_publishers_keep_single_version(tmp_path):
    db = make_case(tmp_path)
    lims = MockLims(tmp_path / 'lims.db')
    barrier = threading.Barrier(2)
    results, errors = [], []

    def worker(nonce):
        try:
            barrier.wait()
            cand = build_candidate(db, 'cat-1', 'strict_gtin')
            results.append(publish(db, 'cat-1', cand['version_id'], nonce, lims))
        except ValueError as exc:
            errors.append(str(exc))

    threads = [threading.Thread(target=worker, args=(f'nonce-{i}',)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(results) + len(errors) == 2
    con = connect(db)
    assert con.execute("SELECT count(*) FROM catalog_version WHERE status='published'").fetchone()[0] == 1
    con.close()
    status = lims.status(TARGET)
    assert status['seq'] == 1
    assert len(json.loads(status['catalog'])['products']) == 3


def test_competing_worker_rejected_then_rebased(tmp_path):
    lims = MockLims(tmp_path / 'lims.db')
    db1 = make_case(tmp_path / 'w1')
    cheaper = (FIX / 'vendor-b.html').read_text().replace('18000', '17000')
    pages = variant_pages(tmp_path / 'w2', **{'vendor-b.html': cheaper})
    db2 = make_case(tmp_path / 'w2', page_dir=pages)
    cand1 = build_candidate(db1, 'cat-1', 'strict_gtin')
    assert publish(db1, 'cat-1', cand1['version_id'], 'w1-n1', lims)['status'] == 'published'
    cand2 = build_candidate(db2, 'cat-1', 'strict_gtin')
    assert cand2['version_id'] != cand1['version_id']
    out = publish(db2, 'cat-1', cand2['version_id'], 'w2-n1', lims)
    assert out['status'] == 'rejected'
    recover(db2, 'cat-1', lims)
    con = connect(db2)
    adopted = con.execute("SELECT * FROM catalog_version WHERE status='adopted'").fetchone()
    assert adopted and adopted['id'] == cand1['version_id']
    con.close()
    cand3 = build_candidate(db2, 'cat-1', 'strict_gtin')
    assert cand3['seq'] == 2
    assert publish(db2, 'cat-1', cand3['version_id'], 'w2-n2', lims)['status'] == 'published'
    catalog = lims_catalog(lims)
    assert catalog['seq'] == 2
    offer_b = next(o for p in catalog['products'] for o in p['offers'] if o['supplier'] == 'B')
    assert offer_b['unit_price_cents'] == 170.0


def test_partial_ack_then_recover_republishes(tmp_path):
    db = make_case(tmp_path)
    lims = MockLims(tmp_path / 'lims.db', faults=['partial_ack'])
    cand = build_candidate(db, 'cat-1', 'strict_gtin')
    out = publish(db, 'cat-1', cand['version_id'], 'n1', lims)
    assert out['status'] == 'uncertain'
    assert lims.status(TARGET) is None
    actions = recover(db, 'cat-1', lims)['actions']
    assert ('republished', cand['version_id']) in actions
    con = connect(db)
    assert con.execute('SELECT status FROM catalog_version').fetchone()[0] == 'published'
    pubs = con.execute('SELECT * FROM lims_publication').fetchall()
    assert len(pubs) == 1 and pubs[0]['acked'] == 1
    con.close()
    assert lims.status(TARGET)['seq'] == 1


def test_lost_ack_recovered_from_remote_status(tmp_path):
    db = make_case(tmp_path)
    lims = MockLims(tmp_path / 'lims.db', faults=['drop_ack'])
    cand = build_candidate(db, 'cat-1', 'signed_alias_merge')
    out = publish(db, 'cat-1', cand['version_id'], 'n1', lims)
    assert out['status'] == 'uncertain' and out['reason'] == 'ack_lost'
    assert lims.status(TARGET)['version_id'] == cand['version_id']
    recover(db, 'cat-1', lims)
    con = connect(db)
    assert con.execute('SELECT status FROM catalog_version').fetchone()[0] == 'published'
    pub = con.execute('SELECT * FROM lims_publication').fetchone()
    assert pub['acked'] == 1
    assert json.loads(pub['receipt'])['products'] == 2
    con.close()
    again = publish(db, 'cat-1', cand['version_id'], 'n1', lims)
    assert again['status'] == 'published'
    assert len(lims_catalog(lims)['products']) == 2


def test_late_old_snapshot_cannot_overwrite(tmp_path):
    db = make_case(tmp_path)
    lims = MockLims(tmp_path / 'lims.db')
    cand = build_candidate(db, 'cat-1', 'strict_gtin')
    publish(db, 'cat-1', cand['version_id'], 'n1', lims)
    old_contract = load_contract(source_set='2026-11-10-night',
                                 source_pages=['vendor-a.html', 'vendor-a-2.html'])
    contract_path = tmp_path / 'old.json'
    contract_path.write_text(json.dumps(old_contract))
    register(db, 'cat-old', contract_path, FIX)
    with pytest.raises(ValueError, match='stale_snapshot'):
        build_candidate(db, 'cat-old', 'strict_gtin')
    status = lims.status(TARGET)
    assert status['version_id'] == cand['version_id']
    assert len(json.loads(status['catalog'])['products']) == 3


def test_sqlite_rollback_cannot_erase_accepted_version(tmp_path):
    lims = MockLims(tmp_path / 'lims.db')
    db1 = make_case(tmp_path / 'first')
    cand = build_candidate(db1, 'cat-1', 'strict_gtin')
    publish(db1, 'cat-1', cand['version_id'], 'n1', lims)
    # 本地库丢失后重建：同一冻结输入重新冻结
    db2 = make_case(tmp_path / 'second')
    con = connect(db2)
    assert con.execute('SELECT count(*) FROM catalog_version').fetchone()[0] == 0
    con.close()
    recover(db2, 'cat-1', lims)
    con = connect(db2)
    adopted = con.execute("SELECT * FROM catalog_version WHERE status='adopted'").fetchone()
    assert adopted and adopted['id'] == cand['version_id'] and adopted['seq'] == 1
    con.close()
    # 远端已接受的 v1 不会被假装成不存在：相同内容只能是 unchanged，不会重发 v1
    same = build_candidate(db2, 'cat-1', 'strict_gtin')
    assert same['status'] == 'unchanged' and same['version_id'] == cand['version_id']
    # 有新内容的更正只能以新版本补正，seq 接续远端最终状态
    cheaper = (FIX / 'vendor-b.html').read_text().replace('18000', '17000')
    pages = variant_pages(tmp_path / 'second', **{'vendor-b.html': cheaper})
    contract_path = tmp_path / 'second' / 'cat-2-contract.json'
    contract_path.write_text(json.dumps(load_contract(source_set='2026-11-18-morning')))
    register(db2, 'cat-2', contract_path, pages)
    cand2 = build_candidate(db2, 'cat-2', 'strict_gtin')
    assert cand2['seq'] == 2
    out = publish(db2, 'cat-2', cand2['version_id'], 'n2', lims)
    assert out['status'] == 'published'
    assert lims.status(TARGET)['seq'] == 2


# ---------- 身份策略不可变与凭证隔离 ----------

def test_identity_strategy_immutable_within_version(tmp_path):
    db = make_case(tmp_path)
    lims = MockLims(tmp_path / 'lims.db')
    cand = build_candidate(db, 'cat-1', 'strict_gtin')
    with pytest.raises(ValueError, match='strategy_immutable'):
        build_candidate(db, 'cat-1', 'signed_alias_merge')
    publish(db, 'cat-1', cand['version_id'], 'n1', lims)
    v2 = build_candidate(db, 'cat-1', 'signed_alias_merge')
    assert v2['seq'] == 2 and v2['strategy'] == 'signed_alias_merge'
    assert 'alias:M-742@2-8C' in v2['diff']['added']
    assert sorted(v2['diff']['removed']) == ['gtin:00012345000017', 'gtin:00012345000031']
    publish(db, 'cat-1', v2['version_id'], 'n2', lims)
    catalog = lims_catalog(lims)
    assert catalog['strategy'] == 'signed_alias_merge'
    assert len(catalog['products']) == 2
    con = connect(db)
    strategies = [r['strategy'] for r in con.execute('SELECT * FROM catalog_version ORDER BY seq')]
    con.close()
    assert strategies == ['strict_gtin', 'signed_alias_merge']


def test_expired_alias_isolates_and_keeps_previous(tmp_path):
    db = make_case(tmp_path)
    lims = MockLims(tmp_path / 'lims.db')
    cand = build_candidate(db, 'cat-1', 'strict_gtin')
    publish(db, 'cat-1', cand['version_id'], 'n1', lims)
    bad = load_contract()
    bad['gtin_aliases'] = [dict(bad['gtin_aliases'][0], expires='2026-01-01')]
    bad['gtin_aliases'][0]['signature'] = sign_alias('MOCK-LAB', bad['gtin_aliases'][0])
    contract_path = tmp_path / 'expired.json'
    contract_path.write_text(json.dumps(bad))
    register(db, 'cat-expired', contract_path, FIX)
    with pytest.raises(ValueError, match='alias_invalid'):
        build_candidate(db, 'cat-expired', 'signed_alias_merge')
    con = connect(db)
    assert con.execute("SELECT count(*) FROM catalog_issue WHERE case_id='cat-expired'"
                       " AND code='alias_invalid'").fetchone()[0] == 1
    con.close()
    assert lims.status(TARGET)['version_id'] == cand['version_id']


def test_unsigned_conflicting_or_missing_alias_rejected(tmp_path):
    bad_sig = load_contract()
    bad_sig['gtin_aliases'][0]['signature'] = '0' * 64
    db = make_case(tmp_path / 'tampered', contract=bad_sig)
    with pytest.raises(ValueError, match='alias_invalid'):
        build_candidate(db, 'cat-1', 'signed_alias_merge')
    conflict = load_contract()
    conflict['gtin_aliases'] = [dict(conflict['gtin_aliases'][0], storage='15-25C')]
    conflict['gtin_aliases'][0]['signature'] = sign_alias('MOCK-LAB', conflict['gtin_aliases'][0])
    db = make_case(tmp_path / 'conflict', contract=conflict)
    with pytest.raises(ValueError, match='alias_invalid'):
        build_candidate(db, 'cat-1', 'signed_alias_merge')
    con = connect(db)
    assert con.execute("SELECT count(*) FROM catalog_issue WHERE code='alias_conflict'").fetchone()[0] >= 1
    con.close()
    missing = load_contract()
    del missing['gtin_aliases']
    db = make_case(tmp_path / 'missing', contract=missing)
    with pytest.raises(ValueError, match='alias_invalid'):
        build_candidate(db, 'cat-1', 'signed_alias_merge')


# ---------- 审计链报告与 CLI ----------

def test_report_exposes_full_audit_chain(tmp_path):
    db = make_case(tmp_path)
    lims = MockLims(tmp_path / 'lims.db')
    cand = build_candidate(db, 'cat-1', 'signed_alias_merge')
    publish(db, 'cat-1', cand['version_id'], 'n1', lims)
    out = catalog_report(db, 'cat-1', tmp_path / 'report.json')
    report = json.loads(out.read_text())
    page_shas = {p['sha256'] for p in report['pages']}
    assert all(len(s) == 64 for s in page_shas)
    assert [c['complete'] for c in report['pagination']] == [True, True]
    offer_shas = {o['offer_sha256'] for o in report['offers']}
    evidence = report['catalog']['evidence']
    assert set(evidence['offers']) == offer_shas
    assert {p['sha256'] for p in evidence['pages']} <= page_shas
    assert evidence['aliases'][0]['signer'] == 'MOCK-LAB'
    assert report['versions'][0]['diff']['added']
    assert report['publications'][0]['receipt']['products'] == 2
    md = out.with_suffix('.md').read_text()
    assert '供应商目录发布报告' in md and '推荐 B' in md


def test_catalog_cli_flow(tmp_path, monkeypatch):
    from samplechain.__main__ import main
    db = tmp_path / 'cli.db'
    init(db, 'b', 's')
    contract_path = tmp_path / 'c.json'
    contract_path.write_text(json.dumps(load_contract()))
    lims_path = tmp_path / 'lims.db'

    def run(*argv):
        monkeypatch.setattr('sys.argv', ['samplechain', *argv])
        main()

    run('catalog-register', '--db', str(db), '--case', 'cat-1',
        '--contract', str(contract_path), '--pages', str(FIX))
    run('catalog-extract', '--db', str(db), '--case', 'cat-1')
    run('catalog-candidate', '--db', str(db), '--case', 'cat-1',
        '--strategy', 'signed_alias_merge')
    con = connect(db)
    version_id = con.execute('SELECT id FROM catalog_version').fetchone()[0]
    con.close()
    run('catalog-publish', '--db', str(db), '--case', 'cat-1',
        '--version', version_id, '--nonce', 'cli-n1', '--lims', str(lims_path))
    run('catalog-report', '--db', str(db), '--case', 'cat-1',
        '--out', str(tmp_path / 'out.json'))
    report = json.loads((tmp_path / 'out.json').read_text())
    assert report['catalog']['strategy'] == 'signed_alias_merge'
    assert len(report['catalog']['products']) == 2
