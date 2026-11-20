"""供应商目录：冻结网页快照 → 解析归一 → 身份归并 → 版本化发布到模拟 LIMS。

不读取 rules.py 的样本放行结论，不生成采购单。每条 源快照→规范商品/报价
边都可追溯：页面 SHA → 分页链 → 抽取字段 → 身份凭证 → 单位换算 → 版本 → LIMS 回执。
"""
import hashlib
import hmac
import json
import re
from collections import defaultdict
from contextlib import contextmanager
from datetime import date
from html.parser import HTMLParser
from pathlib import Path

from .lims import LostAck, NonceConflict, StaleVersion, VersionGap
from .store import connect

STRATEGIES = ('strict_gtin', 'signed_alias_merge')
STRATEGY_RATIONALE = {
    'strict_gtin': '以 GTIN 为商品身份：两份冷藏 R 包装分列两个产品，各自保留报价；'
                   '不依赖别名凭证，但同一试剂的跨包装比价留给采集端。',
    'signed_alias_merge': '以 MOCK-LAB 签署的 GTIN 别名 + 相同储温为商品身份：两份包装归为一个'
                          '规范冷藏 R 产品，含两家按次归一报价并推荐最低价；'
                          '凭证缺失/过期/与网页冲突时整批隔离，绝不降级为相似名合并。',
}
PACK_RE = re.compile(r'^\s*(\d+)\s+([A-Za-z]+)\s*$')
IDENTITY_EVIDENCE = ('manufacturer_code', 'gtin', 'storage')


def register(db_path, case_id, contract_path, page_dir):
    """冻结合同与页面快照；合同可声明 page_sha256 钉扎每页哈希。"""
    contract_path = Path(contract_path).resolve()
    page_dir = Path(page_dir).resolve()
    raw = contract_path.read_bytes()
    contract = json.loads(raw)
    if not case_id or not contract.get('source_pages') or not contract.get('publish_target'):
        raise ValueError('catalog_contract_invalid')
    declared = contract.get('page_sha256') or {}
    pages = []
    for name in contract['source_pages']:
        path = (page_dir / name).resolve()
        if path.parent != page_dir or path.suffix != '.html':
            raise ValueError('catalog_page_invalid')
        body = path.read_bytes()
        digest = hashlib.sha256(body).hexdigest()
        if name in declared and declared[name] != digest:
            raise ValueError('catalog_page_invalid')
        pages.append({'name': name, 'sha256': digest,
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


@contextmanager
def _immediate(db):
    """BEGIN IMMEDIATE 事务：串行化并发发布者，配合唯一约束做栅栏。"""
    db.isolation_level = None
    db.execute('BEGIN IMMEDIATE')
    try:
        yield
    except Exception:
        db.execute('ROLLBACK')
        raise
    else:
        db.execute('COMMIT')
    finally:
        db.isolation_level = ''


# ---------- 页面解析 ----------

class _PageParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.article = None
        self.offers = []
        self.scripts = []
        self._capture = False
        self._buf = []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == 'article' and 'data-supplier' in a and self.article is None:
            self.article = a
        elif tag == 'div' and 'offer' in (a.get('class') or '').split():
            self.offers.append(a)
        elif tag == 'script' and a.get('type') == 'application/ld+json':
            self._capture = True
            self._buf = []

    def handle_data(self, data):
        if self._capture:
            self._buf.append(data)

    def handle_endtag(self, tag):
        if tag == 'script' and self._capture:
            self.scripts.append(''.join(self._buf))
            self._capture = False


def _parse_page(name, body, issues):
    """把一页快照解析为 {supplier, page_no, next, format, raw}；异常记入 issues。"""
    parser = _PageParser()
    parser.feed(body)
    parser.close()
    if parser.article is not None:
        art = parser.article
        try:
            page_no = int(art.get('data-page', '1'))
        except (TypeError, ValueError):
            page_no = 1
            issues.append(('page_invalid', f'{name}:data-page'))
        raw = [{'maker_code': a.get('data-code'), 'gtin': a.get('data-gtin'),
                'name': a.get('data-title'), 'pack': a.get('data-pack'),
                'amount_cents': a.get('data-price-cents'), 'storage': a.get('data-storage'),
                'revision': a.get('data-revision')} for a in parser.offers]
        return {'name': name, 'format': 'html-offers', 'supplier': str(art.get('data-supplier', '')),
                'page_no': page_no, 'next': art.get('data-next'), 'raw': raw}
    if parser.scripts:
        try:
            doc = json.loads(parser.scripts[0])
            seller = str(doc['seller'])
            items = [dict(item) for item in doc['items']]
        except (KeyError, TypeError, ValueError) as exc:
            issues.append(('page_invalid', f'{name}:{exc}'))
            return {'name': name, 'format': 'json-ld', 'supplier': '', 'page_no': 1, 'next': None, 'raw': []}
        return {'name': name, 'format': 'json-ld', 'supplier': seller,
                'page_no': 1, 'next': None, 'raw': items}
    issues.append(('page_format_unknown', name))
    return {'name': name, 'format': 'unknown', 'supplier': '', 'page_no': 1, 'next': None, 'raw': []}


def _unit_canonical_map(equivalence):
    """合同 unit_equivalence 的并查集：每个等价类映射到字典序最小的规范单位。"""
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    for a, b in equivalence.items():
        ra, rb = find(str(a)), find(str(b))
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)
    classes = defaultdict(list)
    for unit in parent:
        classes[find(unit)].append(unit)
    return {unit: min(members) for members in classes.values() for unit in members}


def _as_int(value):
    if isinstance(value, bool):
        raise ValueError('bool')
    if isinstance(value, float) and not value.is_integer():
        raise ValueError('fraction')
    return int(value)


def _normalize_offer(page, raw, canon, issues):
    """跨格式字段映射 + 单位标准化；任何疑点都记入 issues 并丢弃该报价。"""
    supplier, page_name = page['supplier'], page['name']
    try:
        maker = str(raw['maker_code']).strip()
        gtin = str(raw['gtin']).strip()
        title = str(raw['name']).strip()
        storage = str(raw['storage']).strip()
        amount = _as_int(raw['amount_cents'])
        revision = _as_int(raw['revision'])
        if raw.get('pack') is not None:
            match = PACK_RE.match(str(raw['pack']))
            if not match:
                raise ValueError('pack')
            quantity, unit = int(match.group(1)), match.group(2)
        else:
            quantity, unit = _as_int(raw['quantity']), str(raw['unit']).strip()
    except (KeyError, TypeError, ValueError):
        issues.append(('field_invalid', f'{page_name}:{supplier}:{raw}'))
        return None
    if not maker or not gtin or not title or not storage or not unit:
        issues.append(('field_invalid', f'{page_name}:{supplier}:blank'))
        return None
    if not gtin.isdigit() or len(gtin) < 8:
        issues.append(('gtin_invalid', f'{page_name}:{gtin}'))
        return None
    if quantity <= 0:
        issues.append(('pack_invalid', f'{page_name}:{gtin}:{quantity}'))
        return None
    if amount < 0:
        issues.append(('price_invalid', f'{page_name}:{gtin}:{amount}'))
        return None
    canonical = canon.get(unit)
    if canonical is None:
        issues.append(('unit_unknown', f'{page_name}:{gtin}:{unit}'))
        return None
    offer = {'supplier': supplier, 'maker_code': maker, 'gtin': gtin, 'name': title,
             'quantity': quantity, 'unit': unit, 'amount_cents': amount, 'storage': storage,
             'revision': revision, 'canonical_unit': canonical,
             'unit_price_cents': amount / quantity,
             'page': page_name, 'page_sha256': page['sha256']}
    offer['offer_sha256'] = hashlib.sha256(
        json.dumps(offer, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return offer


def _extract_all(contract, pages):
    """从冻结快照抽取报价；任何校验失败 → 整批待核（返回空报价 + 问题清单）。"""
    issues = []
    equivalence = contract.get('unit_equivalence')
    if not isinstance(equivalence, dict) or not equivalence:
        issues.append(('contract_invalid', 'unit_equivalence'))
        equivalence = {}
    if not contract.get('manufacturer') or \
            sorted(contract.get('canonical_identity_evidence') or []) != sorted(IDENTITY_EVIDENCE):
        issues.append(('contract_invalid', 'identity_evidence'))
    allowed = set(contract.get('allowed_sources') or [])
    canon = _unit_canonical_map(equivalence)
    parsed = []
    seen_names, seen_sha = set(), {}
    for record in pages:
        name = record['name']
        if name in seen_names:
            issues.append(('duplicate_response', name))
            continue
        seen_names.add(name)
        if record['sha256'] in seen_sha:
            issues.append(('duplicate_response', f"{seen_sha[record['sha256']]}={name}"))
            continue
        seen_sha[record['sha256']] = name
        if hashlib.sha256(record['bytes'].encode()).hexdigest() != record['sha256']:
            issues.append(('page_sha_mismatch', name))
            continue
        page = _parse_page(name, record['bytes'], issues)
        page['sha256'] = record['sha256']
        parsed.append(page)
    for page in parsed:
        if page['supplier'] and allowed and page['supplier'] not in allowed:
            issues.append(('source_not_allowed', f"{page['name']}:{page['supplier']}"))
    by_supplier = defaultdict(list)
    for page in parsed:
        by_supplier[page['supplier']].append(page)
    offers = []
    for supplier in sorted(by_supplier):
        group = by_supplier[supplier]
        if [p['page_no'] for p in group] != sorted(p['page_no'] for p in group):
            issues.append(('page_out_of_order', supplier))
        ordered = sorted(group, key=lambda p: p['page_no'])
        by_no = {}
        for page in ordered:
            if page['page_no'] in by_no:
                issues.append(('page_conflict', f"{supplier}:{page['page_no']}"))
            by_no[page['page_no']] = page
        numbers = sorted(by_no)
        if numbers and numbers != list(range(1, len(numbers) + 1)):
            issues.append(('pagination_incomplete', f'{supplier}:gap'))
        for page in ordered:
            if page['next']:
                nxt = by_no.get(page['page_no'] + 1)
                if nxt is None:
                    issues.append(('pagination_incomplete', f"{supplier}:{page['page_no']}->{page['next']}"))
                elif page['next'] not in nxt['name']:
                    issues.append(('pagination_broken', f"{supplier}:{page['next']}!={nxt['name']}"))
        for page in ordered:
            for raw in page['raw']:
                offer = _normalize_offer(page, raw, canon, issues)
                if offer:
                    offers.append(offer)
    by_identity = defaultdict(list)
    for offer in offers:
        by_identity[(offer['supplier'], offer['maker_code'], offer['gtin'])].append(offer)
    for (supplier, maker, gtin), group in sorted(by_identity.items()):
        if len({o['revision'] for o in group}) > 1:
            issues.append(('revision_drift', f'{supplier}:{maker}:{gtin}'))
        elif len({(o['name'], o['quantity'], o['unit'], o['amount_cents'], o['storage'])
                  for o in group}) > 1:
            issues.append(('offer_conflict', f'{supplier}:{maker}:{gtin}'))
        elif len(group) > 1:
            issues.append(('duplicate_response', f'{supplier}:{maker}:{gtin}'))
    by_gtin = defaultdict(list)
    for offer in offers:
        by_gtin[offer['gtin']].append(offer)
    for gtin, group in sorted(by_gtin.items()):
        if len({(o['maker_code'], o['storage'], o['quantity'], o['unit']) for o in group}) > 1:
            issues.append(('gtin_conflict', gtin))
    issues = sorted(set(issues))
    if issues:
        return [], issues
    offers.sort(key=lambda o: (o['supplier'], o['gtin'], o['page']))
    return offers, []


def extract(db_path, case_id):
    """解析冻结快照并校验；通过则落 catalog_offer，否则只落 catalog_issue（整批待核）。"""
    db = connect(db_path)
    try:
        row = db.execute('SELECT payload FROM catalog_case WHERE id=?', (case_id,)).fetchone()
        if not row:
            raise ValueError('catalog_case_missing')
        case = json.loads(row['payload'])
        offers, issues = _extract_all(case['contract'], case['pages'])
        with _immediate(db):
            db.execute('DELETE FROM catalog_offer WHERE case_id=?', (case_id,))
            db.execute('DELETE FROM catalog_issue WHERE case_id=?', (case_id,))
            for code, detail in issues:
                db.execute('INSERT INTO catalog_issue(case_id,code,detail) VALUES (?,?,?)',
                           (case_id, code, detail))
            for o in offers:
                db.execute(
                    'INSERT INTO catalog_offer(case_id,supplier,page,page_sha256,maker_code,gtin,name,'
                    'quantity,unit,amount_cents,storage,revision,canonical_unit,unit_price_cents,offer_sha256)'
                    ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (case_id, o['supplier'], o['page'], o['page_sha256'], o['maker_code'], o['gtin'],
                     o['name'], o['quantity'], o['unit'], o['amount_cents'], o['storage'], o['revision'],
                     o['canonical_unit'], o['unit_price_cents'], o['offer_sha256']))
        return {'status': 'pending' if issues else 'ok',
                'offers': 0 if issues else len(offers),
                'issues': [{'code': c, 'detail': d} for c, d in issues]}
    finally:
        db.close()


# ---------- 身份策略 ----------

def _alias_message(alias):
    return json.dumps({'expires': str(alias.get('expires', '')),
                       'gtins': sorted(str(g) for g in (alias.get('gtins') or [])),
                       'maker_code': str(alias.get('maker_code', '')),
                       'storage': str(alias.get('storage', ''))},
                      sort_keys=True, separators=(',', ':')).encode()


def sign_alias(manufacturer, alias):
    """模拟制造商离线签名：HMAC-SHA256，密钥由制造商代号派生。"""
    return hmac.new(f'mock-signing-key:{manufacturer}'.encode(),
                    _alias_message(alias), hashlib.sha256).hexdigest()


def _verify_aliases(contract, offers):
    """核验合同中的 GTIN 别名凭证：签署人、签名、有效期、与网页制造商/储温一致。"""
    issues = []
    groups = []
    aliases = contract.get('gtin_aliases')
    if not aliases:
        return [], [('alias_invalid', 'missing')]
    try:
        crawl_date = date.fromisoformat(str(contract.get('source_set', ''))[:10])
    except ValueError:
        crawl_date = None
        issues.append(('contract_invalid', 'source_set'))
    by_gtin = defaultdict(set)
    for o in offers:
        by_gtin[o['gtin']].add((o['maker_code'], o['storage']))
    seen = set()
    for alias in aliases:
        gtins = sorted(str(g) for g in (alias.get('gtins') or []))
        label = ','.join(gtins)
        if len(gtins) < 2:
            issues.append(('alias_invalid', f'gtins:{label}'))
            continue
        if alias.get('signer') != contract.get('manufacturer'):
            issues.append(('alias_invalid', f'signer:{label}'))
            continue
        if not hmac.compare_digest(sign_alias(contract['manufacturer'], alias),
                                   str(alias.get('signature', ''))):
            issues.append(('alias_invalid', f'signature:{label}'))
            continue
        expires = str(alias.get('expires', ''))
        if crawl_date is None or expires < crawl_date.isoformat():
            issues.append(('alias_invalid', f'expired:{label}'))
            continue
        conflict = False
        for g in gtins:
            if g in seen:
                issues.append(('alias_conflict', f'overlap:{g}'))
                conflict = True
            seen.add(g)
            for maker, storage in by_gtin.get(g, ()):
                if maker != alias.get('maker_code') or storage != alias.get('storage'):
                    issues.append(('alias_conflict', f'web:{g}:{maker}@{storage}'))
                    conflict = True
        if not conflict:
            groups.append({'gtins': gtins, 'maker_code': str(alias.get('maker_code')),
                           'storage': str(alias.get('storage')), 'signer': str(alias.get('signer')),
                           'signature': str(alias.get('signature')), 'expires': expires})
    return groups, sorted(set(issues))


def _public_offer(o):
    return {'supplier': o['supplier'], 'gtin': o['gtin'], 'maker_code': o['maker_code'],
            'name': o['name'], 'pack': f"{o['quantity']} {o['unit']}",
            'amount_cents': o['amount_cents'], 'unit_price_cents': o['unit_price_cents'],
            'canonical_unit': o['canonical_unit'], 'storage': o['storage'],
            'revision': o['revision'], 'page': o['page'], 'page_sha256': o['page_sha256'],
            'offer_sha256': o['offer_sha256']}


def _product(key, offers, identity):
    ordered = sorted(offers, key=lambda o: (o['unit_price_cents'], o['supplier'], o['gtin']))
    best = ordered[0]
    return {'product_key': key, 'name': min(o['name'] for o in offers),
            'maker_code': offers[0]['maker_code'], 'storage': offers[0]['storage'],
            'gtins': sorted({o['gtin'] for o in offers}),
            'canonical_unit': offers[0]['canonical_unit'],
            'offers': [_public_offer(o) for o in ordered],
            'recommended': {'supplier': best['supplier'], 'gtin': best['gtin'],
                            'unit_price_cents': best['unit_price_cents'],
                            'offer_sha256': best['offer_sha256']},
            'identity': identity}


def _build_products(offers, aliases, strategy):
    """身份归并只依据合同证据（GTIN / 签署别名 + 储温），绝不按名称相似度合并。"""
    products = []
    by_gtin = defaultdict(list)
    for o in offers:
        by_gtin[o['gtin']].append(o)
    assigned = set()
    if strategy == 'signed_alias_merge':
        for alias in sorted(aliases, key=lambda a: a['gtins']):
            group = [o for g in alias['gtins'] for o in by_gtin.get(g, [])]
            assigned.update(alias['gtins'])
            if not group:
                continue
            group.sort(key=lambda o: (o['supplier'], o['gtin']))
            products.append(_product(
                f"alias:{alias['maker_code']}@{alias['storage']}", group,
                {'strategy': strategy,
                 'alias': {'signer': alias['signer'], 'signature': alias['signature'],
                           'expires': alias['expires'], 'gtins': alias['gtins']}}))
    for gtin in sorted(by_gtin):
        if gtin in assigned:
            continue
        group = sorted(by_gtin[gtin], key=lambda o: (o['supplier'], o['gtin']))
        products.append(_product(f'gtin:{gtin}', group, {'strategy': strategy, 'alias': None}))
    products.sort(key=lambda p: p['product_key'])
    return products


def _diff(base_products, products):
    base = {p['product_key']: p for p in base_products}
    new = {p['product_key']: p for p in products}
    return {'added': sorted(k for k in new if k not in base),
            'removed': sorted(k for k in base if k not in new),
            'changed': [{'product_key': k,
                         'fields': sorted(f for f in set(new[k]) | set(base[k])
                                          if new[k].get(f) != base[k].get(f))}
                        for k in sorted(set(base) & set(new)) if new[k] != base[k]]}


def build_candidate(db_path, case_id, strategy):
    """生成候选版本与差异；身份策略一经写入候选即不可在同一版本内更换。"""
    if strategy not in STRATEGIES:
        raise ValueError('strategy_unknown')
    result = extract(db_path, case_id)
    if result['status'] != 'ok':
        raise ValueError('catalog_extraction_pending')
    db = connect(db_path)
    try:
        row = db.execute('SELECT payload FROM catalog_case WHERE id=?', (case_id,)).fetchone()
        if not row:
            raise ValueError('catalog_case_missing')
        case = json.loads(row['payload'])
        contract = case['contract']
        target = contract['publish_target']
        offers = [dict(r) for r in db.execute(
            'SELECT * FROM catalog_offer WHERE case_id=? ORDER BY supplier,gtin,page', (case_id,))]
        aliases = []
        if strategy == 'signed_alias_merge':
            aliases, alias_issues = _verify_aliases(contract, offers)
            if alias_issues:
                with _immediate(db):
                    db.execute('DELETE FROM catalog_issue WHERE case_id=?', (case_id,))
                    for code, detail in alias_issues:
                        db.execute('INSERT INTO catalog_issue(case_id,code,detail) VALUES (?,?,?)',
                                   (case_id, code, detail))
                raise ValueError('alias_invalid')
        products = _build_products(offers, aliases, strategy)
        base = db.execute("SELECT * FROM catalog_version WHERE target=? AND status IN ('published','adopted')"
                          ' ORDER BY seq DESC LIMIT 1', (target,)).fetchone()
        if base and base['source_set'] > str(contract.get('source_set', '')):
            raise ValueError('stale_snapshot')
        base_products = json.loads(base['payload'])['products'] if base else []
        diff = _diff(base_products, products)
        diff['base_version'] = base['id'] if base else None
        diff['base_seq'] = base['seq'] if base else 0
        if base and not diff['added'] and not diff['removed'] and not diff['changed']:
            return {'status': 'unchanged', 'version_id': base['id'], 'seq': base['seq'],
                    'strategy': base['strategy'], 'products': len(products), 'diff': diff}
        with _immediate(db):
            inflight = db.execute("SELECT * FROM catalog_version WHERE target=? AND status IN ('candidate','committed')"
                                  ' ORDER BY seq DESC LIMIT 1', (target,)).fetchone()
            replace_id = None
            if inflight:
                if inflight['status'] == 'committed':
                    raise ValueError('publication_in_flight')
                if inflight['case_id'] != case_id:
                    raise ValueError('candidate_in_flight')
                if inflight['strategy'] != strategy:
                    raise ValueError('strategy_immutable')
                seq = inflight['seq']
                replace_id = inflight['id']
            else:
                maxseq = db.execute('SELECT max(seq) m FROM catalog_version WHERE target=?',
                                    (target,)).fetchone()['m']
                seq = (maxseq or 0) + 1
            evidence = {'pages': [{'name': p['name'], 'sha256': p['sha256']} for p in case['pages']],
                        'offers': sorted(o['offer_sha256'] for o in offers),
                        'aliases': [{'gtins': a['gtins'], 'signer': a['signer'],
                                     'signature': a['signature'], 'expires': a['expires']} for a in aliases]}
            evidence['source_digest'] = hashlib.sha256(
                '|'.join(sorted(p['sha256'] for p in case['pages']) + evidence['offers']).encode()).hexdigest()
            payload_obj = {'case_id': case_id, 'seq': seq, 'strategy': strategy,
                           'source_set': contract.get('source_set'),
                           'contract_sha256': case['contract_sha256'], 'publish_target': target,
                           'canonical_units': sorted({p['canonical_unit'] for p in products}),
                           'products': products, 'evidence': evidence}
            payload = json.dumps(payload_obj, sort_keys=True, separators=(',', ':'))
            version_id = hashlib.sha256(payload.encode()).hexdigest()
            if replace_id:
                db.execute('DELETE FROM catalog_version WHERE id=?', (replace_id,))
            db.execute('INSERT INTO catalog_version(id,case_id,target,seq,strategy,source_set,status,payload,diff)'
                       ' VALUES (?,?,?,?,?,?,?,?,?)',
                       (version_id, case_id, target, seq, strategy,
                        str(contract.get('source_set', '')), 'candidate', payload,
                        json.dumps(diff, sort_keys=True)))
        return {'version_id': version_id, 'seq': seq, 'strategy': strategy,
                'products': len(products), 'diff': diff}
    finally:
        db.close()


# ---------- 发布与恢复 ----------

def publish(db_path, case_id, version_id, nonce, lims):
    """两阶段发布：先落可恢复的 nonce 意图，再调用 LIMS；栅栏拒绝旧版本覆盖。"""
    db = connect(db_path)
    try:
        with _immediate(db):
            v = db.execute('SELECT * FROM catalog_version WHERE id=? AND case_id=?',
                           (version_id, case_id)).fetchone()
            if not v:
                raise ValueError('catalog_version_missing')
            if v['status'] == 'published':
                return {'status': 'published', 'version_id': version_id, 'duplicate': True}
            if v['status'] not in ('candidate', 'committed'):
                raise ValueError('version_not_publishable')
            blocker = db.execute("SELECT id FROM catalog_version WHERE target=? AND seq>=?"
                                 " AND status IN ('published','adopted') LIMIT 1",
                                 (v['target'], v['seq'])).fetchone()
            if blocker:
                raise ValueError('version_fence')
            prior = db.execute('SELECT * FROM lims_publication WHERE case_id=? AND nonce=?',
                               (case_id, nonce)).fetchone()
            if prior:
                if prior['version_id'] != version_id:
                    raise ValueError('nonce_conflict')
                if prior['acked'] == 1:
                    return {'status': 'published', 'version_id': version_id, 'duplicate': True}
            else:
                db.execute('INSERT INTO lims_publication(case_id,version_id,nonce,acked,receipt)'
                           ' VALUES (?,?,?,0,?)', (case_id, version_id, nonce, ''))
            db.execute("UPDATE catalog_version SET status='committed' WHERE id=?", (version_id,))
            target, seq, payload = v['target'], v['seq'], v['payload']
        try:
            receipt = lims.apply(target, version_id, seq, nonce, payload)
        except LostAck:
            return {'status': 'uncertain', 'reason': 'ack_lost', 'version_id': version_id}
        except StaleVersion:
            with _immediate(db):
                db.execute("UPDATE catalog_version SET status='rejected' WHERE id=?", (version_id,))
            return {'status': 'rejected', 'reason': 'stale_version', 'version_id': version_id}
        except VersionGap:
            return {'status': 'uncertain', 'reason': 'version_gap', 'version_id': version_id}
        except NonceConflict:
            raise ValueError('nonce_conflict')
        if receipt.get('ack'):
            with _immediate(db):
                db.execute('UPDATE lims_publication SET acked=1, receipt=? WHERE case_id=? AND nonce=?',
                           (json.dumps(receipt, sort_keys=True), case_id, nonce))
                db.execute("UPDATE catalog_version SET status='published' WHERE id=?", (version_id,))
            return {'status': 'published', 'version_id': version_id, 'receipt': receipt}
        return {'status': 'uncertain', 'reason': receipt.get('reason', 'ack_false'),
                'version_id': version_id}
    finally:
        db.close()


def recover(db_path, case_id, lims):
    """核验 LIMS 远端最终状态：补登迟到 ACK、采纳外部已接受版本、按 nonce 幂等重试。"""
    db = connect(db_path)
    try:
        row = db.execute('SELECT payload FROM catalog_case WHERE id=?', (case_id,)).fetchone()
        if not row:
            raise ValueError('catalog_case_missing')
        target = json.loads(row['payload'])['contract']['publish_target']
        remote = lims.status(target)
        actions = []
        with _immediate(db):
            if remote:
                local = db.execute('SELECT * FROM catalog_version WHERE id=?',
                                   (remote['version_id'],)).fetchone()
                if not local:
                    db.execute("UPDATE catalog_version SET status='superseded'"
                               " WHERE target=? AND seq<=? AND status IN ('candidate','committed')",
                               (target, remote['seq']))
                    db.execute("UPDATE lims_publication SET acked=2 WHERE version_id IN"
                               " (SELECT id FROM catalog_version WHERE target=? AND status='superseded')",
                               (target,))
                    obj = json.loads(remote['catalog'])
                    db.execute('INSERT OR IGNORE INTO catalog_version'
                               '(id,case_id,target,seq,strategy,source_set,status,payload,diff)'
                               ' VALUES (?,?,?,?,?,?,?,?,?)',
                               (remote['version_id'], obj.get('case_id', case_id), target,
                                remote['seq'], obj.get('strategy', 'unknown'),
                                str(obj.get('source_set', '')), 'adopted', remote['catalog'], '{}'))
                    actions.append(('adopted', remote['version_id']))
                elif local['status'] in ('candidate', 'committed'):
                    db.execute("UPDATE catalog_version SET status='published' WHERE id=?",
                               (remote['version_id'],))
                    actions.append(('confirmed', remote['version_id']))
            pending = db.execute('SELECT * FROM lims_publication WHERE case_id=? AND acked=0',
                                 (case_id,)).fetchall()
            for pub in pending:
                v = db.execute('SELECT * FROM catalog_version WHERE id=?',
                               (pub['version_id'],)).fetchone()
                if remote and remote['version_id'] == pub['version_id']:
                    receipt = dict(json.loads(remote['receipt']), recovered=True)
                    db.execute('UPDATE lims_publication SET acked=1, receipt=? WHERE id=?',
                               (json.dumps(receipt, sort_keys=True), pub['id']))
                    db.execute("UPDATE catalog_version SET status='published' WHERE id=?", (v['id'],))
                    actions.append(('ack_recovered', v['id']))
                elif remote and remote['seq'] >= v['seq']:
                    db.execute("UPDATE catalog_version SET status='superseded' WHERE id=?", (v['id'],))
                    db.execute('UPDATE lims_publication SET acked=2 WHERE id=?', (pub['id'],))
                    actions.append(('superseded', v['id']))
        retries = db.execute("SELECT p.id pid, p.version_id, p.nonce, v.seq, v.target, v.payload"
                             ' FROM lims_publication p JOIN catalog_version v ON v.id=p.version_id'
                             " WHERE p.case_id=? AND p.acked=0 AND v.status='committed'",
                             (case_id,)).fetchall()
        for pub in retries:
            try:
                receipt = lims.apply(pub['target'], pub['version_id'], pub['seq'],
                                     pub['nonce'], pub['payload'])
            except (LostAck, StaleVersion, VersionGap, NonceConflict):
                continue
            if receipt.get('ack'):
                with _immediate(db):
                    db.execute('UPDATE lims_publication SET acked=1, receipt=? WHERE id=?',
                               (json.dumps(receipt, sort_keys=True), pub['pid']))
                    db.execute("UPDATE catalog_version SET status='published' WHERE id=?",
                               (pub['version_id'],))
                actions.append(('republished', pub['version_id']))
        return {'actions': [(kind, vid) for kind, vid in actions]}
    finally:
        db.close()


# ---------- 报告 ----------

def _markdown(report):
    lines = ['# 供应商目录发布报告', '']
    lines.append(f"- 案例: {report['case_id']}")
    lines.append(f"- 来源集合: {report['source_set']}")
    lines.append(f"- 发布目标: {report['publish_target']}")
    lines.append(f"- 合同 SHA256: {report['contract_sha256']}")
    lines += ['', '## 来源页面']
    for p in report['pages']:
        lines.append(f"- {p['name']}（{p.get('supplier') or '?'} 第{p.get('page_no') or '?'}页）"
                     f" sha256 {p['sha256'][:16]}… 下一页: {p.get('next') or '无'}")
    lines += ['', '## 分页链']
    for chain in report['pagination']:
        state = '完整' if chain['complete'] else '不完整'
        lines.append(f"- {chain['supplier']}: {' → '.join(chain['pages'])}（{state}）")
    lines += ['', '## 抽取报价']
    if report['offers']:
        for o in report['offers']:
            lines.append(f"- {o['supplier']} · {o['name']} · {o['quantity']} {o['unit']} 共 "
                         f"{o['amount_cents']} 分 → {o['unit_price_cents']} 分/{o['canonical_unit']}"
                         f" · 储温 {o['storage']} · 来源 {o['page']}#{o['page_sha256'][:12]}"
                         f" · 报价指纹 {o['offer_sha256'][:12]}")
    else:
        lines.append('- 无')
    lines += ['', '## 身份策略与凭证']
    catalog = report['catalog']
    if catalog:
        lines.append(f"- 当前版本策略: {catalog['strategy']}")
        lines.append(f"- 取舍: {STRATEGY_RATIONALE.get(catalog['strategy'], '')}")
        for a in catalog['evidence'].get('aliases', []):
            lines.append(f"- 别名: {' ⇔ '.join(a['gtins'])}（{a['signer']} 签署，有效期至 "
                         f"{a['expires']}，签名 {a['signature'][:16]}…）")
    else:
        lines.append('- 尚未发布')
    lines += ['', '## 规范产品（当前发布版本）']
    if catalog:
        for p in catalog['products']:
            offers = ' / '.join(f"{o['supplier']} {o['unit_price_cents']}" for o in p['offers'])
            lines.append(f"- {p['product_key']} · {p['name']} · 储温 {p['storage']} · 报价 {offers}"
                         f" 分/{p['canonical_unit']} · 推荐 {p['recommended']['supplier']}")
    else:
        lines.append('- 无')
    lines += ['', '## 版本与差异']
    if report['versions']:
        for v in report['versions']:
            d = v['diff']
            lines.append(f"- v{v['seq']} {v['id'][:12]}… 策略 {v['strategy']} 状态 {v['status']}"
                         f"：新增 {len(d['added'])}，变更 {len(d['changed'])}，下架 {len(d['removed'])}")
    else:
        lines.append('- 无')
    lines += ['', '## LIMS 回执']
    if report['publications']:
        for pub in report['publications']:
            if pub['receipt']:
                rec = pub['receipt']
                lines.append(f"- 版本 {pub['version_id'][:12]}… nonce {pub['nonce']}"
                             f" ack={pub['acked']} products={rec.get('products')} seq={rec.get('seq')}")
            else:
                lines.append(f"- 版本 {pub['version_id'][:12]}… nonce {pub['nonce']}"
                             f" ack={pub['acked']} 回执待收")
    else:
        lines.append('- 无')
    lines += ['', '## 待核问题']
    if report['issues']:
        for i in report['issues']:
            lines.append(f"- {i['code']}: {i['detail']}")
    else:
        lines.append('- 无')
    return '\n'.join(lines) + '\n'


def catalog_report(db_path, case_id, out):
    """输出 JSON + Markdown：网页 SHA→分页链→抽取字段→身份凭证→单位换算→版本/回执。"""
    db = connect(db_path)
    try:
        row = db.execute('SELECT payload FROM catalog_case WHERE id=?', (case_id,)).fetchone()
        if not row:
            raise ValueError('catalog_case_missing')
        case = json.loads(row['payload'])
        contract = case['contract']
        target = contract['publish_target']
        pages = []
        for record in case['pages']:
            probe = _parse_page(record['name'], record['bytes'], [])
            pages.append({'name': record['name'], 'sha256': record['sha256'],
                          'format': probe['format'], 'supplier': probe['supplier'],
                          'page_no': probe['page_no'], 'next': probe['next']})
        pagination = []
        by_supplier = defaultdict(list)
        for p in pages:
            by_supplier[p['supplier']].append(p)
        for supplier in sorted(by_supplier):
            group = sorted(by_supplier[supplier], key=lambda p: p['page_no'])
            by_no = {p['page_no']: p for p in group}
            complete = all((p['page_no'] + 1) in by_no for p in group if p['next'])
            pagination.append({'supplier': supplier, 'pages': [p['name'] for p in group],
                               'complete': complete})
        offers = [{k: r[k] for k in ('supplier', 'maker_code', 'gtin', 'name', 'quantity', 'unit',
                                     'amount_cents', 'storage', 'revision', 'canonical_unit',
                                     'unit_price_cents', 'page', 'page_sha256', 'offer_sha256')}
                  for r in db.execute(
                      'SELECT * FROM catalog_offer WHERE case_id=? ORDER BY supplier,gtin,page',
                      (case_id,))]
        issues = [{'code': r['code'], 'detail': r['detail']} for r in db.execute(
            'SELECT code,detail FROM catalog_issue WHERE case_id=? ORDER BY code,detail', (case_id,))]
        versions = [{'id': r['id'], 'case_id': r['case_id'], 'seq': r['seq'],
                     'strategy': r['strategy'], 'source_set': r['source_set'],
                     'status': r['status'], 'diff': json.loads(r['diff'])}
                    for r in db.execute('SELECT * FROM catalog_version WHERE target=? ORDER BY seq',
                                        (target,))]
        publications = [{'version_id': r['version_id'], 'nonce': r['nonce'],
                         'acked': bool(r['acked']),
                         'receipt': json.loads(r['receipt']) if r['receipt'] else None}
                        for r in db.execute(
                            'SELECT * FROM lims_publication WHERE case_id=? ORDER BY id', (case_id,))]
        current = db.execute("SELECT payload FROM catalog_version WHERE target=?"
                             " AND status IN ('published','adopted') ORDER BY seq DESC LIMIT 1",
                             (target,)).fetchone()
        report = {'case_id': case_id, 'contract_sha256': case['contract_sha256'],
                  'source_set': contract.get('source_set'), 'publish_target': target,
                  'pages': pages, 'pagination': pagination, 'offers': offers, 'issues': issues,
                  'versions': versions, 'publications': publications,
                  'catalog': json.loads(current['payload']) if current else None}
        out = Path(out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
                       encoding='utf-8')
        out.with_suffix('.md').write_text(_markdown(report), encoding='utf-8')
        return out
    finally:
        db.close()
