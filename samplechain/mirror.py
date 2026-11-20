"""原始扫码流归档:内容寻址分块、断点续传、冲突分支与原子水位发布。

- 每块原始字节按内容哈希寻址,经上传会话分段采集,可断点重传;
  同名不同字节是不同内容,不会被误判为幂等。
- 块内校验设备 ID、序号范围、跳号/倒序/重复、签名来源与封存 epoch,
  字节原样留存,归档对象不可改写。
- 重叠区间字节一致则合并证据;冲突则保留两条不可改写分支及来源,
  不按到达先后自动决定真值。
- 发布前冻结权威契约(device_primary/witness_mirror),水位只能从已证明
  连续且所选分支完整的前缀递增;以设备 epoch 比较交换原子提交,
  读者要么拿到旧完整版本,要么拿到新完整版本。
"""
import base64
import json
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path

from .ingest import KINDS, time as parse_ts
from .store import connect

CONTRACT_ORIGIN = {'device_primary': 'primary', 'witness_mirror': 'witness_mirror'}
ORIGINS = frozenset(CONTRACT_ORIGIN.values())
SEGMENT_SIZE = 65536

SCHEMA = """
CREATE TABLE IF NOT EXISTS mirror_case(
  id TEXT PRIMARY KEY, device TEXT NOT NULL, manifest_sha256 TEXT NOT NULL,
  manifest_json TEXT NOT NULL, frozen_chunks_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS signer_credential(
  device TEXT NOT NULL, signer TEXT NOT NULL, revoked INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(device, signer));
CREATE TABLE IF NOT EXISTS upload_session(
  id TEXT PRIMARY KEY, device TEXT NOT NULL, chunk_id TEXT NOT NULL,
  origin TEXT NOT NULL, seq_from INTEGER NOT NULL, seq_to INTEGER NOT NULL,
  seal_epoch INTEGER NOT NULL, signer TEXT NOT NULL,
  expected_sha256 TEXT, expected_size INTEGER,
  state TEXT NOT NULL DEFAULT 'open');
CREATE TABLE IF NOT EXISTS upload_segment(
  session_id TEXT NOT NULL REFERENCES upload_session(id),
  offset INTEGER NOT NULL, data BLOB NOT NULL, sha256 TEXT NOT NULL,
  PRIMARY KEY(session_id, offset));
CREATE TABLE IF NOT EXISTS raw_chunk(
  sha256 TEXT PRIMARY KEY, device TEXT NOT NULL,
  seq_from INTEGER NOT NULL, seq_to INTEGER NOT NULL, raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS chunk_attestation(
  id INTEGER PRIMARY KEY, chunk_sha256 TEXT NOT NULL REFERENCES raw_chunk(sha256),
  chunk_id TEXT NOT NULL, origin TEXT NOT NULL, signer TEXT NOT NULL,
  seal_epoch INTEGER NOT NULL, session_id TEXT NOT NULL REFERENCES upload_session(id),
  committed_seq INTEGER NOT NULL,
  UNIQUE(chunk_sha256, chunk_id, origin, signer));
CREATE TABLE IF NOT EXISTS chunk_line(
  chunk_sha256 TEXT NOT NULL REFERENCES raw_chunk(sha256),
  line_no INTEGER NOT NULL, seq INTEGER NOT NULL,
  line_sha256 TEXT NOT NULL, raw_line BLOB NOT NULL,
  PRIMARY KEY(chunk_sha256, line_no), UNIQUE(chunk_sha256, seq));
CREATE TABLE IF NOT EXISTS authority_contract(
  id TEXT PRIMARY KEY, device TEXT NOT NULL, contract TEXT NOT NULL,
  seal_epoch INTEGER NOT NULL, selection_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS publication(
  version_id TEXT PRIMARY KEY, device TEXT NOT NULL,
  contract_id TEXT NOT NULL REFERENCES authority_contract(id),
  contract TEXT NOT NULL, seal_epoch INTEGER NOT NULL,
  seq_from INTEGER NOT NULL, seq_to INTEGER NOT NULL,
  canonical_sha256 TEXT NOT NULL, canonical_jsonl BLOB NOT NULL,
  mapping_json TEXT NOT NULL, proofs_json TEXT NOT NULL,
  supersedes TEXT, publisher TEXT NOT NULL, committed_seq INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS watermark(
  device TEXT PRIMARY KEY, seal_epoch INTEGER NOT NULL, seq_to INTEGER NOT NULL,
  version_id TEXT NOT NULL REFERENCES publication(version_id));
CREATE TRIGGER IF NOT EXISTS raw_chunk_no_update BEFORE UPDATE ON raw_chunk
  BEGIN SELECT RAISE(ABORT,'immutable_archive'); END;
CREATE TRIGGER IF NOT EXISTS raw_chunk_no_delete BEFORE DELETE ON raw_chunk
  BEGIN SELECT RAISE(ABORT,'immutable_archive'); END;
CREATE TRIGGER IF NOT EXISTS chunk_line_no_update BEFORE UPDATE ON chunk_line
  BEGIN SELECT RAISE(ABORT,'immutable_archive'); END;
CREATE TRIGGER IF NOT EXISTS chunk_line_no_delete BEFORE DELETE ON chunk_line
  BEGIN SELECT RAISE(ABORT,'immutable_archive'); END;
CREATE TRIGGER IF NOT EXISTS attestation_no_update BEFORE UPDATE ON chunk_attestation
  BEGIN SELECT RAISE(ABORT,'immutable_archive'); END;
CREATE TRIGGER IF NOT EXISTS attestation_no_delete BEFORE DELETE ON chunk_attestation
  BEGIN SELECT RAISE(ABORT,'immutable_archive'); END;
CREATE TRIGGER IF NOT EXISTS contract_no_update BEFORE UPDATE ON authority_contract
  BEGIN SELECT RAISE(ABORT,'immutable_archive'); END;
CREATE TRIGGER IF NOT EXISTS contract_no_delete BEFORE DELETE ON authority_contract
  BEGIN SELECT RAISE(ABORT,'immutable_archive'); END;
CREATE TRIGGER IF NOT EXISTS publication_no_update BEFORE UPDATE ON publication
  BEGIN SELECT RAISE(ABORT,'immutable_archive'); END;
CREATE TRIGGER IF NOT EXISTS publication_no_delete BEFORE DELETE ON publication
  BEGIN SELECT RAISE(ABORT,'immutable_archive'); END;
"""


def connect_mirror(db_path):
    """打开数据库并确保镜像归档结构存在;连接处于自动提交,写操作自行开启事务。"""
    con = connect(db_path)
    con.executescript(SCHEMA)
    con.isolation_level = None
    return con


@contextmanager
def _tx(con):
    """BEGIN IMMEDIATE 事务:提交/回滚二选一,崩溃不会留下半截写入。"""
    con.execute('BEGIN IMMEDIATE')
    try:
        yield
    except Exception:
        con.execute('ROLLBACK')
        raise
    con.execute('COMMIT')


def _check_device(con, device):
    if not con.execute('SELECT 1 FROM event WHERE device=?', (device,)).fetchone():
        raise ValueError('unknown_scanner')


def _check_credential(con, device, signer):
    row = con.execute('SELECT revoked FROM signer_credential WHERE device=? AND signer=?',
                      (device, signer)).fetchone()
    if not row:
        raise ValueError('unknown_signer')
    if row['revoked']:
        raise ValueError('credential_revoked')


def grant_credential(con, device, signer):
    """登记签名来源凭证;已吊销的凭证不会被静默恢复。"""
    _check_device(con, device)
    con.execute('INSERT OR IGNORE INTO signer_credential(device,signer,revoked) VALUES (?,?,0)',
                (device, signer))


def revoke_credential(con, device, signer):
    cur = con.execute('UPDATE signer_credential SET revoked=1 WHERE device=? AND signer=?',
                      (device, signer))
    if cur.rowcount == 0:
        raise ValueError('unknown_signer')


def _session_id(device, chunk_id, origin, seq_from, seq_to, seal_epoch, signer,
                expected_sha256, expected_size):
    key = json.dumps([device, chunk_id, origin, seq_from, seq_to, seal_epoch, signer,
                      expected_sha256, expected_size], separators=(',', ':'))
    return 'sess-' + sha256(key.encode()).hexdigest()


def open_session(con, device, chunk_id, origin, seq_from, seq_to, seal_epoch, signer,
                 expected_sha256=None, expected_size=None):
    """打开(或回到)上传会话。会话 ID 由上传参数决定,断线后用相同参数即可续传。"""
    _check_device(con, device)
    if origin not in ORIGINS:
        raise ValueError('unknown_origin')
    seq_from, seq_to, seal_epoch = int(seq_from), int(seq_to), int(seal_epoch)
    if seq_from < 1 or seq_to < seq_from:
        raise ValueError('bad_seq_range')
    if seal_epoch < 1:
        raise ValueError('bad_seal_epoch')
    if not chunk_id or not signer:
        raise ValueError('missing_identity')
    _check_credential(con, device, signer)
    if expected_sha256 is not None:
        expected_sha256 = str(expected_sha256).lower()
        if len(expected_sha256) != 64:
            raise ValueError('bad_expected_sha256')
    sid = _session_id(device, chunk_id, origin, seq_from, seq_to, seal_epoch, signer,
                      expected_sha256, expected_size)
    if not con.execute('SELECT 1 FROM upload_session WHERE id=?', (sid,)).fetchone():
        con.execute('INSERT INTO upload_session VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                    (sid, device, chunk_id, origin, seq_from, seq_to, seal_epoch, signer,
                     expected_sha256, expected_size, 'open'))
    return sid


def stage_segment(con, session_id, offset, data):
    """持久暂存一段字节。同偏移同字节幂等;同偏移不同字节报错,不静默覆盖。"""
    row = con.execute('SELECT state FROM upload_session WHERE id=?', (session_id,)).fetchone()
    if not row:
        raise ValueError('unknown_session')
    if row['state'] != 'open':
        raise ValueError('session_closed')
    offset = int(offset)
    if offset < 0:
        raise ValueError('bad_offset')
    data = bytes(data)
    if not data:
        raise ValueError('empty_segment')
    digest = sha256(data).hexdigest()
    existing = con.execute('SELECT sha256 FROM upload_segment WHERE session_id=? AND offset=?',
                           (session_id, offset)).fetchone()
    if existing:
        if existing['sha256'] == digest:
            return {'stored': False, 'idempotent': True}
        raise ValueError('segment_conflict')
    con.execute('INSERT INTO upload_segment VALUES (?,?,?,?)', (session_id, offset, data, digest))
    return {'stored': True, 'idempotent': False}


def _validate_chunk(device, seq_from, seq_to, raw):
    """校验块内设备 ID、序号范围、跳号/倒序/重复、事件种类与时间戳;返回行字节清单。"""
    if seq_from < 1 or seq_to < seq_from:
        raise ValueError('bad_seq_range')
    parts = raw.split(b'\n')
    if parts and parts[-1] == b'':
        parts.pop()
    if len(parts) != seq_to - seq_from + 1:
        raise ValueError('line_count_mismatch')
    lines = []
    expect = seq_from
    for line_no, line in enumerate(parts, 1):
        if not line.strip():
            raise ValueError('blank_line')
        try:
            row = json.loads(line.decode('utf-8-sig'))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f'parse_error:{exc}')
        if not isinstance(row, dict):
            raise ValueError('parse_error:not_object')
        try:
            actual_device = str(row['device'])
            seq = int(row['seq'])
            kind = row['kind']
            ts = str(row['ts'])
        except KeyError as exc:
            raise ValueError(f'missing_field:{exc}')
        except (TypeError, ValueError) as exc:
            raise ValueError(f'bad_seq:{exc}')
        if actual_device != device:
            raise ValueError('device_mismatch')
        if kind not in KINDS:
            raise ValueError('unknown_kind')
        parse_ts(ts)
        if seq != expect:
            raise ValueError('seq_not_contiguous')
        lines.append((line_no, seq, line))
        expect += 1
    return lines


def commit_session(con, session_id):
    """组装暂存段、校验并入库。整个提交是一个事务:失败不留残块,会话保持打开。"""
    with _tx(con):
        row = con.execute('SELECT * FROM upload_session WHERE id=?', (session_id,)).fetchone()
        if not row:
            raise ValueError('unknown_session')
        if row['state'] == 'committed':
            att = con.execute('SELECT chunk_sha256 FROM chunk_attestation WHERE session_id=?',
                              (session_id,)).fetchone()
            n = con.execute('SELECT count(*) FROM chunk_line WHERE chunk_sha256=?',
                            (att['chunk_sha256'],)).fetchone()[0]
            return {'chunk_sha256': att['chunk_sha256'], 'lines': n, 'idempotent': True}
        if row['state'] != 'open':
            raise ValueError('session_closed')
        segments = con.execute('SELECT offset,data FROM upload_segment WHERE session_id=? '
                               'ORDER BY offset', (session_id,)).fetchall()
        if not segments:
            raise ValueError('no_segments')
        assembled = bytearray()
        for seg in segments:
            if seg['offset'] != len(assembled):
                raise ValueError('segment_gap')
            assembled += seg['data']
        raw = bytes(assembled)
        if row['expected_size'] is not None and len(raw) != row['expected_size']:
            raise ValueError('length_mismatch')
        digest = sha256(raw).hexdigest()
        if row['expected_sha256'] and digest != row['expected_sha256']:
            raise ValueError('content_mismatch')
        lines = _validate_chunk(row['device'], row['seq_from'], row['seq_to'], raw)
        _check_credential(con, row['device'], row['signer'])
        existed = con.execute('SELECT 1 FROM raw_chunk WHERE sha256=?', (digest,)).fetchone()
        if not existed:
            con.execute('INSERT INTO raw_chunk VALUES (?,?,?,?,?)',
                        (digest, row['device'], row['seq_from'], row['seq_to'], raw))
            for line_no, seq, line in lines:
                con.execute('INSERT INTO chunk_line VALUES (?,?,?,?,?)',
                            (digest, line_no, seq, sha256(line).hexdigest(), line))
        seq_no = con.execute('SELECT COALESCE(MAX(committed_seq),0)+1 FROM chunk_attestation'
                             ).fetchone()[0]
        con.execute('INSERT OR IGNORE INTO chunk_attestation'
                    '(chunk_sha256,chunk_id,origin,signer,seal_epoch,session_id,committed_seq)'
                    ' VALUES (?,?,?,?,?,?,?)',
                    (digest, row['chunk_id'], row['origin'], row['signer'], row['seal_epoch'],
                     session_id, seq_no))
        con.execute("UPDATE upload_session SET state='committed' WHERE id=?", (session_id,))
        return {'chunk_sha256': digest, 'lines': len(lines), 'idempotent': bool(existed)}


def register(db_path, manifest_path):
    """冻结清单与冲突原件,并把清单分块经上传会话管线采入内容寻址归档。"""
    manifest_path = Path(manifest_path)
    raw = manifest_path.read_bytes()
    manifest = json.loads(raw)
    case_id = manifest['case_id']
    device = manifest['device']
    seal_epoch = int(manifest['seal_epoch'])
    for contract in manifest['authority_contracts']:
        if contract not in CONTRACT_ORIGIN:
            raise ValueError('unknown_contract')
    con = connect_mirror(db_path)
    try:
        _check_device(con, device)
        manifest_hash = sha256(raw).hexdigest()
        existing = con.execute('SELECT manifest_sha256 FROM mirror_case WHERE id=?',
                               (case_id,)).fetchone()
        if existing and existing['manifest_sha256'] != manifest_hash:
            raise ValueError('case_conflict')
        chunks = {}
        for item in manifest['chunks']:
            if item['origin'] not in ORIGINS:
                raise ValueError('unknown_origin')
            data = (manifest_path.parent / item['file']).read_bytes()
            chunks[item['id']] = {'sha256': sha256(data).hexdigest(),
                                  'raw_base64': base64.b64encode(data).decode()}
        if not existing:
            con.execute('INSERT INTO mirror_case VALUES (?,?,?,?,?)',
                        (case_id, device, manifest_hash, raw.decode(),
                         json.dumps(chunks, sort_keys=True)))
        for item in manifest['chunks']:
            data = (manifest_path.parent / item['file']).read_bytes()
            signer = item.get('signer') or f"{device}-{item['origin']}"
            grant_credential(con, device, signer)
            sid = open_session(con, device, item['id'], item['origin'],
                               int(item['seq_from']), int(item['seq_to']), seal_epoch, signer,
                               expected_sha256=sha256(data).hexdigest(), expected_size=len(data))
            for off in range(0, len(data), SEGMENT_SIZE):
                stage_segment(con, sid, off, data[off:off + SEGMENT_SIZE])
            commit_session(con, sid)
        return case_id
    finally:
        con.close()


def _load_streams(con, device):
    """按序号归集所有已提交行版本:相同行字节合并证据,不同字节各成分支。"""
    rows = con.execute(
        'SELECT l.seq AS seq, l.line_sha256 AS line_sha256, l.raw_line AS raw_line,'
        ' l.line_no AS line_no, l.chunk_sha256 AS chunk_sha256,'
        ' a.chunk_id AS chunk_id, a.origin AS origin, a.signer AS signer,'
        ' a.seal_epoch AS seal_epoch, a.committed_seq AS committed_seq, a.id AS att_id'
        ' FROM chunk_line l'
        ' JOIN raw_chunk c ON c.sha256 = l.chunk_sha256'
        ' JOIN chunk_attestation a ON a.chunk_sha256 = l.chunk_sha256'
        ' WHERE c.device = ?'
        ' ORDER BY l.seq, a.committed_seq, a.id', (device,)).fetchall()
    streams = {}
    for r in rows:
        version = streams.setdefault(r['seq'], {}).setdefault(
            r['line_sha256'], {'raw_line': bytes(r['raw_line']), 'attestations': []})
        version['attestations'].append(
            {'att_id': r['att_id'], 'chunk_sha256': r['chunk_sha256'], 'chunk_id': r['chunk_id'],
             'origin': r['origin'], 'signer': r['signer'], 'seal_epoch': r['seal_epoch'],
             'line_no': r['line_no']})
    return streams


def branches(con, device):
    """设备每个序号的行版本:conflict=冲突分支并存,merged=字节一致合并证据。"""
    streams = _load_streams(con, device)
    out = []
    for seq in sorted(streams):
        versions = []
        for line_sha, info in sorted(streams[seq].items()):
            versions.append({'line_sha256': line_sha,
                             'raw_line': info['raw_line'].decode('utf-8'),
                             'chunks': [{'chunk_sha256': a['chunk_sha256'],
                                         'chunk_id': a['chunk_id'], 'origin': a['origin'],
                                         'signer': a['signer'], 'seal_epoch': a['seal_epoch'],
                                         'line_no': a['line_no']}
                                        for a in info['attestations']]})
        chunk_count = len({c['chunk_sha256'] for v in versions for c in v['chunks']})
        status = 'conflict' if len(versions) > 1 else ('merged' if chunk_count > 1 else 'single')
        out.append({'seq': seq, 'status': status, 'versions': versions})
    return out


def _choose(versions, origin):
    """按契约来源选取版本;来源缺失且全网字节一致才回退,冲突时不按到达先后取舍。"""
    contracted = [ls for ls, info in versions.items()
                  if any(a['origin'] == origin for a in info['attestations'])]
    if len(contracted) > 1:
        raise ValueError('origin_conflict')
    if contracted:
        return contracted[0]
    if len(versions) == 1:
        return next(iter(versions))
    return None


def _pick_attestation(info, origin):
    return next((a for a in info['attestations'] if a['origin'] == origin),
                info['attestations'][0])


def _device_expectations(con, device):
    row = con.execute('SELECT manifest_json FROM mirror_case WHERE device=? '
                      'ORDER BY rowid DESC LIMIT 1', (device,)).fetchone()
    if not row:
        return 1, None
    manifest = json.loads(row['manifest_json'])
    expect_to = manifest.get('expected_seq_to')
    return int(manifest.get('expected_seq_from', 1)), (int(expect_to) if expect_to else None)


def plan_publish(con, device, contract, seal_epoch, publisher,
                 seq_from=None, expected_seq_to=None):
    """只读地计算发布计划:冻结契约选择、连续前缀、规范字节与缺口/冲突证明。"""
    if contract not in CONTRACT_ORIGIN:
        raise ValueError('unknown_contract')
    seal_epoch = int(seal_epoch)
    if seal_epoch < 1:
        raise ValueError('bad_seal_epoch')
    if not publisher:
        raise ValueError('missing_publisher')
    default_from, default_to = _device_expectations(con, device)
    seq_from = int(seq_from) if seq_from is not None else default_from
    if expected_seq_to is None:
        expected_seq_to = default_to
    origin = CONTRACT_ORIGIN[contract]
    streams = _load_streams(con, device)
    selection, conflicts, merged = {}, [], []
    for seq in sorted(streams):
        versions = streams[seq]
        chosen = _choose(versions, origin)
        if len(versions) > 1:
            conflicts.append({'seq': seq, 'selected_line_sha256': chosen,
                              'branches': [{'line_sha256': ls,
                                            'chunks': _branch_chunks(info)}
                                           for ls, info in sorted(versions.items())]})
        chunk_shas = {a['chunk_sha256'] for info in versions.values()
                      for a in info['attestations']}
        if len(versions) == 1 and len(chunk_shas) > 1:
            merged.append({'seq': seq, 'line_sha256': next(iter(versions)),
                           'chunks': sorted(chunk_shas)})
        if chosen:
            selection[seq] = chosen
    prefix = []
    seq = seq_from
    while seq in selection:
        prefix.append(seq)
        seq += 1
    if not prefix:
        raise ValueError('nothing_continuous')
    seq_to = prefix[-1]
    horizon = max([seq_to] + list(streams) + ([expected_seq_to] if expected_seq_to else []))
    gaps = [s for s in range(seq_to + 1, horizon + 1) if s not in selection]
    held = [s for s in range(seq_to + 1, horizon + 1) if s in selection]
    mapping, lines = [], []
    for s in prefix:
        info = streams[s][selection[s]]
        att = _pick_attestation(info, origin)
        mapping.append({'seq': s, 'line_sha256': selection[s],
                        'chunk_sha256': att['chunk_sha256'], 'chunk_id': att['chunk_id'],
                        'origin': att['origin'], 'line_no': att['line_no']})
        lines.append(info['raw_line'])
    canonical = b''.join(line + b'\n' for line in lines)
    unselected = []
    for conflict in conflicts:
        if conflict['seq'] in selection and conflict['seq'] <= seq_to:
            for branch in conflict['branches']:
                if branch['line_sha256'] != selection[conflict['seq']]:
                    unselected.append({'seq': conflict['seq'],
                                       'line_sha256': branch['line_sha256'],
                                       'chunks': branch['chunks'],
                                       'reason': f'contract_{contract}',
                                       'retained_in': 'raw_chunk'})
    sel_json = {str(s): {'line_sha256': ls,
                         'chunk_sha256': _pick_attestation(streams[s][ls], origin)['chunk_sha256']}
                for s, ls in selection.items()}
    contract_id = 'ctr-' + sha256(json.dumps(
        {'device': device, 'contract': contract, 'seal_epoch': seal_epoch,
         'selection': sel_json}, sort_keys=True).encode()).hexdigest()
    canonical_sha = sha256(canonical).hexdigest()
    version_id = 'pub-' + sha256(f'{contract_id}:{canonical_sha}'.encode()).hexdigest()
    current = con.execute('SELECT * FROM watermark WHERE device=?', (device,)).fetchone()
    proofs = {'conflicts': conflicts, 'merged': merged,
              'gaps': [{'seq': s, 'reason': 'no_selected_version'} for s in gaps],
              'held': [{'seq': s, 'reason': 'blocked_by_earlier_gap'} for s in held],
              'unselected': unselected}
    return {'device': device, 'contract': contract, 'seal_epoch': seal_epoch,
            'publisher': publisher, 'seq_from': seq_from, 'seq_to': seq_to,
            'canonical': canonical, 'canonical_sha256': canonical_sha,
            'mapping': mapping, 'proofs': proofs,
            'contract_id': contract_id,
            'selection_json': json.dumps(sel_json, sort_keys=True),
            'version_id': version_id,
            'supersedes': current['version_id'] if current else None,
            'expected_current': dict(current) if current else None}


def _branch_chunks(info):
    return [{'chunk_sha256': a['chunk_sha256'], 'chunk_id': a['chunk_id'],
             'origin': a['origin'], 'signer': a['signer'], 'seal_epoch': a['seal_epoch'],
             'line_no': a['line_no']} for a in info['attestations']]


def commit_plan(con, plan):
    """原子提交发布计划:epoch 比较交换 + 水位单调递增,一个事务内完成。"""
    with _tx(con):
        current = con.execute('SELECT * FROM watermark WHERE device=?',
                              (plan['device'],)).fetchone()
        expected = plan.get('expected_current')
        current_version = current['version_id'] if current else None
        if current_version != (expected['version_id'] if expected else None):
            raise ValueError('watermark_conflict')
        if current:
            if plan['seal_epoch'] < current['seal_epoch']:
                raise ValueError('stale_epoch')
            if plan['seal_epoch'] == current['seal_epoch']:
                if current_version == plan['version_id'] and plan['seq_to'] == current['seq_to']:
                    return _version_result(con, plan['version_id'], idempotent=True)
                if plan['seq_to'] <= current['seq_to']:
                    raise ValueError('watermark_not_advanced')
        con.execute('INSERT OR IGNORE INTO authority_contract VALUES (?,?,?,?,?)',
                    (plan['contract_id'], plan['device'], plan['contract'],
                     plan['seal_epoch'], plan['selection_json']))
        if not con.execute('SELECT 1 FROM publication WHERE version_id=?',
                           (plan['version_id'],)).fetchone():
            seq_no = con.execute('SELECT COALESCE(MAX(committed_seq),0)+1 FROM publication'
                                 ).fetchone()[0]
            con.execute('INSERT INTO publication VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                        (plan['version_id'], plan['device'], plan['contract_id'],
                         plan['contract'], plan['seal_epoch'], plan['seq_from'], plan['seq_to'],
                         plan['canonical_sha256'], plan['canonical'],
                         json.dumps(plan['mapping']), json.dumps(plan['proofs']),
                         plan['supersedes'], plan['publisher'], seq_no))
        con.execute('INSERT OR REPLACE INTO watermark VALUES (?,?,?,?)',
                    (plan['device'], plan['seal_epoch'], plan['seq_to'], plan['version_id']))
        return _version_result(con, plan['version_id'], idempotent=False)


def publish(con, device, contract, seal_epoch, publisher, seq_from=None, expected_seq_to=None):
    """计划并原子发布;版本内容寻址,重复发布同一结果幂等。"""
    return commit_plan(con, plan_publish(con, device, contract, seal_epoch, publisher,
                                         seq_from, expected_seq_to))


def _version_result(con, version_id, idempotent):
    r = con.execute('SELECT * FROM publication WHERE version_id=?', (version_id,)).fetchone()
    return {'version_id': r['version_id'], 'device': r['device'], 'contract': r['contract'],
            'contract_id': r['contract_id'], 'seal_epoch': r['seal_epoch'],
            'seq_from': r['seq_from'], 'seq_to': r['seq_to'],
            'canonical_sha256': r['canonical_sha256'], 'supersedes': r['supersedes'],
            'publisher': r['publisher'], 'idempotent': idempotent,
            'mapping': json.loads(r['mapping_json']), 'proofs': json.loads(r['proofs_json'])}


def fetch_version(con, version_id):
    """按版本 ID 取不可变归档;已发给下游的旧版本字节永不改写。"""
    r = con.execute('SELECT * FROM publication WHERE version_id=?', (version_id,)).fetchone()
    if not r:
        return None
    return {'version_id': r['version_id'], 'device': r['device'], 'contract': r['contract'],
            'contract_id': r['contract_id'], 'seal_epoch': r['seal_epoch'],
            'seq_from': r['seq_from'], 'seq_to': r['seq_to'],
            'canonical_sha256': r['canonical_sha256'],
            'canonical_jsonl': bytes(r['canonical_jsonl']),
            'mapping': json.loads(r['mapping_json']), 'proofs': json.loads(r['proofs_json']),
            'supersedes': r['supersedes'], 'publisher': r['publisher'],
            'committed_seq': r['committed_seq']}


def current_stream(con, device):
    """读取当前水位指向的完整版本;版本对象不可变,读者不会看到混杂流。"""
    w = con.execute('SELECT * FROM watermark WHERE device=?', (device,)).fetchone()
    if not w:
        return None
    return fetch_version(con, w['version_id'])


def mirror_report(con, device, out):
    """输出 JSON/Markdown 归档报告:水位、版本链、冲突分支与未选分支留存位置。"""
    wm = current_stream(con, device)
    versions = [fetch_version(con, r['version_id']) for r in con.execute(
        'SELECT version_id FROM publication WHERE device=? ORDER BY committed_seq', (device,))]
    for v in versions:
        v['canonical_jsonl'] = v['canonical_jsonl'].decode('utf-8')
    chunks = []
    for c in con.execute('SELECT * FROM raw_chunk WHERE device=? ORDER BY sha256', (device,)):
        atts = [dict(a) for a in con.execute(
            'SELECT chunk_id,origin,signer,seal_epoch,committed_seq FROM chunk_attestation'
            ' WHERE chunk_sha256=? ORDER BY committed_seq', (c['sha256'],))]
        chunks.append({'sha256': c['sha256'], 'seq_from': c['seq_from'], 'seq_to': c['seq_to'],
                       'size': len(c['raw']), 'attestations': atts})
    used = {m['chunk_sha256'] for m in wm['mapping']} if wm else set()
    unselected = [{'chunk_sha256': c['sha256'], 'chunk_id': c['attestations'][0]['chunk_id'],
                   'origin': c['attestations'][0]['origin'],
                   'reason': 'not_in_current_version', 'retained_in': 'raw_chunk'}
                  for c in chunks if c['sha256'] not in used]
    branch_rows = branches(con, device)
    payload = {'device': device,
               'watermark': ({'seal_epoch': wm['seal_epoch'], 'seq_to': wm['seq_to'],
                              'version_id': wm['version_id'], 'contract': wm['contract'],
                              'canonical_sha256': wm['canonical_sha256']} if wm else None),
               'versions': versions, 'branches': branch_rows, 'chunks': chunks,
               'unselected_retained': unselected}
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    lines = [f'# 设备 {device} 原始流镜像归档报告', '']
    if wm:
        lines += [f"- 封存 epoch: {wm['seal_epoch']}", f"- 发布水位: seq {wm['seq_to']}",
                  f"- 当前版本: `{wm['version_id']}`", f"- 权威契约: {wm['contract']}",
                  f"- 规范流 sha256: `{wm['canonical_sha256']}`", '']
    else:
        lines += ['- 尚未发布水位', '']
    lines.append('## 版本链')
    if versions:
        for v in versions:
            sup = f"（替代 `{v['supersedes']}`）" if v['supersedes'] else ''
            lines.append(f"- `{v['version_id']}`: 契约 {v['contract']}, epoch {v['seal_epoch']}, "
                         f"范围 {v['seq_from']}–{v['seq_to']}, 发布者 {v['publisher']}{sup}")
    else:
        lines.append('- 无')
    lines += ['', '## 冲突分支']
    any_conflict = False
    for b in branch_rows:
        if b['status'] != 'conflict':
            continue
        any_conflict = True
        lines.append(f"- seq {b['seq']}:")
        for v in b['versions']:
            for c in v['chunks']:
                lines.append(f"  - 行哈希 `{v['line_sha256'][:16]}…` 来源 {c['origin']}"
                             f"（块 {c['chunk_id']}，签名 {c['signer']}，"
                             f"块 sha256 `{c['chunk_sha256'][:16]}…`）")
    if not any_conflict:
        lines.append('- 无')
    lines += ['', '## 未选分支留存']
    if unselected:
        for u in unselected:
            lines.append(f"- 块 {u['chunk_id']}（{u['origin']}）sha256 `{u['chunk_sha256']}`"
                         f" 字节原样留存于 raw_chunk 表")
    else:
        lines.append('- 无')
    lines += ['', '## 分块清单']
    for c in chunks:
        lines.append(f"- `{c['sha256'][:16]}…` seq {c['seq_from']}–{c['seq_to']}，"
                     f"{c['size']} 字节，凭证 {len(c['attestations'])} 条")
    lines.append('')
    out.with_suffix('.md').write_text('\n'.join(lines), encoding='utf-8')
    return out
