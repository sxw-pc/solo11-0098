import json
import sqlite3
import threading
from hashlib import sha256
from pathlib import Path

import pytest

from samplechain.__main__ import init
from samplechain.ingest import ingest
from samplechain.mirror import (branches, commit_plan, commit_session, connect_mirror,
                                current_stream, fetch_version, grant_credential, mirror_report,
                                open_session, plan_publish, publish, register, revoke_credential,
                                stage_segment)

FIXTURES = Path(__file__).parents[1] / 'fixtures'
KINDS = {1: 'collect', 2: 'handoff', 3: 'receive', 4: 'temperature', 5: 'fridge_out', 6: 'test'}


def make_db(db):
    init(db, 'batch-1', 'S1')
    ingest(FIXTURES / 'primary-1.jsonl', db)  # D1 真实存在: 序号1-3已导入
    return db


def registered(db):
    make_db(db)
    register(db, FIXTURES / 'mirror-manifest.json')
    return db


def chunk_bytes(seqs, value=None):
    lines = []
    for s in seqs:
        row = {'kind': KINDS[s], 'device': 'D1', 'seq': s, 'sample_id': 'S1',
               'ts': f'2026-05-06T00:0{s}:00Z', 'actor': 'tech'}
        if s == 4 and value is not None:
            row['value'] = value
        lines.append(json.dumps(row))
    return ('\n'.join(lines) + '\n').encode()


def commit_chunk(con, chunk_id, origin, seq_from, seq_to, epoch, signer, data):
    sid = open_session(con, 'D1', chunk_id, origin, seq_from, seq_to, epoch, signer,
                       expected_sha256=sha256(data).hexdigest(), expected_size=len(data))
    stage_segment(con, sid, 0, data)
    return commit_session(con, sid)


def test_segmented_upload_resumes_after_reconnect(tmp_path):
    db = make_db(tmp_path / 'lab.db')
    con = connect_mirror(db)
    grant_credential(con, 'D1', 'sig-1')
    data = chunk_bytes([4, 5, 6], value=4)
    digest = sha256(data).hexdigest()
    sid = open_session(con, 'D1', 'P2', 'primary', 4, 6, 7, 'sig-1',
                       expected_sha256=digest, expected_size=len(data))
    stage_segment(con, sid, 0, data[:100])
    con.close()  # 上传只到一半就崩溃
    con = connect_mirror(db)
    assert con.execute('SELECT count(*) FROM raw_chunk').fetchone()[0] == 0  # 未提交不留块
    assert current_stream(con, 'D1') is None
    sid2 = open_session(con, 'D1', 'P2', 'primary', 4, 6, 7, 'sig-1',
                        expected_sha256=digest, expected_size=len(data))
    assert sid2 == sid  # 断点续传回到同一会话
    assert stage_segment(con, sid, 0, data[:100])['idempotent']  # 重传同字节幂等
    stage_segment(con, sid, 100, data[100:])
    res = commit_session(con, sid)
    assert res['chunk_sha256'] == digest and res['lines'] == 3 and not res['idempotent']
    assert commit_session(con, sid)['idempotent']
    con.close()


def test_segment_offset_conflict_and_gap(tmp_path):
    db = make_db(tmp_path / 'lab.db')
    con = connect_mirror(db)
    grant_credential(con, 'D1', 'sig-1')
    data = chunk_bytes([4, 5, 6], value=4)
    sid = open_session(con, 'D1', 'G1', 'primary', 4, 6, 7, 'sig-1')
    stage_segment(con, sid, 0, data[:50])
    stage_segment(con, sid, 60, data[60:])  # 分块错位: 50..60 缺失
    with pytest.raises(ValueError, match='segment_gap'):
        commit_session(con, sid)
    sid2 = open_session(con, 'D1', 'G2', 'primary', 4, 6, 7, 'sig-1')
    stage_segment(con, sid2, 0, data[:50])
    with pytest.raises(ValueError, match='segment_conflict'):
        stage_segment(con, sid2, 0, b'X' + data[1:50])  # 同偏移不同字节
    assert stage_segment(con, sid2, 0, data[:50])['idempotent']
    assert con.execute('SELECT count(*) FROM raw_chunk').fetchone()[0] == 0
    con.close()


def test_chunk_content_validation(tmp_path):
    db = make_db(tmp_path / 'lab.db')
    con = connect_mirror(db)
    grant_credential(con, 'D1', 'sig-1')

    def line(seq, kind='temperature', device='D1'):
        return json.dumps({'kind': kind, 'device': device, 'seq': seq, 'sample_id': 'S1',
                           'ts': f'2026-05-06T00:0{seq}:00Z', 'actor': 'tech'})

    def attempt(chunk_id, lines, seq_to=6):
        data = ('\n'.join(lines) + '\n').encode()
        sid = open_session(con, 'D1', chunk_id, 'primary', 4, seq_to, 7, 'sig-1')
        stage_segment(con, sid, 0, data)
        return commit_session(con, sid)

    with pytest.raises(ValueError, match='seq_not_contiguous'):
        attempt('B1', [line(4), line(5), line(5)])  # 重复序号
    with pytest.raises(ValueError, match='seq_not_contiguous'):
        attempt('B2', [line(4), line(6), line(5)])  # 倒序/跳号
    with pytest.raises(ValueError, match='device_mismatch'):
        attempt('B3', [line(4), line(5, device='D9'), line(6)])
    with pytest.raises(ValueError, match='unknown_kind'):
        attempt('B4', [line(4), line(5, kind='teleport'), line(6)])
    with pytest.raises(ValueError, match='line_count_mismatch'):
        attempt('B5', [line(4), line(5)])
    with pytest.raises(ValueError, match='missing_field'):
        attempt('B6', [line(4), json.dumps({'device': 'D1', 'seq': 5}), line(6)])
    assert con.execute('SELECT count(*) FROM raw_chunk').fetchone()[0] == 0  # 校验失败不留残块
    con.close()


def test_same_name_different_bytes_is_not_idempotent(tmp_path):
    db = make_db(tmp_path / 'lab.db')
    con = connect_mirror(db)
    grant_credential(con, 'D1', 'sig-1')
    a = chunk_bytes([4, 5, 6], value=4)
    b = chunk_bytes([4, 5, 6], value=5)
    # 声明了内容哈希却上传不同字节 -> 明确报错而不是幂等成功
    sid = open_session(con, 'D1', 'X1', 'primary', 4, 6, 7, 'sig-1',
                       expected_sha256=sha256(a).hexdigest())
    stage_segment(con, sid, 0, b)
    with pytest.raises(ValueError, match='content_mismatch'):
        commit_session(con, sid)
    assert con.execute('SELECT count(*) FROM raw_chunk').fetchone()[0] == 0
    # 同名分块的不同字节成为两条独立内容, 形成冲突分支而不是互相覆盖
    commit_chunk(con, 'X2', 'primary', 4, 6, 7, 'sig-1', a)
    commit_chunk(con, 'X2', 'primary', 4, 6, 7, 'sig-1', b)
    assert con.execute('SELECT count(*) FROM raw_chunk').fetchone()[0] == 2
    seq4 = [x for x in branches(con, 'D1') if x['seq'] == 4][0]
    assert seq4['status'] == 'conflict' and len(seq4['versions']) == 2
    # 同一来源自相矛盾时也不按到达先后取舍
    with pytest.raises(ValueError, match='origin_conflict'):
        publish(con, 'D1', 'device_primary', 7, 'op')
    # 真正的幂等: 相同字节重复提交不新增内容
    sid_a = open_session(con, 'D1', 'X2', 'primary', 4, 6, 7, 'sig-1',
                         expected_sha256=sha256(a).hexdigest(), expected_size=len(a))
    again = commit_session(con, sid_a)
    assert again['idempotent']
    assert con.execute('SELECT count(*) FROM raw_chunk').fetchone()[0] == 2
    con.close()


def test_overlapping_identical_bytes_merge(tmp_path):
    db = make_db(tmp_path / 'lab.db')
    con = connect_mirror(db)
    grant_credential(con, 'D1', 'sig-1')
    commit_chunk(con, 'A1', 'primary', 1, 3, 7, 'sig-1', chunk_bytes([1, 2, 3]))
    commit_chunk(con, 'B1', 'primary', 3, 6, 7, 'sig-1', chunk_bytes([3, 4, 5, 6], value=4))
    seq3 = [x for x in branches(con, 'D1') if x['seq'] == 3][0]
    assert seq3['status'] == 'merged'  # 重叠且字节一致 -> 合并证据
    assert len({c['chunk_sha256'] for v in seq3['versions'] for c in v['chunks']}) == 2
    res = publish(con, 'D1', 'device_primary', 7, 'op')
    assert res['seq_to'] == 6
    assert any(m['seq'] == 3 for m in res['proofs']['merged'])
    con.close()


def test_conflicting_overlap_keeps_two_immutable_branches(tmp_path):
    db = registered(tmp_path / 'lab.db')
    con = connect_mirror(db)
    seq4 = [b for b in branches(con, 'D1') if b['seq'] == 4][0]
    assert seq4['status'] == 'conflict' and len(seq4['versions']) == 2
    res = publish(con, 'D1', 'device_primary', 7, 'op-1')
    assert res['seq_to'] == 6
    conflict = res['proofs']['conflicts'][0]
    assert conflict['seq'] == 4 and len(conflict['branches']) == 2
    unselected = res['proofs']['unselected']
    assert unselected and unselected[0]['retained_in'] == 'raw_chunk'
    # 未选分支字节原样留存, 可独立复现
    m2_sha = unselected[0]['chunks'][0]['chunk_sha256']
    raw = con.execute('SELECT raw FROM raw_chunk WHERE sha256=?', (m2_sha,)).fetchone()['raw']
    assert bytes(raw) == (FIXTURES / 'mirror-2.jsonl').read_bytes()
    stream = current_stream(con, 'D1')
    assert b'"value":4' in stream['canonical_jsonl']
    assert b'"value":5' not in stream['canonical_jsonl']
    con.close()


def test_contract_selection_ignores_arrival_order(tmp_path):
    db = make_db(tmp_path / 'lab.db')
    con = connect_mirror(db)
    grant_credential(con, 'D1', 'sig-p')
    grant_credential(con, 'D1', 'sig-m')
    p1 = (FIXTURES / 'primary-1.jsonl').read_bytes()
    p2 = (FIXTURES / 'primary-2.jsonl').read_bytes()
    m2 = (FIXTURES / 'mirror-2.jsonl').read_bytes()
    commit_chunk(con, 'M2', 'witness_mirror', 4, 6, 7, 'sig-m', m2)  # 见证镜像先到达
    commit_chunk(con, 'P1', 'primary', 1, 3, 7, 'sig-p', p1)
    commit_chunk(con, 'P2', 'primary', 4, 6, 7, 'sig-p', p2)
    publish(con, 'D1', 'device_primary', 7, 'op')
    stream = current_stream(con, 'D1')
    assert b'"value":4' in stream['canonical_jsonl']  # 契约选主设备, 不是先到先得
    assert b'"value":5' not in stream['canonical_jsonl']
    con.close()


def test_two_contracts_reproduce_full_streams_independently(tmp_path):
    db1 = registered(tmp_path / 'primary.db')
    db2 = registered(tmp_path / 'mirror.db')
    con1, con2 = connect_mirror(db1), connect_mirror(db2)
    r1 = publish(con1, 'D1', 'device_primary', 7, 'op-1')
    r2 = publish(con2, 'D1', 'witness_mirror', 7, 'op-1')
    s1, s2 = current_stream(con1, 'D1'), current_stream(con2, 'D1')
    assert r1['seq_to'] == r2['seq_to'] == 6  # 两份规范流均覆盖1-6
    assert s1['canonical_sha256'] != s2['canonical_sha256']  # 归档摘要不同
    l1, l2 = s1['canonical_jsonl'].splitlines(), s2['canonical_jsonl'].splitlines()
    assert l1[:3] == l2[:3] and l1[4:] == l2[4:]  # 其他事件字节相同
    assert l1[3] != l2[3]  # 序号4传感器字节不同
    assert b'"value":4' in l1[3] and b'"value":5' in l2[3]
    assert [m['seq'] for m in s1['mapping']] == [1, 2, 3, 4, 5, 6]
    assert s1['mapping'][3]['origin'] == 'primary'
    assert s2['mapping'][3]['origin'] == 'witness_mirror'
    # 未选分支在各自归档中留存, 报告指明位置
    for con, other_file in ((con1, 'mirror-2.jsonl'), (con2, 'primary-2.jsonl')):
        out = mirror_report(con, 'D1', tmp_path / f'report-{other_file}.json')
        payload = json.loads(out.read_text(encoding='utf-8'))
        kept = {u['chunk_sha256'] for u in payload['unselected_retained']}
        assert sha256((FIXTURES / other_file).read_bytes()).hexdigest() in kept
        assert all(u['retained_in'] == 'raw_chunk' for u in payload['unselected_retained'])
    con1.close()
    con2.close()


def test_gap_blocks_watermark_until_filled(tmp_path):
    db = make_db(tmp_path / 'lab.db')
    con = connect_mirror(db)
    grant_credential(con, 'D1', 'sig-1')
    commit_chunk(con, 'C1', 'primary', 1, 3, 7, 'sig-1', chunk_bytes([1, 2, 3]))
    commit_chunk(con, 'C3', 'primary', 5, 6, 7, 'sig-1', chunk_bytes([5, 6]))
    res = publish(con, 'D1', 'device_primary', 7, 'op')
    assert res['seq_to'] == 3  # 缺序号4, 5-6不能先发布
    assert [g['seq'] for g in res['proofs']['gaps']] == [4]
    assert [h['seq'] for h in res['proofs']['held']] == [5, 6]
    assert len(current_stream(con, 'D1')['canonical_jsonl'].splitlines()) == 3
    commit_chunk(con, 'C2', 'primary', 4, 6, 7, 'sig-1', chunk_bytes([4, 5, 6], value=4))
    res2 = publish(con, 'D1', 'device_primary', 7, 'op')
    assert res2['seq_to'] == 6 and res2['supersedes'] == res['version_id']
    con.close()


def test_out_of_order_chunks_publish_in_sequence(tmp_path):
    db = make_db(tmp_path / 'lab.db')
    con = connect_mirror(db)
    grant_credential(con, 'D1', 'sig-1')
    commit_chunk(con, 'L2', 'primary', 4, 6, 7, 'sig-1', chunk_bytes([4, 5, 6], value=4))
    with pytest.raises(ValueError, match='nothing_continuous'):
        publish(con, 'D1', 'device_primary', 7, 'op')  # 乱序到达: 缺1-3不能发布4-6
    commit_chunk(con, 'L1', 'primary', 1, 3, 7, 'sig-1', chunk_bytes([1, 2, 3]))
    assert publish(con, 'D1', 'device_primary', 7, 'op')['seq_to'] == 6
    con.close()


def test_revoked_credential_blocks_commit(tmp_path):
    db = make_db(tmp_path / 'lab.db')
    con = connect_mirror(db)
    data = chunk_bytes([4, 5, 6], value=4)
    with pytest.raises(ValueError, match='unknown_signer'):
        open_session(con, 'D1', 'R1', 'primary', 4, 6, 7, 'ghost')
    grant_credential(con, 'D1', 'sig-1')
    sid = open_session(con, 'D1', 'R1', 'primary', 4, 6, 7, 'sig-1')
    stage_segment(con, sid, 0, data)
    revoke_credential(con, 'D1', 'sig-1')  # 凭证在上传后被吊销
    with pytest.raises(ValueError, match='credential_revoked'):
        commit_session(con, sid)
    assert con.execute('SELECT count(*) FROM raw_chunk').fetchone()[0] == 0
    with pytest.raises(ValueError, match='unknown_signer'):
        revoke_credential(con, 'D1', 'ghost')
    con.close()


def test_watermark_replay_and_stale_epoch_rejected(tmp_path):
    db = make_db(tmp_path / 'lab.db')
    con = connect_mirror(db)
    grant_credential(con, 'D1', 'sig-1')
    commit_chunk(con, 'W1', 'primary', 1, 3, 7, 'sig-1', chunk_bytes([1, 2, 3]))
    plan1 = plan_publish(con, 'D1', 'device_primary', 7, 'op')
    commit_plan(con, plan1)
    commit_chunk(con, 'W2', 'primary', 4, 6, 7, 'sig-1', chunk_bytes([4, 5, 6], value=4))
    r2 = publish(con, 'D1', 'device_primary', 7, 'op')
    assert r2['seq_to'] == 6
    with pytest.raises(ValueError, match='watermark_conflict'):
        commit_plan(con, plan1)  # 旧水位重放: 期望的当前版本已过期
    with pytest.raises(ValueError, match='stale_epoch'):
        commit_plan(con, plan_publish(con, 'D1', 'device_primary', 6, 'op'))  # 旧 epoch 重放
    with pytest.raises(ValueError, match='watermark_not_advanced'):
        commit_plan(con, plan_publish(con, 'D1', 'device_primary', 7, 'op', seq_from=5))
    assert current_stream(con, 'D1')['version_id'] == r2['version_id']
    con.close()


def test_dual_publisher_conflict_is_atomic(tmp_path):
    db = registered(tmp_path / 'lab.db')
    con1, con2 = connect_mirror(db), connect_mirror(db)
    plan_a = plan_publish(con1, 'D1', 'device_primary', 7, 'alice')
    plan_b = plan_publish(con2, 'D1', 'witness_mirror', 7, 'bob')
    ra = commit_plan(con1, plan_a)
    with pytest.raises(ValueError, match='watermark_conflict'):
        commit_plan(con2, plan_b)  # 竞争的发布者基于过期水位 -> 拒绝
    stream = current_stream(connect_mirror(db), 'D1')  # 重启后仍一致
    assert stream['version_id'] == ra['version_id']
    assert b'"value":4' in stream['canonical_jsonl']
    assert b'"value":5' not in stream['canonical_jsonl']  # 不混杂 P2/M2
    assert {m['origin'] for m in stream['mapping']} == {'primary'}
    con1.close()
    con2.close()


def test_dual_publisher_threads_single_winner(tmp_path):
    db = registered(tmp_path / 'lab.db')
    results = {}
    barrier = threading.Barrier(2)

    def run(contract, publisher, key):
        con = connect_mirror(db)
        try:
            plan = plan_publish(con, 'D1', contract, 7, publisher)
            barrier.wait(timeout=10)  # 双方都基于同一旧水位计划后再竞相提交
            results[key] = ('ok', commit_plan(con, plan)['version_id'])
        except ValueError as exc:
            results[key] = ('err', str(exc))
        finally:
            con.close()

    threads = [threading.Thread(target=run, args=('device_primary', 'alice', 'a')),
               threading.Thread(target=run, args=('witness_mirror', 'bob', 'b'))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    winners = [v for v in results.values() if v[0] == 'ok']
    losers = [v for v in results.values() if v[0] == 'err']
    assert len(winners) == 1 and len(losers) == 1
    assert 'watermark_conflict' in losers[0][1]
    stream = current_stream(connect_mirror(db), 'D1')
    assert stream['version_id'] == winners[0][1]


def test_uncommitted_publication_rolls_back(tmp_path):
    db = registered(tmp_path / 'lab.db')
    con = connect_mirror(db)
    plan = plan_publish(con, 'D1', 'device_primary', 7, 'op')
    # 模拟"规范流写入完成但水位未更新"即崩溃
    con.execute('BEGIN IMMEDIATE')
    con.execute('INSERT OR IGNORE INTO authority_contract VALUES (?,?,?,?,?)',
                (plan['contract_id'], 'D1', plan['contract'], 7, plan['selection_json']))
    con.execute('INSERT INTO publication VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (plan['version_id'], 'D1', plan['contract_id'], plan['contract'], 7,
                 plan['seq_from'], plan['seq_to'], plan['canonical_sha256'], plan['canonical'],
                 json.dumps(plan['mapping']), json.dumps(plan['proofs']), None, 'op', 1))
    con.close()  # 未提交即断开 = 崩溃
    con = connect_mirror(db)
    assert current_stream(con, 'D1') is None  # 重启不展示超前水位
    assert con.execute('SELECT count(*) FROM publication').fetchone()[0] == 0
    assert con.execute('SELECT count(*) FROM authority_contract').fetchone()[0] == 0
    r = commit_plan(con, plan)
    assert current_stream(con, 'D1')['version_id'] == r['version_id']
    con.close()


def test_superseded_version_stays_fetchable(tmp_path):
    db = registered(tmp_path / 'lab.db')
    con = connect_mirror(db)
    r1 = publish(con, 'D1', 'device_primary', 7, 'op-1')
    got = current_stream(con, 'D1')  # 下游已取走旧版本
    r2 = publish(con, 'D1', 'witness_mirror', 8, 'op-2')  # 契约修订: 新 epoch 新版本
    assert r2['supersedes'] == r1['version_id']  # 替代关系
    old = fetch_version(con, r1['version_id'])
    assert old['canonical_jsonl'] == got['canonical_jsonl']  # 旧字节摘要未被悄悄改写
    assert old['canonical_sha256'] == got['canonical_sha256']
    now = current_stream(con, 'D1')
    assert now['version_id'] == r2['version_id']
    assert b'"value":5' in now['canonical_jsonl']
    con.close()


def test_archive_objects_are_immutable(tmp_path):
    db = registered(tmp_path / 'lab.db')
    con = connect_mirror(db)
    publish(con, 'D1', 'device_primary', 7, 'op')
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("UPDATE publication SET publisher='x'")
    with pytest.raises(sqlite3.IntegrityError):
        con.execute('DELETE FROM raw_chunk')
    with pytest.raises(sqlite3.IntegrityError):
        con.execute('UPDATE chunk_line SET seq=99')
    with pytest.raises(sqlite3.IntegrityError):
        con.execute('DELETE FROM authority_contract')
    con.close()


def test_mirror_pipeline_never_judges_samples(tmp_path):
    db = registered(tmp_path / 'lab.db')
    con = connect_mirror(db)
    publish(con, 'D1', 'device_primary', 7, 'op')
    for table in ('decision', 'run', 'evidence'):
        assert con.execute(f'SELECT count(*) FROM {table}').fetchone()[0] == 0
    assert con.execute('SELECT count(*) FROM event').fetchone()[0] == 3  # 归档不注入事件流
    con.close()


def test_archive_report_json_and_markdown(tmp_path):
    db = registered(tmp_path / 'lab.db')
    con = connect_mirror(db)
    publish(con, 'D1', 'device_primary', 7, 'op-1')
    out = mirror_report(con, 'D1', tmp_path / 'archive.json')
    payload = json.loads(out.read_text(encoding='utf-8'))
    assert payload['watermark']['seq_to'] == 6
    assert payload['watermark']['seal_epoch'] == 7
    assert payload['watermark']['contract'] == 'device_primary'
    version = payload['versions'][0]
    assert [m['seq'] for m in version['mapping']] == [1, 2, 3, 4, 5, 6]
    assert all(m['chunk_sha256'] and m['line_sha256'] and m['line_no']
               for m in version['mapping'])
    conflict = [b for b in payload['branches'] if b['seq'] == 4][0]
    assert conflict['status'] == 'conflict' and len(conflict['versions']) == 2
    assert payload['unselected_retained'][0]['retained_in'] == 'raw_chunk'
    md = out.with_suffix('.md').read_text(encoding='utf-8')
    assert 'device_primary' in md and '未选分支留存' in md and '冲突分支' in md
    con.close()


def test_cli_mirror_flow(tmp_path, monkeypatch, capsys):
    from samplechain.__main__ import main
    db = make_db(tmp_path / 'lab.db')
    monkeypatch.setattr('sys.argv', ['samplechain', 'mirror-register', '--db', str(db),
                                     '--manifest', str(FIXTURES / 'mirror-manifest.json')])
    main()
    monkeypatch.setattr('sys.argv', ['samplechain', 'mirror-publish', '--db', str(db),
                                     '--device', 'D1', '--contract', 'device_primary',
                                     '--seal-epoch', '7', '--publisher', 'op-1'])
    main()
    assert 'pub-' in capsys.readouterr().out
    monkeypatch.setattr('sys.argv', ['samplechain', 'mirror-stream', '--db', str(db),
                                     '--device', 'D1'])
    main()
    out = capsys.readouterr().out
    stream = json.loads(out)
    assert '"value":4' in stream['canonical_jsonl']
    assert '"value":5' not in stream['canonical_jsonl']
