import pytest
from pathlib import Path
from samplechain.__main__ import init
from samplechain.ingest import ingest
from samplechain.mirror import branches, connect_mirror, register
from samplechain.store import connect

FIXTURES = Path(__file__).parents[1] / 'fixtures'


def test_mirror_case_freezes_conflicting_chunks_not_sample_decision(tmp_path):
    db = tmp_path / 'lab.db'
    init(db, 'batch-1', 'S1')
    result = ingest(FIXTURES / 'primary-1.jsonl', db)
    assert result['imported'] == 3
    con = connect(db)
    # 源设备真实存在: D1 的原始事件已真正导入事件表
    assert con.execute("SELECT count(*) FROM event WHERE device='D1'").fetchone()[0] == 3
    con.close()
    assert register(db, FIXTURES / 'mirror-manifest.json') == 'scanner-D1-2026-05-06'
    con = connect(db)
    row = con.execute('SELECT * FROM mirror_case').fetchone()
    assert row['device'] == 'D1' and 'raw_base64' in row['frozen_chunks_json']
    # 无样本判定: 镜像归档不运行 rules.evaluate, 不产生判定/运行/证据
    assert con.execute('SELECT count(*) FROM decision').fetchone()[0] == 0
    assert con.execute('SELECT count(*) FROM run').fetchone()[0] == 0
    assert con.execute('SELECT count(*) FROM evidence').fetchone()[0] == 0
    con.close()
    con = connect_mirror(db)
    # 三块原始分块按内容哈希归档, 序号4存在两条不可改写冲突分支
    assert con.execute('SELECT count(*) FROM raw_chunk').fetchone()[0] == 3
    seq4 = [b for b in branches(con, 'D1') if b['seq'] == 4][0]
    assert seq4['status'] == 'conflict' and len(seq4['versions']) == 2
    origins = {c['origin'] for v in seq4['versions'] for c in v['chunks']}
    assert origins == {'primary', 'witness_mirror'}
    con.close()


def test_register_rejects_unknown_scanner(tmp_path):
    # 源设备必须真实存在: 未导入过原始事件的设备不能登记镜像
    db = tmp_path / 'lab.db'
    init(db, 'batch-1', 'S1')
    with pytest.raises(ValueError, match='unknown_scanner'):
        register(db, FIXTURES / 'mirror-manifest.json')
