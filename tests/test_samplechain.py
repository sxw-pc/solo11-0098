import json
from samplechain.__main__ import demo,init
from samplechain.ingest import ingest
from samplechain.rules import evaluate
from samplechain.store import connect

def test_demo(tmp_path):
    data=json.loads(demo(tmp_path).read_text(encoding='utf-8'))
    assert data['decision'][0]['result']=='可检测'
    assert len(data['event'])==6
def test_duplicate_conflict(tmp_path):
    db=tmp_path/'db.db';init(db,'b','s')
    item={'kind':'collect','device':'d','seq':1,'sample_id':'s','ts':'2026-01-01T00:00:00Z'}
    a=tmp_path/'a.jsonl';a.write_text(json.dumps(item),encoding='utf-8')
    assert ingest(a,db)['imported']==1
    assert ingest(a,db)['duplicate_file']
    item['kind']='handoff';b=tmp_path/'b.jsonl';b.write_text(json.dumps(item),encoding='utf-8');ingest(b,db)
    assert connect(db).execute("SELECT count(*) FROM issue WHERE code='identity_conflict'").fetchone()[0]==1
def test_missing_chain(tmp_path):
    db=tmp_path/'db.db';init(db,'b','s')
    assert evaluate(db)['s']=='不可检测'
def test_temperature(tmp_path):
    db=tmp_path/'db.db';init(db,'b','s')
    p=tmp_path/'a.jsonl';p.write_text(json.dumps({'kind':'temperature','device':'d','seq':1,'sample_id':'s','ts':'2026-01-01T00:00:00Z','value':15}),encoding='utf-8')
    ingest(p,db);evaluate(db)
    assert connect(db).execute("SELECT count(*) FROM issue WHERE code='temperature'").fetchone()[0]==1


def event(kind,seq,ts=None,**kw):
    return {'kind':kind,'device':'d','seq':seq,'sample_id':'s','ts':ts or f'2026-01-01T00:{seq*5:02d}:00Z',**kw}
def load(tmp_path,db,rows):
    p=tmp_path/'stream.jsonl';p.write_text('\n'.join(json.dumps(r) for r in rows),encoding='utf-8');ingest(p,db)
def test_gap_marks_uncertain(tmp_path):
    db=tmp_path/'x.db';init(db,'b','s')
    load(tmp_path,db,[event('collect',1),event('handoff',3)])
    evaluate(db)
    assert connect(db).execute("SELECT count(*) FROM issue WHERE code='sequence_gap'").fetchone()[0]
def test_seal_mismatch(tmp_path):
    db=tmp_path/'x.db';init(db,'b','s')
    load(tmp_path,db,[event('collect',1,seal='A'),event('handoff',2,seal='B')])
    evaluate(db)
    assert connect(db).execute("SELECT count(*) FROM issue WHERE code='seal_mismatch'").fetchone()[0]
def test_outside_too_long(tmp_path):
    db=tmp_path/'x.db';init(db,'b','s')
    rows=[event(k,i) for i,k in enumerate(['collect','handoff','receive','fridge_in','fridge_out'],1)]
    rows.append(event('test',6,'2026-01-01T01:00:00Z'))
    load(tmp_path,db,rows);assert evaluate(db)['s']=='不可检测'
    assert connect(db).execute("SELECT count(*) FROM issue WHERE code='out_time'").fetchone()[0]
def test_void_and_recollect(tmp_path):
    db=tmp_path/'x.db';init(db,'b','s')
    load(tmp_path,db,[event('void',1),event('recollect',2)])
    evaluate(db)
    assert connect(db).execute("SELECT count(*) FROM issue WHERE code='recollect_invalid'").fetchone()[0]
def test_expired(tmp_path):
    db=tmp_path/'x.db';init(db,'b','s',expiry='2025-01-01T00:00:00Z')
    load(tmp_path,db,[event('collect',1)]);evaluate(db)
    assert connect(db).execute("SELECT count(*) FROM issue WHERE code='expired'").fetchone()[0]


def test_overlapping_outside_events_are_not_double_counted(tmp_path):
    db=tmp_path/'x.db';init(db,'b','s')
    rows=[event(k,i) for i,k in enumerate(['collect','handoff','receive','fridge_in','fridge_out'],1)]
    rows.append(event('fridge_out',6,'2026-01-01T00:26:00Z'))
    rows.append(event('test',7,'2026-01-01T00:30:00Z'))
    load(tmp_path,db,rows)
    assert evaluate(db)['s']=='可检测'
def test_conflicting_batch_and_sample(tmp_path):
    import pytest
    db=tmp_path/'x.db';init(db,'b','s')
    with pytest.raises(ValueError,match='batch_contract_conflict'):init(db,'b','s',expiry='2028-01-01T00:00:00Z')
    with pytest.raises(ValueError,match='sample_batch_conflict'):init(db,'other','s')

def test_demo_repeat_is_deterministic(tmp_path):
    first=demo(tmp_path).read_bytes()
    second=demo(tmp_path).read_bytes()
    assert first==second
