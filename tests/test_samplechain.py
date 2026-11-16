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
