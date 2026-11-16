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

def test_report_contains_raw_line_hash(tmp_path):
    data=json.loads(demo(tmp_path).read_text(encoding='utf-8'))
    assert len(data['source'][0]['sha256'])==64
    assert len(data['event'][0]['sha256'])==64
def test_unknown_sample_is_isolated(tmp_path):
    db=tmp_path/'x.db';init(db,'b','s')
    row=event('collect',1);row['sample_id']='other';load(tmp_path,db,[row])
    assert connect(db).execute("SELECT count(*) FROM issue WHERE code='unknown_sample'").fetchone()[0]==1
def test_recollection_links_replacement(tmp_path):
    db=tmp_path/'x.db';init(db,'b','s');con=connect(db)
    with con:con.execute('INSERT INTO sample(id,batch_id,replacement_of) VALUES (?,?,?)',('s2','b','s'))
    load(tmp_path,db,[event('recollect',1)])
    evaluate(db)
    assert con.execute("SELECT count(*) FROM issue WHERE code='recollect_invalid'").fetchone()[0]==0
def test_markdown_report_exists(tmp_path):
    assert demo(tmp_path).with_suffix('.md').exists()

def test_evidence_and_run_are_persisted(tmp_path):
    data=json.loads(demo(tmp_path).read_text(encoding='utf-8'))
    assert data["run"]
    assert data['evidence']
    assert all(e['run_id'] for e in data['evidence'])

def test_valid_seal_change_after_open(tmp_path):
    db=tmp_path/'x.db';init(db,'b','s')
    load(tmp_path,db,[event('collect',1,seal='A'),event('open',2,seal='A'),event('reseal',3,seal='B'),event('handoff',4,seal='B')]);evaluate(db)
    assert connect(db).execute("SELECT count(*) FROM issue WHERE code='seal_mismatch'").fetchone()[0]==0
def test_in_range_temperature_is_not_violating(tmp_path):
    db=tmp_path/'x.db';init(db,'b','s')
    load(tmp_path,db,[event('temperature',1,value=4)]);evaluate(db)
    assert connect(db).execute("SELECT count(*) FROM issue WHERE code='temperature'").fetchone()[0]==0
def test_decision_links_run_and_evidence(tmp_path):
    data=json.loads(demo(tmp_path).read_text(encoding='utf-8'))
    assert data['decision'][0]['run_id']==data['run'][0]['id']
    assert set(json.loads(data['decision'][0]['event_ids'])) <= {e['object_id'] for e in data['evidence']}

def test_raw_source_is_retained_after_input_changes(tmp_path):
    path=demo(tmp_path)
    before=path.read_bytes()
    db=tmp_path/"samplechain.db"
    con=connect(db)
    row=con.execute("SELECT path,raw FROM source LIMIT 1").fetchone() if "source"!='source_file' else con.execute('SELECT name AS path,raw FROM source_file LIMIT 1').fetchone()
    from pathlib import Path
    preserved=bytes(row['raw'])
    Path(row['path']).write_bytes(b'changed after import')
    assert bytes(con.execute("SELECT raw FROM source LIMIT 1").fetchone()[0])==preserved
    assert before and preserved


# 换采共用 fixture：父样本 p 在子样本 c 收集后、c 入库前到期（01:20），c 到库后链条完整
PARENT_EXPIRY='2026-01-01T01:20:00Z'
PARENT_EVENTS=[
    {'kind':'collect','device':'dp','seq':1,'sample_id':'p','ts':'2026-01-01T00:00:00Z','seal':'SP'},
    {'kind':'handoff','device':'dp','seq':2,'sample_id':'p','ts':'2026-01-01T00:05:00Z','seal':'SP'},
    {'kind':'receive','device':'dp','seq':3,'sample_id':'p','ts':'2026-01-01T00:10:00Z','seal':'SP'},
    {'kind':'fridge_in','device':'dp','seq':4,'sample_id':'p','ts':'2026-01-01T00:15:00Z','seal':'SP'},
    {'kind':'recollect','device':'dp','seq':5,'sample_id':'p','ts':'2026-01-01T01:00:00Z','replacement':'c'},
    {'kind':'fridge_out','device':'dp','seq':6,'sample_id':'p','ts':'2026-01-01T01:05:00Z','seal':'SP'},
    {'kind':'test','device':'dp','seq':7,'sample_id':'p','ts':'2026-01-01T01:10:00Z','seal':'SP'},
]
CHILD_EVENTS=[
    {'kind':'collect','device':'dc','seq':1,'sample_id':'c','ts':'2026-01-01T01:00:00Z','seal':'SC'},
    {'kind':'handoff','device':'dc','seq':2,'sample_id':'c','ts':'2026-01-01T01:10:00Z','seal':'SC'},
    {'kind':'receive','device':'dc','seq':3,'sample_id':'c','ts':'2026-01-01T01:15:00Z','seal':'SC'},
    {'kind':'fridge_in','device':'dc','seq':4,'sample_id':'c','ts':'2026-01-01T01:30:00Z','seal':'SC'},
    {'kind':'fridge_out','device':'dc','seq':5,'sample_id':'c','ts':'2026-01-01T01:40:00Z','seal':'SC'},
    {'kind':'test','device':'dc','seq':6,'sample_id':'c','ts':'2026-01-01T01:50:00Z','seal':'SC'},
]
def setup_replacement(tmp_path,policy,parent_events=PARENT_EVENTS,child_events=CHILD_EVENTS,extra=()):
    db=tmp_path/'x.db'
    init(db,'b','p',sample_expiry=PARENT_EXPIRY,policy=policy)
    init(db,'b','c',replaces='p')
    load(tmp_path,db,[*parent_events,*child_events,*extra])
    return db
def decision_of(db,sample):
    return connect(db).execute('SELECT * FROM decision WHERE sample_id=?',(sample,)).fetchone()
def takeover_of(db,parent,child):
    return connect(db).execute('SELECT * FROM takeover WHERE parent=? AND child=?',(parent,child)).fetchone()

def test_collect_policy_leaves_no_testable_sample_in_waiting_window(tmp_path):
    db=setup_replacement(tmp_path,'collect')
    res=evaluate(db)
    assert res['p']=='不可检测' and res['c']=='可检测'
    dp,dc=decision_of(db,'p'),decision_of(db,'c')
    assert 'replaced' in json.loads(dp['reasons'])
    assert dp['window_end']=='2026-01-01T01:00:00Z'
    assert dc['window_start']=='2026-01-01T01:00:00Z'
    t=takeover_of(db,'p','c')
    assert t['status']=='effective' and t['effective']=='2026-01-01T01:00:00Z' and t['policy']=='collect'
    assert dp['active']==0 and dc['active']==1
    con=connect(db)
    child_fridge_in=con.execute("SELECT ts FROM event WHERE sample_id='c' AND kind='fridge_in'").fetchone()['ts']
    child_test=con.execute("SELECT ts FROM event WHERE sample_id='c' AND kind='test'").fetchone()['ts']
    assert dp['window_end']<child_fridge_in<child_test

def test_acceptance_policy_tests_parent_then_switches_to_child(tmp_path):
    db=setup_replacement(tmp_path,'acceptance')
    res=evaluate(db)
    assert res['p']=='可检测' and res['c']=='可检测'
    dp,dc=decision_of(db,'p'),decision_of(db,'c')
    assert json.loads(dp['reasons'])==[]
    assert dp['window_start']=='2026-01-01T00:00:00Z'
    assert dp['window_end']==PARENT_EXPIRY
    assert dc['window_start']=='2026-01-01T01:30:00Z'
    t=takeover_of(db,'p','c')
    assert t['status']=='effective' and t['effective']=='2026-01-01T01:30:00Z' and t['policy']=='acceptance'
    assert dp['active']==0 and dc['active']==1
    pe,ce=json.loads(dp['event_ids']),json.loads(dc['event_ids'])
    assert len(pe)==7 and len(ce)==6 and not set(pe)&set(ce)
    assert connect(db).execute("SELECT count(*) FROM issue WHERE code='recollect_mismatch'").fetchone()[0]==0

def test_acceptance_takeover_rejected_when_child_transport_overtemperature(tmp_path):
    excursion={'kind':'temperature','device':'dc','seq':7,'sample_id':'c','ts':'2026-01-01T01:12:00Z','value':15}
    db=setup_replacement(tmp_path,'acceptance',extra=[excursion])
    res=evaluate(db)
    assert res['c']=='不可检测' and 'temperature' in json.loads(decision_of(db,'c')['reasons'])
    assert res['p']=='可检测'
    t=takeover_of(db,'p','c')
    assert t['status']=='rejected' and t['effective'] is None
    assert decision_of(db,'p')['active']==1 and decision_of(db,'c')['active']==0

def test_parent_expires_before_child_acceptance(tmp_path):
    parent=[e for e in PARENT_EVENTS if e['kind'] not in ('fridge_out','test')]
    parent.append({'kind':'temperature','device':'dp','seq':6,'sample_id':'p','ts':'2026-01-01T01:25:00Z','value':4})
    db=setup_replacement(tmp_path,'acceptance',parent_events=parent)
    res=evaluate(db)
    dp=decision_of(db,'p')
    assert res['p']=='不可检测' and 'expired' in json.loads(dp['reasons'])
    assert dp['window_end']==PARENT_EXPIRY
    assert res['c']=='可检测' and decision_of(db,'c')['active']==1

def test_replacement_cycle_is_blocked(tmp_path):
    db=tmp_path/'x.db';init(db,'b','cy1');init(db,'b','cy2')
    con=connect(db)
    with con:
        con.execute("UPDATE sample SET replacement_of='cy2' WHERE id='cy1'")
        con.execute("UPDATE sample SET replacement_of='cy1' WHERE id='cy2'")
    load(tmp_path,db,[
        {'kind':'collect','device':'d1','seq':1,'sample_id':'cy1','ts':'2026-01-01T00:00:00Z'},
        {'kind':'collect','device':'d2','seq':1,'sample_id':'cy2','ts':'2026-01-01T00:00:00Z'}])
    res=evaluate(db)
    assert res['cy1']=='不可检测' and res['cy2']=='不可检测'
    con=connect(db)
    assert con.execute("SELECT count(*) FROM issue WHERE code='replacement_cycle'").fetchone()[0]==2
    assert con.execute('SELECT count(*) FROM decision WHERE active=1').fetchone()[0]==0
    assert con.execute("SELECT count(*) FROM takeover WHERE status='effective'").fetchone()[0]==0

def test_two_replacements_of_one_sample_are_blocked(tmp_path):
    db=tmp_path/'x.db';init(db,'b','p');init(db,'b','c1',replaces='p');init(db,'b','c2',replaces='p')
    load(tmp_path,db,[{'kind':'collect','device':f'd{i}','seq':1,'sample_id':s,'ts':'2026-01-01T00:00:00Z'} for i,s in enumerate(('p','c1','c2'),1)])
    res=evaluate(db)
    assert res['p']=='不可检测' and res['c1']=='不可检测' and res['c2']=='不可检测'
    con=connect(db)
    assert con.execute("SELECT count(*) FROM issue WHERE code='replacement_conflict'").fetchone()[0]==3
    assert con.execute("SELECT count(*) FROM takeover WHERE status='blocked'").fetchone()[0]==2

def test_cross_batch_replacement_is_blocked(tmp_path):
    db=tmp_path/'x.db';init(db,'b1','p');init(db,'b2','c',replaces='p')
    load(tmp_path,db,[
        {'kind':'collect','device':'d1','seq':1,'sample_id':'p','ts':'2026-01-01T00:00:00Z'},
        {'kind':'collect','device':'d2','seq':1,'sample_id':'c','ts':'2026-01-01T01:00:00Z'}])
    res=evaluate(db)
    assert res['p']=='不可检测' and res['c']=='不可检测'
    con=connect(db)
    assert con.execute("SELECT count(*) FROM issue WHERE code='replacement_cross_batch'").fetchone()[0]==2
    assert takeover_of(db,'p','c')['status']=='blocked'

def test_multi_generation_chain_takes_over_in_order(tmp_path):
    db=tmp_path/'x.db'
    init(db,'b','p',sample_expiry=PARENT_EXPIRY,policy='acceptance')
    init(db,'b','c',replaces='p',sample_expiry='2026-01-01T02:30:00Z')
    init(db,'b','g',replaces='c')
    grand=[
        {'kind':'collect','device':'dg','seq':1,'sample_id':'g','ts':'2026-01-01T01:35:00Z','seal':'SG'},
        {'kind':'handoff','device':'dg','seq':2,'sample_id':'g','ts':'2026-01-01T01:40:00Z','seal':'SG'},
        {'kind':'receive','device':'dg','seq':3,'sample_id':'g','ts':'2026-01-01T01:45:00Z','seal':'SG'},
        {'kind':'fridge_in','device':'dg','seq':4,'sample_id':'g','ts':'2026-01-01T01:55:00Z','seal':'SG'},
        {'kind':'fridge_out','device':'dg','seq':5,'sample_id':'g','ts':'2026-01-01T02:00:00Z','seal':'SG'},
        {'kind':'test','device':'dg','seq':6,'sample_id':'g','ts':'2026-01-01T02:05:00Z','seal':'SG'},
    ]
    child=[
        {'kind':'collect','device':'dc','seq':1,'sample_id':'c','ts':'2026-01-01T01:00:00Z','seal':'SC'},
        {'kind':'handoff','device':'dc','seq':2,'sample_id':'c','ts':'2026-01-01T01:10:00Z','seal':'SC'},
        {'kind':'receive','device':'dc','seq':3,'sample_id':'c','ts':'2026-01-01T01:15:00Z','seal':'SC'},
        {'kind':'fridge_in','device':'dc','seq':4,'sample_id':'c','ts':'2026-01-01T01:30:00Z','seal':'SC'},
        {'kind':'recollect','device':'dc','seq':5,'sample_id':'c','ts':'2026-01-01T01:35:00Z','replacement':'g'},
        {'kind':'fridge_out','device':'dc','seq':6,'sample_id':'c','ts':'2026-01-01T01:40:00Z','seal':'SC'},
        {'kind':'test','device':'dc','seq':7,'sample_id':'c','ts':'2026-01-01T01:50:00Z','seal':'SC'},
    ]
    load(tmp_path,db,[*PARENT_EVENTS,*child,*grand])
    res=evaluate(db)
    assert res['p']=='可检测' and res['c']=='可检测' and res['g']=='可检测'
    assert takeover_of(db,'p','c')['effective']=='2026-01-01T01:30:00Z'
    assert takeover_of(db,'c','g')['effective']=='2026-01-01T01:55:00Z'
    dc,dg=decision_of(db,'c'),decision_of(db,'g')
    assert dc['window_start']=='2026-01-01T01:30:00Z' and dc['window_end']=='2026-01-01T01:55:00Z'
    assert dg['window_start']=='2026-01-01T01:55:00Z'
    con=connect(db)
    assert [r['sample_id'] for r in con.execute('SELECT sample_id FROM decision WHERE active=1')]==['g']

def test_replacement_history_is_not_overwritten(tmp_path):
    db=tmp_path/'x.db'
    init(db,'b','p',sample_expiry=PARENT_EXPIRY,policy='acceptance')
    init(db,'b','c',replaces='p')
    p1=tmp_path/'p.jsonl';p1.write_text('\n'.join(json.dumps(r) for r in PARENT_EVENTS),encoding='utf-8');ingest(p1,db)
    assert evaluate(db)['p']=='可检测'
    before=decision_of(db,'p')
    c1=tmp_path/'c.jsonl';c1.write_text('\n'.join(json.dumps(r) for r in CHILD_EVENTS),encoding='utf-8');ingest(c1,db)
    res=evaluate(db)
    after=decision_of(db,'p')
    for key in ('result','reasons','window_start','window_end','event_ids'):
        assert before[key]==after[key]
    assert after['result']=='可检测' and json.loads(after['reasons'])==[]
    assert res['c']=='可检测' and decision_of(db,'c')['active']==1
    first=[dict(r) for r in connect(db).execute('SELECT * FROM decision ORDER BY sample_id')]
    evaluate(db)
    assert first==[dict(r) for r in connect(db).execute('SELECT * FROM decision ORDER BY sample_id')]

def test_declared_replacement_must_match_registration(tmp_path):
    db=tmp_path/'x.db';init(db,'b','p',policy='acceptance');init(db,'b','c')
    load(tmp_path,db,[
        {'kind':'collect','device':'dp','seq':1,'sample_id':'p','ts':'2026-01-01T00:00:00Z'},
        {'kind':'recollect','device':'dp','seq':2,'sample_id':'p','ts':'2026-01-01T01:00:00Z','replacement':'c'}])
    res=evaluate(db)
    assert res['p']=='不可检测'
    assert connect(db).execute("SELECT count(*) FROM issue WHERE code='recollect_mismatch'").fetchone()[0]==1

def test_report_lists_generations_windows_and_responsible(tmp_path):
    db=setup_replacement(tmp_path,'acceptance')
    evaluate(db)
    from samplechain.__main__ import report
    data=json.loads(report(db,tmp_path/'report.json').read_text(encoding='utf-8'))
    chain=next(c for c in data['lineage'] if c['root']=='p')
    assert chain['responsible']=='c'
    gens={g['sample_id']:g for g in chain['generations']}
    assert gens['p']['window_end']==PARENT_EXPIRY and gens['p']['responsible'] is False
    assert gens['c']['window_start']=='2026-01-01T01:30:00Z' and gens['c']['responsible'] is True
    assert len(gens['p']['event_ids'])==7 and len(gens['c']['event_ids'])==6
    assert not set(gens['p']['event_ids'])&set(gens['c']['event_ids'])
    assert data['takeover'][0]['effective']=='2026-01-01T01:30:00Z'
