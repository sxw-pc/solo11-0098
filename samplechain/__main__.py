import argparse
import base64
import json
import tempfile
from collections import defaultdict
from pathlib import Path
from .store import connect
from .ingest import ingest, time
from .rules import evaluate, POLICIES

def init(db_path,batch,sample,expiry='2027-01-01T00:00:00Z',sample_expiry=None,replaces=None,policy=None):
    con=connect(db_path)
    with con:
        old=con.execute('SELECT * FROM batch WHERE id=?',(batch,)).fetchone()
        if old and old['expiry']!=expiry:raise ValueError('batch_contract_conflict')
        con.execute('INSERT OR IGNORE INTO batch VALUES (?,?,?,?,?)',(batch,expiry,30,2,8))
        old_sample=con.execute('SELECT batch_id FROM sample WHERE id=?',(sample,)).fetchone()
        if old_sample and old_sample['batch_id']!=batch:raise ValueError('sample_batch_conflict')
        if sample_expiry:time(sample_expiry)
        con.execute('INSERT OR IGNORE INTO sample(id,batch_id,replacement_of,expiry) VALUES (?,?,?,?)',(sample,batch,replaces,sample_expiry))
        if policy is not None:
            if policy not in POLICIES:raise ValueError('unknown_takeover_policy')
            con.execute("INSERT OR REPLACE INTO policy(id,value) VALUES ('takeover',?)",(policy,))
    con.close()
def lineage(samples,decisions):
    dec={d['sample_id']:d for d in decisions}
    children=defaultdict(list)
    for s in samples:
        if s['replacement_of']:children[s['replacement_of']].append(s['id'])
    def info(sid):
        d=dec.get(sid,{})
        return {'sample_id':sid,'result':d.get('result'),'reasons':json.loads(d['reasons']) if d.get('reasons') else [],
            'window_start':d.get('window_start'),'window_end':d.get('window_end'),
            'responsible':bool(d.get('active')),'event_ids':json.loads(d['event_ids']) if d.get('event_ids') else []}
    chains=[];seen=set()
    for s in samples:
        if s['replacement_of']:continue
        order=[];queue=[s['id']]
        while queue:
            cur=queue.pop(0)
            if cur in seen:continue
            seen.add(cur);order.append(cur);queue.extend(sorted(children.get(cur,[])))
        chains.append({'root':s['id'],'responsible':next((x for x in order if dec.get(x,{}).get('active')),None),
            'generations':[info(x) for x in order]})
    leftovers=sorted(s['id'] for s in samples if s['id'] not in seen)
    if leftovers:chains.append({'root':None,'responsible':None,'generations':[info(x) for x in leftovers]})
    return chains
def report(db_path,out):
    con=connect(db_path); data={name:[dict(r) for r in con.execute('SELECT * FROM '+name)] for name in ('batch','sample','source','event','run','evidence','issue','decision','takeover','policy')}
    for row in data['issue']:row.pop('id',None)
    data['lineage']=lineage(data['sample'],data['decision'])
    out=Path(out);out.parent.mkdir(parents=True,exist_ok=True);out.write_text(json.dumps(data,ensure_ascii=False,indent=2,default=lambda v:{'base64':base64.b64encode(v).decode('ascii')}),encoding='utf-8')
    lines=['# 样本可检测性','']
    for d in data['decision']:
        mark='，当前责任样本' if d['active'] else ''
        lines.append(f"- {d['sample_id']}: {d['result']}（规则 {d['rules_version']}，授权窗口 {d['window_start'] or '—'} → {d['window_end'] or '—'}{mark}）")
    if data['takeover']:
        lines+=['','## 换采接管']
        for t in data['takeover']:
            lines.append(f"- {t['parent']} → {t['child']}: {t['status']}（策略 {t['policy']}，生效 {t['effective'] or '—'}）")
    out.with_suffix('.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    con.close();return out
def demo(folder):
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True);db=folder/'samplechain.db';init(db,'batch-1','sample-1')
    kinds=['collect','handoff','receive','fridge_in','fridge_out','test']
    lines=[{'kind':kind,'device':'reader-1','seq':i,'sample_id':'sample-1','ts':f'2026-01-01T00:{i*5:02d}:00Z','actor':'staff-1'} for i,kind in enumerate(kinds)]
    src=folder/'scan.jsonl';src.write_text('\n'.join(map(json.dumps,lines)),encoding='utf-8')
    ingest(src,db);evaluate(db);return report(db,folder/'report.json')
def main():
    p=argparse.ArgumentParser(description='离线样本保管链核验')
    sub=p.add_subparsers(dest='cmd',required=True)
    d=sub.add_parser('demo');d.add_argument('--workdir')
    i=sub.add_parser('init');i.add_argument('--db',required=True);i.add_argument('--batch',required=True);i.add_argument('--sample',required=True)
    i.add_argument('--expiry',default='2027-01-01T00:00:00Z');i.add_argument('--sample-expiry');i.add_argument('--replaces');i.add_argument('--policy',choices=list(POLICIES))
    a=sub.add_parser('ingest');a.add_argument('path');a.add_argument('--db',required=True)
    e=sub.add_parser('evaluate');e.add_argument('--db',required=True);e.add_argument('--policy',choices=list(POLICIES))
    r=sub.add_parser('report');r.add_argument('--db',required=True);r.add_argument('--out',required=True)
    x=sub.add_parser('inspect');x.add_argument('--db',required=True);x.add_argument('--sample',required=True)
    n=sub.add_parser('register-sample');n.add_argument('--db',required=True);n.add_argument('--batch',required=True);n.add_argument('--sample',required=True);n.add_argument('--replaces')
    args=p.parse_args()
    if args.cmd=='demo':print(demo(args.workdir or tempfile.mkdtemp(prefix='samplechain-')))
    elif args.cmd=='init':init(args.db,args.batch,args.sample,expiry=args.expiry,sample_expiry=args.sample_expiry,replaces=args.replaces,policy=args.policy)
    elif args.cmd=='ingest':print(ingest(args.path,args.db))
    elif args.cmd=='evaluate':print(evaluate(args.db,policy=args.policy))
    elif args.cmd=='report':print(report(args.db,args.out))
    elif args.cmd=='register-sample':
        con=connect(args.db)
        with con:con.execute('INSERT INTO sample(id,batch_id,replacement_of) VALUES (?,?,?)',(args.sample,args.batch,args.replaces))
        con.close()
    else:
        con=connect(args.db);print(json.dumps({'decision':[dict(r) for r in con.execute('SELECT * FROM decision WHERE sample_id=?',(args.sample,))],'events':[dict(r) for r in con.execute('SELECT * FROM event WHERE sample_id=? ORDER BY ts,id',(args.sample,))]},ensure_ascii=False));con.close()
if __name__=='__main__':main()
