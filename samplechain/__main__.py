import argparse
import base64
import json
import tempfile
from pathlib import Path
from .store import connect
from .ingest import ingest, POLICIES
from .rules import evaluate

def init(db_path,batch,sample,expiry='2027-01-01T00:00:00Z',policy=None):
    con=connect(db_path)
    with con:
        old=con.execute('SELECT * FROM batch WHERE id=?',(batch,)).fetchone()
        if old and old['expiry']!=expiry:raise ValueError('batch_contract_conflict')
        con.execute('INSERT OR IGNORE INTO batch VALUES (?,?,?,?,?)',(batch,expiry,30,2,8))
        old_sample=con.execute('SELECT batch_id FROM sample WHERE id=?',(sample,)).fetchone()
        if old_sample and old_sample['batch_id']!=batch:raise ValueError('sample_batch_conflict')
        con.execute('INSERT OR IGNORE INTO sample(id,batch_id) VALUES (?,?)',(sample,batch))
        if policy:
            if policy not in POLICIES:raise ValueError('unknown_policy')
            con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES ('replacement_policy',?)",(policy,))
    con.close()
def report(db_path,out):
    con=connect(db_path); data={name:[dict(r) for r in con.execute('SELECT * FROM '+name)] for name in ('batch','sample','source','event','run','evidence','issue','decision','custody','meta')}
    for row in data['issue']:row.pop('id',None)
    out=Path(out);out.parent.mkdir(parents=True,exist_ok=True);out.write_text(json.dumps(data,ensure_ascii=False,indent=2,default=lambda v:{'base64':base64.b64encode(v).decode('ascii')}),encoding='utf-8')
    lines=['# 样本可检测性','']
    lines+=[f"- {d['sample_id']}: {d['result']}（规则 {d['rules_version']}）" for d in data['decision']]
    if data['custody']:
        lines+=['','## 换采接管','',f"放行策略: {data['custody'][0]['basis']}"]
        by_id={c['sample_id']:c for c in data['custody']}
        child_ids={c['replaced_by'] for c in data['custody'] if c['replaced_by']}
        events=sorted(data['event'],key=lambda e:(e['ts'],e['id']))
        for root in [c for c in data['custody'] if c['sample_id'] not in child_ids]:
            chain=[];cur=root['sample_id'];seen=set()
            while cur and cur in by_id and cur not in seen:
                seen.add(cur);chain.append(by_id[cur]);cur=by_id[cur]['replaced_by']
            lines+=['','- 替代链: '+' → '.join(c['sample_id'] for c in chain)]
            for c in chain:
                mark='（当前责任样本）' if c['current'] else ''
                lines.append(f"  - {c['sample_id']} 授权窗口: {c['window_start'] or '-'} ~ {c['window_end'] or '-'}{mark}")
                chain_events=[e for e in events if e['sample_id']==c['sample_id']]
                lines.append('    事件链: '+(' → '.join(f"{e['kind']}@{e['ts']}" for e in chain_events) or '-'))
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
    i=sub.add_parser('init');i.add_argument('--db',required=True);i.add_argument('--batch',required=True);i.add_argument('--sample',required=True);i.add_argument('--policy',choices=POLICIES)
    a=sub.add_parser('ingest');a.add_argument('path');a.add_argument('--db',required=True)
    e=sub.add_parser('evaluate');e.add_argument('--db',required=True);e.add_argument('--policy',choices=POLICIES)
    r=sub.add_parser('report');r.add_argument('--db',required=True);r.add_argument('--out',required=True)
    x=sub.add_parser('inspect');x.add_argument('--db',required=True);x.add_argument('--sample',required=True)
    n=sub.add_parser('register-sample');n.add_argument('--db',required=True);n.add_argument('--batch',required=True);n.add_argument('--sample',required=True);n.add_argument('--replaces');n.add_argument('--expiry')
    sp=sub.add_parser('set-policy');sp.add_argument('--db',required=True);sp.add_argument('--policy',choices=POLICIES,required=True)
    args=p.parse_args()
    if args.cmd=='demo':print(demo(args.workdir or tempfile.mkdtemp(prefix='samplechain-')))
    elif args.cmd=='init':init(args.db,args.batch,args.sample,policy=args.policy)
    elif args.cmd=='ingest':print(ingest(args.path,args.db))
    elif args.cmd=='evaluate':print(evaluate(args.db,policy=args.policy))
    elif args.cmd=='report':print(report(args.db,args.out))
    elif args.cmd=='set-policy':
        con=connect(args.db)
        with con:con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES ('replacement_policy',?)",(args.policy,))
        con.close()
    elif args.cmd=='register-sample':
        con=connect(args.db)
        with con:con.execute('INSERT INTO sample(id,batch_id,replacement_of,expiry) VALUES (?,?,?,?)',(args.sample,args.batch,args.replaces,args.expiry))
        con.close()
    else:
        con=connect(args.db);print(json.dumps({'decision':[dict(r) for r in con.execute('SELECT * FROM decision WHERE sample_id=?',(args.sample,))],'events':[dict(r) for r in con.execute('SELECT * FROM event WHERE sample_id=? ORDER BY ts,id',(args.sample,))]},ensure_ascii=False));con.close()
if __name__=='__main__':main()
