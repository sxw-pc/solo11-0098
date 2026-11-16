import argparse
import json
import tempfile
from pathlib import Path
from .store import connect
from .ingest import ingest
from .rules import evaluate

def init(db_path,batch,sample,expiry='2027-01-01T00:00:00Z'):
    con=connect(db_path)
    with con:
        con.execute('INSERT OR IGNORE INTO batch VALUES (?,?,?,?,?)',(batch,expiry,30,2,8))
        con.execute('INSERT OR IGNORE INTO sample(id,batch_id) VALUES (?,?)',(sample,batch))
    con.close()
def report(db_path,out):
    con=connect(db_path); data={name:[dict(r) for r in con.execute('SELECT * FROM '+name)] for name in ('batch','sample','source','event','issue','decision')}
    out=Path(out);out.parent.mkdir(parents=True,exist_ok=True);out.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
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
    a=sub.add_parser('ingest');a.add_argument('path');a.add_argument('--db',required=True)
    e=sub.add_parser('evaluate');e.add_argument('--db',required=True)
    r=sub.add_parser('report');r.add_argument('--db',required=True);r.add_argument('--out',required=True)
    args=p.parse_args()
    if args.cmd=='demo':print(demo(args.workdir or tempfile.mkdtemp(prefix='samplechain-')))
    elif args.cmd=='init':init(args.db,args.batch,args.sample)
    elif args.cmd=='ingest':print(ingest(args.path,args.db))
    elif args.cmd=='evaluate':print(evaluate(args.db))
    else:print(report(args.db,args.out))
if __name__=='__main__':main()
