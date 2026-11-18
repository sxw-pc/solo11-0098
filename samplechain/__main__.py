import argparse
import base64
import json
import tempfile
from pathlib import Path
from .store import connect
from .ingest import ingest
from .rules import evaluate
from . import water_loop

def init(db_path,batch,sample,expiry='2027-01-01T00:00:00Z'):
    con=connect(db_path)
    with con:
        old=con.execute('SELECT * FROM batch WHERE id=?',(batch,)).fetchone()
        if old and old['expiry']!=expiry:raise ValueError('batch_contract_conflict')
        con.execute('INSERT OR IGNORE INTO batch VALUES (?,?,?,?,?)',(batch,expiry,30,2,8))
        old_sample=con.execute('SELECT batch_id FROM sample WHERE id=?',(sample,)).fetchone()
        if old_sample and old_sample['batch_id']!=batch:raise ValueError('sample_batch_conflict')
        con.execute('INSERT OR IGNORE INTO sample(id,batch_id) VALUES (?,?)',(sample,batch))
    con.close()
def report(db_path,out):
    con=connect(db_path); data={name:[dict(r) for r in con.execute('SELECT * FROM '+name)] for name in ('batch','sample','source','event','run','evidence','issue','decision')}
    for row in data['issue']:row.pop('id',None)
    out=Path(out);out.parent.mkdir(parents=True,exist_ok=True);out.write_text(json.dumps(data,ensure_ascii=False,indent=2,default=lambda v:{'base64':base64.b64encode(v).decode('ascii')}),encoding='utf-8')
    out.with_suffix('.md').write_text('# 样本可检测性\n\n'+'\n'.join(f"- {d['sample_id']}: {d['result']}（规则 {d['rules_version']}）" for d in data['decision'])+'\n',encoding='utf-8')
    con.close();return out
def demo(folder):
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True);db=folder/'samplechain.db';init(db,'batch-1','sample-1')
    kinds=['collect','handoff','receive','fridge_in','fridge_out','test']
    lines=[{'kind':kind,'device':'reader-1','seq':i,'sample_id':'sample-1','ts':f'2026-01-01T00:{i*5:02d}:00Z','actor':'staff-1'} for i,kind in enumerate(kinds)]
    src=folder/'scan.jsonl';src.write_text('\n'.join(map(json.dumps,lines)),encoding='utf-8')
    ingest(src,db);evaluate(db);return report(db,folder/'report.json')
def water_demo(folder, route='direct_first'):
    """用合成本地 fixture 跑完整纯水闭环：冻结→快照→采录→计划→PLC 执行→报告。"""
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
    db=folder/'water.db'
    fix=Path(__file__).resolve().parents[1]/'fixtures'
    init(db,'batch-w','sample-1');init(db,'batch-w','sample-2')
    ingest(fix/'water-events.jsonl',db)
    water_loop.register(db,'water-1',fix/'water-contract.json',fix/'water-sensors.csv')
    water_loop.create_snapshot(db,'water-1')
    water_loop.load_frozen_telemetry(db,'water-1')
    water_loop.freeze_plan(db,'water-1',route)
    water_loop.execute_plan(db,'water-1',water_loop.SimPLC())
    return water_loop.water_report(db,'water-1',folder/'water-report.json')
def water_snapshot(args):
    print(water_loop.create_snapshot(args.db,args.case))
def water_telemetry(args):
    print(json.dumps(water_loop.load_frozen_telemetry(args.db,args.case),ensure_ascii=False))
def water_plan(args):
    print(json.dumps(water_loop.freeze_plan(args.db,args.case,args.route),ensure_ascii=False))
def water_run(args):
    faults=set(args.fault.split(',') if args.fault else ())
    plc=water_loop.SimPLC(faults=faults)
    if args.membrane_fail_after_ml is not None:
        plc.membrane_fail_after_ml=args.membrane_fail_after_ml
    print(json.dumps(water_loop.execute_plan(args.db,args.case,plc),ensure_ascii=False))
def water_claim(args):
    print(json.dumps(water_loop.claim_order(args.db,args.case,args.order,args.worker),ensure_ascii=False))
def water_naive(args):
    try:
        print(json.dumps(water_loop.attempt_naive_direct(
            args.db,args.case,args.order,args.ml,args.window,water_loop.SimPLC()),ensure_ascii=False))
    except water_loop.WaterError as exc:
        print(json.dumps({'blocked':exc.code,'detail':exc.detail},ensure_ascii=False))
def water_recall(args):
    print(json.dumps(water_loop.retest_delivery(
        args.db,args.case,args.delivery,args.measured_us_cm,water_loop.SimPLC()),ensure_ascii=False))
def water_makeup(args):
    print(json.dumps(water_loop.makeup_delivery(
        args.db,args.case,args.order,args.ml,args.source,water_loop.SimPLC()),ensure_ascii=False))
def water_report_cmd(args):
    print(water_loop.water_report(args.db,args.case,args.out))
def main():
    p=argparse.ArgumentParser(description='离线样本保管链核验')
    sub=p.add_subparsers(dest='cmd',required=True)
    d=sub.add_parser('demo');d.add_argument('--workdir')
    i=sub.add_parser('init');i.add_argument('--db',required=True);i.add_argument('--batch',required=True);i.add_argument('--sample',required=True)
    a=sub.add_parser('ingest');a.add_argument('path');a.add_argument('--db',required=True)
    e=sub.add_parser('evaluate');e.add_argument('--db',required=True)
    r=sub.add_parser('report');r.add_argument('--db',required=True);r.add_argument('--out',required=True)
    x=sub.add_parser('inspect');x.add_argument('--db',required=True);x.add_argument('--sample',required=True)
    n=sub.add_parser('register-sample');n.add_argument('--db',required=True);n.add_argument('--batch',required=True);n.add_argument('--sample',required=True);n.add_argument('--replaces')
    wd=sub.add_parser('water-demo');wd.add_argument('--workdir');wd.add_argument('--route',default='direct_first',choices=['direct_first','reserve_first'])
    ws=sub.add_parser('water-snapshot');ws.add_argument('--db',required=True);ws.add_argument('--case',required=True)
    wt=sub.add_parser('water-telemetry');wt.add_argument('--db',required=True);wt.add_argument('--case',required=True)
    wp=sub.add_parser('water-plan');wp.add_argument('--db',required=True);wp.add_argument('--case',required=True);wp.add_argument('--route',required=True,choices=['direct_first','reserve_first'])
    wr=sub.add_parser('water-run');wr.add_argument('--db',required=True);wr.add_argument('--case',required=True);wr.add_argument('--fault');wr.add_argument('--membrane-fail-after-ml',type=int,dest='membrane_fail_after_ml')
    wc=sub.add_parser('water-claim');wc.add_argument('--db',required=True);wc.add_argument('--case',required=True);wc.add_argument('--order',required=True);wc.add_argument('--worker',required=True)
    wn=sub.add_parser('water-naive');wn.add_argument('--db',required=True);wn.add_argument('--case',required=True);wn.add_argument('--order',required=True);wn.add_argument('--ml',type=int,required=True);wn.add_argument('--window',required=True)
    wrec=sub.add_parser('water-recall');wrec.add_argument('--db',required=True);wrec.add_argument('--case',required=True);wrec.add_argument('--delivery',required=True);wrec.add_argument('--measured-us-cm',type=float,required=True,dest='measured_us_cm')
    wmk=sub.add_parser('water-makeup');wmk.add_argument('--db',required=True);wmk.add_argument('--case',required=True);wmk.add_argument('--order',required=True);wmk.add_argument('--ml',type=int,required=True);wmk.add_argument('--source',required=True,choices=['polished_direct','qualified_reserve'])
    wrep=sub.add_parser('water-report');wrep.add_argument('--db',required=True);wrep.add_argument('--case',required=True);wrep.add_argument('--out',required=True)
    args=p.parse_args()
    if args.cmd=='demo':print(demo(args.workdir or tempfile.mkdtemp(prefix='samplechain-')))
    elif args.cmd=='water-demo':print(water_demo(args.workdir or tempfile.mkdtemp(prefix='water-'),args.route))
    elif args.cmd=='water-snapshot':water_snapshot(args)
    elif args.cmd=='water-telemetry':water_telemetry(args)
    elif args.cmd=='water-plan':water_plan(args)
    elif args.cmd=='water-run':water_run(args)
    elif args.cmd=='water-claim':water_claim(args)
    elif args.cmd=='water-naive':water_naive(args)
    elif args.cmd=='water-recall':water_recall(args)
    elif args.cmd=='water-makeup':water_makeup(args)
    elif args.cmd=='water-report':water_report_cmd(args)
    elif args.cmd=='init':init(args.db,args.batch,args.sample)
    elif args.cmd=='ingest':print(ingest(args.path,args.db))
    elif args.cmd=='evaluate':print(evaluate(args.db))
    elif args.cmd=='report':print(report(args.db,args.out))
    elif args.cmd=='register-sample':
        con=connect(args.db)
        with con:con.execute('INSERT INTO sample(id,batch_id,replacement_of) VALUES (?,?,?)',(args.sample,args.batch,args.replaces))
        con.close()
    else:
        con=connect(args.db);print(json.dumps({'decision':[dict(r) for r in con.execute('SELECT * FROM decision WHERE sample_id=?',(args.sample,))],'events':[dict(r) for r in con.execute('SELECT * FROM event WHERE sample_id=? ORDER BY ts,id',(args.sample,))]},ensure_ascii=False));con.close()
if __name__=='__main__':main()
