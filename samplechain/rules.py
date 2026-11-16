import json
from hashlib import sha256
from collections import defaultdict
from datetime import timedelta, timezone
from .ingest import time
from .store import connect

CHAIN=['collect','handoff','receive','fridge_in','fridge_out','test']
RULES_VERSION='2'
POLICIES=('collect','acceptance')
BLOCKING={'voided','chain_invalid','seal_mismatch','out_time','temperature','expired','replaced',
    'replacement_cycle','replacement_conflict','replacement_cross_batch','recollect_mismatch'}
ACCEPTANCE_FAILURE={'seal_mismatch','temperature','voided'}
ISSUE_CODES=('sequence_gap','chain_invalid','seal_mismatch','out_time','out_unknown','temperature','expired','voided',
    'recollect_invalid','recollect_missing','recollect_mismatch','replaced',
    'replacement_cycle','replacement_conflict','replacement_cross_batch')

def iso(stamp): return stamp.astimezone(timezone.utc).isoformat().replace('+00:00','Z')

def cycle_members(samples):
    members=set()
    for sid in samples:
        path=[];seen=set();cur=sid
        while cur is not None and cur in samples and cur not in seen:
            seen.add(cur);path.append(cur);cur=samples[cur]['replacement_of']
        if cur is not None and cur in seen:members.update(path[path.index(cur):])
    return members

def evaluate(db_path,policy=None):
    con=connect(db_path); results={}
    with con:
        if policy is not None:
            if policy not in POLICIES:raise ValueError('unknown_takeover_policy')
            con.execute("INSERT OR REPLACE INTO policy(id,value) VALUES ('takeover',?)",(policy,))
        stored=con.execute("SELECT value FROM policy WHERE id='takeover'").fetchone()
        if stored:policy=stored['value']
        elif policy is None:
            policy='acceptance'
            con.execute("INSERT INTO policy(id,value) VALUES ('takeover',?)",(policy,))
        con.execute('DELETE FROM decision')
        con.execute('DELETE FROM takeover')
        con.execute('DELETE FROM issue WHERE code IN (%s)'%','.join('?'*len(ISSUE_CODES)),ISSUE_CODES)
        events=[dict(r) for r in con.execute('SELECT * FROM event ORDER BY ts,id')]
        source_digest=sha256('|'.join(r['sha256'] for r in con.execute('SELECT sha256 FROM source ORDER BY sha256')).encode()).hexdigest()
        run_id=sha256((RULES_VERSION+':'+policy+':'+source_digest).encode()).hexdigest()
        con.execute('INSERT OR IGNORE INTO run VALUES (?,?,?,?)',(run_id,RULES_VERSION,source_digest,policy))
        for event in events:
            con.execute('INSERT OR REPLACE INTO evidence VALUES (?,?,?,?,?,?,?)',(event['sha256'],run_id,event['sample_id'],event['source_id'],'event',event['id'],event['sha256']))
        by_sample=defaultdict(list);by_device=defaultdict(list)
        for event in events:by_sample[event['sample_id']].append(event);by_device[event['device']].append(event)
        gap_devices=set()
        for device,stream in by_device.items():
            seqs={e['seq'] for e in stream}
            if seqs and max(seqs)-min(seqs)+1!=len(seqs):
                gap_devices.add(device);con.execute('INSERT INTO issue(code,detail) VALUES (?,?)',('sequence_gap',device))
        samples={r['id']:dict(r) for r in con.execute('SELECT * FROM sample')}
        batches={r['id']:dict(r) for r in con.execute('SELECT * FROM batch')}
        children=defaultdict(list)
        for s in samples.values():
            if s['replacement_of']:children[s['replacement_of']].append(s['id'])
        # 换采结构校验：跨批次、一个样本两个有效替代者、循环
        structural=defaultdict(set);cross=set()
        for s in samples.values():
            p=s['replacement_of']
            if p and p in samples and samples[p]['batch_id']!=s['batch_id']:
                cross.add((p,s['id']));structural[p].add('replacement_cross_batch');structural[s['id']].add('replacement_cross_batch')
        conflict_parents={p for p,kids in children.items() if len(kids)>1}
        for p in conflict_parents:
            structural[p].add('replacement_conflict')
            for kid in children[p]:structural[kid].add('replacement_conflict')
        cycles=cycle_members(samples)
        for sid in cycles:structural[sid].add('replacement_cycle')
        def link_invalid(p,c):return (p,c) in cross or p in conflict_parents or p in cycles or c in cycles
        # 每个样本先按自身证据链独立判定
        base={}
        for sid,s in samples.items():
            stream=by_sample.get(sid,[]);batch=batches[s['batch_id']]
            expiry=time(s['expiry'] or batch['expiry'])
            reasons=set();stage=-1;seal=None;opened=False;outside=None;outside_total=timedelta()
            tested_at=None;accepted_at=None;bad_transition=False
            for e in stream:
                kind=e['kind'];stamp=time(e['ts'])
                if kind in CHAIN:
                    index=CHAIN.index(kind)
                    ok=index==stage+1 or (stage==4 and kind in ('fridge_in','test')) or (stage==3 and kind=='test') or (stage==4 and kind=='fridge_out')
                    if not ok:reasons.add('chain_invalid');bad_transition=True
                    stage=index
                    if e['seal'] and seal and e['seal']!=seal and not opened:reasons.add('seal_mismatch')
                    if e['seal'] and seal is None:seal=e['seal']
                    if kind=='fridge_out':
                        if outside is None:outside=stamp
                    if kind in ('fridge_in','test') and outside is not None:
                        outside_total+=stamp-outside;outside=None
                    if kind=='fridge_in' and index==3 and accepted_at is None and ok and not bad_transition:accepted_at=stamp
                    if kind=='test' and tested_at is None and ok and not bad_transition:tested_at=stamp
                elif kind=='open':
                    if opened:reasons.add('seal_mismatch')
                    opened=True;seal=e['seal']
                elif kind=='reseal':
                    if not opened or not e['seal']:reasons.add('seal_mismatch')
                    else:seal=e['seal']
                    opened=False
                elif kind=='void':reasons.add('voided')
                elif kind=='recollect':
                    if not any(x['replacement_of']==sid for x in samples.values()):reasons.add('recollect_invalid')
                elif kind=='temperature' and (e['value'] is None or e['value']<batch['min_temp'] or e['value']>batch['max_temp']):
                    reasons.add('temperature')
                if stamp>expiry:reasons.add('expired')
            if stage!=len(CHAIN)-1:reasons.add('chain_invalid')
            if opened:reasons.add('seal_mismatch')
            if outside is not None:reasons.add('out_unknown')
            if outside_total>timedelta(minutes=batch['max_out_minutes']):reasons.add('out_time')
            if any(e['device'] in gap_devices for e in stream):reasons.add('sequence_gap')
            if con.execute("SELECT 1 FROM issue WHERE sample_id=? AND code IN ('identity_conflict','unknown_sample')",(sid,)).fetchone():reasons.add('identity_conflict')
            base[sid]={'reasons':reasons,'stream':stream,'expiry':expiry,'tested_at':tested_at,'accepted_at':accepted_at,'bad_transition':bad_transition}
        # 每条换采链按放行契约计算接管时点
        effective={}
        for p,kids in children.items():
            for c in kids:
                if p not in samples or c not in samples:continue
                if link_invalid(p,c):status='blocked';eff=None
                elif policy=='collect':
                    recs=[time(e['ts']) for e in by_sample.get(p,[]) if e['kind']=='recollect']
                    eff=min(recs) if recs else None
                    status='effective' if eff else 'pending'
                else:
                    info=base[c];failed=info['reasons']&ACCEPTANCE_FAILURE
                    if info['accepted_at'] and not failed and not info['bad_transition']:eff=info['accepted_at'];status='effective'
                    elif failed:eff=None;status='rejected'
                    else:eff=None;status='pending'
                effective[(p,c)]=eff
                con.execute('INSERT INTO takeover VALUES (?,?,?,?,?,?)',(p,c,policy,status,iso(eff) if eff else None,run_id))
        # recollect 事件声明的替代样本须与登记链接一致
        mismatch=set()
        for link in con.execute('SELECT * FROM recollect_link'):
            child=samples.get(link['child'])
            if child is None or child['replacement_of']!=link['parent']:
                mismatch.add(link['parent'])
                con.execute('INSERT INTO issue(sample_id,event_id,code,detail) VALUES (?,?,?,?)',(link['parent'],link['event_id'],'recollect_mismatch',link['child']))
        # 当前责任样本：从每条的根沿已生效接管走到最新一代
        active=set()
        for sid,s in samples.items():
            if s['replacement_of']:continue
            cur=sid;seen=set()
            while cur not in seen:
                seen.add(cur)
                kids=[c for c in children.get(cur,[]) if not link_invalid(cur,c)]
                nxt=kids[0] if len(kids)==1 and effective.get((cur,kids[0])) is not None else None
                if nxt is None:active.add(cur);break
                cur=nxt
        for sid,s in samples.items():
            info=base[sid]
            reasons=set(info['reasons'])|structural.get(sid,set())
            if children.get(sid) and not any(e['kind']=='recollect' for e in info['stream']):reasons.add('recollect_missing')
            if sid in mismatch:reasons.add('recollect_mismatch')
            outs=[effective[(sid,c)] for c in children.get(sid,[]) if effective.get((sid,c))]
            takeover_out=min(outs) if outs else None
            if takeover_out and (info['tested_at'] is None or info['tested_at']>takeover_out):reasons.add('replaced')
            window_end=min(info['expiry'],takeover_out) if takeover_out else info['expiry']
            parent=s['replacement_of']
            takeover_in=effective.get((parent,sid)) if parent else None
            if takeover_in:window_start=iso(takeover_in)
            elif info['stream']:window_start=iso(time(info['stream'][0]['ts']))
            else:window_start=None
            reasons=sorted(reasons)
            result='不可检测' if BLOCKING&set(reasons) else ('证据不足' if reasons else '可检测')
            for code in reasons:
                if code=='recollect_mismatch':continue
                con.execute('INSERT INTO issue(sample_id,code,detail) VALUES (?,?,?)',(sid,code,sid))
            con.execute('INSERT INTO decision VALUES (?,?,?,?,?,?,?,?,?)',
                (sid,result,json.dumps(reasons),json.dumps([e['id'] for e in info['stream']]),RULES_VERSION,run_id,window_start,iso(window_end),1 if sid in active else 0))
            results[sid]=result
    con.close();return results
