import json
from hashlib import sha256
from collections import defaultdict
from datetime import timedelta
from .ingest import time, POLICIES, DEFAULT_POLICY
from .store import connect

CHAIN=['collect','handoff','receive','fridge_in','fridge_out','test']
RULES_VERSION='2'
BLOCKING=('voided','chain_invalid','seal_mismatch','out_time','temperature','expired','replacement_cycle','replacement_conflict','replacement_cross_batch')
DERIVED_ISSUES=('sequence_gap','chain_invalid','seal_mismatch','out_time','out_unknown','temperature','expired','voided','recollect_invalid','replacement_cycle','replacement_conflict','replacement_cross_batch')

def scan(stream,expiry,max_out,min_temp,max_temp,upto=None):
    """单一样本自身证据链的问题扫描;upto 给定时刻时只统计该时刻之前(含)的事件。"""
    reasons=[];stage=-1;seal=None;opened=False;outside=None;outside_total=timedelta()
    for e in stream:
        stamp=time(e['ts'])
        if upto is not None and stamp>upto:continue
        kind=e['kind']
        if kind in CHAIN:
            index=CHAIN.index(kind)
            if index!=stage+1 and not (stage==4 and kind in ('fridge_in','test')) and not (stage==3 and kind=='test') and not (stage==4 and kind=='fridge_out'):
                reasons.append('chain_invalid')
            stage=index
            if e['seal'] and seal and e['seal']!=seal and not opened:reasons.append('seal_mismatch')
            if e['seal'] and seal is None:seal=e['seal']
            if kind=='fridge_out':
                if outside is None:outside=stamp
            if kind in ('fridge_in','test') and outside is not None:
                outside_total+=stamp-outside;outside=None
        elif kind=='open':
            if opened:reasons.append('seal_mismatch')
            opened=True;seal=e['seal']
        elif kind=='reseal':
            if not opened or not e['seal']:reasons.append('seal_mismatch')
            else:seal=e['seal']
            opened=False
        elif kind=='void':reasons.append('voided')
        elif kind=='temperature' and (e['value'] is None or e['value']<min_temp or e['value']>max_temp):
            reasons.append('temperature')
        if stamp>expiry:reasons.append('expired')
    if stage!=len(CHAIN)-1:reasons.append('chain_invalid')
    if opened:reasons.append('seal_mismatch')
    if outside is not None:reasons.append('out_unknown')
    if outside_total>timedelta(minutes=max_out):reasons.append('out_time')
    return reasons

def acceptance(stream,batch):
    """验收后替代:子样本按序完成 collect→receive→fridge_in 且此前封签/温度证据合格的时刻,否则 None。"""
    stage=-1;seal=None;opened=False
    for e in stream:
        kind=e['kind']
        if kind in CHAIN:
            index=CHAIN.index(kind)
            if index<=3:
                if index!=stage+1:return None
                stage=index
                if e['seal']:
                    if seal and e['seal']!=seal and not opened:return None
                    if seal is None:seal=e['seal']
                if kind=='fridge_in':return (e['ts'],e['id'])
        elif kind=='open':
            if opened:return None
            opened=True;seal=e['seal']
        elif kind=='reseal':
            if not opened or not e['seal']:return None
            seal=e['seal'];opened=False
        elif kind=='void':return None
        elif kind=='temperature' and (e['value'] is None or e['value']<batch['min_temp'] or e['value']>batch['max_temp']):
            return None
    return None

def replacement_blocks(samples):
    """替代图校验:循环、同一样本两个有效替代者、跨批次链接 -> {sample_id: {code}}。相关样本检测一律阻断。"""
    blocks=defaultdict(set)
    children=defaultdict(list)
    for sid,s in samples.items():
        if s['replacement_of']:children[s['replacement_of']].append(sid)
    for parent,kids in children.items():
        if len(kids)>1:
            blocks[parent].add('replacement_conflict')
            for kid in kids:blocks[kid].add('replacement_conflict')
    for sid,s in samples.items():
        parent=s['replacement_of']
        if parent and parent in samples and samples[parent]['batch_id']!=s['batch_id']:
            blocks[sid].add('replacement_cross_batch');blocks[parent].add('replacement_cross_batch')
    for start in samples:
        seen={};path=[];cur=start
        while cur and cur in samples:
            if cur in seen:
                for node in path[seen[cur]:]:blocks[node].add('replacement_cycle')
                break
            seen[cur]=len(path);path.append(cur)
            cur=samples[cur]['replacement_of']
    return blocks

def replacement_links(samples,blocks):
    """有效替代链接 parent -> child:唯一替代者、链接双方均未被阻断。替代链允许多代。"""
    children=defaultdict(list)
    for sid,s in samples.items():
        if s['replacement_of']:children[s['replacement_of']].append(sid)
    links={}
    for parent,kids in children.items():
        if len(kids)!=1 or parent not in samples:continue
        child=kids[0]
        if blocks.get(parent) or blocks.get(child):continue
        links[parent]=child
    return links

def compute_custody(samples,streams,batches,links,policy,run_id):
    """按放行契约计算每代样本的授权窗口与当前责任样本;父样本证据不回写、历史判定不覆盖。"""
    takeover={}
    for parent,child in links.items():
        if policy=='collect':
            ev=next((e for e in streams[parent] if e['kind']=='recollect'),None)
            if ev:takeover[parent]=(ev['ts'],ev['id'])
        else:
            moment=acceptance(streams[child],batches[samples[child]['batch_id']])
            if moment:takeover[parent]=moment
    parent_of={child:parent for parent,child in links.items()}
    participants=set(links)|set(links.values())
    current=set()
    for sid in participants:
        if sid not in parent_of:
            cur=sid
            while cur in takeover:cur=links[cur]
            current.add(cur)
    rows=[]
    for sid in sorted(participants):
        origin=takeover.get(parent_of.get(sid))
        end=takeover.get(sid)
        rows.append((sid,links.get(sid),origin[0] if origin else None,end[0] if end else None,origin[1] if origin else None,1 if sid in current else 0,policy,run_id))
    return rows

def evaluate(db_path,policy=None):
    con=connect(db_path);results={}
    with con:
        con.execute('DELETE FROM decision')
        con.execute('DELETE FROM custody')
        con.execute("DELETE FROM issue WHERE code IN (%s)" % ','.join('?'*len(DERIVED_ISSUES)),DERIVED_ISSUES)
        events=[dict(r) for r in con.execute('SELECT * FROM event ORDER BY ts,id')]
        source_digest=sha256('|'.join(r['sha256'] for r in con.execute('SELECT sha256 FROM source ORDER BY sha256')).encode()).hexdigest()
        if policy is None:
            row=con.execute("SELECT value FROM meta WHERE key='replacement_policy'").fetchone()
            policy=row['value'] if row else DEFAULT_POLICY
        if policy not in POLICIES:raise ValueError('unknown_policy')
        run_id=sha256((RULES_VERSION+':'+policy+':'+source_digest).encode()).hexdigest()
        con.execute('INSERT OR IGNORE INTO run VALUES (?,?,?)',(run_id,RULES_VERSION,source_digest))
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
        blocks=replacement_blocks(samples)
        replaced_ids={s['replacement_of'] for s in samples.values()}
        for sid,sample in samples.items():
            stream=by_sample[sid];batch=batches[sample['batch_id']]
            expiry=time(sample['expiry'] or batch['expiry'])
            reasons=scan(stream,expiry,batch['max_out_minutes'],batch['min_temp'],batch['max_temp'])
            if any(e['kind']=='recollect' for e in stream) and sid not in replaced_ids:reasons.append('recollect_invalid')
            if any(e['device'] in gap_devices for e in stream):reasons.append('sequence_gap')
            if con.execute("SELECT 1 FROM issue WHERE sample_id=? AND code IN ('identity_conflict','unknown_sample')",(sid,)).fetchone():reasons.append('identity_conflict')
            reasons+=sorted(blocks.get(sid,()))
            reasons=sorted(set(reasons))
            for code in reasons:con.execute('INSERT INTO issue(sample_id,code,detail) VALUES (?,?,?)',(sid,code,sid))
            result='不可检测' if any(x in reasons for x in BLOCKING) else ('证据不足' if reasons else '可检测')
            con.execute('INSERT INTO decision VALUES (?,?,?,?,?,?)',(sid,result,json.dumps(reasons),json.dumps([e['id'] for e in stream]),RULES_VERSION,run_id))
            results[sid]=result
        links=replacement_links(samples,blocks)
        for row in compute_custody(samples,by_sample,batches,links,policy,run_id):
            con.execute('INSERT INTO custody VALUES (?,?,?,?,?,?,?,?)',row)
    con.close();return results

def _gap_devices(con):
    by_device=defaultdict(set)
    for r in con.execute('SELECT device,seq FROM event'):by_device[r['device']].add(r['seq'])
    return {device for device,seqs in by_device.items() if seqs and max(seqs)-min(seqs)+1!=len(seqs)}

def testable_at(db_path,sample_id,moment):
    """moment 时刻该样本依自身证据与有效期是否可送检(不含替代责任判断)。"""
    con=connect(db_path)
    try:
        sample=con.execute('SELECT * FROM sample WHERE id=?',(sample_id,)).fetchone()
        if not sample:return False
        moment=time(moment) if isinstance(moment,str) else moment
        batch=con.execute('SELECT * FROM batch WHERE id=?',(sample['batch_id'],)).fetchone()
        expiry=time(sample['expiry'] or batch['expiry'])
        if moment>expiry:return False
        stream=[dict(r) for r in con.execute('SELECT * FROM event WHERE sample_id=? ORDER BY ts,id',(sample_id,))]
        reasons=scan(stream,expiry,batch['max_out_minutes'],batch['min_temp'],batch['max_temp'],upto=moment)
        if any(e['kind']=='recollect' for e in stream if time(e['ts'])<=moment):
            if not con.execute('SELECT 1 FROM sample WHERE replacement_of=?',(sample_id,)).fetchone():reasons.append('recollect_invalid')
        gaps=_gap_devices(con)
        if any(e['device'] in gaps for e in stream):reasons.append('sequence_gap')
        if con.execute("SELECT 1 FROM issue WHERE sample_id=? AND code IN ('identity_conflict','unknown_sample')",(sample_id,)).fetchone():reasons.append('identity_conflict')
        samples={r['id']:dict(r) for r in con.execute('SELECT * FROM sample')}
        reasons+=sorted(replacement_blocks(samples).get(sample_id,()))
        return not reasons
    finally:con.close()

def responsible_at(db_path,moment):
    """moment 时刻各替代链中授权窗口覆盖该时刻的责任样本。"""
    con=connect(db_path)
    try:
        moment=time(moment) if isinstance(moment,str) else moment
        rows=con.execute('SELECT * FROM custody').fetchall()
        return sorted(r['sample_id'] for r in rows if (r['window_start'] is None or time(r['window_start'])<=moment) and (r['window_end'] is None or moment<time(r['window_end'])))
    finally:con.close()
