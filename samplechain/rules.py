import json
from collections import defaultdict
from datetime import timedelta
from .ingest import time
from .store import connect

CHAIN=['collect','handoff','receive','fridge_in','fridge_out','test']
def evaluate(db_path):
    con=connect(db_path); results={}
    with con:
        con.execute('DELETE FROM decision')
        con.execute("DELETE FROM issue WHERE code IN ('sequence_gap','chain_invalid','seal_mismatch','out_time','out_unknown','temperature','expired','voided','recollect_invalid')")
        events=[dict(r) for r in con.execute('SELECT * FROM event ORDER BY ts,id')]
        by_sample=defaultdict(list);by_device=defaultdict(list)
        for event in events:by_sample[event['sample_id']].append(event);by_device[event['device']].append(event)
        gap_devices=set()
        for device,stream in by_device.items():
            seqs={e['seq'] for e in stream}
            if seqs and max(seqs)-min(seqs)+1!=len(seqs):
                gap_devices.add(device);con.execute('INSERT INTO issue(code,detail) VALUES (?,?)',('sequence_gap',device))
        for sample in con.execute('SELECT * FROM sample'):
            stream=by_sample[sample['id']];batch=con.execute('SELECT * FROM batch WHERE id=?',(sample['batch_id'],)).fetchone()
            reasons=[];stage=-1;seal=None;opened=False;outside=None;outside_total=timedelta();voided=False
            for e in stream:
                kind=e['kind'];stamp=time(e['ts'])
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
                elif kind=='void':voided=True;reasons.append('voided')
                elif kind=='recollect':
                    replacement=con.execute('SELECT id FROM sample WHERE replacement_of=?',(sample['id'],)).fetchone()
                    if not replacement:reasons.append('recollect_invalid')
                elif kind=='temperature' and (e['value'] is None or e['value']<batch['min_temp'] or e['value']>batch['max_temp']):
                    reasons.append('temperature')
                if stamp>time(batch['expiry']):reasons.append('expired')
            if stage!=len(CHAIN)-1:reasons.append('chain_invalid')
            if opened:reasons.append('seal_mismatch')
            if outside is not None:reasons.append('out_unknown')
            if outside_total>timedelta(minutes=batch['max_out_minutes']):reasons.append('out_time')
            if any(e['device'] in gap_devices for e in stream):reasons.append('sequence_gap')
            if con.execute("SELECT 1 FROM issue WHERE sample_id=? AND code IN ('identity_conflict','unknown_sample')",(sample['id'],)).fetchone():reasons.append('identity_conflict')
            reasons=sorted(set(reasons))
            for code in reasons:con.execute('INSERT INTO issue(sample_id,code,detail) VALUES (?,?,?)',(sample['id'],code,sample['id']))
            result='不可检测' if any(x in reasons for x in ('voided','chain_invalid','seal_mismatch','out_time','temperature','expired')) else ('证据不足' if reasons else '可检测')
            con.execute('INSERT INTO decision VALUES (?,?,?,?,?)',(sample['id'],result,json.dumps(reasons),json.dumps([e['id'] for e in stream]),'1'))
            results[sample['id']]=result
    con.close();return results
