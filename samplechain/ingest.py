import hashlib
import json
from datetime import datetime
from pathlib import Path
from .store import connect

KINDS={'collect','handoff','receive','fridge_in','fridge_out','test','open','reseal','void','recollect','temperature'}
def sha(data): return hashlib.sha256(data).hexdigest()
def time(value):
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if not result.tzinfo: raise ValueError('timezone_required')
    return result

def ingest(path,db_path):
    raw=Path(path).read_bytes(); file_hash=sha(raw); con=connect(db_path)
    if con.execute('SELECT 1 FROM source WHERE sha256=?',(file_hash,)).fetchone():
        con.close();return {'imported':0,'duplicate_file':True}
    imported=0
    with con:
        source=con.execute('INSERT INTO source(path,sha256,raw) VALUES (?,?,?)',(str(path),file_hash,raw)).lastrowid
        for line_no,line in enumerate(raw.decode('utf-8-sig').splitlines(),1):
            try:
                if not line.strip():continue
                row=json.loads(line); kind=row['kind']; device=str(row['device']); seq=int(row['seq']); sample=str(row['sample_id']); ts=str(row['ts'])
                if kind not in KINDS:raise ValueError('unknown_kind')
                time(ts)
                if not device or not sample:raise ValueError('missing_identity')
                event_hash=sha(json.dumps(row,sort_keys=True,ensure_ascii=False).encode())
                prior=con.execute('SELECT sha256 FROM event WHERE device=? AND seq=?',(device,seq)).fetchone()
                if prior:
                    if prior['sha256']!=event_hash:con.execute('INSERT INTO issue(sample_id,code,detail) VALUES (?,?,?)',(sample,'identity_conflict',f'{device}:{seq}'))
                    continue
                cur=con.execute('INSERT INTO event(source_id,line_no,device,seq,sample_id,kind,ts,actor,seal,value,payload,sha256) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                    (source,line_no,device,seq,sample,kind,ts,row.get('actor'),row.get('seal'),row.get('value'),line,event_hash))
                imported+=1
                if kind=='recollect' and row.get('replacement'):
                    con.execute('INSERT INTO recollect_link(event_id,parent,child) VALUES (?,?,?)',(cur.lastrowid,sample,str(row['replacement'])))
                if not con.execute('SELECT 1 FROM sample WHERE id=?',(sample,)).fetchone():
                    con.execute('INSERT INTO issue(sample_id,event_id,code,detail) VALUES (?,?,?,?)',(sample,cur.lastrowid,'unknown_sample',sample))
            except (KeyError,ValueError,TypeError,json.JSONDecodeError) as exc:
                con.execute('INSERT INTO issue(code,detail) VALUES (?,?)',('parse_error',f'{line_no}: {exc}; raw={line[:160]}'))
    con.close();return {'imported':imported,'duplicate_file':False}
