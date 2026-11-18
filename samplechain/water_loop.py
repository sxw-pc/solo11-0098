"""纯水供给闭环：冻结登记、传感采录、膜/储罐/公共管道守恒、模拟 PLC 与实物交付。

本模块只操作独立的纯水管路与储罐，不判定样本可检测性，也不修改既有 decision。
所有体积以整数毫升（mL）计；电导率以毫单位（µS/cm × 1000 的整数）计；
溶质质量以 mass = conductivity_milli × mL 计，混水按质量守恒做有理数核算，
绝不对两个探头读数直接平均，也不把未观测水质当作零。
"""
import csv
import hashlib
import io
import json
from collections import deque
from datetime import datetime
from pathlib import Path

from .store import connect
from .ingest import time as parse_ts

ML_PER_L = 1000
CONDUCT_SCALE = 1000
WATER_SCHEMA = """
CREATE TABLE IF NOT EXISTS water_snapshot(id TEXT PRIMARY KEY, sensor_versions TEXT NOT NULL,
  valve_versions TEXT NOT NULL, order_version TEXT NOT NULL, basis TEXT NOT NULL, created_ts TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS water_telemetry(id INTEGER PRIMARY KEY AUTOINCREMENT, case_id TEXT NOT NULL,
  sensor TEXT NOT NULL, seq INTEGER NOT NULL, ts TEXT NOT NULL, conductivity_milli INTEGER NOT NULL,
  flow_ml INTEGER NOT NULL, cal_version TEXT NOT NULL, snapshot_id TEXT NOT NULL, source_sha TEXT,
  status TEXT NOT NULL, detail TEXT, UNIQUE(case_id,sensor,seq));
CREATE TABLE IF NOT EXISTS water_plan(case_id TEXT NOT NULL, route TEXT NOT NULL, frozen_ts TEXT NOT NULL,
  plan_json TEXT NOT NULL, plan_sha256 TEXT NOT NULL, snapshot_id TEXT NOT NULL, PRIMARY KEY(case_id,route));
CREATE TABLE IF NOT EXISTS water_active_plan(case_id TEXT PRIMARY KEY, route TEXT NOT NULL, frozen_ts TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS water_command(id TEXT PRIMARY KEY, case_id TEXT NOT NULL, nonce INTEGER NOT NULL,
  valve TEXT NOT NULL, action TEXT NOT NULL, valve_version TEXT NOT NULL, snapshot_id TEXT NOT NULL,
  step_id TEXT, issued_ts TEXT NOT NULL, ack TEXT NOT NULL, detail TEXT, UNIQUE(case_id,nonce));
CREATE TABLE IF NOT EXISTS water_meter(id INTEGER PRIMARY KEY AUTOINCREMENT, case_id TEXT NOT NULL,
  step_id TEXT, valve TEXT NOT NULL, ml INTEGER NOT NULL, requested_ml INTEGER NOT NULL, pulses INTEGER NOT NULL,
  ts TEXT NOT NULL, status TEXT NOT NULL, handle TEXT);
CREATE TABLE IF NOT EXISTS water_ledger(id INTEGER PRIMARY KEY AUTOINCREMENT, case_id TEXT NOT NULL,
  step_id TEXT, kind TEXT NOT NULL, ts TEXT NOT NULL, detail_json TEXT NOT NULL, handle TEXT);
CREATE TABLE IF NOT EXISTS water_claim(case_id TEXT NOT NULL, order_id TEXT NOT NULL, worker TEXT NOT NULL,
  ts TEXT NOT NULL, snapshot_id TEXT NOT NULL, PRIMARY KEY(case_id,order_id));
CREATE TABLE IF NOT EXISTS water_delivery(id TEXT PRIMARY KEY, case_id TEXT NOT NULL, order_id TEXT NOT NULL,
  route TEXT NOT NULL, kind TEXT NOT NULL, seq INTEGER NOT NULL, source TEXT NOT NULL, window TEXT,
  ml INTEGER NOT NULL, mass INTEGER NOT NULL, conductivity_milli INTEGER NOT NULL, qualified INTEGER NOT NULL,
  receipt_json TEXT NOT NULL, snapshot_id TEXT NOT NULL, ts TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS water_notice(id TEXT PRIMARY KEY, case_id TEXT NOT NULL, code TEXT NOT NULL,
  delivery_id TEXT, order_id TEXT, detail_json TEXT NOT NULL, ts TEXT NOT NULL);
"""

# 默认校准清单：传感器身份 → (校准版本, 生效起, 生效止)
DEFAULT_CALIBRATION = {
    'RAW-1': ('cal-raw-1', '2026-11-01T00:00:00+08:00', '2026-12-01T00:00:00+08:00'),
    'POLISH-1': ('cal-pol-1', '2026-11-01T00:00:00+08:00', '2026-12-01T00:00:00+08:00'),
    'RESERVE-1': ('cal-res-1', '2026-11-01T00:00:00+08:00', '2026-12-01T00:00:00+08:00'),
    'PIPE-1': ('cal-pipe-1', '2026-11-01T00:00:00+08:00', '2026-12-01T00:00:00+08:00'),
}
DEFAULT_VALVE_VERSIONS = {'V-DIRECT': 'fw-vd-3', 'V-RESERVE': 'fw-vr-3', 'V-DRAIN': 'fw-vdr-2'}
ORDER_VERSION = 'order-schema-1'
MEMBRANE_CAPACITY_ML = 8000
SLOT_CAP_ML = {'t0': 4000, 't1': 4000}
WINDOWS = (('t0', '2026-11-17T08:00:00+08:00', 1), ('t1', '2026-11-17T08:10:00+08:00', 2))


class WaterError(Exception):
    def __init__(self, code, detail=''):
        super().__init__(code + (f': {detail}' if detail else ''))
        self.code = code
        self.detail = detail


class PLCError(WaterError):
    pass


class PLCAckLost(PLCError):
    def __init__(self, record):
        super().__init__('plc_ack_lost', json.dumps(record, sort_keys=True))
        self.record = record


class PLCInterlock(PLCError):
    pass


class MembraneFailed(WaterError):
    def __init__(self, flowed_ml, reading=None):
        super().__init__('membrane_failed', f'flowed_ml={flowed_ml}')
        self.flowed_ml = flowed_ml
        self.reading = reading


class MeterPending(WaterError):
    def __init__(self, handle):
        super().__init__('meter_pending', handle)
        self.handle = handle


def milli(us_cm):
    """浮点 µS/cm 输入转整数毫单位；台账内部永不保留浮点电导。"""
    return int(round(float(us_cm) * CONDUCT_SCALE))


def us_cm(value_milli):
    return value_milli / CONDUCT_SCALE


def liters(ml):
    return ml / ML_PER_L


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def sha256_text(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def ensure_schema(db):
    db.executescript(WATER_SCHEMA)


# --------------------------------------------------------------------------- #
# 冻结登记（保持原有行为，仅依赖 water_case，不创建任何控制表）
# --------------------------------------------------------------------------- #
def register(db_path, case_id, contract_path, sensor_path):
    contract_raw = Path(contract_path).read_bytes()
    sensor_raw = Path(sensor_path).read_bytes()
    contract = json.loads(contract_raw)
    if not case_id or not contract.get('orders') or not contract.get('valves'):
        raise ValueError('water_contract_invalid')
    db = connect(db_path)
    try:
        observations = []
        for sample_id in contract['samples']:
            sample = db.execute('SELECT id,batch_id FROM sample WHERE id=?', (sample_id,)).fetchone()
            event = db.execute("SELECT e.id,s.sha256 AS source_sha FROM event e JOIN source s ON s.id=e.source_id WHERE e.sample_id=? AND e.kind='collect' ORDER BY e.id LIMIT 1", (sample_id,)).fetchone()
            if sample is None or sample['batch_id'] != contract['batch'] or event is None:
                raise ValueError('water_lab_context_missing')
            observations.append({'sample': dict(sample), 'collect': dict(event)})
        payload = json.dumps({'contract': contract,
                              'contract_sha256': hashlib.sha256(contract_raw).hexdigest(),
                              'sensor_csv': sensor_raw.decode('utf-8-sig'),
                              'sensor_sha256': hashlib.sha256(sensor_raw).hexdigest(),
                              'observations': observations}, sort_keys=True, separators=(',', ':'))
        with db:
            old = db.execute('SELECT payload FROM water_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('water_case_conflict')
            db.execute('INSERT OR IGNORE INTO water_case VALUES (?,?)', (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'orders': len(contract['orders'])}


def load_case(db, case_id):
    row = db.execute('SELECT payload FROM water_case WHERE id=?', (case_id,)).fetchone()
    if row is None:
        raise WaterError('water_case_missing', case_id)
    return json.loads(row['payload'])


# --------------------------------------------------------------------------- #
# 版本快照：传感器校准版本、阀版本、订单版本强制同一快照
# --------------------------------------------------------------------------- #
def create_snapshot(db_path, case_id, calibration=None, valve_versions=None,
                    order_version=ORDER_VERSION, created_ts='2026-11-17T07:59:00+08:00'):
    calibration = dict(DEFAULT_CALIBRATION if calibration is None else calibration)
    valve_versions = dict(DEFAULT_VALVE_VERSIONS if valve_versions is None else valve_versions)
    basis = {'sensor_versions': {k: list(v) for k, v in sorted(calibration.items())},
             'valve_versions': dict(sorted(valve_versions.items())),
             'order_version': order_version}
    snapshot_id = sha256_text(canonical(basis))
    db = connect(db_path)
    ensure_schema(db)
    try:
        with db:
            db.execute('INSERT OR IGNORE INTO water_snapshot VALUES (?,?,?,?,?,?)',
                       (snapshot_id, canonical(basis['sensor_versions']),
                        canonical(basis['valve_versions']), order_version, canonical(basis), created_ts))
    finally:
        db.close()
    return snapshot_id


def current_snapshot(db, case_id):
    row = db.execute('SELECT * FROM water_snapshot ORDER BY created_ts DESC,id DESC LIMIT 1').fetchone()
    if row is None:
        raise WaterError('snapshot_missing', case_id)
    return dict(row)


# --------------------------------------------------------------------------- #
# 传感器采录：身份、单调序号、校准有效期、时间窗口、快照版本
# --------------------------------------------------------------------------- #
def _validate_row(row, cal_versions, last_seq):
    sensor = str(row.get('sensor', ''))
    if sensor not in cal_versions:
        return 'unknown_sensor', f'sensor={sensor}'
    try:
        seq = int(row['seq'])
        ts = parse_ts(str(row['ts']))
        value_milli = milli(row['conductivity_us_cm'])
        flow_ml = int(round(float(row['flow_l']) * ML_PER_L))
    except (KeyError, TypeError, ValueError) as exc:
        return 'sensor_row_invalid', str(exc)
    cal_version, valid_from, valid_to = cal_versions[sensor]
    if str(row.get('cal_version', cal_version)) != cal_version:
        return 'calibration_version_mismatch', f"{sensor}:{row.get('cal_version')}!={cal_version}"
    if not (parse_ts(valid_from) <= ts <= parse_ts(valid_to)):
        return 'calibration_expired', f'{sensor}:{row["ts"]} outside {valid_from}..{valid_to}'
    if sensor in last_seq and seq <= last_seq[sensor]:
        return 'sequence_out_of_order', f'{sensor}: seq {seq} <= {last_seq[sensor]}'
    window_names = {w[0] for w in WINDOWS}
    if str(row.get('window', '')) and row['window'] not in window_names:
        return 'unknown_window', str(row['window'])
    return None, {'sensor': sensor, 'seq': seq, 'ts': str(row['ts']),
                  'conductivity_milli': value_milli, 'flow_ml': flow_ml,
                  'cal_version': cal_version}


def ingest_telemetry(db_path, case_id, rows, snapshot_id=None, source_sha=None):
    """采录传感器行。返回 {accepted, rejected:[{row,code,detail}]}。

    重复且内容一致的行忽略；同序号不同内容判 identity_conflict 拒收；
    乱序、校准过期、版本不匹配一律拒收并留痕，不覆盖既有证据。
    """
    db = connect(db_path)
    ensure_schema(db)
    try:
        return _insert_rows(db, case_id, rows, snapshot_id, source_sha)
    finally:
        db.close()


def _insert_rows(db, case_id, rows, snapshot_id=None, source_sha=None):
    case = load_case(db, case_id)
    snap = current_snapshot(db, case_id) if snapshot_id is None else \
        db.execute('SELECT * FROM water_snapshot WHERE id=?', (snapshot_id,)).fetchone()
    if snap is None:
        raise WaterError('snapshot_missing', case_id)
    snapshot_id = snap['id']
    cal_versions = json.loads(snap['sensor_versions'])
    out = {'accepted': 0, 'rejected': []}
    last_seq = {r['sensor']: r['seq'] for r in db.execute(
        'SELECT sensor,MAX(seq) AS seq FROM water_telemetry WHERE case_id=? AND status=? GROUP BY sensor',
        (case_id, 'accepted'))}
    commands_exist = db.execute(
        'SELECT 1 FROM water_command WHERE case_id=? LIMIT 1', (case_id,)).fetchone()
    for row in rows:
        prior = db.execute('SELECT * FROM water_telemetry WHERE case_id=? AND sensor=? AND seq=?',
                           (case_id, str(row.get('sensor', '')), row.get('seq'))).fetchone()
        if prior is not None:
            same = (prior['conductivity_milli'] == milli(row.get('conductivity_us_cm'))
                    and prior['ts'] == str(row.get('ts')))
            code = 'sensor_duplicate_ignored' if same else 'sensor_identity_conflict'
            if not same:
                out['rejected'].append({'row': row, 'code': code, 'detail': 'same seq different payload'})
                with db:
                    db.execute('INSERT INTO water_notice(id,case_id,code,detail_json,ts) VALUES (?,?,?,?,?)',
                               (sha256_text(canonical(row) + case_id + code), case_id, code,
                                canonical({'row': row, 'existing_id': prior['id']}), str(row.get('ts', ''))))
            continue
        if commands_exist and str(row.get('snapshot_id', snapshot_id)) != snapshot_id:
            code, detail = 'stale_snapshot', 'late row carrying an older snapshot cannot override commands'
        else:
            code, detail = _validate_row(row, cal_versions, last_seq)
        if code is None:
            parsed = detail
            with db:
                db.execute('INSERT INTO water_telemetry(case_id,sensor,seq,ts,conductivity_milli,flow_ml,'
                           'cal_version,snapshot_id,source_sha,status,detail) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                           (case_id, parsed['sensor'], parsed['seq'], parsed['ts'],
                            parsed['conductivity_milli'], parsed['flow_ml'], parsed['cal_version'],
                            snapshot_id, source_sha, 'accepted', None))
            last_seq[parsed['sensor']] = parsed['seq']
            out['accepted'] += 1
        else:
            with db:
                db.execute('INSERT INTO water_telemetry(case_id,sensor,seq,ts,conductivity_milli,flow_ml,'
                           'cal_version,snapshot_id,source_sha,status,detail) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                           (case_id, str(row.get('sensor', '')), int(row.get('seq', -1)),
                            str(row.get('ts', '')), milli(row.get('conductivity_us_cm', 0)),
                            int(round(float(row.get('flow_l', 0) or 0) * ML_PER_L)),
                            str(row.get('cal_version', 'unknown')), snapshot_id, source_sha,
                            'rejected:' + code, str(detail)))
            out['rejected'].append({'row': row, 'code': code, 'detail': detail})
    return out


def load_frozen_telemetry(db_path, case_id):
    """从冻结的 water_case 原字节解析 RAW/POLISH/RESERVE 行并经同一校验入口采录。"""
    db = connect(db_path)
    ensure_schema(db)
    case = load_case(db, case_id)
    snap = current_snapshot(db, case_id)
    snapshot_id = snap['id']
    rows = []
    for raw in csv.DictReader(io.StringIO(case['sensor_csv'])):
        window = next((w for w in WINDOWS if w[1] == raw['ts']), None)
        item = dict(raw)
        item['window'] = window[0] if window else ''
        item['cal_version'] = DEFAULT_CALIBRATION[raw['sensor']][0]
        item['snapshot_id'] = snap['id']
        rows.append(item)
    db.close()
    return ingest_telemetry(db_path, case_id, rows, snapshot_id=snap['id'],
                            source_sha=case['sensor_sha256'])


def accepted_evidence(db, case_id, sensor, seq=None):
    sql = 'SELECT * FROM water_telemetry WHERE case_id=? AND sensor=? AND status=?'
    params = [case_id, sensor, 'accepted']
    if seq is not None:
        sql += ' AND seq=?'
        params.append(seq)
    sql += ' ORDER BY seq DESC LIMIT 1'
    return db.execute(sql, params).fetchone()


# --------------------------------------------------------------------------- #
# 模拟 PLC：nonce/状态查询、ACK 丢失、断联、晚到流量计、阀互锁、膜故障注入
# --------------------------------------------------------------------------- #
class SimPLC:
    def __init__(self, valve_versions=None, faults=()):
        self.valve_versions = dict(DEFAULT_VALVE_VERSIONS if valve_versions is None else valve_versions)
        self.nonce = 0
        self.open = set()
        self.commands = []
        self.connected = True
        self.faults = set(faults)
        self.drop_next_ack = 'drop_ack' in self.faults
        self.delay_meter = 'delay_meter' in self.faults
        self.membrane_fail_after_ml = None
        self._membrane_passed = 0
        self.pending_meters = {}
        self.probe_overrides = {}

    def query_status(self):
        if not self.connected:
            raise PLCError('plc_offline', 'status query failed')
        return {'nonce': self.nonce, 'open': sorted(self.open),
                'meter_totals': dict(self._meter_totals), 'valve_versions': dict(self.valve_versions)}

    def query_nonce(self):
        return self.query_status()['nonce']

    @property
    def _meter_totals(self):
        return self.__dict__.setdefault('_SimPLC__meter_totals', {})

    def command(self, valve, action):
        if not self.connected:
            raise PLCError('plc_offline', f'{valve}.{action}')
        if valve not in self.valve_versions:
            raise PLCError('unknown_valve', valve)
        if action == 'open' and valve in ('V-DIRECT', 'V-RESERVE'):
            other = 'V-RESERVE' if valve == 'V-DIRECT' else 'V-DIRECT'
            if other in self.open:
                raise PLCInterlock('concurrent_valve', f'{other} already open while opening {valve}')
        self.nonce += 1
        record = {'nonce': self.nonce, 'valve': valve, 'action': action,
                  'version': self.valve_versions[valve]}
        if action == 'open':
            self.open.add(valve)
        elif action == 'close':
            self.open.discard(valve)
        self.commands.append(record)
        if self.drop_next_ack:
            self.drop_next_ack = False
            raise PLCAckLost(record)
        return record

    def dispense(self, valve, ml):
        if not self.connected:
            raise PLCError('plc_offline', f'dispense {valve}')
        if valve not in self.open:
            raise PLCError('valve_not_open', valve)
        flowed = ml
        if self.membrane_fail_after_ml is not None and valve == 'V-DIRECT':
            remaining = self.membrane_fail_after_ml - self._membrane_passed
            if remaining <= 0:
                raise MembraneFailed(0)
            flowed = min(ml, max(remaining, 0))
        reading = {'valve': valve, 'requested_ml': ml, 'ml': flowed, 'pulses': flowed * 10}
        self._meter_totals[valve] = self._meter_totals.get(valve, 0) + flowed
        if valve == 'V-DIRECT':
            self._membrane_passed += flowed
        if self.delay_meter:
            handle = f'meter-{len(self.pending_meters) + 1}'
            self.pending_meters[handle] = reading
            raise MeterPending(handle)
        if flowed < ml:
            raise MembraneFailed(flowed, reading)
        return reading

    def collect_meter(self, handle):
        if handle not in self.pending_meters:
            raise PLCError('meter_handle_unknown', handle)
        return self.pending_meters.pop(handle)

    def read_probe(self, sensor, true_milli):
        if not self.connected:
            raise PLCError('plc_offline', f'probe {sensor}')
        return {'sensor': sensor, 'conductivity_milli': self.probe_overrides.get(sensor, true_milli),
                'cal_version': DEFAULT_CALIBRATION[sensor][0]}


# --------------------------------------------------------------------------- #
# 实物台账：直供膜、储罐、公共管道滞留液（整数毫升 + 质量守恒，塞流模型）
# --------------------------------------------------------------------------- #
def plug_transfer(pipe, in_ml, in_milli):
    """公共管道恒容塞流：推入 in_ml 新水，顶出同体积前段水，返回 (出水段, 新管段)。

    推入量可大于管容：先排空滞留段，余量以新水自身计为出水；
    管内最终只保留新水的最后“管容”毫升。
    """
    pipe_vol = sum(ml for ml, _ in pipe)
    segments = deque([list(s) for s in pipe])
    remaining = in_ml
    out = []
    while remaining > 0 and segments:
        head = segments[0]
        take = min(head[0], remaining)
        out.append([take, head[1]])
        head[0] -= take
        remaining -= take
        if head[0] == 0:
            segments.popleft()
    if remaining > 0:
        out.append([remaining, in_milli])
    final_new_hold = min(in_ml, pipe_vol)
    new_pipe = [[final_new_hold, in_milli]] if final_new_hold else []
    return out, new_pipe


def mix_mass(segments):
    return sum(ml * c for ml, c in segments)


class Ledger:
    def __init__(self, case):
        contract = case['contract']
        reserve = next(s for s in contract['sources'] if s['id'] == 'qualified_reserve')
        self.tank_ml = int(reserve['initial_l'] * ML_PER_L)
        self.tank_mass = self.tank_ml * milli(reserve['initial_conductivity_us_cm'])
        holdup_ml = int(contract['mixed_holdup_l'] * ML_PER_L)
        # 初始滞留液水质锚定 RAW-1 实测行，绝不把未观测水质当零
        self.pipe = [[holdup_ml, milli(10.0)]]
        self.membrane_used = 0
        self.slot_used = {'t0': 0, 't1': 0}
        self.membrane_status = 'ok'
        self.flushed_windows = []
        self.drain_ml = 0
        self.drain_mass = 0
        self.delivered_ml = 0
        self.delivered_mass = 0

    def apply(self, event):
        kind = event['kind']
        d = json.loads(event['detail_json'])
        if kind in ('flush', 'direct_flow'):
            self.membrane_used += d['membrane_delta_ml']
            self.slot_used[d['window']] = self.slot_used.get(d['window'], 0) + d['membrane_delta_ml']
            self.pipe = d['pipe_after']
            self.drain_ml += d.get('drain_delta_ml', 0)
            self.drain_mass += d.get('drain_delta_mass', 0)
            self.delivered_ml += d.get('delivered_delta_ml', 0)
            self.delivered_mass += d.get('delivered_delta_mass', 0)
            if kind == 'flush':
                self.flushed_windows.append(d['window'])
        elif kind == 'reserve_flow':
            self.tank_ml -= d['tank_delta_ml']
            self.tank_mass -= d['tank_delta_mass']
            self.delivered_ml += d.get('delivered_delta_ml', 0)
            self.delivered_mass += d.get('delivered_delta_mass', 0)
            self.drain_ml += d.get('drain_delta_ml', 0)
            self.drain_mass += d.get('drain_delta_mass', 0)
        elif kind == 'membrane_fail':
            self.membrane_status = 'failed'

    @property
    def membrane_remaining(self):
        return MEMBRANE_CAPACITY_ML - self.membrane_used


def rebuild_ledger(db, case_id, case):
    ledger = Ledger(case)
    for ev in db.execute('SELECT * FROM water_ledger WHERE case_id=? ORDER BY id', (case_id,)):
        ledger.apply(ev)
    return ledger


# --------------------------------------------------------------------------- #
# 路线计划：执行前冻结，两条路线不可在同一版本混用
# --------------------------------------------------------------------------- #
def build_plan(route):
    if route == 'direct_first':
        return {'route': route, 'steps': [
            {'id': 'df-1-flush', 'op': 'flush', 'window': 't0', 'ts': '2026-11-17T08:02:00+08:00', 'ml': 1000},
            {'id': 'df-2-w1', 'op': 'deliver_direct', 'window': 't0', 'ts': '2026-11-17T08:04:00+08:00',
             'order': 'W1', 'ml': 3000},
            {'id': 'df-3-w2', 'op': 'deliver_direct', 'window': 't1', 'ts': '2026-11-17T08:12:00+08:00',
             'order': 'W2', 'ml': 2000}]}
    if route == 'reserve_first':
        return {'route': route, 'steps': [
            {'id': 'rf-1-w1', 'op': 'deliver_reserve', 'window': 't0', 'ts': '2026-11-17T08:03:00+08:00',
             'order': 'W1', 'ml': 3000},
            {'id': 'rf-2-flush', 'op': 'flush', 'window': 't1', 'ts': '2026-11-17T08:11:00+08:00', 'ml': 1000},
            {'id': 'rf-3-w2', 'op': 'deliver_direct', 'window': 't1', 'ts': '2026-11-17T08:14:00+08:00',
             'order': 'W2', 'ml': 2000}]}
    raise WaterError('route_not_allowed', route)


def freeze_plan(db_path, case_id, route, frozen_ts='2026-11-17T07:58:00+08:00'):
    db = connect(db_path)
    ensure_schema(db)
    try:
        case = load_case(db, case_id)
        if route not in case['contract'].get('allowed_routes', []):
            raise WaterError('route_not_allowed', route)
        plan = build_plan(route)
        _validate_plan(case, plan)
        snap = current_snapshot(db, case_id)
        plan_sha = sha256_text(canonical(plan))
        with db:
            existing = db.execute('SELECT route FROM water_active_plan WHERE case_id=?', (case_id,)).fetchone()
            if existing and existing['route'] != route:
                raise WaterError('route_already_frozen', f"{existing['route']} active; cannot mix {route}")
            db.execute('INSERT OR IGNORE INTO water_plan VALUES (?,?,?,?,?,?)',
                       (case_id, route, frozen_ts, canonical(plan), plan_sha, snap['id']))
            db.execute('INSERT OR IGNORE INTO water_active_plan VALUES (?,?,?)', (case_id, route, frozen_ts))
        return {'case_id': case_id, 'route': route, 'plan_sha256': plan_sha}
    finally:
        db.close()


def _validate_plan(case, plan):
    orders = {o['id']: o for o in case['contract']['orders']}
    required = {oid: int(o['liters'] * ML_PER_L) for oid, o in orders.items()}
    planned = {oid: 0 for oid in required}
    membrane_need = 0
    slot_need = {'t0': 0, 't1': 0}
    tank_need = 0
    flushed = False
    for step in plan['steps']:
        if step['op'] in ('flush', 'deliver_direct'):
            # 首次使用直供路径前必须先排空公共管道滞留液
            if step['op'] == 'deliver_direct' and not flushed:
                raise WaterError('plan_invalid', f"{step['id']}: direct delivery before holdup flush")
            if step['op'] == 'flush':
                flushed = True
            membrane_need += step['ml']
            slot_need[step['window']] += step['ml']
        elif step['op'] == 'deliver_reserve':
            tank_need += step['ml']
        if 'order' in step:
            if step['order'] not in orders:
                raise WaterError('plan_invalid', f"unknown order {step['order']}")
            if parse_ts(step['ts']) > parse_ts(orders[step['order']]['ts']):
                raise WaterError('deadline_violation', f"{step['id']} after {step['order']} deadline")
            planned[step['order']] += step['ml']
    if planned != required:
        raise WaterError('plan_invalid', f'coverage {planned} != required {required}')
    for window, need in slot_need.items():
        if need > SLOT_CAP_ML[window]:
            raise WaterError('slot_capacity_exceeded', f'{window}: {need} > {SLOT_CAP_ML[window]}')
    if membrane_need > MEMBRANE_CAPACITY_ML:
        raise WaterError('membrane_capacity_exceeded', f'{membrane_need} > {MEMBRANE_CAPACITY_ML}')
    reserve = next(s for s in case['contract']['sources'] if s['id'] == 'qualified_reserve')
    if tank_need > int(reserve['initial_l'] * ML_PER_L):
        raise WaterError('reserve_capacity_exceeded', str(tank_need))
    return True


# --------------------------------------------------------------------------- #
# 订单签收占用：两工人抢同一订单只有一个成功
# --------------------------------------------------------------------------- #
def claim_order(db_path, case_id, order_id, worker, ts='2026-11-17T07:59:30+08:00'):
    db = connect(db_path)
    ensure_schema(db)
    try:
        case = load_case(db, case_id)
        if not any(o['id'] == order_id for o in case['contract']['orders']):
            raise WaterError('order_unknown', order_id)
        snap = current_snapshot(db, case_id)
        with db:
            try:
                db.execute('INSERT INTO water_claim VALUES (?,?,?,?,?)',
                           (case_id, order_id, worker, ts, snap['id']))
            except Exception:
                row = db.execute('SELECT worker FROM water_claim WHERE case_id=? AND order_id=?',
                                 (case_id, order_id)).fetchone()
                raise WaterError('order_already_claimed', f'{order_id} held by {row["worker"]}')
        return {'case_id': case_id, 'order_id': order_id, 'worker': worker}
    finally:
        db.close()


# --------------------------------------------------------------------------- #
# 执行引擎：source SHA → 水质证据 → 阀命令 → 流量计 → 订单回执
# --------------------------------------------------------------------------- #
def _window_evidence(db, case_id, window, sensor='POLISH-1'):
    seq = next(w[2] for w in WINDOWS if w[0] == window)
    row = accepted_evidence(db, case_id, sensor, seq)
    if row is None:
        raise WaterError('evidence_missing', f'{sensor} seq {seq} for {window}')
    return row


def _record_command(db, case_id, plc, valve, action, step_id, snap):
    ts = datetime.now().astimezone().isoformat()
    try:
        rec = plc.command(valve, action)
        ack = 'ack'
        detail = None
    except PLCAckLost as lost:
        rec = lost.record
        status = plc.query_status()
        ack = f'lost_reconciled_nonce_{rec["nonce"]}'
        detail = canonical({'recovered_by_status_query': status})
    cid = f'{case_id}:cmd:{rec["nonce"]}'
    with db:
        db.execute('INSERT OR IGNORE INTO water_command VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                   (cid, case_id, rec['nonce'], valve, action, rec['version'], snap['id'],
                    step_id, ts, ack, detail))
    return cid, rec


def _append_meter(db, case_id, step_id, reading, status='settled', handle=None):
    with db:
        db.execute('INSERT INTO water_meter(case_id,step_id,valve,ml,requested_ml,pulses,ts,status,handle)'
                   ' VALUES (?,?,?,?,?,?,?,?,?)',
                   (case_id, step_id, reading['valve'], reading['ml'], reading['requested_ml'],
                    reading['pulses'], datetime.now().astimezone().isoformat(), status, handle))


def _append_ledger(db, case_id, step_id, kind, detail, handle=None):
    with db:
        db.execute('INSERT INTO water_ledger(case_id,step_id,kind,ts,detail_json,handle)'
                   ' VALUES (?,?,?,?,?,?)',
                   (case_id, step_id, kind, datetime.now().astimezone().isoformat(),
                    canonical(detail), handle))


def _ledger_has(db, case_id, step_id, kind):
    return db.execute('SELECT 1 FROM water_ledger WHERE case_id=? AND step_id=? AND kind=? LIMIT 1',
                      (case_id, step_id, kind)).fetchone() is not None


def _persist_delivery(db, case_id, order, route, kind, seq, source, window, ml, mass, milli_v,
                      receipt, snap, ts):
    did = f'{case_id}:{order}:{route}:{kind}:{seq}'
    with db:
        db.execute('INSERT OR IGNORE INTO water_delivery VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (did, case_id, order, route, kind, seq, source, window, ml, mass, milli_v,
                    1, canonical(receipt), snap['id'], ts))
    return did


def execute_plan(db_path, case_id, plc):
    db = connect(db_path)
    ensure_schema(db)
    try:
        case = load_case(db, case_id)
        snap = current_snapshot(db, case_id)
        active = db.execute('SELECT * FROM water_active_plan WHERE case_id=?', (case_id,)).fetchone()
        if active is None:
            raise WaterError('plan_not_frozen', case_id)
        plan_row = db.execute('SELECT plan_json FROM water_plan WHERE case_id=? AND route=?',
                              (case_id, active['route'])).fetchone()
        plan = json.loads(plan_row['plan_json'])
        orders = {o['id']: o for o in case['contract']['orders']}
        for oid in orders:
            if not db.execute('SELECT 1 FROM water_claim WHERE case_id=? AND order_id=?',
                              (case_id, oid)).fetchone():
                with db:
                    db.execute('INSERT OR IGNORE INTO water_claim VALUES (?,?,?,?,?)',
                               (case_id, oid, 'water-engine', '2026-11-17T07:59:30+08:00', snap['id']))
        ledger = rebuild_ledger(db, case_id, case)
        result = {'case_id': case_id, 'route': plan['route'], 'steps': [], 'awaiting_meter': False}
        for step in plan['steps']:
            if _ledger_has(db, case_id, step['id'], 'done'):
                result['steps'].append({'step': step['id'], 'status': 'already_done'})
                continue
            if step['op'] == 'flush':
                _do_flush(db, case_id, plc, step, snap, case, ledger)
            elif step['op'] == 'deliver_direct':
                _do_deliver_direct(db, case_id, plc, step, snap, case, ledger, orders)
            elif step['op'] == 'deliver_reserve':
                _do_deliver_reserve(db, case_id, plc, step, snap, case, ledger, orders)
            result['steps'].append({'step': step['id'], 'status': 'done'})
        return result
    except MeterPending as pending:
        return {'case_id': case_id, 'route': case_id and _active_route(db, case_id),
                'awaiting_meter': True, 'handle': pending.handle, 'steps': []}
    finally:
        db.close()


def _active_route(db, case_id):
    row = db.execute('SELECT route FROM water_active_plan WHERE case_id=?', (case_id,)).fetchone()
    return row['route'] if row else None


def _do_flush(db, case_id, plc, step, snap, case, ledger):
    window = step['window']
    if window in ledger.flushed_windows:
        _append_ledger(db, case_id, step['id'], 'done', {'reason': 'flush already recorded'})
        return
    polish = _window_evidence(db, case_id, window)
    in_milli = polish['conductivity_milli']
    if ledger.membrane_status != 'ok':
        raise MembraneFailed(0)
    if ledger.slot_used.get(window, 0) + step['ml'] > SLOT_CAP_ML[window]:
        raise WaterError('slot_capacity_exceeded', step['id'])
    if ledger.membrane_remaining < step['ml']:
        raise WaterError('membrane_capacity_exceeded', step['id'])
    cids = []
    for valve in ('V-DIRECT', 'V-DRAIN'):
        cids.append(_record_command(db, case_id, plc, valve, 'open', step['id'], snap)[0])
    reading = _dispense_with_meter(db, case_id, plc, 'V-DIRECT', step['ml'], step['id'])
    for valve in ('V-DRAIN', 'V-DIRECT'):
        cids.append(_record_command(db, case_id, plc, valve, 'close', step['id'], snap)[0])
    out_segments, pipe_after = plug_transfer(ledger.pipe, reading['ml'], in_milli)
    drain_mass = mix_mass(out_segments)
    probe = plc.read_probe('PIPE-1', pipe_after[0][1])
    probe_row = {'sensor': 'PIPE-1', 'seq': 100 + (1 if window == 't0' else 2),
                 'ts': step['ts'], 'conductivity_us_cm': us_cm(probe['conductivity_milli']),
                 'flow_l': 0.0, 'cal_version': probe['cal_version'], 'window': window,
                 'snapshot_id': snap['id']}
    inserted = _insert_rows(db, case_id, [probe_row], snapshot_id=snap['id'])
    if inserted['rejected']:
        raise WaterError('holdup_retest_rejected', canonical(inserted['rejected'][0]))
    retest = accepted_evidence(db, case_id, 'PIPE-1', probe_row['seq'])
    if retest is None:
        raise WaterError('holdup_retest_rejected', step['id'])
    limit_milli = min(milli(o['max_conductivity_us_cm']) for o in case['contract']['orders'])
    if retest['conductivity_milli'] > limit_milli:
        raise WaterError('holdup_retest_over_limit',
                         f"{retest['conductivity_milli']} > {limit_milli}")
    ledger.membrane_used += reading['ml']
    ledger.slot_used[window] += reading['ml']
    ledger.pipe = pipe_after
    ledger.drain_ml += reading['ml']
    ledger.drain_mass += drain_mass
    ledger.flushed_windows.append(window)
    detail = {'window': window, 'membrane_delta_ml': reading['ml'], 'in_milli': in_milli,
              'out_segments': out_segments, 'pipe_after': pipe_after,
              'drain_delta_ml': reading['ml'], 'drain_delta_mass': drain_mass,
              'commands': cids, 'meter_ml': reading['ml'], 'retest_telemetry_id': retest['id'],
              'polish_telemetry_id': polish['id']}
    _append_ledger(db, case_id, step['id'], 'flush', detail)
    _append_ledger(db, case_id, step['id'], 'done', detail)


def _quality_gate(out_segments, limit_milli, ml):
    mass = mix_mass(out_segments)
    return mass, mass <= limit_milli * ml


def _do_deliver_direct(db, case_id, plc, step, snap, case, ledger, orders):
    window = step['window']
    order = orders[step['order']]
    limit_milli = milli(order['max_conductivity_us_cm'])
    polish = _window_evidence(db, case_id, window)
    in_milli = polish['conductivity_milli']
    prospective, _ = plug_transfer(ledger.pipe, step['ml'], in_milli)
    prospective_mass, prospective_ok = _quality_gate(prospective, limit_milli, step['ml'])
    if not ledger.flushed_windows and not prospective_ok:
        # 简单补丁失败点：只看 POLISH 合格探头而无视公共管道高电导滞留液
        raise WaterError('holdup_unverified',
                         canonical({'step': step['id'], 'prospective_segments': prospective,
                                    'prospective_mass': prospective_mass,
                                    'prospective_conductivity_milli': prospective_mass // step['ml'],
                                    'limit_milli': limit_milli, 'would_pass': prospective_ok}))
    if ledger.membrane_status != 'ok':
        raise MembraneFailed(0)
    if ledger.slot_used.get(window, 0) + step['ml'] > SLOT_CAP_ML[window]:
        raise WaterError('slot_capacity_exceeded', step['id'])
    if ledger.membrane_remaining < step['ml']:
        raise WaterError('membrane_capacity_exceeded', step['id'])
    # 入膜/入管水源本身超限即不合格：不允许靠与滞留液混合稀释后“凑”过整单限值
    source_over = in_milli > limit_milli
    out_segments, pipe_after = plug_transfer(ledger.pipe, step['ml'], in_milli)
    mass, ok = _quality_gate(out_segments, limit_milli, step['ml'])
    if source_over or not ok:
        # 水质过限只能排水：直供阀与排水阀同时开，绝不开向收水端，也不回写已交付水
        _record_command(db, case_id, plc, 'V-DIRECT', 'open', step['id'], snap)
        _record_command(db, case_id, plc, 'V-DRAIN', 'open', step['id'], snap)
        reading = _dispense_with_meter(db, case_id, plc, 'V-DIRECT', step['ml'], step['id'], drain=True)
        _record_command(db, case_id, plc, 'V-DRAIN', 'close', step['id'], snap)
        _record_command(db, case_id, plc, 'V-DIRECT', 'close', step['id'], snap)
        ledger.membrane_used += reading['ml']
        ledger.slot_used[window] += reading['ml']
        ledger.pipe = pipe_after
        ledger.drain_ml += reading['ml']
        ledger.drain_mass += mass
        _append_ledger(db, case_id, step['id'], 'direct_flow',
                       {'window': window, 'membrane_delta_ml': reading['ml'], 'in_milli': in_milli,
                        'out_segments': out_segments, 'pipe_after': pipe_after,
                        'drain_delta_ml': reading['ml'], 'drain_delta_mass': mass, 'diverted': True,
                        'source_over_limit': source_over})
        _notice(db, case_id, 'quality_over_limit', None, step['order'],
                {'step': step['id'], 'mass': mass, 'limit_mass': limit_milli * step['ml'],
                 'incoming_milli': in_milli, 'source_over_limit': source_over})
        raise WaterError('quality_over_limit', step['id'])
    cid = _record_command(db, case_id, plc, 'V-DIRECT', 'open', step['id'], snap)[0]
    try:
        reading = _dispense_with_meter(db, case_id, plc, 'V-DIRECT', step['ml'], step['id'])
    except MembraneFailed as failed:
        _record_command(db, case_id, plc, 'V-DIRECT', 'close', step['id'], snap)
        _handle_membrane_failure(db, case_id, plc, step, snap, case, ledger, failed, in_milli, orders)
        raise
    _record_command(db, case_id, plc, 'V-DIRECT', 'close', step['id'], snap)
    if reading['ml'] != step['ml']:
        raise WaterError('meter_shortfall', f"{reading['ml']} < {step['ml']}")
    ledger.membrane_used += reading['ml']
    ledger.slot_used[window] += reading['ml']
    ledger.pipe = pipe_after
    ledger.delivered_ml += reading['ml']
    ledger.delivered_mass += mass
    receipt = _receipt(case, snap, [polish['id']], [cid], reading, step, order,
                       {'out_segments': out_segments, 'mass': mass, 'limit_mass': limit_milli * step['ml'],
                        'conductivity_milli': mass // step['ml'], 'source': 'polished_direct'})
    seq = _next_delivery_seq(db, case_id, step['order'])
    did = _persist_delivery(db, case_id, step['order'], _active_route(db, case_id), 'delivery', seq,
                            'polished_direct', window, reading['ml'], mass, mass // step['ml'],
                            receipt, snap, step['ts'])
    _append_ledger(db, case_id, step['id'], 'direct_flow',
                   {'window': window, 'membrane_delta_ml': reading['ml'], 'in_milli': in_milli,
                    'out_segments': out_segments, 'pipe_after': pipe_after,
                    'delivered_delta_ml': reading['ml'], 'delivered_delta_mass': mass,
                    'delivery_id': did})
    _append_ledger(db, case_id, step['id'], 'done', {'delivery_id': did})


def _do_deliver_reserve(db, case_id, plc, step, snap, case, ledger, orders):
    order = orders[step['order']]
    limit_milli = milli(order['max_conductivity_us_cm'])
    reserve_row = accepted_evidence(db, case_id, 'RESERVE-1', 1)
    if reserve_row is None:
        raise WaterError('evidence_missing', 'RESERVE-1')
    contract_c = next(s for s in case['contract']['sources']
                      if s['id'] == 'qualified_reserve')['initial_conductivity_us_cm']
    if reserve_row['conductivity_milli'] != milli(contract_c):
        raise WaterError('source_conflict',
                         f"RESERVE-1 {reserve_row['conductivity_milli']} != contract {milli(contract_c)}")
    if ledger.tank_ml < step['ml']:
        raise WaterError('reserve_capacity_exceeded', step['id'])
    mass = step['ml'] * reserve_row['conductivity_milli']
    if mass > limit_milli * step['ml']:
        raise WaterError('quality_over_limit', step['id'])
    cid = _record_command(db, case_id, plc, 'V-RESERVE', 'open', step['id'], snap)[0]
    reading = _dispense_with_meter(db, case_id, plc, 'V-RESERVE', step['ml'], step['id'])
    _record_command(db, case_id, plc, 'V-RESERVE', 'close', step['id'], snap)
    ledger.tank_ml -= reading['ml']
    ledger.tank_mass -= mass
    ledger.delivered_ml += reading['ml']
    ledger.delivered_mass += mass
    receipt = _receipt(case, snap, [reserve_row['id']], [cid], reading, step, order,
                       {'mass': mass, 'limit_mass': limit_milli * step['ml'],
                        'conductivity_milli': reserve_row['conductivity_milli'],
                        'source': 'qualified_reserve'})
    seq = _next_delivery_seq(db, case_id, step['order'])
    did = _persist_delivery(db, case_id, step['order'], _active_route(db, case_id), 'delivery', seq,
                            'qualified_reserve', None, reading['ml'], mass,
                            reserve_row['conductivity_milli'], receipt, snap, step['ts'])
    _append_ledger(db, case_id, step['id'], 'reserve_flow',
                   {'tank_delta_ml': reading['ml'], 'tank_delta_mass': mass,
                    'delivered_delta_ml': reading['ml'], 'delivered_delta_mass': mass, 'delivery_id': did})
    _append_ledger(db, case_id, step['id'], 'done', {'delivery_id': did})


def _dispense_with_meter(db, case_id, plc, valve, ml, step_id, drain=False):
    pending = db.execute('SELECT id,handle FROM water_ledger WHERE case_id=? AND step_id=? AND kind=? LIMIT 1',
                         (case_id, step_id, 'flow_pending')).fetchone()
    if pending:
        reading = plc.collect_meter(pending['handle'])
        _append_meter(db, case_id, step_id, reading, status='settled', handle=pending['handle'])
        with db:
            db.execute("UPDATE water_ledger SET kind='flow_settled' WHERE id=?", (pending['id'],))
        return reading
    try:
        reading = plc.dispense(valve, ml)
    except MeterPending as p:
        _append_ledger(db, case_id, step_id, 'flow_pending', {'valve': valve, 'requested_ml': ml},
                       handle=p.handle)
        raise
    else:
        _append_meter(db, case_id, step_id, reading)
        return reading


def _handle_membrane_failure(db, case_id, plc, step, snap, case, ledger, failed, in_milli, orders):
    partial = failed.reading or {'valve': 'V-DIRECT', 'requested_ml': step['ml'],
                                 'ml': failed.flowed_ml, 'pulses': failed.flowed_ml * 10}
    if partial['ml']:
        _append_meter(db, case_id, step['id'], partial)
        out_segments, pipe_after = plug_transfer(ledger.pipe, partial['ml'], in_milli)
        mass = mix_mass(out_segments)
        ledger.membrane_used += partial['ml']
        ledger.slot_used[step['window']] += partial['ml']
        ledger.pipe = pipe_after
        order = orders[step['order']]
        limit_milli = milli(order['max_conductivity_us_cm'])
        if mass <= limit_milli * partial['ml'] and in_milli <= limit_milli:
            ledger.delivered_ml += partial['ml']
            ledger.delivered_mass += mass
            receipt = _receipt(case, snap, [], [], partial, step, order,
                               {'partial': True, 'mass': mass, 'source': 'polished_direct',
                                'membrane_failed': True})
            seq = _next_delivery_seq(db, case_id, step['order'])
            did = _persist_delivery(db, case_id, step['order'], _active_route(db, case_id), 'delivery', seq,
                                    'polished_direct', step['window'], partial['ml'], mass,
                                    mass // partial['ml'], receipt, snap, step['ts'])
            _append_ledger(db, case_id, step['id'], 'direct_flow',
                           {'window': step['window'], 'membrane_delta_ml': partial['ml'],
                            'in_milli': in_milli, 'out_segments': out_segments, 'pipe_after': pipe_after,
                            'delivered_delta_ml': partial['ml'], 'delivered_delta_mass': mass,
                            'delivery_id': did, 'partial': True})
        else:
            ledger.drain_ml += partial['ml']
            ledger.drain_mass += mass
            _append_ledger(db, case_id, step['id'], 'direct_flow',
                           {'window': step['window'], 'membrane_delta_ml': partial['ml'],
                            'drain_delta_ml': partial['ml'], 'drain_delta_mass': mass,
                            'pipe_after': pipe_after, 'diverted': True})
    ledger.membrane_status = 'failed'
    _append_ledger(db, case_id, step['id'], 'membrane_fail', {'flowed_ml': partial['ml']})
    _notice(db, case_id, 'membrane_failed', None, step['order'],
            {'step': step['id'], 'flowed_ml': partial['ml'], 'requested_ml': step['ml']})


def _receipt(case, snap, telemetry_ids, command_ids, reading, step, order, quality):
    return {'case_id': case['contract']['lab'], 'contract_sha256': case['contract_sha256'],
            'sensor_sha256': case['sensor_sha256'], 'snapshot_id': snap['id'],
            'order_version': snap['order_version'], 'order_id': order['id'],
            'step_id': step['id'], 'window': step.get('window'),
            'telemetry_ids': telemetry_ids, 'command_ids': command_ids,
            'meter': {'valve': reading['valve'], 'ml': reading['ml'], 'pulses': reading['pulses']},
            'quality': quality,
            'chain': ['source_sha', 'water_telemetry', 'water_command', 'water_meter', 'water_delivery']}


def _next_delivery_seq(db, case_id, order_id):
    row = db.execute('SELECT COALESCE(MAX(seq),0)+1 AS seq FROM water_delivery WHERE case_id=? AND order_id=?',
                     (case_id, order_id)).fetchone()
    return row['seq']


def _notice(db, case_id, code, delivery_id, order_id, detail):
    nid = sha256_text(canonical({'code': code, 'detail': detail, 'order': order_id,
                                 'delivery': delivery_id}) + case_id)
    with db:
        db.execute('INSERT OR IGNORE INTO water_notice VALUES (?,?,?,?,?,?,?)',
                   (nid, case_id, code, delivery_id, order_id, canonical(detail),
                    datetime.now().astimezone().isoformat()))
    return nid


# --------------------------------------------------------------------------- #
# 安全门演示：无视滞留液、只凭合格 POLISH 探头直接开阀的简单补丁必然失败
# --------------------------------------------------------------------------- #
def attempt_naive_direct(db_path, case_id, order_id, ml, window, plc):
    db = connect(db_path)
    ensure_schema(db)
    try:
        case = load_case(db, case_id)
        snap = current_snapshot(db, case_id)
        order = next(o for o in case['contract']['orders'] if o['id'] == order_id)
        ledger = rebuild_ledger(db, case_id, case)
        polish = _window_evidence(db, case_id, window)
        limit_milli = milli(order['max_conductivity_us_cm'])
        prospective, _ = plug_transfer(ledger.pipe, ml, polish['conductivity_milli'])
        mass, ok = _quality_gate(prospective, limit_milli, ml)
        # 安全门拦截：改向排水，绝不写入交付事实
        step = {'id': f'naive-{order_id}-{window}', 'op': 'deliver_direct', 'window': window,
                'ts': '2026-11-17T08:04:30+08:00', 'order': order_id, 'ml': ml}
        cids = [_record_command(db, case_id, plc, 'V-DIRECT', 'open', step['id'], snap)[0],
                _record_command(db, case_id, plc, 'V-DRAIN', 'open', step['id'], snap)[0]]
        reading = _dispense_with_meter(db, case_id, plc, 'V-DIRECT', ml, step['id'], drain=True)
        _record_command(db, case_id, plc, 'V-DRAIN', 'close', step['id'], snap)
        _record_command(db, case_id, plc, 'V-DIRECT', 'close', step['id'], snap)
        out_segments, pipe_after = plug_transfer(ledger.pipe, reading['ml'], polish['conductivity_milli'])
        ledger.membrane_used += reading['ml']
        ledger.slot_used[window] = ledger.slot_used.get(window, 0) + reading['ml']
        ledger.pipe = pipe_after
        ledger.drain_ml += reading['ml']
        ledger.drain_mass += mass
        _append_ledger(db, case_id, step['id'], 'direct_flow',
                       {'window': window, 'membrane_delta_ml': reading['ml'],
                        'in_milli': polish['conductivity_milli'], 'out_segments': out_segments,
                        'pipe_after': pipe_after, 'drain_delta_ml': reading['ml'],
                        'drain_delta_mass': mass, 'diverted': True, 'naive': True})
        nid = _notice(db, case_id, 'holdup_unverified' if not ok else 'naive_diverted', None, order_id,
                      {'prospective_segments': prospective, 'prospective_mass': mass,
                       'prospective_conductivity_milli': mass // ml, 'limit_milli': limit_milli,
                       'commands': cids, 'drained_ml': reading['ml']})
        if not ok:
            raise WaterError('holdup_unverified',
                             canonical({'notice': nid, 'prospective_conductivity_milli': mass // ml,
                                        'limit_milli': limit_milli, 'drained_ml': reading['ml']}))
        return {'notice': nid, 'drained_ml': reading['ml']}
    finally:
        db.close()


# --------------------------------------------------------------------------- #
# 召回与补水：保留已交付事实，以新单据处理，重算后续订单与膜寿命
# --------------------------------------------------------------------------- #
def retest_delivery(db_path, case_id, delivery_id, measured_us_cm, plc, ts='2026-11-17T09:00:00+08:00'):
    db = connect(db_path)
    ensure_schema(db)
    try:
        case = load_case(db, case_id)
        snap = current_snapshot(db, case_id)
        delivery = db.execute('SELECT * FROM water_delivery WHERE id=?', (delivery_id,)).fetchone()
        if delivery is None:
            raise WaterError('delivery_unknown', delivery_id)
        order = next(o for o in case['contract']['orders'] if o['id'] == delivery['order_id'])
        measured = milli(measured_us_cm)
        # 确定性序号：复测行从 200 起按已采录 PIPE-1 行数递增，不依赖进程随机 hash
        base = db.execute("SELECT COALESCE(MAX(seq),199) AS m FROM water_telemetry "
                          "WHERE case_id=? AND sensor='PIPE-1' AND seq>=200", (case_id,)).fetchone()['m']
        seq = max(200, base + 1)
        probe = plc.read_probe('PIPE-1', measured)
        row = {'sensor': 'PIPE-1', 'seq': seq, 'ts': ts,
               'conductivity_us_cm': us_cm(probe['conductivity_milli']), 'flow_l': 0.0,
               'cal_version': probe['cal_version'], 'snapshot_id': snap['id']}
        inserted = _insert_rows(db, case_id, [row], snapshot_id=snap['id'])
        if inserted['rejected']:
            raise WaterError('retest_row_rejected', canonical(inserted['rejected'][0]))
        recorded = accepted_evidence(db, case_id, 'PIPE-1', seq)
        limit_milli = milli(order['max_conductivity_us_cm'])
        if recorded['conductivity_milli'] > limit_milli:
            nid = _notice(db, case_id, 'recall_over_limit', delivery_id, delivery['order_id'],
                          {'measured_milli': recorded['conductivity_milli'], 'limit_milli': limit_milli,
                           'telemetry_id': recorded['id'], 'original_delivery_preserved': True})
            return {'recall': nid, 'delivery_id': delivery_id, 'measured_us_cm': measured_us_cm}
        return {'recall': None, 'delivery_id': delivery_id, 'measured_us_cm': measured_us_cm}
    finally:
        db.close()


def makeup_delivery(db_path, case_id, order_id, ml, source, plc, ts='2026-11-17T09:10:00+08:00'):
    db = connect(db_path)
    ensure_schema(db)
    try:
        case = load_case(db, case_id)
        snap = current_snapshot(db, case_id)
        order = next(o for o in case['contract']['orders'] if o['id'] == order_id)
        limit_milli = milli(order['max_conductivity_us_cm'])
        ledger = rebuild_ledger(db, case_id, case)
        step = {'id': f'makeup-{order_id}-{source}', 'op': 'makeup', 'window': None,
                'ts': ts, 'order': order_id, 'ml': ml}
        if source == 'qualified_reserve':
            reserve_row = accepted_evidence(db, case_id, 'RESERVE-1', 1)
            if reserve_row is None:
                raise WaterError('evidence_missing', 'RESERVE-1')
            if ledger.tank_ml < ml:
                raise WaterError('reserve_capacity_exceeded', str(ml))
            mass = ml * reserve_row['conductivity_milli']
            if mass > limit_milli * ml:
                raise WaterError('quality_over_limit', 'makeup reserve')
            cid = _record_command(db, case_id, plc, 'V-RESERVE', 'open', step['id'], snap)[0]
            reading = _dispense_with_meter(db, case_id, plc, 'V-RESERVE', ml, step['id'])
            _record_command(db, case_id, plc, 'V-RESERVE', 'close', step['id'], snap)
            ledger.tank_ml -= ml
            ledger.tank_mass -= mass
            ledger.delivered_ml += ml
            ledger.delivered_mass += mass
            receipt = _receipt(case, snap, [reserve_row['id']], [cid], reading, step, order,
                               {'mass': mass, 'source': 'qualified_reserve', 'makeup': True})
            seq = _next_delivery_seq(db, case_id, order_id)
            did = _persist_delivery(db, case_id, order_id, _active_route(db, case_id), 'makeup', seq,
                                    'qualified_reserve', None, ml, mass,
                                    reserve_row['conductivity_milli'], receipt, snap, ts)
            _append_ledger(db, case_id, step['id'], 'reserve_flow',
                           {'tank_delta_ml': ml, 'tank_delta_mass': mass,
                            'delivered_delta_ml': ml, 'delivered_delta_mass': mass, 'delivery_id': did})
            return {'delivery_id': did, 'tank_remaining_ml': ledger.tank_ml}
        if source == 'polished_direct':
            if ledger.membrane_status != 'ok':
                raise MembraneFailed(0)
            if ledger.membrane_remaining < ml:
                raise WaterError('membrane_capacity_exceeded', f'makeup needs {ml}, remains {ledger.membrane_remaining}')
            if not ledger.flushed_windows:
                raise WaterError('holdup_unverified', 'common pipe never flushed; cannot makeup via direct')
            latest = WINDOWS[-1]
            polish = _window_evidence(db, case_id, latest[0])
            in_milli = polish['conductivity_milli']
            out_segments, pipe_after = plug_transfer(ledger.pipe, ml, in_milli)
            mass, ok = _quality_gate(out_segments, limit_milli, ml)
            if in_milli > limit_milli or not ok:
                raise WaterError('quality_over_limit', 'makeup direct')
            cid = _record_command(db, case_id, plc, 'V-DIRECT', 'open', step['id'], snap)[0]
            reading = _dispense_with_meter(db, case_id, plc, 'V-DIRECT', ml, step['id'])
            _record_command(db, case_id, plc, 'V-DIRECT', 'close', step['id'], snap)
            ledger.membrane_used += ml
            ledger.slot_used[latest[0]] = ledger.slot_used.get(latest[0], 0) + ml
            ledger.pipe = pipe_after
            ledger.delivered_ml += ml
            ledger.delivered_mass += mass
            step['window'] = latest[0]
            receipt = _receipt(case, snap, [polish['id']], [cid], reading, step, order,
                               {'mass': mass, 'source': 'polished_direct', 'makeup': True,
                                'out_segments': out_segments})
            seq = _next_delivery_seq(db, case_id, order_id)
            did = _persist_delivery(db, case_id, order_id, _active_route(db, case_id), 'makeup', seq,
                                    'polished_direct', latest[0], ml, mass, mass // ml,
                                    receipt, snap, ts)
            _append_ledger(db, case_id, step['id'], 'direct_flow',
                           {'window': latest[0], 'membrane_delta_ml': ml, 'in_milli': in_milli,
                            'out_segments': out_segments, 'pipe_after': pipe_after,
                            'delivered_delta_ml': ml, 'delivered_delta_mass': mass, 'delivery_id': did})
            return {'delivery_id': did, 'membrane_remaining_ml': ledger.membrane_remaining}
        raise WaterError('source_not_available_for_makeup', source)
    finally:
        db.close()


def order_accounts(db, case_id, case):
    accounts = {}
    for order in case['contract']['orders']:
        oid = order['id']
        required_ml = int(order['liters'] * ML_PER_L)
        rows = db.execute('SELECT * FROM water_delivery WHERE case_id=? AND order_id=? ORDER BY id',
                          (case_id, oid)).fetchall()
        delivered_ml = sum(r['ml'] for r in rows if r['kind'] == 'delivery')
        makeup_ml = sum(r['ml'] for r in rows if r['kind'] == 'makeup')
        recalls = db.execute('SELECT * FROM water_notice WHERE case_id=? AND order_id=? AND code=?',
                             (case_id, oid, 'recall_over_limit')).fetchall()
        recalled_ml = 0
        for notice in recalls:
            d = db.execute('SELECT ml FROM water_delivery WHERE id=?', (notice['delivery_id'],)).fetchone()
            if d:
                recalled_ml += d['ml']
        qualified_ml = delivered_ml - recalled_ml + makeup_ml
        accounts[oid] = {'required_ml': required_ml, 'delivered_ml': delivered_ml,
                         'makeup_ml': makeup_ml, 'recalled_ml': recalled_ml,
                         'qualified_ml': min(qualified_ml, required_ml),
                         'pending_ml': max(required_ml - qualified_ml, 0)}
    return accounts


# --------------------------------------------------------------------------- #
# 报告：每单合格/待核升数、守恒、电导证据、膜状态、命令与 ACK
# --------------------------------------------------------------------------- #
def conservation(db, case_id, case, ledger):
    contract = case['contract']
    reserve = next(s for s in contract['sources'] if s['id'] == 'qualified_reserve')
    tank_initial_ml = int(reserve['initial_l'] * ML_PER_L)
    tank_initial_mass = tank_initial_ml * milli(reserve['initial_conductivity_us_cm'])
    holdup_ml = int(contract['mixed_holdup_l'] * ML_PER_L)
    pipe_initial_mass = holdup_ml * milli(10.0)
    membrane_in_mass = 0
    for window, _ts, seq in WINDOWS:
        row = accepted_evidence(db, case_id, 'POLISH-1', seq)
        used = ledger.slot_used.get(window, 0)
        if used and row:
            membrane_in_mass += used * row['conductivity_milli']
    pipe_final_mass = mix_mass(ledger.pipe)
    volume_in = ledger.membrane_used + tank_initial_ml
    volume_out = ledger.delivered_ml + ledger.drain_ml + ledger.tank_ml
    mass_in = membrane_in_mass + tank_initial_mass + pipe_initial_mass
    mass_out = ledger.delivered_mass + ledger.drain_mass + ledger.tank_mass + pipe_final_mass
    return {
        'volume_ml': {
            'membrane_used': ledger.membrane_used, 'tank_initial': tank_initial_ml,
            'delivered': ledger.delivered_ml, 'drain': ledger.drain_ml,
            'tank_remaining': ledger.tank_ml, 'pipe_volume_held': holdup_ml,
            'in_total': volume_in, 'out_total_plus_remaining': volume_out,
            'balanced': volume_in == volume_out},
        'mass_milli_ml': {
            'membrane_in': membrane_in_mass, 'tank_initial': tank_initial_mass,
            'pipe_initial': pipe_initial_mass, 'delivered': ledger.delivered_mass,
            'drain': ledger.drain_mass, 'tank_remaining': ledger.tank_mass,
            'pipe_final': pipe_final_mass, 'in_total': mass_in,
            'out_total_plus_remaining': mass_out, 'balanced': mass_in == mass_out}}


def water_report(db_path, case_id, out_prefix):
    db = connect(db_path)
    ensure_schema(db)
    try:
        case = load_case(db, case_id)
        snap = current_snapshot(db, case_id)
        ledger = rebuild_ledger(db, case_id, case)
        active = db.execute('SELECT * FROM water_active_plan WHERE case_id=?', (case_id,)).fetchone()
        telemetry = [dict(r) for r in db.execute(
            'SELECT * FROM water_telemetry WHERE case_id=? ORDER BY sensor,seq', (case_id,))]
        for t in telemetry:
            t['conductivity_us_cm'] = us_cm(t.pop('conductivity_milli'))
        commands = [dict(r) for r in db.execute(
            'SELECT * FROM water_command WHERE case_id=? ORDER BY nonce', (case_id,))]
        meters = [dict(r) for r in db.execute(
            'SELECT * FROM water_meter WHERE case_id=? ORDER BY id', (case_id,))]
        deliveries = [dict(r) for r in db.execute(
            'SELECT * FROM water_delivery WHERE case_id=? ORDER BY id', (case_id,))]
        for d in deliveries:
            d['conductivity_us_cm'] = us_cm(d.pop('conductivity_milli'))
            d['liters'] = liters(d['ml'])
            d['receipt'] = json.loads(d.pop('receipt_json'))
        notices = [dict(r) for r in db.execute(
            'SELECT * FROM water_notice WHERE case_id=? ORDER BY id', (case_id,))]
        for n in notices:
            n['detail'] = json.loads(n.pop('detail_json'))
        data = {
            'case_id': case_id, 'lab': case['contract']['lab'],
            'route': active['route'] if active else None,
            'frozen_input': {'contract_sha256': case['contract_sha256'],
                             'sensor_sha256': case['sensor_sha256']},
            'snapshot': {'id': snap['id'], 'sensor_versions': json.loads(snap['sensor_versions']),
                         'valve_versions': json.loads(snap['valve_versions']),
                         'order_version': snap['order_version']},
            'orders': order_accounts(db, case_id, case),
            'membrane': {'capacity_ml': MEMBRANE_CAPACITY_ML, 'used_ml': ledger.membrane_used,
                         'remaining_ml': ledger.membrane_remaining, 'status': ledger.membrane_status,
                         'slot_used_ml': ledger.slot_used, 'slot_capacity_ml': SLOT_CAP_ML,
                         'flushed_windows': ledger.flushed_windows},
            'tank': {'remaining_ml': ledger.tank_ml,
                     'remaining_conductivity_us_cm': us_cm(ledger.tank_mass // ledger.tank_ml)
                     if ledger.tank_ml else None},
            'common_pipe': {'segments': ledger.pipe,
                            'mixed_conductivity_us_cm': us_cm(mix_mass(ledger.pipe) // ledger.pipe[0][0])
                            if ledger.pipe else None},
            'conservation': conservation(db, case_id, case, ledger),
            'telemetry': telemetry, 'commands': commands, 'meters': meters,
            'deliveries': deliveries, 'notices': notices}
        out_prefix = Path(out_prefix)
        out_prefix.parent.mkdir(parents=True, exist_ok=True)
        out_prefix.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        out_prefix.with_suffix('.md').write_text(_render_markdown(data), encoding='utf-8')
        return out_prefix
    finally:
        db.close()


def _render_markdown(data):
    lines = [f"# 纯水实物交付报告（{data['case_id']}）", '',
             f"- 实验室：{data['lab']}；冻结路线：`{data['route']}`",
             f"- 合同 SHA：`{data['frozen_input']['contract_sha256'][:16]}…`；"
             f"传感器 CSV SHA：`{data['frozen_input']['sensor_sha256'][:16]}…`",
             f"- 快照：`{data['snapshot']['id'][:16]}…`；订单版本 {data['snapshot']['order_version']}", '']
    lines.append('## 订单合格/待核水量')
    for oid, a in data['orders'].items():
        lines.append(f"- {oid}：需求 {liters(a['required_ml'])}L，合格 {liters(a['qualified_ml'])}L，"
                     f"待核 {liters(a['pending_ml'])}L，召回 {liters(a['recalled_ml'])}L，"
                     f"补水 {liters(a['makeup_ml'])}L")
    m = data['membrane']
    lines += ['', '## 直供膜状态',
              f"- 容量 {m['capacity_ml']}mL，已用 {m['used_ml']}mL，剩余 {m['remaining_ml']}mL，"
              f"状态 {m['status']}",
              f"- 分窗用量 {m['slot_used_ml']} / 窗容 {m['slot_capacity_ml']}；"
              f"已排空复测窗口 {m['flushed_windows']}"]
    c = data['conservation']
    lines += ['', '## 体积/质量守恒（整数台账）',
              f"- 体积：入 {c['volume_ml']['in_total']}mL = 出+存 {c['volume_ml']['out_total_plus_remaining']}mL"
              f"（balanced={c['volume_ml']['balanced']}）",
              f"- 质量：入 {c['mass_milli_ml']['in_total']} = 出+存 {c['mass_milli_ml']['out_total_plus_remaining']}"
              f"（balanced={c['mass_milli_ml']['balanced']}）",
              f"- 排水 {c['volume_ml']['drain']}mL / 储罐余 {c['volume_ml']['tank_remaining']}mL / "
              f"管道持液 {c['volume_ml']['pipe_volume_held']}mL"]
    lines += ['', '## 电导率证据（含拒收行）']
    for t in data['telemetry']:
        lines.append(f"- {t['ts']} {t['sensor']} seq{t['seq']}：{t['conductivity_us_cm']} µS/cm，"
                     f"校准 {t['cal_version']}，状态 {t['status']}")
    lines += ['', '## PLC 阀命令与 ACK']
    for cmd in data['commands']:
        lines.append(f"- nonce {cmd['nonce']} {cmd['valve']}.{cmd['action']}（{cmd['valve_version']}，"
                     f"步骤 {cmd['step_id']}）→ {cmd['ack']}")
    lines += ['', '## 实物交付回执']
    for d in data['deliveries']:
        q = data['orders'][d['order_id']]
        lines.append(f"- {d['id']}：{d['order_id']} {d['liters']}L，来源 {d['source']}，"
                     f"混水 {d['conductivity_us_cm']} µS/cm，类型 {d['kind']}")
    if data['notices']:
        lines += ['', '## 待核/召回通知']
        for n in data['notices']:
            lines.append(f"- {n['code']}（订单 {n['order_id']}）：{canonical(n['detail'])[:160]}")
    lines.append('')
    return '\n'.join(lines)
