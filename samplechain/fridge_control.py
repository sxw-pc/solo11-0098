"""Freeze real in-fridge samples and sensor readings; plan and control cooling.

The :func:`register` entry point only attests physical evidence (real
``fridge_in`` events, raw sensor bytes and SHA). Planning, execution and late
feedback never rewrite those records or the existing detectability decisions;
they add their own versioned, hash-anchored rows.
"""
from __future__ import annotations
import csv
import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path

from .store import connect
from .ingest import time as parse_ts
from . import solver

PLAN_ACTIVE = 'active'
PLAN_HELD = 'held'
PLAN_SUPERSEDED = 'superseded'
PLAN_COMPLETED = 'completed'
PLAN_INFEASIBLE = 'infeasible'
CMD_PENDING = 'pending'
CMD_EXECUTED = 'executed'
CMD_HELD = 'held'
FEED_ACCEPTED = 'accepted'
FEED_REJECTED = 'rejected'


# --------------------------------------------------------------------------- #
# Evidence registration (original behaviour, unchanged)
# --------------------------------------------------------------------------- #
def register(db_path, case_id, fixture, sensors):
    fixture_path = Path(fixture).resolve()
    sensor_path = Path(sensors).resolve()
    case_raw = fixture_path.read_bytes()
    sensor_raw = sensor_path.read_bytes()
    case = json.loads(case_raw)
    readings = list(csv.DictReader(io.StringIO(sensor_raw.decode('utf-8-sig'))))
    if not case_id or not case.get('zones') or not case.get('contracts'):
        raise ValueError('fridge_case_invalid')
    sensor_by_zone = {r['zone']: r for r in readings}
    if len(sensor_by_zone) != len(case['zones']) or len(readings) != len(case['zones']):
        raise ValueError('fridge_sensor_count')
    db = connect(db_path)
    try:
        observed = []
        for zone in case['zones']:
            sid = zone['sample_id']
            reading = sensor_by_zone.get(zone['id'])
            if reading is None or float(reading['temperature_c']) != zone['initial_c']:
                raise ValueError('fridge_sensor_mismatch')
            sample = db.execute('SELECT id,batch_id FROM sample WHERE id=?', (sid,)).fetchone()
            events = [dict(row) for row in db.execute(
                'SELECT e.id,e.ts,e.device,s.sha256 AS source_sha256 '
                'FROM event e JOIN source s ON s.id=e.source_id '
                'WHERE e.sample_id=? AND e.kind=? ORDER BY e.ts,e.id', (sid, 'fridge_in'))]
            if sample is None or not events:
                raise ValueError('fridge_sample_not_observed')
            observed.append({'zone': zone['id'], 'sample': dict(sample),
                             'fridge_events': events, 'sensor': reading})
        payload = json.dumps({'case': case, 'case_sha256': hashlib.sha256(case_raw).hexdigest(),
                              'sensor_sha256': hashlib.sha256(sensor_raw).hexdigest(),
                              'fixture': str(fixture_path), 'sensors': str(sensor_path),
                              'observed': observed}, sort_keys=True, separators=(',', ':'))
        with db:
            old = db.execute('SELECT payload FROM fridge_case WHERE id=?', (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('fridge_case_conflict')
            db.execute('INSERT OR IGNORE INTO fridge_case VALUES (?,?)', (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'zones': len(observed)}


# --------------------------------------------------------------------------- #
# Internal helpers
# --------------------------------------------------------------------------- #
def _canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def _load_case(db, case_id):
    row = db.execute('SELECT payload FROM fridge_case WHERE id=?', (case_id,)).fetchone()
    if row is None:
        raise ValueError('fridge_case_unknown')
    payload = json.loads(row['payload'])
    return payload, payload['case']


def _latest_plan(db, case_id):
    row = db.execute(
        'SELECT * FROM fridge_plan WHERE case_id=? ORDER BY plan_version DESC LIMIT 1',
        (case_id,)).fetchone()
    return dict(row) if row else None


def _next_version(db, case_id, contract):
    row = db.execute(
        'SELECT COALESCE(MAX(plan_version),0)+1 AS v FROM fridge_plan WHERE case_id=? AND contract=?',
        (case_id, contract)).fetchone()
    return row['v']


def _plan_id(basis):
    # Identity covers only the physical inputs/objectives; lineage pointers and
    # version numbers are metadata so identical inputs reproduce identical ids.
    identity = {k: v for k, v in basis.items() if k not in ('supersedes',)}
    return hashlib.sha256(_canonical(identity).encode('utf-8')).hexdigest()


def _hold(db, case_id, reason, detail, plan_id=None):
    db.execute('INSERT INTO fridge_hold(plan_id,case_id,reason,detail,created_at,cleared) '
               'VALUES (?,?,?,?,?,0)',
               (plan_id, case_id, reason, detail,
                datetime.now(timezone.utc).isoformat()))


def _hold_pending(db, plan_id, case_id, reason, detail):
    """Freeze every unexecuted command and park the active plan for review."""
    db.execute("UPDATE fridge_command SET status=? WHERE plan_id=? AND status=?",
               (CMD_HELD, plan_id, CMD_PENDING))
    db.execute("UPDATE fridge_plan SET status=? WHERE id=?", (PLAN_HELD, plan_id))
    _hold(db, case_id, reason, detail, plan_id)


def _supersede(db, case_id, keep_id, clear_holds=True):
    rows = db.execute(
        "SELECT id FROM fridge_plan WHERE case_id=? AND id!=? AND status IN (?,?,?)",
        (case_id, keep_id, PLAN_ACTIVE, PLAN_HELD, PLAN_INFEASIBLE)).fetchall()
    for row in rows:
        db.execute("UPDATE fridge_plan SET status=? WHERE id=?",
                   (PLAN_SUPERSEDED, row['id']))
        db.execute("UPDATE fridge_command SET status=? WHERE plan_id=? AND status=?",
                   (CMD_HELD, row['id'], CMD_PENDING))
    if clear_holds:
        db.execute("UPDATE fridge_hold SET cleared=1 WHERE case_id=? AND cleared=0",
                   (case_id,))


def _persist_plan(db, case_id, contract, version, basis, result, horizon,
                  prefix_commands, prefix_states, boundary_readings, status):
    plan_id = _plan_id(basis)
    if db.execute('SELECT id FROM fridge_plan WHERE id=?', (plan_id,)).fetchone():
        return plan_id, False
    summary = {
        'contract': contract,
        'status': status,
        'energy_cents': result.get('energy_cents', 0),
        'zone_exposure_c': result.get('zone_exposure_c', {}),
        'max_zone_exposure_c': result.get('max_zone_exposure_c'),
        'total_exposure_c': result.get('total_exposure_c'),
        'total_exposure_limit': result['evidence'].get('total_exposure_cap_c'),
        'actions': result.get('actions', []),
        'periods': horizon,
    }
    db.execute('INSERT INTO fridge_plan VALUES (?,?,?,?,?,?,?,?,?,?)',
               (plan_id, case_id, contract, version, status,
                _canonical(basis), _canonical(summary),
                _canonical(result.get('evidence', {})),
                basis.get('supersedes'), datetime.now(timezone.utc).isoformat()))
    for cmd in prefix_commands:
        db.execute('INSERT INTO fridge_command(plan_id,period,zone_id,action,status,'
                   'tariff_cents,energy_cents) VALUES (?,?,?,?,?,?,?)',
                   (plan_id, cmd['period'], cmd['zone_id'], cmd['action'], cmd['status'],
                    cmd['tariff_cents'], cmd['energy_cents']))
    for st in prefix_states:
        db.execute('INSERT INTO fridge_period_state(plan_id,period,zone_id,temp_before_c,'
                   'temp_after_c,reading_c,exposure_increment_c,cumulative_exposure_c,'
                   'tariff_cents,energy_cents,compressor_zone) '
                   'VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                   (plan_id, st['period'], st['zone_id'], st['temp_before_c'],
                    st['temp_after_c'], st['reading_c'], st['exposure_increment_c'],
                    st['cumulative_exposure_c'], st['tariff_cents'], st['energy_cents'],
                    st['compressor_zone']))
    if result.get('feasible'):
        for t, row in enumerate(result['trace']):
            period = row['period']
            action = result['actions'][t]
            zone_id = None if action == solver.ACTION_IDLE else action
            db.execute('INSERT INTO fridge_command(plan_id,period,zone_id,action,status,'
                       'tariff_cents,energy_cents) VALUES (?,?,?,?,?,?,?)',
                       (plan_id, period, zone_id, action, CMD_PENDING,
                        row['tariff_cents'], row['energy_cents']))
            for zid, zrow in row['zones'].items():
                reading = boundary_readings.get(zid) if period == basis['start_period'] else None
                db.execute('INSERT INTO fridge_period_state(plan_id,period,zone_id,temp_before_c,'
                           'temp_after_c,reading_c,exposure_increment_c,cumulative_exposure_c,'
                           'tariff_cents,energy_cents,compressor_zone) '
                           'VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                           (plan_id, period, zid, zrow['temp_before_c'],
                            zrow['temp_after_c'], reading, zrow['exposure_increment_c'],
                            zrow['cumulative_exposure_c'], row['tariff_cents'],
                            row['energy_cents'], row['compressor_zone']))
    return plan_id, True


def _executed_prefix(db, plan_id, horizon):
    """Immutable physical facts: executed commands and their incurred effects."""
    cmds = [dict(r) for r in db.execute(
        'SELECT * FROM fridge_command WHERE plan_id=? ORDER BY period', (plan_id,))]
    executed = [c['period'] for c in cmds if c['status'] == CMD_EXECUTED]
    if executed != list(range(len(executed))):
        raise ValueError('fridge_execution_gap')
    e = len(executed)
    states = [dict(r) for r in db.execute(
        'SELECT * FROM fridge_period_state WHERE plan_id=? ORDER BY period,zone_id',
        (plan_id,))]
    return e, [c for c in cmds if c['period'] < e], [s for s in states if s['period'] < e]


def states_at_period(db, plan_id, period):
    return [dict(r) for r in db.execute(
        'SELECT * FROM fridge_period_state WHERE plan_id=? AND period=? ORDER BY zone_id',
        (plan_id, period))]


# --------------------------------------------------------------------------- #
# Planning
# --------------------------------------------------------------------------- #
def plan(db_path, case_id, contract):
    db = connect(db_path)
    try:
        payload, case = _load_case(db, case_id)
        if contract not in solver.CONTRACTS or contract not in case['contracts']:
            raise ValueError('fridge_contract_unknown')
        solver.validate_case(case)
        latest = _latest_plan(db, case_id)
        if latest and latest['status'] == PLAN_HELD:
            raise ValueError('fridge_orders_pending_review')
        horizon = len(case['zones'][0]['warming_c'])
        version = _next_version(db, case_id, contract)
        boundary_readings = {}
        prefix_commands, prefix_states = [], []
        start_period, locked_energy = 0, 0
        start_temps = {z['id']: z['initial_c'] for z in case['zones']}
        start_exposure = {z['id']: 0.0 for z in case['zones']}
        supersedes = latest['id'] if latest else None

        if latest and latest['status'] in (PLAN_ACTIVE, PLAN_INFEASIBLE):
            e, prefix_commands, prefix_states = _executed_prefix(db, latest['id'], horizon)
            if e > 0:
                start_period = e
                locked_energy = sum(c['energy_cents'] for c in prefix_commands)
                for s in states_at_period(db, latest['id'], e - 1):
                    start_exposure[s['zone_id']] = s['cumulative_exposure_c']
                    start_temps[s['zone_id']] = s['temp_after_c']

        # plan_version is a column, not part of the identity hash, so identical
        # inputs reproduce the identical plan id (stable re-computation).
        basis = {
            'case_id': case_id,
            'case_sha256': payload['case_sha256'],
            'sensor_sha256': payload['sensor_sha256'],
            'contract': contract,
            'mode': 'full' if start_period == 0 else 'suffix',
            'start_period': start_period,
            'start_temps_c': start_temps,
            'start_exposure_c': start_exposure,
            'locked_energy_cents': locked_energy,
            'feed_sha256': None,
            'supersedes': supersedes,
        }
        ordered = [z['id'] for z in case['zones']]
        result = solver.solve(
            case, contract,
            start_temps_c=[start_temps[z] for z in ordered],
            start_exposure_c=[start_exposure[z] for z in ordered],
            start_period=start_period,
            locked_energy_cents=locked_energy)
        with db:
            if start_period == 0:
                boundary_readings = {
                    z['id']: float(obs['sensor']['temperature_c'])
                    for z, obs in zip(case['zones'], payload['observed'])}
            if not result['feasible']:
                plan_id, created = _persist_plan(
                    db, case_id, contract, version, basis, result, horizon,
                    prefix_commands, prefix_states, boundary_readings, PLAN_INFEASIBLE)
                if created:
                    if latest:
                        _supersede(db, case_id, plan_id, clear_holds=False)
                        db.execute('UPDATE fridge_hold SET cleared=1 WHERE plan_id IS NOT NULL '
                                   'AND plan_id!=?', (plan_id,))
                    _hold(db, case_id, 'fridge_no_feasible_plan',
                          f'contract={contract} start_period={start_period}', plan_id)
            else:
                plan_id, created = _persist_plan(
                    db, case_id, contract, version, basis, result, horizon,
                    prefix_commands, prefix_states, boundary_readings, PLAN_ACTIVE)
                if created and latest:
                    _supersede(db, case_id, plan_id)
        return _plan_info(db, plan_id)
    finally:
        db.close()


# --------------------------------------------------------------------------- #
# Execution
# --------------------------------------------------------------------------- #
def execute(db_path, plan_id, period):
    db = connect(db_path)
    try:
        plan_row = db.execute('SELECT * FROM fridge_plan WHERE id=?', (plan_id,)).fetchone()
        if plan_row is None:
            raise ValueError('fridge_plan_unknown')
        if plan_row['status'] != PLAN_ACTIVE:
            raise ValueError('fridge_plan_not_active')
        cmd = db.execute('SELECT * FROM fridge_command WHERE plan_id=? AND period=?',
                         (plan_id, period)).fetchone()
        if cmd is None:
            raise ValueError('fridge_command_unknown')
        if cmd['status'] != CMD_PENDING:
            raise ValueError('fridge_command_not_pending')
        gap = db.execute(
            'SELECT COUNT(*) AS c FROM fridge_command WHERE plan_id=? AND period<? AND status!=?',
            (plan_id, period, CMD_EXECUTED)).fetchone()
        if gap['c']:
            raise ValueError('fridge_execution_gap')
        horizon = json.loads(plan_row['summary'])['periods']
        with db:
            db.execute('UPDATE fridge_command SET status=? WHERE id=?',
                       (CMD_EXECUTED, cmd['id']))
            if period == horizon - 1:
                db.execute('UPDATE fridge_plan SET status=? WHERE id=?',
                           (PLAN_COMPLETED, plan_id))
        status = db.execute('SELECT status FROM fridge_plan WHERE id=?',
                            (plan_id,)).fetchone()['status']
        return {'plan_id': plan_id, 'period': period, 'action': cmd['action'],
                'zone_id': cmd['zone_id'], 'energy_cents': cmd['energy_cents'],
                'plan_status': status}
    finally:
        db.close()


# --------------------------------------------------------------------------- #
# Late sensor feedback
# --------------------------------------------------------------------------- #
def _parse_feedback(raw, case):
    text = raw.decode('utf-8-sig')
    reader = csv.DictReader(io.StringIO(text))
    required = {'zone', 'at', 'temperature_c', 'unit'}
    if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
        raise ValueError('fridge_feed_format')
    zone_ids = [z['id'] for z in case['zones']]
    seen = set()
    parsed = []
    stamp = None
    for row in reader:
        zid = (row.get('zone') or '').strip()
        if zid not in zone_ids or zid in seen:
            raise ValueError('fridge_zone_identity_conflict')
        seen.add(zid)
        raw_temp = (row.get('temperature_c') or '').strip()
        if raw_temp == '':
            raise ValueError('fridge_reading_missing')
        try:
            temp = float(raw_temp)
        except ValueError:
            raise ValueError('fridge_reading_missing')
        if temp != temp or temp in (float('inf'), float('-inf')):
            raise ValueError('fridge_reading_missing')
        if temp < -273.15:
            raise ValueError('fridge_reading_nonphysical')
        try:
            ts = parse_ts((row.get('at') or '').strip())
        except ValueError:
            raise ValueError('fridge_feedback_timezone')
        if stamp is None:
            stamp = ts
        elif ts != stamp:
            raise ValueError('fridge_feedback_time_mismatch')
        if (row.get('unit') or '').strip() not in ('C', '°C'):
            raise ValueError('fridge_feedback_unit')
        parsed.append({'zone': zid, 'at': ts.isoformat(), 'temperature_c': temp})
    if seen != set(zone_ids):
        raise ValueError('fridge_reading_missing')
    return parsed, stamp


def ingest_feedback(db_path, case_id, feed_path):
    feed_path = Path(feed_path).resolve()
    raw = feed_path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    db = connect(db_path)
    try:
        payload, case = _load_case(db, case_id)
        solver.validate_case(case)
        duplicate = db.execute('SELECT id,status FROM fridge_feed WHERE sha256=?',
                               (digest,)).fetchone()
        if duplicate:
            return {'case_id': case_id, 'duplicate_feed': True,
                    'feed_id': duplicate['id'], 'replanned': False}
        latest = _latest_plan(db, case_id)
        if latest is None:
            raise ValueError('fridge_plan_unknown')
        try:
            parsed, stamp = _parse_feedback(raw, case)
        except ValueError as exc:
            with db:
                cur = db.execute(
                    'INSERT INTO fridge_feed(case_id,sha256,path,raw,status,detail) '
                    'VALUES (?,?,?,?,?,?)',
                    (case_id, digest, str(feed_path), raw, FEED_REJECTED, str(exc)))
                if latest['status'] in (PLAN_ACTIVE, PLAN_HELD):
                    _hold_pending(db, latest['id'], case_id, str(exc),
                                  f'feed={feed_path} sha256={digest}')
            raise
        horizon = len(case['zones'][0]['warming_c'])
        e, prefix_commands, prefix_states = _executed_prefix(db, latest['id'], horizon)
        with db:
            cur = db.execute(
                'INSERT INTO fridge_feed(case_id,sha256,path,raw,status,detail) '
                'VALUES (?,?,?,?,?,?)',
                (case_id, digest, str(feed_path), raw, FEED_ACCEPTED, stamp.isoformat()))
            feed_id = cur.lastrowid
            if e >= horizon:
                # The horizon is physically complete: real late readings are
                # retained as evidence but cannot alter executed commands.
                return {'case_id': case_id, 'feed_id': feed_id, 'replanned': False,
                        'reason': 'horizon_complete'}
            contract = latest['contract']
            version = _next_version(db, case_id, contract)
            start_exposure = {z['id']: 0.0 for z in case['zones']}
            for s in prefix_states:
                if s['period'] == e - 1:
                    start_exposure[s['zone_id']] = s['cumulative_exposure_c']
            start_temps = {r['zone']: r['temperature_c'] for r in parsed}
            locked_energy = sum(c['energy_cents'] for c in prefix_commands)
            basis = {
                'case_id': case_id,
                'case_sha256': payload['case_sha256'],
                'sensor_sha256': payload['sensor_sha256'],
                'contract': contract,
                'mode': 'full' if e == 0 else 'suffix',
                'start_period': e,
                'start_temps_c': start_temps,
                'start_exposure_c': start_exposure,
                'locked_energy_cents': locked_energy,
                'feed_sha256': digest,
                'supersedes': latest['id'],
            }
            ordered = [z['id'] for z in case['zones']]
            result = solver.solve(
                case, contract,
                start_temps_c=[start_temps[z] for z in ordered],
                start_exposure_c=[start_exposure[z] for z in ordered],
                start_period=e, locked_energy_cents=locked_energy)
            old_suffix = [r['action'] for r in db.execute(
                'SELECT action FROM fridge_command WHERE plan_id=? AND period>=? '
                'ORDER BY period', (latest['id'], e))]
            if not result['feasible']:
                plan_id, created = _persist_plan(
                    db, case_id, contract, version, basis, result, horizon,
                    prefix_commands, prefix_states, start_temps, PLAN_INFEASIBLE)
                if created:
                    # The new plan itself is the open review item: keep its
                    # hold row and only retire holds attached to prior plans.
                    _supersede(db, case_id, plan_id, clear_holds=False)
                    db.execute('UPDATE fridge_hold SET cleared=1 WHERE plan_id IS NOT NULL '
                               'AND plan_id!=?', (plan_id,))
                    _hold(db, case_id, 'fridge_no_feasible_plan',
                          f'feedback sha256={digest}', plan_id)
                return {'case_id': case_id, 'feed_id': feed_id, 'replanned': True,
                        'plan_id': plan_id, 'status': PLAN_INFEASIBLE,
                        'commands_changed': True}
            plan_id, created = _persist_plan(
                db, case_id, contract, version, basis, result, horizon,
                prefix_commands, prefix_states, start_temps, PLAN_ACTIVE)
            if created:
                _supersede(db, case_id, plan_id)
        return {'case_id': case_id, 'feed_id': feed_id, 'replanned': True,
                'previous_plan_id': latest['id'], 'plan_id': plan_id,
                'boundary_period': e,
                'commands_changed': old_suffix != result['actions'],
                'plan': _plan_info(db, plan_id)}
    finally:
        db.close()


# --------------------------------------------------------------------------- #
# Read models
# --------------------------------------------------------------------------- #
def _plan_info(db, plan_id):
    plan_row = db.execute('SELECT * FROM fridge_plan WHERE id=?', (plan_id,)).fetchone()
    if plan_row is None:
        raise ValueError('fridge_plan_unknown')
    commands = [dict(r) for r in db.execute(
        'SELECT period,zone_id,action,status,tariff_cents,energy_cents FROM fridge_command '
        'WHERE plan_id=? ORDER BY period', (plan_id,))]
    states = [dict(r) for r in db.execute(
        'SELECT * FROM fridge_period_state WHERE plan_id=? ORDER BY period,zone_id',
        (plan_id,))]
    return {'plan_id': plan_id, 'case_id': plan_row['case_id'],
            'contract': plan_row['contract'],
            'plan_version': plan_row['plan_version'],
            'status': plan_row['status'],
            'basis': json.loads(plan_row['basis']),
            'summary': json.loads(plan_row['summary']),
            'evidence': json.loads(plan_row['evidence']),
            'commands': commands, 'period_states': states}


def get_plan(db_path, plan_id):
    db = connect(db_path)
    try:
        return _plan_info(db, plan_id)
    finally:
        db.close()
