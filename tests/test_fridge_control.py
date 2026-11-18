"""End-to-end tests: planning, execution, late feedback, holds and reporting."""
import json
from pathlib import Path

import pytest

from samplechain import fridge_control as fc
from samplechain.fridge_control import (PLAN_ACTIVE, PLAN_HELD, PLAN_INFEASIBLE,
                                        PLAN_SUPERSEDED, PLAN_COMPLETED,
                                        CMD_PENDING, CMD_EXECUTED, CMD_HELD)
from samplechain.store import connect
from samplechain.report import build_report, write_report
from tests.fridge_helpers import (seed_case, default_case, initial_sensors,
                                  write_feed, source_hashes, FIX)


def plan(db, case_id='fridge-01', contract='min_energy'):
    return fc.plan(db, case_id, contract)


def active_plan_id(db, case_id='fridge-01'):
    con = connect(db)
    row = con.execute(
        "SELECT id FROM fridge_plan WHERE case_id=? AND status='active' "
        "ORDER BY plan_version DESC LIMIT 1", (case_id,)).fetchone()
    con.close()
    return row['id'] if row else None


# --------------------------------------------------------------------------- #
# Fixture contracts
# --------------------------------------------------------------------------- #
def test_fixture_min_energy_plan_persists_commands_and_states(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    info = plan(db)
    assert info['status'] == PLAN_ACTIVE
    assert info['summary']['energy_cents'] == 100
    assert [c['action'] for c in info['commands']] == ['Z1', 'idle']
    assert info['summary']['zone_exposure_c'] == {'Z1': 2.0, 'Z2': 4.0}
    # One compressor occupancy per period, never two zones at once.
    con = connect(db)
    rows = con.execute(
        'SELECT period, COUNT(*) AS c FROM fridge_command WHERE action!=? '
        'GROUP BY period', ('idle',)).fetchall()
    assert all(r['c'] == 1 for r in rows)
    con.close()


def test_fixture_min_max_contract_plan(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    info = plan(db, contract='min_max_zone_exposure')
    assert [c['action'] for c in info['commands']] == ['Z1', 'Z2']
    assert info['summary']['energy_cents'] == 1100
    assert info['summary']['zone_exposure_c'] == {'Z1': 2.0, 'Z2': 2.0}
    assert info['summary']['max_zone_exposure_c'] == 2.0


def test_original_chain_and_sha_remain_untouched(tmp_path):
    case = default_case()
    db = seed_case(tmp_path, case, initial_sensors(case))
    before = source_hashes(db)
    plan(db)
    info = plan(db, contract='min_max_zone_exposure')
    write_feed(tmp_path, [('Z1', 5.0), ('Z2', 6.0)])
    fc.execute(db, info['plan_id'], 0)
    fc.ingest_feedback(db, 'fridge-01', tmp_path / 'feed.csv')
    assert source_hashes(db) == before
    con = connect(db)
    # Existing detectability machinery tables are untouched by fridge control.
    assert con.execute('SELECT COUNT(*) FROM decision').fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM event WHERE kind='fridge_in'").fetchone()[0] == 2
    con.close()


# --------------------------------------------------------------------------- #
# Determinism / idempotency
# --------------------------------------------------------------------------- #
def test_replan_same_inputs_is_stable(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    first = plan(db)
    second = plan(db)
    assert first['plan_id'] == second['plan_id']
    assert first['commands'] == second['commands']
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM fridge_plan').fetchone()[0] == 1
    con.close()


def test_report_files_are_deterministic(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    plan(db)
    write_report(db, tmp_path / 'r.json')
    json_a = (tmp_path / 'r.json').read_bytes()
    md_a = (tmp_path / 'r.md').read_bytes()
    write_report(db, tmp_path / 'r2.json')
    # Same rows, only path/name differs; compare normalized JSON content.
    assert json.loads((tmp_path / 'r.json').read_text()) == json.loads(
        (tmp_path / 'r2.json').read_text())
    assert md_a == (tmp_path / 'r2.md').read_bytes()
    assert json_a  # non-empty


# --------------------------------------------------------------------------- #
# Execution and conservation
# --------------------------------------------------------------------------- #
def test_execute_marks_commands_and_completes_plan(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    info = plan(db, contract='min_max_zone_exposure')
    pid = info['plan_id']
    r0 = fc.execute(db, pid, 0)
    assert r0['action'] == 'Z1' and r0['energy_cents'] == 100
    r1 = fc.execute(db, pid, 1)
    assert r1['action'] == 'Z2' and r1['plan_status'] == PLAN_COMPLETED
    con = connect(db)
    assert con.execute(
        "SELECT COUNT(*) FROM fridge_command WHERE status='executed'").fetchone()[0] == 2
    con.close()


def test_cannot_execute_out_of_order_or_twice(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    pid = plan(db)['plan_id']
    with pytest.raises(ValueError, match='fridge_command_unknown'):
        fc.execute(db, pid, 9)
    fc.execute(db, pid, 0)
    with pytest.raises(ValueError, match='fridge_command_not_pending'):
        fc.execute(db, pid, 0)


def test_energy_and_exposure_conservation_through_feedback(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    pid = plan(db, contract='min_max_zone_exposure')['plan_id']
    fc.execute(db, pid, 0)  # Z1 cooled at t0, 100 cents, incurred exposure Z2=1
    feed = write_feed(tmp_path, [('Z1', 5.0), ('Z2', 6.0)])
    out = fc.ingest_feedback(db, 'fridge-01', feed)
    new = fc.get_plan(db, out['plan_id'])
    # Executed prefix is copied verbatim: same action, status, money spent.
    assert new['commands'][0]['action'] == 'Z1'
    assert new['commands'][0]['status'] == CMD_EXECUTED
    assert new['basis']['locked_energy_cents'] == 100
    assert new['summary']['energy_cents'] >= 100
    # Already-incurred exposure cannot be erased: Z2 keeps its 1.0 from t0.
    z2_states = [s for s in new['period_states']
                 if s['zone_id'] == 'Z2' and s['period'] == 0]
    assert z2_states[0]['cumulative_exposure_c'] == 1.0


# --------------------------------------------------------------------------- #
# Late feedback
# --------------------------------------------------------------------------- #
def test_late_feedback_changes_unexecuted_commands_causally(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    # min_energy's original suffix is idle; an overheating Z2 must flip the
    # still-pending period 1 command to cooling Z2.
    pid = plan(db, contract='min_energy')['plan_id']
    fc.execute(db, pid, 0)  # t0: cool Z1
    feed = write_feed(tmp_path, [('Z1', 5.0), ('Z2', 7.0)])
    out = fc.ingest_feedback(db, 'fridge-01', feed)
    assert out['commands_changed'] is True
    new = fc.get_plan(db, out['plan_id'])
    suffix = [c for c in new['commands'] if c['period'] == 1][0]
    assert suffix['action'] == 'Z2' and suffix['status'] == CMD_PENDING
    # The old plan's pending command was held, not deleted or re-executed.
    con = connect(db)
    old = con.execute('SELECT status FROM fridge_command WHERE plan_id=? AND period=1',
                      (pid,)).fetchone()
    assert old['status'] == CMD_HELD
    assert con.execute(
        'SELECT reading_c FROM fridge_period_state WHERE plan_id=? AND period=1 AND zone_id=?',
        (out['plan_id'], 'Z2')).fetchone()['reading_c'] == 7.0
    con.close()


def test_feedback_matching_forecast_keeps_suffix_actions(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    pid = plan(db)['plan_id']
    fc.execute(db, pid, 0)
    feed = write_feed(tmp_path, [('Z1', 5.0), ('Z2', 6.0)])
    out = fc.ingest_feedback(db, 'fridge-01', feed)
    assert out['commands_changed'] is False
    new = fc.get_plan(db, out['plan_id'])
    # The original suffix command survives as an executable pending command.
    assert [c for c in new['commands'] if c['period'] == 1][0]['action'] == 'idle'


def test_duplicate_feedback_is_idempotent(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    pid = plan(db)['plan_id']
    fc.execute(db, pid, 0)
    feed = write_feed(tmp_path, [('Z1', 5.0), ('Z2', 6.0)])
    first = fc.ingest_feedback(db, 'fridge-01', feed)
    again = fc.ingest_feedback(db, 'fridge-01', feed)
    assert again['duplicate_feed'] is True and again['replanned'] is False
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM fridge_feed').fetchone()[0] == 1
    assert con.execute('SELECT COUNT(*) FROM fridge_plan').fetchone()[0] == 2
    con.close()


def test_feedback_before_any_execution_replans_full_horizon(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    plan(db)
    # Nothing executed yet: feedback replaces the initial readings and must
    # produce a full-horizon re-plan, not a suffix with a fabricated prefix.
    feed = write_feed(tmp_path, [('Z1', 4.0), ('Z2', 4.0)])
    out = fc.ingest_feedback(db, 'fridge-01', feed)
    new = fc.get_plan(db, out['plan_id'])
    assert new['basis']['mode'] == 'full'
    assert new['basis']['start_period'] == 0
    assert new['basis']['start_temps_c'] == {'Z1': 4.0, 'Z2': 4.0}
    assert len(new['commands']) == 2
    assert all(c['status'] == CMD_PENDING for c in new['commands'])


def test_infeasible_feedback_at_boundary_holds_without_publishing_commands(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    plan(db)
    # Starting already this hot makes the cap unreachable over the horizon.
    feed = write_feed(tmp_path, [('Z1', 5.0), ('Z2', 6.0)])
    out = fc.ingest_feedback(db, 'fridge-01', feed)
    assert out['status'] == PLAN_INFEASIBLE
    con = connect(db)
    row = con.execute(
        "SELECT COUNT(*) AS c FROM fridge_hold WHERE reason='fridge_no_feasible_plan' "
        "AND cleared=0").fetchone()
    assert row['c'] == 1
    assert con.execute(
        "SELECT COUNT(*) FROM fridge_command WHERE status='pending'").fetchone()[0] == 0
    con.close()


def test_feedback_records_real_readings_but_cannot_rewrite_executed(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    pid = plan(db, contract='min_max_zone_exposure')['plan_id']
    fc.execute(db, pid, 0)
    feed = write_feed(tmp_path, [('Z1', 5.5), ('Z2', 6.5)])
    out = fc.ingest_feedback(db, 'fridge-01', feed)
    new = fc.get_plan(db, out['plan_id'])
    # Prefix rows still show the forecast t0 values, feedback attaches at t1.
    prefix_z1 = [s for s in new['period_states']
                 if s['period'] == 0 and s['zone_id'] == 'Z1'][0]
    assert prefix_z1['temp_after_c'] == 5.0 and prefix_z1['reading_c'] == 4.0
    boundary = [s for s in new['period_states']
                if s['period'] == 1 and s['zone_id'] == 'Z1'][0]
    assert boundary['reading_c'] == 5.5 and boundary['temp_before_c'] == 5.5


# --------------------------------------------------------------------------- #
# Holds: missing reading, identity conflict, timezone mismatch, non-physical
# --------------------------------------------------------------------------- #
def _held_state(db):
    con = connect(db)
    plan_status = con.execute(
        "SELECT status FROM fridge_plan ORDER BY plan_version DESC LIMIT 1").fetchone()[0]
    holds = [dict(r) for r in con.execute('SELECT reason FROM fridge_hold WHERE cleared=0')]
    pending = con.execute(
        "SELECT COUNT(*) FROM fridge_command WHERE status='pending'").fetchone()[0]
    feed = dict(con.execute('SELECT status,detail FROM fridge_feed').fetchone())
    con.close()
    return plan_status, holds, pending, feed


def test_single_zone_missing_reading_holds_whole_order(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    plan(db)
    feed = tmp_path / 'bad.csv'
    feed.write_text('zone,at,temperature_c,unit\n'
                    'Z1,2026-01-01T10:00:00Z,5.0,C\n'
                    'Z2,2026-01-01T10:00:00Z,,C\n', encoding='utf-8')
    with pytest.raises(ValueError, match='fridge_reading_missing'):
        fc.ingest_feedback(db, 'fridge-01', feed)
    plan_status, holds, pending, feed_row = _held_state(db)
    assert plan_status == PLAN_HELD
    assert holds and holds[0]['reason'] == 'fridge_reading_missing'
    assert pending == 0  # no executable fragment remains published
    assert feed_row['status'] == 'rejected'
    # No new plan may be published while orders await review.
    with pytest.raises(ValueError, match='fridge_orders_pending_review'):
        plan(db)


def test_unknown_zone_and_duplicate_identity_conflict_hold(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    plan(db)
    feed = tmp_path / 'conflict.csv'
    feed.write_text('zone,at,temperature_c,unit\n'
                    'Z1,2026-01-01T10:00:00Z,5.0,C\n'
                    'Z9,2026-01-01T10:00:00Z,6.0,C\n', encoding='utf-8')
    with pytest.raises(ValueError, match='fridge_zone_identity_conflict'):
        fc.ingest_feedback(db, 'fridge-01', feed)
    assert _held_state(db)[0] == PLAN_HELD
    feed2 = tmp_path / 'dup.csv'
    feed2.write_text('zone,at,temperature_c,unit\n'
                     'Z1,2026-01-01T10:00:00Z,5.0,C\n'
                     'Z1,2026-01-01T10:00:00Z,6.0,C\n', encoding='utf-8')
    with pytest.raises(ValueError, match='fridge_zone_identity_conflict'):
        fc.ingest_feedback(db, 'fridge-01', feed2)


def test_timezone_mismatch_and_naive_stamps_hold(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    plan(db)
    feed = tmp_path / 'tz.csv'
    feed.write_text('zone,at,temperature_c,unit\n'
                    'Z1,2026-01-01T10:00:00Z,5.0,C\n'
                    'Z2,2026-01-01T11:00:00+02:00,6.0,C\n', encoding='utf-8')
    with pytest.raises(ValueError, match='fridge_feedback_time_mismatch'):
        fc.ingest_feedback(db, 'fridge-01', feed)
    assert _held_state(db)[0] == PLAN_HELD
    feed_naive = tmp_path / 'naive.csv'
    feed_naive.write_text('zone,at,temperature_c,unit\n'
                          'Z1,2026-01-01T10:00:00,5.0,C\n'
                          'Z2,2026-01-01T10:00:00,6.0,C\n', encoding='utf-8')
    with pytest.raises(ValueError, match='fridge_feedback_timezone'):
        fc.ingest_feedback(db, 'fridge-01', feed_naive)


def test_nonphysical_reading_holds(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    plan(db)
    feed = write_feed(tmp_path, [('Z1', -300.0), ('Z2', 6.0)])
    with pytest.raises(ValueError, match='fridge_reading_nonphysical'):
        fc.ingest_feedback(db, 'fridge-01', feed)
    assert _held_state(db)[0] == PLAN_HELD


def test_valid_feedback_clears_hold_and_publishes_new_commands(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    pid = plan(db)['plan_id']
    fc.execute(db, pid, 0)
    bad = tmp_path / 'bad.csv'
    bad.write_text('zone,at,temperature_c,unit\n'
                   'Z1,2026-01-01T10:00:00Z,,C\n'
                   'Z2,2026-01-01T10:00:00Z,6.0,C\n', encoding='utf-8')
    with pytest.raises(ValueError):
        fc.ingest_feedback(db, 'fridge-01', bad)
    good = write_feed(tmp_path, [('Z1', 5.0), ('Z2', 6.0)])
    out = fc.ingest_feedback(db, 'fridge-01', good)
    assert out['replanned'] is True
    con = connect(db)
    assert con.execute(
        'SELECT COUNT(*) FROM fridge_hold WHERE cleared=0').fetchone()[0] == 0
    assert con.execute(
        "SELECT status FROM fridge_plan WHERE id=?", (out['plan_id'],)
    ).fetchone()[0] == PLAN_ACTIVE
    assert con.execute(
        "SELECT COUNT(*) FROM fridge_command WHERE plan_id=? AND status='pending'",
        (out['plan_id'],)).fetchone()[0] == 1
    con.close()


# --------------------------------------------------------------------------- #
# Infeasibility
# --------------------------------------------------------------------------- #
def test_no_feasible_plan_publishes_infeasible_record_not_commands(tmp_path):
    case = default_case(total_exposure_limit=0)
    db = seed_case(tmp_path, case, initial_sensors(case))
    info = plan(db)
    assert info['status'] == PLAN_INFEASIBLE
    assert info['commands'] == []
    con = connect(db)
    assert con.execute('SELECT reason FROM fridge_hold').fetchone()['reason'] \
        == 'fridge_no_feasible_plan'
    con.close()
    with pytest.raises(ValueError, match='fridge_plan_not_active'):
        fc.execute(db, info['plan_id'], 0)


def test_feedback_rendering_suffix_infeasible_holds_orders(tmp_path):
    # Cap 6 is feasible for the full horizon, but a much hotter Z2 arriving
    # after period 0 makes every suffix breach the remaining exposure budget.
    case = default_case(total_exposure_limit=6)
    db = seed_case(tmp_path, case, initial_sensors(case))
    pid = plan(db)['plan_id']
    fc.execute(db, pid, 0)
    feed = write_feed(tmp_path, [('Z1', 5.0), ('Z2', 9.0)])
    out = fc.ingest_feedback(db, 'fridge-01', feed)
    assert out['status'] == PLAN_INFEASIBLE
    new = fc.get_plan(db, out['plan_id'])
    # Executed prefix retained, no new executable suffix published.
    assert [c['action'] for c in new['commands']] == ['Z1']
    con = connect(db)
    assert con.execute(
        'SELECT COUNT(*) FROM fridge_hold WHERE reason=? AND cleared=0',
        ('fridge_no_feasible_plan',)).fetchone()[0] == 1
    con.close()


# --------------------------------------------------------------------------- #
# Contract switching and compressor saturation
# --------------------------------------------------------------------------- #
def test_contract_switching_replans_with_distinct_actions_and_cost(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    cheap = plan(db, contract='min_energy')
    costly = plan(db, contract='min_max_zone_exposure')
    assert cheap['plan_id'] != costly['plan_id']
    assert cheap['summary']['energy_cents'] == 100
    assert costly['summary']['energy_cents'] == 1100
    con = connect(db)
    assert con.execute(
        "SELECT status FROM fridge_plan WHERE id=?",
        (cheap['plan_id'],)).fetchone()[0] == PLAN_SUPERSEDED
    con.close()


def test_compressor_serves_exactly_one_zone_every_period_when_saturated(tmp_path):
    # Two zones both want cooling every period; planner still emits one command
    # per period and the executed prefix saturates the compressor fully.
    case = {
        'zones': [
            {'id': 'Z1', 'sample_id': 'sample-1', 'initial_c': 4,
             'guard_c': 5, 'warming_c': [3, 3]},
            {'id': 'Z2', 'sample_id': 'sample-2', 'initial_c': 4,
             'guard_c': 5, 'warming_c': [3, 3]}],
        'cooler': {'max_zones_per_period': 1, 'cooling_c': 2,
                   'tariff_cents': [100, 100]},
        'total_exposure_limit': 8,
        'contracts': ['min_max_zone_exposure']}
    db = seed_case(tmp_path, case, initial_sensors(case))
    info = plan(db, contract='min_max_zone_exposure')
    con = connect(db)
    per_period = con.execute(
        'SELECT period, COUNT(*) AS c FROM fridge_command '
        'WHERE plan_id=? GROUP BY period', (info['plan_id'],)).fetchall()
    assert {r['period']: r['c'] for r in per_period} == {0: 1, 1: 1}
    cool_rows = con.execute(
        "SELECT period, COUNT(*) AS c FROM fridge_command WHERE plan_id=? AND action!='idle' "
        'GROUP BY period', (info['plan_id'],)).fetchall()
    assert all(r['c'] == 1 for r in cool_rows)
    con.close()
    for period in (0, 1):
        fc.execute(db, info['plan_id'], period)
    assert fc.get_plan(db, info['plan_id'])['status'] == PLAN_COMPLETED


def test_late_feedback_after_completion_is_evidence_only(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    pid = plan(db, contract='min_max_zone_exposure')['plan_id']
    fc.execute(db, pid, 0)
    fc.execute(db, pid, 1)
    feed = write_feed(tmp_path, [('Z1', 5.1), ('Z2', 6.2)],
                      at='2026-01-01T11:00:00Z')
    out = fc.ingest_feedback(db, 'fridge-01', feed)
    assert out['replanned'] is False and out['reason'] == 'horizon_complete'
    con = connect(db)
    assert con.execute(
        "SELECT status FROM fridge_plan WHERE id=?", (pid,)).fetchone()[0] \
        == PLAN_COMPLETED
    assert con.execute(
        "SELECT COUNT(*) FROM fridge_command WHERE plan_id=? AND status='executed'",
        (pid,)).fetchone()[0] == 2
    assert con.execute(
        "SELECT status FROM fridge_feed").fetchone()[0] == 'accepted'
    con.close()


def test_invalid_case_structure_blocks_planning(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    con = connect(db)
    row = con.execute('SELECT payload FROM fridge_case').fetchone()
    payload = json.loads(row['payload'])
    payload['case']['cooler']['cooling_c'] = -1
    with con:
        con.execute('UPDATE fridge_case SET payload=?', (json.dumps(payload),))
    con.close()
    with pytest.raises(ValueError, match='fridge_cooling_nonphysical'):
        plan(db)
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM fridge_plan').fetchone()[0] == 0
    con.close()


def test_full_feedback_cycle_is_deterministic_across_rebuilds(tmp_path):
    dir_a, dir_b = tmp_path / 'a', tmp_path / 'b'
    dir_a.mkdir(), dir_b.mkdir()
    db1 = seed_case(dir_a, default_case(), initial_sensors(default_case()))
    db2 = seed_case(dir_b, default_case(), initial_sensors(default_case()))
    for d, db in ((dir_a, db1), (dir_b, db2)):
        pid = plan(db, contract='min_energy')['plan_id']
        fc.execute(db, pid, 0)
        fc.ingest_feedback(db, 'fridge-01',
                           write_feed(d, [('Z1', 5.0), ('Z2', 7.0)], name='f.csv'))
    p1 = fc.get_plan(db1, active_plan_id(db1))
    p2 = fc.get_plan(db2, active_plan_id(db2))
    assert p1['plan_id'] == p2['plan_id']
    assert p1['commands'] == p2['commands']
    assert p1['period_states'] == p2['period_states']


def test_simultaneous_zone_requests_resolve_to_single_compressor_assignment(tmp_path):
    # Both zones breach guard in the same period; only one can be cooled, and
    # the DP picks globally rather than the locally hottest zone.
    case = default_case()
    db = seed_case(tmp_path, case, initial_sensors(case))
    info = plan(db, contract='min_max_zone_exposure')
    t0 = info['commands'][0]
    assert t0['zone_id'] in ('Z1', 'Z2')
    # The choice is the globally optimal one (brute-force verified in solver
    # tests); here we assert exactly one zone gets the compressor at t0.
    assert info['commands'][0]['action'] != 'idle'


# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #
def test_report_contains_commands_risks_tariffs_sources_and_versions(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    info = plan(db, contract='min_max_zone_exposure')
    fc.execute(db, info['plan_id'], 0)
    feed = write_feed(tmp_path, [('Z1', 5.0), ('Z2', 6.0)])
    fc.ingest_feedback(db, 'fridge-01', feed)
    out = write_report(db, tmp_path / 'control.json')
    data = json.loads(out.read_text(encoding='utf-8'))
    assert {p['contract'] for p in data['fridge_plan']} == {'min_max_zone_exposure'}
    latest = max(data['fridge_plan'], key=lambda p: p['plan_version'])
    assert latest['basis']['feed_sha256']
    assert latest['evidence']['method'] == 'exact_state_merge_dynamic_programming'
    assert any(c['status'] == 'executed' for c in data['fridge_command'])
    assert any(s['reading_c'] == 6.0 for s in data['fridge_period_state'])
    assert data['fridge_feed'][0]['status'] == 'accepted'
    md = out.with_suffix('.md').read_text(encoding='utf-8')
    assert '双区冷藏控制报告' in md and '最优性证据' in md and '电费' in md


def test_sensor_feedback_causally_moves_pending_not_just_chart(tmp_path):
    db = seed_case(tmp_path, default_case(), initial_sensors(default_case()))
    # min_energy leaves period 1 idle; an overheating Z2 forces the suffix to
    # spend the expensive tariff on cooling it — a real command change.
    pid = plan(db, contract='min_energy')['plan_id']
    fc.execute(db, pid, 0)
    before = [c['action'] for c in fc.get_plan(db, pid)['commands']
              if c['period'] == 1]
    assert before == ['idle']
    feed = write_feed(tmp_path, [('Z1', 5.0), ('Z2', 8.0)])
    out = fc.ingest_feedback(db, 'fridge-01', feed)
    after = [c['action'] for c in fc.get_plan(db, out['plan_id'])['commands']
             if c['period'] == 1]
    assert out['commands_changed'] is True
    assert after == ['Z2']
    con = connect(db)
    # Executed action/money immutable; pending rows belong to the new plan.
    assert con.execute(
        "SELECT energy_cents FROM fridge_command WHERE plan_id=? AND period=0 AND status='executed'",
        (pid,)).fetchone()[0] == 100
    con.close()
