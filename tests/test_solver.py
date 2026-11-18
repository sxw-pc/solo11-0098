"""Unit tests for the exact DP solver and the fixture's two contracts."""
import itertools
import json
from pathlib import Path

import pytest

from samplechain import solver
from tests.fridge_helpers import default_case

FIX = Path(__file__).resolve().parents[1] / 'fixtures'


def load_fixture_case():
    return json.loads((FIX / 'fridge-case.json').read_text(encoding='utf-8'))


def brute_force(case, contract, start_period=0, start_temps=None,
                start_exposure=None, locked_energy=0):
    """Reference exhaustive enumeration over every compressor assignment."""
    zones = case['zones']
    n = len(zones)
    periods = len(zones[0]['warming_c'])
    guard = [z['guard_c'] for z in zones]
    warming = [z['warming_c'] for z in zones]
    cooling = case['cooler']['cooling_c']
    tariff = case['cooler']['tariff_cents']
    limit = case['total_exposure_limit']
    temps = list(start_temps) if start_temps else [z['initial_c'] for z in zones]
    expos = list(start_exposure) if start_exposure else [0.0] * n
    best = None

    def key(energy, exposures):
        mx = max(exposures)
        return (energy, mx) if contract == 'min_energy' else (mx, energy)

    for assignment in itertools.product([None] + list(range(n)),
                                        repeat=periods - start_period):
        t_now, e_now = list(temps), list(expos)
        energy = locked_energy
        for k, action in enumerate(assignment):
            t = start_period + k
            for z in range(n):
                t_now[z] += warming[z][t] - (cooling if action == z else 0)
                if t_now[z] > guard[z]:
                    e_now[z] += t_now[z] - guard[z]
            energy += 0 if action is None else tariff[t]
        if sum(e_now) <= limit + 1e-9:
            candidate = (key(energy, e_now), assignment, list(t_now), list(e_now), energy)
            if best is None or candidate[0] < best[0]:
                best = candidate
    return best

def test_fixture_min_energy_matches_contract_figures():
    result = solver.solve(load_fixture_case(), 'min_energy')
    assert result['feasible']
    assert result['actions'] == ['Z1', 'idle']
    assert result['energy_cents'] == 100
    assert result['zone_exposure_c'] == {'Z1': 2.0, 'Z2': 4.0}
    assert result['max_zone_exposure_c'] == 4.0
    assert result['total_exposure_c'] == 6.0


def test_fixture_min_max_exposure_matches_contract_figures():
    result = solver.solve(load_fixture_case(), 'min_max_zone_exposure')
    assert result['feasible']
    assert result['actions'] == ['Z1', 'Z2']
    assert result['energy_cents'] == 1100
    assert result['zone_exposure_c'] == {'Z1': 2.0, 'Z2': 2.0}
    assert result['max_zone_exposure_c'] == 2.0


def test_no_period_cools_two_zones_and_physics_replay():
    case = load_fixture_case()
    for contract in solver.CONTRACTS:
        result = solver.solve(case, contract)
        for row in result['trace']:
            # At most one zone may be served by the shared compressor.
            warming_by_id = {z['id']: z['warming_c'][row['period']] for z in case['zones']}
            cooled = [zid for zid, zr in row['zones'].items()
                      if zr['temp_after_c'] < zr['temp_before_c'] + warming_by_id[zid]]
            assert len(cooled) == (1 if row['compressor_zone'] else 0)
            assert row['compressor_zone'] in (None, 'Z1', 'Z2')
        # Replay every period from the trace: before -> warm -> cool -> after.
        for t, row in enumerate(result['trace']):
            for z in case['zones']:
                zr = row['zones'][z['id']]
                expected_after = (zr['temp_before_c'] + z['warming_c'][t]
                                  - (case['cooler']['cooling_c']
                                     if row['compressor_zone'] == z['id'] else 0))
                assert abs(zr['temp_after_c'] - expected_after) < 1e-9
                expected_inc = max(zr['temp_after_c'] - z['guard_c'], 0)
                assert abs(zr['exposure_increment_c'] - expected_inc) < 1e-9


def test_dp_matches_brute_force_on_fixture():
    case = load_fixture_case()
    for contract in solver.CONTRACTS:
        dp = solver.solve(case, contract)
        ref = brute_force(case, contract)
        assert dp['energy_cents'] == ref[4]
        assert abs(dp['max_zone_exposure_c'] - max(ref[3])) < 1e-9
        assert abs(dp['total_exposure_c'] - sum(ref[3])) < 1e-9
        # The DP's own plan must be feasible and attain the brute-force optimum.
        assert tuple(dp['actions']) in all_optimal_actions(case, contract)


def all_optimal_actions(case, contract):
    """Every assignment attaining the brute-force optimum (ties included)."""
    ref = brute_force(case, contract)
    optimal_key = ref[0]
    zones = case['zones']
    n = len(zones)
    periods = len(zones[0]['warming_c'])
    guard = [z['guard_c'] for z in zones]
    warming = [z['warming_c'] for z in zones]
    cooling = case['cooler']['cooling_c']
    tariff = case['cooler']['tariff_cents']
    limit = case['total_exposure_limit']
    winners = set()
    for assignment in itertools.product([None] + list(range(n)), repeat=periods):
        t_now = [z['initial_c'] for z in zones]
        e_now = [0.0] * n
        energy = 0
        for t, action in enumerate(assignment):
            for z in range(n):
                t_now[z] += warming[z][t] - (cooling if action == z else 0)
                if t_now[z] > guard[z]:
                    e_now[z] += t_now[z] - guard[z]
            energy += 0 if action is None else tariff[t]
        if sum(e_now) <= limit + 1e-9:
            mx = max(e_now)
            lex = (energy, mx) if contract == 'min_energy' else (mx, energy)
            if lex == optimal_key:
                winners.add(tuple('idle' if a is None else zones[a]['id']
                                  for a in assignment))
    return winners


def test_dp_matches_brute_force_on_random_cases():
    import random
    rng = random.Random(20260101)
    for seed in range(40):
        rng.seed(1000 + seed)
        n_zones = rng.randint(1, 4)
        periods = rng.randint(1, 8)
        zones = []
        for z in range(n_zones):
            zones.append({
                'id': f'Z{z}', 'sample_id': f's{z}',
                'initial_c': rng.randint(2, 6),
                'guard_c': rng.randint(5, 8),
                'warming_c': [rng.randint(0, 4) for _ in range(periods)]})
        case = {'zones': zones,
                'cooler': {'max_zones_per_period': 1,
                           'cooling_c': rng.choice([1, 2, 3]),
                           'tariff_cents': [rng.choice([0, 50, 100, 900])
                                            for _ in range(periods)]},
                'total_exposure_limit': rng.randint(0, 20),
                'contracts': list(solver.CONTRACTS)}
        for contract in solver.CONTRACTS:
            dp = solver.solve(case, contract)
            ref = brute_force(case, contract)
            if ref is None:
                assert not dp['feasible']
                continue
            assert dp['feasible']
            assert dp['energy_cents'] == ref[4]
            assert abs(dp['max_zone_exposure_c'] - max(ref[3])) < 1e-9
            assert abs(dp['total_exposure_c'] - sum(ref[3])) < 1e-9


def test_suffix_solve_matches_brute_force_with_locked_prefix():
    case = load_fixture_case()
    # Period 0 executed: Z1 cooled; real boundary temps (5, 6), exposures (0, 1).
    for contract in solver.CONTRACTS:
        dp = solver.solve(case, contract, start_temps_c=[5.0, 6.0],
                          start_exposure_c=[0.0, 1.0], start_period=1,
                          locked_energy_cents=100)
        ref = brute_force(case, contract, start_period=1,
                          start_temps=[5.0, 6.0], start_exposure=[0.0, 1.0],
                          locked_energy=100)
        assert dp['feasible']
        assert dp['energy_cents'] == ref[4]
        assert abs(dp['max_zone_exposure_c'] - max(ref[3])) < 1e-9
        # Suffix action is optimal under the locked prefix lexicographic key.
        suffix_winners = suffix_optimal_actions(
            case, contract, start_period=1, start_temps=[5.0, 6.0],
            start_exposure=[0.0, 1.0], locked_energy=100)
        assert tuple(dp['actions']) in suffix_winners


def suffix_optimal_actions(case, contract, start_period, start_temps,
                           start_exposure, locked_energy):
    ref = brute_force(case, contract, start_period=start_period,
                      start_temps=list(start_temps),
                      start_exposure=list(start_exposure),
                      locked_energy=locked_energy)
    zones = case['zones']
    n = len(zones)
    periods = len(zones[0]['warming_c'])
    guard = [z['guard_c'] for z in zones]
    warming = [z['warming_c'] for z in zones]
    cooling = case['cooler']['cooling_c']
    tariff = case['cooler']['tariff_cents']
    limit = case['total_exposure_limit']
    winners = set()
    span = periods - start_period
    for assignment in itertools.product([None] + list(range(n)), repeat=span):
        t_now = list(start_temps)
        e_now = list(start_exposure)
        energy = locked_energy
        for k, action in enumerate(assignment):
            t = start_period + k
            for z in range(n):
                t_now[z] += warming[z][t] - (cooling if action == z else 0)
                if t_now[z] > guard[z]:
                    e_now[z] += t_now[z] - guard[z]
            energy += 0 if action is None else tariff[t]
        if sum(e_now) <= limit + 1e-9:
            mx = max(e_now)
            lex = (energy, mx) if contract == 'min_energy' else (mx, energy)
            if lex == ref[0]:
                winners.add(tuple('idle' if a is None else zones[a]['id']
                                  for a in assignment))
    return winners


def test_infeasible_when_cap_always_breached():
    case = default_case(total_exposure_limit=0)
    # Any plan ends with positive exposure on both zones -> cap 0 impossible.
    result = solver.solve(case, 'min_energy')
    assert not result['feasible']
    assert result['status'] == 'infeasible'
    assert result['evidence']['feasible_terminal_states'] == 0


def test_exposure_never_offsets_and_idle_is_legal():
    case = default_case()
    result = solver.solve(case, 'min_energy')
    cumulative = {'Z1': 0.0, 'Z2': 0.0}
    for row in result['trace']:
        for zid, zr in row['zones'].items():
            assert zr['exposure_increment_c'] >= 0
            cumulative[zid] += zr['exposure_increment_c']
            assert abs(zr['cumulative_exposure_c'] - cumulative[zid]) < 1e-9


def test_deterministic_recompute():
    case = load_fixture_case()
    a = solver.solve(case, 'min_max_zone_exposure')
    b = solver.solve(case, 'min_max_zone_exposure')
    assert a == b


def test_validation_rejects_bad_inputs():
    with pytest.raises(ValueError, match='fridge_cooling_nonphysical'):
        solver.validate_case(default_case(
            cooler={'max_zones_per_period': 1, 'cooling_c': 0,
                    'tariff_cents': [100, 1000]}))
    with pytest.raises(ValueError, match='fridge_compressor_capacity'):
        solver.validate_case(default_case(
            cooler={'max_zones_per_period': 2, 'cooling_c': 2,
                    'tariff_cents': [100, 1000]}))
    bad = default_case()
    bad['zones'][0]['warming_c'] = [3]
    with pytest.raises(ValueError, match='fridge_warming_missing'):
        solver.validate_case(bad)
    bad = default_case()
    bad['zones'][1]['id'] = 'Z1'
    with pytest.raises(ValueError, match='fridge_zone_identity_conflict'):
        solver.validate_case(bad)
    with pytest.raises(ValueError, match='fridge_contract_unknown'):
        solver.solve(default_case(), 'greedy_hottest')
