"""Exact global optimizer for the finite-horizon multi-zone fridge control case.

Physics (per zone z, per period t, all arithmetic on integer milli-degrees):
    temp_after = temp_before + warming[z][t] - (cooling_c if the shared
                 compressor serves z in t else 0)
The compressor serves at most one zone per period. Per-period zone exposure
is max(temp_after - guard[z], 0); cumulative exposure is the positive sum and
can never be offset by later "negative exposure".

Two lexicographic objectives share the same physics and total exposure cap:
  * min_energy:              (energy_cents, max zone exposure)
  * min_max_zone_exposure:   (max zone exposure, energy_cents)

The optimizer is an exact shortest-path DP over merged states
(period, temperatures, cumulative exposures) with cap pruning, so the returned
plan is globally optimal with an auditable proof; it never relies on a
hottest-zone greedy choice. Local cases are bounded to <= 4 zones and
<= 8 periods by :func:`validate_case`.
"""
from __future__ import annotations

M = 1000  # milli-degrees per degree
MAX_ZONES = 4
MAX_PERIODS = 8
ACTION_IDLE = 'idle'
STATUS_OPTIMAL = 'optimal'
STATUS_INFEASIBLE = 'infeasible'
CONTRACTS = ('min_energy', 'min_max_zone_exposure')


def validate_case(case):
    """Structural/physical validation shared by planning and feedback intake."""
    if not isinstance(case, dict):
        raise ValueError('fridge_case_invalid')
    zones = case.get('zones')
    cooler = case.get('cooler')
    if not zones or not cooler or not case.get('contracts'):
        raise ValueError('fridge_case_invalid')
    if not 1 <= len(zones) <= MAX_ZONES:
        raise ValueError('fridge_case_too_large')
    periods = None
    seen = set()
    for zone in zones:
        zid = zone.get('id')
        if not zid or zid in seen:
            raise ValueError('fridge_zone_identity_conflict')
        seen.add(zid)
        warming = zone.get('warming_c')
        if not isinstance(warming, list) or not warming:
            raise ValueError('fridge_warming_missing')
        if periods is None:
            periods = len(warming)
        elif len(warming) != periods:
            raise ValueError('fridge_warming_missing')
        if periods > MAX_PERIODS:
            raise ValueError('fridge_case_too_large')
        for key in ('initial_c', 'guard_c'):
            value = zone.get(key)
            if not isinstance(value, (int, float)) or isinstance(value, bool) \
                    or value != value:
                raise ValueError('fridge_case_invalid')
        for w in warming:
            if not isinstance(w, (int, float)) or isinstance(w, bool) \
                    or w != w or w < 0:
                raise ValueError('fridge_warming_missing')
    cooling = cooler.get('cooling_c')
    cap = cooler.get('max_zones_per_period')
    tariff = cooler.get('tariff_cents')
    if not isinstance(cooling, (int, float)) or isinstance(cooling, bool) \
            or cooling != cooling or cooling <= 0:
        raise ValueError('fridge_cooling_nonphysical')
    if cap != 1:
        raise ValueError('fridge_compressor_capacity')
    if not isinstance(tariff, list) or len(tariff) != periods:
        raise ValueError('fridge_tariff_mismatch')
    if any(not isinstance(p, int) or isinstance(p, bool) or p < 0 for p in tariff):
        raise ValueError('fridge_tariff_mismatch')
    limit = case.get('total_exposure_limit')
    if not isinstance(limit, (int, float)) or isinstance(limit, bool) \
            or limit < 0:
        raise ValueError('fridge_case_invalid')
    return periods


def _milli(value):
    return int(round(float(value) * M))


def solve(case, contract, start_temps_c=None, start_exposure_c=None,
          start_period=0, locked_energy_cents=0):
    """Return the exact lexicographic optimum for the (suffix) horizon.

    ``start_temps_c`` gives the real/forecast temperature each zone starts
    period ``start_period`` at; ``start_exposure_c`` is already-incurred,
    immutable exposure that still counts toward the cap and the objective.
    """
    if contract not in CONTRACTS:
        raise ValueError('fridge_contract_unknown')
    periods_total = validate_case(case)
    zones = case['zones']
    n = len(zones)
    cooling_m = _milli(case['cooler']['cooling_c'])
    tariff = case['cooler']['tariff_cents']
    guard_m = [_milli(z['guard_c']) for z in zones]
    limit_m = _milli(case['total_exposure_limit'])
    warming_m = [[_milli(w) for w in z['warming_c']] for z in zones]
    if start_temps_c is None:
        temp_m = [_milli(z['initial_c']) for z in zones]
    else:
        if len(start_temps_c) != n:
            raise ValueError('fridge_sensor_count')
        temp_m = [_milli(v) for v in start_temps_c]
    if start_exposure_c is None:
        expo_m = [0] * n
    else:
        if len(start_exposure_c) != n:
            raise ValueError('fridge_case_invalid')
        expo_m = [_milli(v) for v in start_exposure_c]
    if not 0 <= start_period <= periods_total:
        raise ValueError('fridge_period_invalid')
    if any(v != v for v in temp_m):
        raise ValueError('fridge_reading_missing')

    # Deterministic action order: idle first, then zones in case order.
    actions = [None] + list(range(n))

    def lex_key(energy, exposures):
        max_expo = max(exposures)
        if contract == 'min_energy':
            return energy, max_expo
        return max_expo, energy

    # Every layer maps state (period-end temps, cumulative exposures) to
    # (best prefix energy, parent state, action index). Exposures live in the
    # state, so equal states differ only in prefix energy. Layer k corresponds
    # to period start_period + k (layer 0 holds the immutable starting state).
    layers = [{(tuple(temp_m), tuple(expo_m)): (0, None, None)}]
    pruned_cap = 0
    for t in range(start_period, periods_total):
        cur = layers[-1]
        nxt = {}
        for (temps, expos), (prefix_energy, _, _) in cur.items():
            for action in actions:
                new_temps = list(temps)
                new_expos = list(expos)
                for z in range(n):
                    after = temps[z] + warming_m[z][t]
                    if action == z:
                        after -= cooling_m
                    new_temps[z] = after
                    if after > guard_m[z]:
                        new_expos[z] += after - guard_m[z]
                if sum(new_expos) > limit_m:
                    pruned_cap += 1
                    continue
                state = (tuple(new_temps), tuple(new_expos))
                energy = prefix_energy + (0 if action is None else tariff[t])
                old = nxt.get(state)
                if old is None or energy < old[0]:
                    nxt[state] = (energy, (temps, expos), action)
        layers.append(nxt)
        if not nxt:
            break

    stats = {
        'method': 'exact_state_merge_dynamic_programming',
        'action_order': [ACTION_IDLE] + [z['id'] for z in zones],
        'zones': n,
        'periods_total': periods_total,
        'solved_periods': periods_total - start_period,
        'start_period': start_period,
        'cap_pruned_transitions': pruned_cap,
        'layer_state_counts': [len(layer) for layer in layers],
        'feasible_terminal_states': len(layers[-1]),
        'integer_unit': 'milli_degree_c',
        'objectives_lexicographic': (
            ['energy_cents', 'max_zone_exposure_c'] if contract == 'min_energy'
            else ['max_zone_exposure_c', 'energy_cents']),
        'total_exposure_cap_c': case['total_exposure_limit'],
    }

    if not layers[-1] or len(layers) != periods_total - start_period + 1:
        stats['proof'] = (
            'the DP frontier became empty: every transition sequence was '
            'pruned because no assignment of at most one cooled zone per '
            'period keeps total exposure within the contract cap')
        return {'feasible': False, 'status': STATUS_INFEASIBLE,
                'contract': contract, 'evidence': stats}

    terminal = layers[-1]
    best_state = None
    best_lex = None
    for state, (energy, _, _) in terminal.items():
        candidate = lex_key(locked_energy_cents + energy, list(state[1]))
        if best_lex is None or candidate < best_lex:
            best_lex = candidate
            best_state = state
    stats['optimal_terminal_key'] = list(best_lex)

    chosen = [None] * periods_total
    cursor = best_state
    for k in range(len(layers) - 1, 0, -1):
        _, parent, action = layers[k][cursor]
        chosen[start_period + k - 1] = action
        cursor = parent

    trace = []
    initial_state = next(iter(layers[0]))
    temps = list(initial_state[0])
    expos = list(initial_state[1])
    cumulative_energy = locked_energy_cents
    for t in range(start_period, periods_total):
        action = chosen[t]
        zone_temps = {}
        for z, zone in enumerate(zones):
            before = temps[z]
            after = before + warming_m[z][t] - (cooling_m if action == z else 0)
            increment = max(after - guard_m[z], 0)
            expos[z] += increment
            zone_temps[zone['id']] = {
                'temp_before_c': round(before / M, 6),
                'temp_after_c': round(after / M, 6),
                'exposure_increment_c': round(increment / M, 6),
                'cumulative_exposure_c': round(expos[z] / M, 6)}
            temps[z] = after
        period_energy = 0 if action is None else tariff[t]
        cumulative_energy += period_energy
        trace.append({
            'period': t,
            'compressor_zone': None if action is None else zones[action]['id'],
            'tariff_cents': tariff[t],
            'energy_cents': period_energy,
            'cumulative_energy_cents': cumulative_energy,
            'zones': zone_temps})

    stats['proof'] = (
        'the DP merges all plans reaching identical (period-end temperatures, '
        'cumulative exposures) and retains only the cheapest prefix per state; '
        'transitions whose cumulative exposure exceeds the cap are pruned, and '
        'the terminal state with the best lexicographic contract key is '
        'selected, so no feasible plan can improve on it')

    suffix_actions = [solver_idle(chosen[t], zones)
                      for t in range(start_period, periods_total)]
    return {
        'feasible': True,
        'status': STATUS_OPTIMAL,
        'contract': contract,
        'start_period': start_period,
        'actions': suffix_actions,
        'trace': trace,
        'energy_cents': cumulative_energy,
        'suffix_energy_cents': cumulative_energy - locked_energy_cents,
        'zone_exposure_c': {zones[z]['id']: round(best_state[1][z] / M, 6)
                            for z in range(n)},
        'max_zone_exposure_c': round(max(best_state[1]) / M, 6),
        'total_exposure_c': round(sum(best_state[1]) / M, 6),
        'evidence': stats}


def solver_idle(action, zones):
    return ACTION_IDLE if action is None else zones[action]['id']
