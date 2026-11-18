"""机器人质控板制备:命令计划、模拟执行、中断对账与审计报告。

register() 只把合同冻结进库(绑定真实批次,不产生命令或液体移动);
plan() 按批准的制备法生成逐条吸排命令;execute() 驱动模拟机器人做真实
物料移动;recover() 用不可改写的机器人消耗日志与独立孔位回读对账;
finalize() 终验;report() 输出 JSON/Markdown 板图、库存账与机器审计。
"""
import json
from hashlib import sha256
from pathlib import Path

from .store import connect

EPS = 1e-9
JOURNAL_GENESIS = '0' * 64


class PipetteError(Exception):
    """控制器拒绝违反约束的命令;code 为机器可读错误码。"""

    def __init__(self, code, detail=''):
        super().__init__(f'{code}: {detail}' if detail else code)
        self.code = code
        self.detail = detail


class AckLost(Exception):
    """机器人未返回 ACK;物理动作是否发生对控制器未知。"""


class ReadbackOffline(Exception):
    """独立孔位回读不可用。"""


class PipetteInterrupted(Exception):
    """执行中断:存在已发送但未确认的命令,禁止盲重放,须先 recover()。"""

    def __init__(self, command_id):
        super().__init__(command_id)
        self.command_id = command_id


SCHEMA = '''
CREATE TABLE IF NOT EXISTS pipette_case(
  id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batch(id),
  contract_sha256 TEXT NOT NULL, contract_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pipette_command(
  id TEXT PRIMARY KEY,
  case_id TEXT NOT NULL REFERENCES pipette_case(id),
  method TEXT NOT NULL,
  seq INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK(kind IN ('aspirate','dispense')),
  reservoir_id TEXT,
  well_id TEXT,
  volume_ul REAL NOT NULL,
  tip_id TEXT NOT NULL,
  expect_well_ul REAL,
  depends_on TEXT,
  retries INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL DEFAULT 'planned' CHECK(state IN ('planned','sent','acked','reconciled','void')),
  UNIQUE(case_id,method,seq,kind)
);
CREATE TABLE IF NOT EXISTS robot_journal(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  robot_id TEXT NOT NULL,
  command_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  reservoir_id TEXT,
  well_id TEXT,
  volume_ul REAL NOT NULL,
  tip_id TEXT NOT NULL,
  prev_sha256 TEXT NOT NULL,
  sha256 TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS robot_journal_no_update BEFORE UPDATE ON robot_journal
BEGIN SELECT RAISE(ABORT,'journal_immutable'); END;
CREATE TRIGGER IF NOT EXISTS robot_journal_no_delete BEFORE DELETE ON robot_journal
BEGIN SELECT RAISE(ABORT,'journal_immutable'); END;
CREATE TABLE IF NOT EXISTS reservoir_ledger(
  id INTEGER PRIMARY KEY,
  case_id TEXT NOT NULL,
  method TEXT NOT NULL,
  reservoir_id TEXT NOT NULL,
  command_id TEXT NOT NULL,
  delta_ul REAL NOT NULL,
  delta_mass REAL NOT NULL,
  balance_ul REAL NOT NULL,
  balance_mass REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS well_state(
  case_id TEXT NOT NULL,
  method TEXT NOT NULL,
  well_id TEXT NOT NULL,
  volume_ul REAL NOT NULL DEFAULT 0,
  mass REAL NOT NULL DEFAULT 0,
  state TEXT NOT NULL DEFAULT 'open' CHECK(state IN ('open','complete','quarantined','discarded','failed')),
  replaces TEXT,
  PRIMARY KEY(case_id,method,well_id)
);
CREATE TABLE IF NOT EXISTS tip_ledger(
  case_id TEXT NOT NULL,
  method TEXT NOT NULL,
  tip_id TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'fresh' CHECK(state IN ('fresh','loaded','disposed','quarantined')),
  reservoir_id TEXT,
  concentration REAL,
  PRIMARY KEY(case_id,method,tip_id)
);
CREATE TABLE IF NOT EXISTS well_readback(
  id INTEGER PRIMARY KEY,
  case_id TEXT NOT NULL,
  method TEXT NOT NULL,
  well_id TEXT NOT NULL,
  context TEXT NOT NULL,
  measured_ul REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS pipette_issue(
  id INTEGER PRIMARY KEY,
  case_id TEXT NOT NULL,
  method TEXT,
  well_id TEXT,
  code TEXT NOT NULL,
  detail TEXT NOT NULL
);
'''


def ensure_schema(con):
    con.executescript(SCHEMA)


def register(db_path, contract_path):
    """冻结制备合同:仅绑定真实批次,不生成命令、不动任何液体。"""
    raw = Path(contract_path).read_bytes()
    contract = json.loads(raw)
    con = connect(db_path)
    ensure_schema(con)
    if not con.execute('SELECT 1 FROM batch WHERE id=?', (contract['batch_id'],)).fetchone():
        con.close()
        raise ValueError('unknown_lab_batch')
    with con:
        con.execute('INSERT INTO pipette_case VALUES (?,?,?,?)',
                    (contract['case_id'], contract['batch_id'], sha256(raw).hexdigest(), raw.decode()))
    con.close()
    return contract['case_id']


# ---------------------------------------------------------------- 模拟机器人

class SimRobot:
    """模拟移液机器人:只认物理约束,忠诚记录消耗日志,可注入故障。"""

    def __init__(self, contract):
        robot = contract['robot']
        self.robot_id = robot['id']
        self.max_aspirate = float(robot['max_aspirate_ul'])
        self.tip_count = int(robot['tip_count'])
        self.reservoirs = {
            r['id']: {'volume': float(r['available_ul']), 'concentration': float(r['concentration']),
                      'mass': float(r['available_ul']) * float(r['concentration'])}
            for r in contract['reservoirs']}
        self.wells = {w['id']: {'volume': 0.0, 'mass': 0.0} for w in contract['wells']}
        self.tips = {}
        self.journal = []
        self.journal_sink = None
        self.faults = {}

    def execute(self, cmd):
        if cmd['kind'] == 'aspirate':
            return self._aspirate(cmd)
        return self._dispense(cmd)

    def _aspirate(self, cmd):
        vol = float(cmd['volume_ul'])
        res = cmd['reservoir_id']
        tip_id = cmd['tip_id']
        if cmd['id'] in self.faults.get('lose_execute', ()):
            raise AckLost(cmd['id'])  # 断电:什么都没发生
        if vol > self.max_aspirate + EPS:
            raise PipetteError('aspirate_limit_exceeded', f'{vol}>{self.max_aspirate}')
        r = self.reservoirs[res]
        if r['volume'] + EPS < vol:
            raise PipetteError('insufficient_reservoir', res)
        tip = self.tips.setdefault(tip_id, {'loaded': 0.0, 'mass': 0.0, 'concentration': 0.0, 'uses': 0})
        if tip['loaded'] > EPS:
            raise PipetteError('tip_loaded', tip_id)
        mass = vol * r['concentration']
        r['volume'] -= vol
        r['mass'] -= mass
        tip['loaded'] += vol
        tip['mass'] += mass
        tip['concentration'] = r['concentration']
        tip['uses'] += 1
        self._journal(cmd, vol)
        if cmd['id'] in self.faults.get('drop_ack', ()):
            raise AckLost(cmd['id'])  # 物理动作已发生,回执丢失
        return {'command_id': cmd['id'], 'actual_volume_ul': vol, 'actual_mass': mass,
                'reservoir_id': res, 'concentration': r['concentration']}

    def _dispense(self, cmd):
        tip_id = cmd['tip_id']
        if cmd['id'] in self.faults.get('lose_execute', ()):
            raise AckLost(cmd['id'])
        tip = self.tips.get(tip_id)
        if not tip or tip['loaded'] <= EPS:
            raise PipetteError('tip_empty', tip_id)
        target = self.faults.get('misaddress', {}).get(cmd['id'], cmd['well_id'])
        well = self.wells.setdefault(target, {'volume': 0.0, 'mass': 0.0})
        vol = tip['loaded']
        mass = tip['mass']
        well['volume'] += vol
        well['mass'] += mass
        tip['loaded'] = 0.0
        tip['mass'] = 0.0
        self._journal(cmd, vol)
        if cmd['id'] in self.faults.get('drop_ack', ()):
            raise AckLost(cmd['id'])
        return {'command_id': cmd['id'], 'actual_volume_ul': vol, 'actual_mass': mass, 'well_id': target}

    def _journal(self, cmd, actual_ul):
        entry = {'robot_id': self.robot_id, 'command_id': cmd['id'], 'kind': cmd['kind'],
                 'reservoir_id': cmd.get('reservoir_id'), 'well_id': cmd.get('well_id'),
                 'volume_ul': actual_ul, 'tip_id': cmd['tip_id']}
        self.journal.append(entry)
        if self.journal_sink:
            self.journal_sink(entry)

    def readback(self, well_id):
        """独立孔位体积回读(模拟称重),不受控制器账目影响。"""
        if self.faults.get('readback_offline'):
            raise ReadbackOffline(well_id)
        override = self.faults.get('readback_override', {})
        if well_id in override:
            return float(override[well_id])
        return self.wells.get(well_id, {'volume': 0.0})['volume']

    def reservoir_level(self, reservoir_id):
        return self.reservoirs[reservoir_id]['volume']


# ---------------------------------------------------------------- 内部工具

def _contract(con, case_id):
    row = con.execute('SELECT * FROM pipette_case WHERE id=?', (case_id,)).fetchone()
    if not row:
        raise PipetteError('unknown_case', case_id)
    return json.loads(row['contract_json'])


def _concentrations(contract):
    return {r['id']: float(r['concentration']) for r in contract['reservoirs']}


def _balances(con, case_id, method, contract):
    bal = {r['id']: {'ul': float(r['available_ul']),
                     'mass': float(r['available_ul']) * float(r['concentration'])}
           for r in contract['reservoirs']}
    for row in con.execute(
            'SELECT reservoir_id, COALESCE(sum(delta_ul),0) d, COALESCE(sum(delta_mass),0) m'
            ' FROM reservoir_ledger WHERE case_id=? AND method=? GROUP BY reservoir_id',
            (case_id, method)):
        bal[row['reservoir_id']]['ul'] += row['d']
        bal[row['reservoir_id']]['mass'] += row['m']
    return bal


def _issue(con, case_id, method, well_id, code, detail):
    con.execute('INSERT INTO pipette_issue(case_id,method,well_id,code,detail) VALUES (?,?,?,?,?)',
                (case_id, method, well_id, code, detail))


def _issue_once(con, case_id, method, well_id, code, detail):
    if not con.execute('SELECT 1 FROM pipette_issue WHERE case_id=? AND method=? AND code=? AND detail=?',
                       (case_id, method, code, detail)).fetchone():
        _issue(con, case_id, method, well_id, code, detail)


def _root_well(con, case_id, method, well_id):
    seen = set()
    while well_id not in seen:
        seen.add(well_id)
        row = con.execute('SELECT replaces FROM well_state WHERE case_id=? AND method=? AND well_id=?',
                          (case_id, method, well_id)).fetchone()
        if not row or not row['replaces']:
            return well_id
        well_id = row['replaces']
    return well_id


def persist_journal(db_path, entry):
    """机器人消耗日志的持久镜像:追加写、哈希链、触发器禁止改删。"""
    con = connect(db_path)
    ensure_schema(con)
    prev = con.execute('SELECT sha256 FROM robot_journal ORDER BY seq DESC LIMIT 1').fetchone()
    prev_sha = prev['sha256'] if prev else JOURNAL_GENESIS
    payload = json.dumps({**entry, 'prev_sha256': prev_sha}, sort_keys=True, ensure_ascii=False)
    digest = sha256(payload.encode()).hexdigest()
    with con:
        con.execute(
            'INSERT INTO robot_journal(robot_id,command_id,kind,reservoir_id,well_id,volume_ul,tip_id,prev_sha256,sha256)'
            ' VALUES (?,?,?,?,?,?,?,?,?)',
            (entry['robot_id'], entry['command_id'], entry['kind'], entry.get('reservoir_id'),
             entry.get('well_id'), entry['volume_ul'], entry['tip_id'], prev_sha, digest))
    con.close()
    return digest


def verify_journal(con):
    prev = JOURNAL_GENESIS
    for row in con.execute('SELECT * FROM robot_journal ORDER BY seq'):
        payload = json.dumps({'robot_id': row['robot_id'], 'command_id': row['command_id'],
                              'kind': row['kind'], 'reservoir_id': row['reservoir_id'],
                              'well_id': row['well_id'], 'volume_ul': row['volume_ul'],
                              'tip_id': row['tip_id'], 'prev_sha256': prev},
                             sort_keys=True, ensure_ascii=False)
        if sha256(payload.encode()).hexdigest() != row['sha256']:
            return False
        prev = row['sha256']
    return True


def recipe_for(contract, method):
    """按批准的制备法推导每孔的物料移动腿(源容器, 体积)。"""
    recipe = {}
    if method == 'direct_stock':
        for w in contract['wells']:
            match = [r for r in contract['reservoirs']
                     if float(r['concentration']) == float(w['target_concentration'])]
            if not match:
                raise PipetteError('plan_infeasible', f"no stock at {w['target_concentration']}")
            recipe[w['id']] = [(match[0]['id'], float(w['target_ul']))]
    elif method == 'dilute_from_high':
        high = max(contract['reservoirs'], key=lambda r: r['concentration'])
        zeros = [r for r in contract['reservoirs'] if float(r['concentration']) == 0.0]
        if not zeros:
            raise PipetteError('plan_infeasible', 'no zero-concentration diluent')
        diluent = zeros[0]
        hc = float(high['concentration'])
        for w in contract['wells']:
            c = float(w['target_concentration'])
            volume = float(w['target_ul'])
            if c > hc + EPS:
                raise PipetteError('plan_infeasible', f'target {c} above stock {hc}')
            if abs(c - hc) <= EPS:
                legs = [(high['id'], volume)]
            elif abs(c) <= EPS:
                legs = [(diluent['id'], volume)]
            else:
                vh = volume * c / hc
                legs = [(high['id'], vh), (diluent['id'], volume - vh)]
            recipe[w['id']] = legs
    else:
        raise PipetteError('unknown_method', method)
    return recipe


# ---------------------------------------------------------------- 计划

def plan(db_path, case_id, method):
    """生成可执行命令序列;幂等,已存在则不重复生成。"""
    con = connect(db_path)
    ensure_schema(con)
    try:
        contract = _contract(con, case_id)
        existing = con.execute('SELECT count(*) c FROM pipette_command WHERE case_id=? AND method=?',
                               (case_id, method)).fetchone()['c']
        if existing:
            return {'planned': existing, 'already': True}
        recipe = recipe_for(contract, method)
        max_asp = float(contract['robot']['max_aspirate_ul'])
        legs_total = sum(len(v) for v in recipe.values())
        if legs_total > int(contract['robot']['tip_count']):
            raise PipetteError('plan_infeasible',
                               f"tips {legs_total}>{contract['robot']['tip_count']}")
        conc_of = _concentrations(contract)
        totals = {}
        for w in contract['wells']:
            volume = 0.0
            mass = 0.0
            for res, vol in recipe[w['id']]:
                if vol > max_asp + EPS:
                    raise PipetteError('plan_infeasible', f'leg {vol}µL exceeds max {max_asp}')
                if vol <= EPS:
                    raise PipetteError('plan_infeasible', 'empty leg')
                totals[res] = totals.get(res, 0.0) + vol
                volume += vol
                mass += vol * conc_of[res]
            if abs(volume - float(w['target_ul'])) > EPS:
                raise PipetteError('plan_infeasible', f"well {w['id']} volume {volume}")
            if abs(mass / volume - float(w['target_concentration'])) > EPS:
                raise PipetteError('plan_infeasible', f"well {w['id']} concentration")
        for r in contract['reservoirs']:
            if totals.get(r['id'], 0.0) > float(r['available_ul']) + EPS:
                raise PipetteError('plan_infeasible', f"reservoir {r['id']} short")
        seq = 0
        tip_n = 0
        with con:
            for w in contract['wells']:
                cum = 0.0
                for res, vol in recipe[w['id']]:
                    tip_n += 1
                    tip = f'T{tip_n}'
                    cum += vol
                    asp = f'{case_id}:{method}:{seq:02d}:asp'
                    dsp = f'{case_id}:{method}:{seq:02d}:dsp'
                    con.execute(
                        'INSERT INTO pipette_command(id,case_id,method,seq,kind,reservoir_id,well_id,'
                        'volume_ul,tip_id,expect_well_ul,depends_on,retries,state)'
                        ' VALUES (?,?,?,?,?,?,?,?,?,?,?,0,?)',
                        (asp, case_id, method, seq, 'aspirate', res, w['id'], vol, tip, cum, None, 'planned'))
                    con.execute(
                        'INSERT INTO pipette_command(id,case_id,method,seq,kind,reservoir_id,well_id,'
                        'volume_ul,tip_id,expect_well_ul,depends_on,retries,state)'
                        ' VALUES (?,?,?,?,?,?,?,?,?,?,?,0,?)',
                        (dsp, case_id, method, seq, 'dispense', res, w['id'], vol, tip, cum, asp, 'planned'))
                    con.execute('INSERT INTO tip_ledger VALUES (?,?,?,?,?,?)',
                                (case_id, method, tip, 'fresh', None, None))
                    seq += 1
            for w in contract['wells']:
                con.execute('INSERT INTO well_state VALUES (?,?,?,0,0,?,NULL)',
                            (case_id, method, w['id'], 'open'))
        return {'planned': seq * 2, 'already': False}
    finally:
        con.close()


# ---------------------------------------------------------------- 执行

def _validate(con, contract, case_id, method, cmd):
    conc_of = _concentrations(contract)
    if cmd['kind'] == 'aspirate':
        if cmd['volume_ul'] > float(contract['robot']['max_aspirate_ul']) + EPS:
            raise PipetteError('aspirate_limit_exceeded', cmd['id'])
        bal = _balances(con, case_id, method, contract)[cmd['reservoir_id']]['ul']
        if bal + EPS < cmd['volume_ul']:
            raise PipetteError('insufficient_reservoir', f"{cmd['reservoir_id']} {bal:g}µL")
        tip = con.execute('SELECT * FROM tip_ledger WHERE case_id=? AND method=? AND tip_id=?',
                          (case_id, method, cmd['tip_id'])).fetchone()
        if not tip:
            raise PipetteError('unknown_tip', cmd['tip_id'])
        if tip['state'] != 'fresh':
            carried = tip['concentration'] or 0.0
            if carried > conc_of[cmd['reservoir_id']] + EPS:
                raise PipetteError('tip_contamination_risk',
                                   f"{cmd['tip_id']} {carried:g} -> {cmd['reservoir_id']}")
            raise PipetteError('tip_reuse', cmd['tip_id'])
    else:
        asp = con.execute('SELECT * FROM pipette_command WHERE id=?', (cmd['depends_on'],)).fetchone()
        if not asp or asp['state'] not in ('acked', 'reconciled'):
            raise PipetteError('unconfirmed_aspirate', cmd['id'])
        dup = con.execute("SELECT 1 FROM pipette_command WHERE depends_on=? AND kind='dispense'"
                          " AND id<>? AND state IN ('acked','reconciled')",
                          (cmd['depends_on'], cmd['id'])).fetchone()
        if dup:
            raise PipetteError('liquid_already_consumed', cmd['id'])
        tip = con.execute('SELECT * FROM tip_ledger WHERE case_id=? AND method=? AND tip_id=?',
                          (case_id, method, cmd['tip_id'])).fetchone()
        if not tip or tip['state'] != 'loaded':
            raise PipetteError('tip_not_loaded', cmd['tip_id'])
        well = con.execute('SELECT state FROM well_state WHERE case_id=? AND method=? AND well_id=?',
                           (case_id, method, cmd['well_id'])).fetchone()
        if well and well['state'] != 'open':
            raise PipetteError('well_not_serviceable', cmd['well_id'])


def _apply(con, contract, case_id, method, cmd, ack, state):
    """按实物回执落账:吸入扣容器,排出加孔位,枪头随之流转。"""
    if cmd['kind'] == 'aspirate':
        conc = _concentrations(contract)[cmd['reservoir_id']]
        vol = float(ack['actual_volume_ul'])
        mass = float(ack['actual_mass'])
        bal = _balances(con, case_id, method, contract)[cmd['reservoir_id']]
        con.execute('INSERT INTO reservoir_ledger(case_id,method,reservoir_id,command_id,'
                    'delta_ul,delta_mass,balance_ul,balance_mass) VALUES (?,?,?,?,?,?,?,?)',
                    (case_id, method, cmd['reservoir_id'], cmd['id'], -vol, -mass,
                     bal['ul'] - vol, bal['mass'] - mass))
        con.execute("UPDATE tip_ledger SET state='loaded', reservoir_id=?, concentration=?"
                    ' WHERE case_id=? AND method=? AND tip_id=?',
                    (cmd['reservoir_id'], conc, case_id, method, cmd['tip_id']))
    else:
        con.execute('UPDATE well_state SET volume_ul=volume_ul+?, mass=mass+?'
                    ' WHERE case_id=? AND method=? AND well_id=?',
                    (float(ack['actual_volume_ul']), float(ack['actual_mass']),
                     case_id, method, cmd['well_id']))
        con.execute("UPDATE tip_ledger SET state='disposed' WHERE case_id=? AND method=? AND tip_id=?",
                    (case_id, method, cmd['tip_id']))
    con.execute('UPDATE pipette_command SET state=? WHERE id=?', (state, cmd['id']))


def _send_one(con, db_path, robot, contract, case_id, method, cmd):
    with con:
        con.execute("UPDATE pipette_command SET state='sent' WHERE id=?", (cmd['id'],))
    try:
        ack = robot.execute(dict(cmd))
    except AckLost:
        raise PipetteInterrupted(cmd['id'])
    except PipetteError:
        with con:  # 机器人物理拒绝 = 什么都没发生,可安全回到 planned
            con.execute("UPDATE pipette_command SET state='planned' WHERE id=?", (cmd['id'],))
        raise
    with con:
        _apply(con, contract, case_id, method, cmd, ack, 'acked')
    return ack


def send_command(db_path, robot, case_id, method, command_id):
    """低层单命令入口:已确认/在途/作废的命令一律拒绝,绝不盲重放。"""
    con = connect(db_path)
    ensure_schema(con)
    try:
        contract = _contract(con, case_id)
        robot.journal_sink = lambda entry: persist_journal(db_path, entry)
        cmd = con.execute('SELECT * FROM pipette_command WHERE id=?', (command_id,)).fetchone()
        if not cmd:
            raise PipetteError('unknown_command', command_id)
        if cmd['state'] in ('acked', 'reconciled'):
            raise PipetteError('duplicate_command', command_id)
        if cmd['state'] == 'sent':
            raise PipetteError('command_in_flight', command_id)
        if cmd['state'] == 'void':
            raise PipetteError('command_void', command_id)
        _validate(con, contract, case_id, method, cmd)
        return _send_one(con, db_path, robot, contract, case_id, method, cmd)
    finally:
        con.close()


def execute(db_path, case_id, method, robot):
    """按计划顺序执行;已确认命令幂等跳过;在途命令触发中断而非重放。"""
    con = connect(db_path)
    ensure_schema(con)
    try:
        contract = _contract(con, case_id)
        robot.journal_sink = lambda entry: persist_journal(db_path, entry)
        rows = con.execute('SELECT * FROM pipette_command WHERE case_id=? AND method=?'
                           ' ORDER BY seq, kind', (case_id, method)).fetchall()
        for cmd in rows:
            if cmd['state'] in ('acked', 'reconciled', 'void'):
                continue
            if cmd['state'] == 'sent':
                raise PipetteInterrupted(cmd['id'])
            well = con.execute('SELECT state FROM well_state WHERE case_id=? AND method=? AND well_id=?',
                               (case_id, method, cmd['well_id'])).fetchone()
            if well and well['state'] != 'open':
                with con:
                    _issue_once(con, case_id, method, cmd['well_id'], 'command_skipped',
                                f"{cmd['id']}: well {well['state']}")
                continue
            try:
                _validate(con, contract, case_id, method, cmd)
            except PipetteError as exc:
                with con:
                    _issue(con, case_id, method, cmd['well_id'], exc.code, cmd['id'])
                    if cmd['well_id']:
                        con.execute("UPDATE well_state SET state='failed' WHERE case_id=? AND method=?"
                                    " AND well_id=? AND state='open'",
                                    (case_id, method, cmd['well_id']))
                continue
            _send_one(con, db_path, robot, contract, case_id, method, cmd)
    finally:
        con.close()


# ---------------------------------------------------------------- 对账恢复

def _readback(con, robot, case_id, method, well_id, context):
    try:
        measured = robot.readback(well_id)
    except ReadbackOffline:
        return None
    with con:
        con.execute('INSERT INTO well_readback(case_id,method,well_id,context,measured_ul)'
                    ' VALUES (?,?,?,?,?)', (case_id, method, well_id, context, measured))
    return measured


def _quarantine(con, case_id, method, well_id, cmd, code, detail):
    with con:
        con.execute("UPDATE well_state SET state='quarantined' WHERE case_id=? AND method=? AND well_id=?",
                    (case_id, method, well_id))
        con.execute("UPDATE tip_ledger SET state='quarantined' WHERE case_id=? AND method=? AND tip_id=?",
                    (case_id, method, cmd['tip_id']))
        con.execute("UPDATE pipette_command SET state='void' WHERE id=?", (cmd['id'],))
        _issue(con, case_id, method, well_id, code, detail)


def _discard_and_replan(con, contract, case_id, method, well_id, measured, cmd, actions):
    """过量/污染不可回滚:原孔标记废弃并保留实测,按真实库存另排新孔。"""
    conc_of = _concentrations(contract)
    like_self = f'{case_id}:{method}:%'
    like_case = f'{case_id}:%'
    mass = 0.0
    for entry in con.execute(
            "SELECT * FROM robot_journal WHERE well_id=? AND kind='dispense'"
            ' AND (command_id LIKE ? OR command_id NOT LIKE ?)',
            (well_id, like_self, like_case)):
        mass += entry['volume_ul'] * conc_of.get(entry['reservoir_id'], 0.0)
    with con:
        con.execute("UPDATE well_state SET volume_ul=?, mass=?, state='discarded'"
                    ' WHERE case_id=? AND method=? AND well_id=?',
                    (measured, mass, case_id, method, well_id))
        con.execute("UPDATE tip_ledger SET state='quarantined' WHERE case_id=? AND method=? AND tip_id=?",
                    (case_id, method, cmd['tip_id']))
        con.execute("UPDATE pipette_command SET state='void' WHERE id=?", (cmd['id'],))
        _issue(con, case_id, method, well_id, 'well_discarded',
               f'physical {measured:g}µL exceeds expectation; liquid cannot be rolled back')
    actions.append({'well': well_id, 'action': 'discarded', 'measured_ul': measured})
    legs = recipe_for(contract, method)[well_id]
    balances = _balances(con, case_id, method, contract)
    tips_taken = con.execute('SELECT count(*) c FROM tip_ledger WHERE case_id=? AND method=?',
                             (case_id, method)).fetchone()['c']
    tips_free = int(contract['robot']['tip_count']) - tips_taken
    short = [res for res, vol in legs if balances[res]['ul'] + EPS < vol]
    if short or tips_free < len(legs):
        detail = []
        if short:
            detail.append('reservoirs short: ' + ','.join(short))
        if tips_free < len(legs):
            detail.append(f'tips {tips_free}<{len(legs)}')
        with con:
            _issue(con, case_id, method, well_id, 'replan_blocked', '; '.join(detail))
        actions.append({'well': well_id, 'action': 'replan_blocked'})
        return
    n = con.execute('SELECT count(*) c FROM well_state WHERE case_id=? AND method=? AND replaces=?',
                    (case_id, method, well_id)).fetchone()['c'] + 1
    new_well = f'{well_id}R{n}'
    seq = con.execute('SELECT max(seq) m FROM pipette_command WHERE case_id=? AND method=?',
                      (case_id, method)).fetchone()['m'] + 1
    with con:
        con.execute('INSERT INTO well_state VALUES (?,?,?,0,0,?,?)',
                    (case_id, method, new_well, 'open', well_id))
        cum = 0.0
        for res, vol in legs:
            tips_taken += 1
            tip = f'T{tips_taken}'
            cum += vol
            asp = f'{case_id}:{method}:{seq:02d}:asp'
            dsp = f'{case_id}:{method}:{seq:02d}:dsp'
            con.execute('INSERT INTO pipette_command(id,case_id,method,seq,kind,reservoir_id,well_id,'
                        'volume_ul,tip_id,expect_well_ul,depends_on,retries,state)'
                        ' VALUES (?,?,?,?,?,?,?,?,?,?,?,0,?)',
                        (asp, case_id, method, seq, 'aspirate', res, new_well, vol, tip, cum, None, 'planned'))
            con.execute('INSERT INTO pipette_command(id,case_id,method,seq,kind,reservoir_id,well_id,'
                        'volume_ul,tip_id,expect_well_ul,depends_on,retries,state)'
                        ' VALUES (?,?,?,?,?,?,?,?,?,?,?,0,?)',
                        (dsp, case_id, method, seq, 'dispense', res, new_well, vol, tip, cum, asp, 'planned'))
            con.execute('INSERT INTO tip_ledger VALUES (?,?,?,?,?,?)',
                        (case_id, method, tip, 'fresh', None, None))
            seq += 1
        _issue(con, case_id, method, new_well, 'well_replanned', f'replaces {well_id}')
    actions.append({'well': new_well, 'action': 'replanned', 'replaces': well_id})


def recover(db_path, case_id, method, robot):
    """对账:容器前向更正、在途命令按日志+回读判定、全板巡检。绝不盲重放。"""
    con = connect(db_path)
    ensure_schema(con)
    try:
        contract = _contract(con, case_id)
        robot.journal_sink = lambda entry: persist_journal(db_path, entry)
        actions = []
        if not verify_journal(con):
            with con:
                _issue(con, case_id, method, None, 'journal_corrupt', 'robot_journal')
            actions.append({'action': 'journal_corrupt'})
        conc_of = _concentrations(contract)
        # 1) 容器实物对账:只前向更正,绝不靠回滚抹去液体。
        #    在途吸入(日志已记录、命令未确认)留给步骤 2 对账,不算差异;
        #    日志里有记录但命令表没有的消耗(如绕开控制器的重试)必须前向补账。
        sent_asp = {}
        for row in con.execute("SELECT id, reservoir_id, volume_ul FROM pipette_command"
                               " WHERE case_id=? AND method=? AND state='sent' AND kind='aspirate'",
                               (case_id, method)):
            if con.execute('SELECT 1 FROM robot_journal WHERE command_id=?', (row['id'],)).fetchone():
                sent_asp[row['reservoir_id']] = sent_asp.get(row['reservoir_id'], 0.0) + row['volume_ul']
        unlogged = {}
        for row in con.execute("SELECT command_id, reservoir_id, volume_ul FROM robot_journal"
                               " WHERE kind='aspirate' AND command_id NOT IN"
                               ' (SELECT id FROM pipette_command)'):
            unlogged[row['reservoir_id']] = unlogged.get(row['reservoir_id'], 0.0) + row['volume_ul']
        for res_id, bal in _balances(con, case_id, method, contract).items():
            physical = robot.reservoir_level(res_id)
            extra = unlogged.get(res_id, 0.0)
            if abs(extra) > EPS:
                with con:
                    con.execute('INSERT INTO reservoir_ledger(case_id,method,reservoir_id,command_id,'
                                'delta_ul,delta_mass,balance_ul,balance_mass) VALUES (?,?,?,?,?,?,?,?)',
                                (case_id, method, res_id, 'UNLOGGED', -extra, -extra * conc_of[res_id],
                                 bal['ul'] - extra, bal['mass'] - extra * conc_of[res_id]))
                    _issue(con, case_id, method, None, 'unlogged_reservoir_consumption',
                           f'{res_id} {-extra:+.6g}µL')
                bal['ul'] -= extra
                bal['mass'] -= extra * conc_of[res_id]
                actions.append({'reservoir': res_id, 'action': 'corrected_to_physical',
                                'delta_ul': -extra})
            drift = physical - (bal['ul'] - sent_asp.get(res_id, 0.0))
            if abs(drift) > EPS:
                with con:
                    con.execute('INSERT INTO reservoir_ledger(case_id,method,reservoir_id,command_id,'
                                'delta_ul,delta_mass,balance_ul,balance_mass) VALUES (?,?,?,?,?,?,?,?)',
                                (case_id, method, res_id, 'DRIFT', drift, drift * conc_of[res_id],
                                 bal['ul'] + drift, bal['mass'] + drift * conc_of[res_id]))
                    _issue(con, case_id, method, None, 'reservoir_drift',
                           f'{res_id} {drift:+.6g}µL')
                actions.append({'reservoir': res_id, 'action': 'drift_corrected', 'delta_ul': drift})
        # 2) 在途命令:消耗日志 + 独立回读联合判定
        sent = con.execute("SELECT * FROM pipette_command WHERE case_id=? AND method=? AND state='sent'"
                           ' ORDER BY seq, kind', (case_id, method)).fetchall()
        for cmd in sent:
            entries = con.execute('SELECT * FROM robot_journal WHERE command_id=? ORDER BY seq',
                                  (cmd['id'],)).fetchall()
            if cmd['kind'] == 'dispense':
                well_id = cmd['well_id']
                measured = _readback(con, robot, case_id, method, well_id, 'recover')
                prior = con.execute('SELECT volume_ul FROM well_state WHERE case_id=? AND method=?'
                                    ' AND well_id=?', (case_id, method, well_id)).fetchone()['volume_ul']
                if (measured is not None and len(entries) == 1
                        and abs(measured - (prior + cmd['volume_ul'])) <= EPS):
                    ack = {'actual_volume_ul': entries[0]['volume_ul'],
                           'actual_mass': entries[0]['volume_ul'] * conc_of[cmd['reservoir_id']]}
                    with con:
                        _apply(con, contract, case_id, method, cmd, ack, 'reconciled')
                    actions.append({'command': cmd['id'], 'action': 'reconciled'})
                elif measured is not None and len(entries) == 0 and abs(measured - prior) <= EPS:
                    if cmd['retries'] >= 1:
                        _quarantine(con, case_id, method, well_id, cmd, 'retry_exhausted', cmd['id'])
                        actions.append({'command': cmd['id'], 'action': 'quarantined', 'well': well_id})
                    else:
                        with con:
                            con.execute("UPDATE pipette_command SET state='planned', retries=retries+1"
                                        ' WHERE id=?', (cmd['id'],))
                        actions.append({'command': cmd['id'], 'action': 'proved_not_executed'})
                elif measured is not None and (len(entries) > 1
                                               or measured > prior + cmd['volume_ul'] + EPS):
                    _discard_and_replan(con, contract, case_id, method, well_id,
                                        measured, cmd, actions)
                else:
                    _quarantine(con, case_id, method, well_id, cmd, 'unverifiable_dispense', cmd['id'])
                    actions.append({'command': cmd['id'], 'action': 'quarantined', 'well': well_id})
            else:
                level = robot.reservoir_level(cmd['reservoir_id'])
                bal = _balances(con, case_id, method, contract)[cmd['reservoir_id']]['ul']
                if len(entries) == 1 and abs(level - (bal - cmd['volume_ul'])) <= EPS:
                    ack = {'actual_volume_ul': entries[0]['volume_ul'],
                           'actual_mass': entries[0]['volume_ul'] * conc_of[cmd['reservoir_id']]}
                    with con:
                        _apply(con, contract, case_id, method, cmd, ack, 'reconciled')
                    actions.append({'command': cmd['id'], 'action': 'reconciled'})
                elif len(entries) == 0 and abs(level - bal) <= EPS:
                    with con:
                        con.execute("UPDATE pipette_command SET state='planned', retries=retries+1"
                                    ' WHERE id=?', (cmd['id'],))
                    actions.append({'command': cmd['id'], 'action': 'proved_not_executed'})
                else:
                    with con:
                        con.execute("UPDATE tip_ledger SET state='quarantined'"
                                    ' WHERE case_id=? AND method=? AND tip_id=?',
                                    (case_id, method, cmd['tip_id']))
                        con.execute("UPDATE pipette_command SET state='void' WHERE id=?", (cmd['id'],))
                        _issue(con, case_id, method, cmd['well_id'], 'unverifiable_aspirate', cmd['id'])
                    actions.append({'command': cmd['id'], 'action': 'quarantined_tip'})
        # 3) 全板回读巡检:发现串扰与未记录液体
        for well in con.execute('SELECT * FROM well_state WHERE case_id=? AND method=?',
                                (case_id, method)).fetchall():
            if well['state'] != 'open':
                continue
            measured = _readback(con, robot, case_id, method, well['well_id'], 'sweep')
            if measured is None:
                continue
            if abs(measured - well['volume_ul']) > EPS:
                code = 'unexpected_well_volume' if measured > well['volume_ul'] else 'well_volume_deficit'
                with con:
                    con.execute("UPDATE well_state SET state='quarantined'"
                                ' WHERE case_id=? AND method=? AND well_id=?',
                                (case_id, method, well['well_id']))
                    _issue(con, case_id, method, well['well_id'], code,
                           f"measured {measured:g} != booked {well['volume_ul']:g}")
                actions.append({'well': well['well_id'], 'action': 'quarantined', 'code': code})
        return actions
    finally:
        con.close()


# ---------------------------------------------------------------- 终验

def finalize(db_path, case_id, method, robot):
    """终验:逐孔回读对账,浓度=溶质/实测体积,容器账实核对。"""
    con = connect(db_path)
    ensure_schema(con)
    try:
        contract = _contract(con, case_id)
        targets = {w['id']: (float(w['target_ul']), float(w['target_concentration']))
                   for w in contract['wells']}
        wells = {}
        for well in con.execute('SELECT * FROM well_state WHERE case_id=? AND method=? ORDER BY well_id',
                                (case_id, method)).fetchall():
            wid = well['well_id']
            measured = _readback(con, robot, case_id, method, wid, 'finalize')
            state = well['state']
            if (measured is not None and abs(measured - well['volume_ul']) > EPS
                    and state in ('open', 'failed')):
                with con:
                    con.execute("UPDATE well_state SET state='quarantined'"
                                ' WHERE case_id=? AND method=? AND well_id=?', (case_id, method, wid))
                    _issue(con, case_id, method, wid, 'readback_mismatch',
                           f"measured {measured:g} != booked {well['volume_ul']:g}")
                state = 'quarantined'
            elif state == 'open':
                tv, tc = targets[_root_well(con, case_id, method, wid)]
                conc = well['mass'] / well['volume_ul'] if well['volume_ul'] > EPS else 0.0
                if abs(well['volume_ul'] - tv) <= EPS and abs(conc - tc) <= EPS:
                    with con:
                        con.execute("UPDATE well_state SET state='complete'"
                                    ' WHERE case_id=? AND method=? AND well_id=?', (case_id, method, wid))
                    state = 'complete'
                else:
                    with con:
                        con.execute("UPDATE well_state SET state='failed'"
                                    ' WHERE case_id=? AND method=? AND well_id=?', (case_id, method, wid))
                        _issue(con, case_id, method, wid, 'target_mismatch',
                               f"{well['volume_ul']:g}µL @{conc:g}")
                    state = 'failed'
            wells[wid] = {'state': state, 'volume_ul': well['volume_ul'], 'mass': well['mass'],
                          'concentration': (well['mass'] / well['volume_ul']
                                            if well['volume_ul'] > EPS else 0.0)}
        reservoirs = {}
        for res_id, bal in _balances(con, case_id, method, contract).items():
            physical = robot.reservoir_level(res_id)
            if abs(physical - bal['ul']) > EPS:
                with con:
                    _issue(con, case_id, method, None, 'reservoir_drift',
                           f"{res_id} physical {physical:g} != ledger {bal['ul']:g}")
            reservoirs[res_id] = {'remaining_ul': bal['ul'], 'physical_ul': physical}
        complete = True
        for w in contract['wells']:
            wid = w['id']
            ok = wells.get(wid, {}).get('state') == 'complete'
            if not ok:
                repl = con.execute('SELECT well_id FROM well_state WHERE case_id=? AND method=?'
                                   ' AND replaces=?', (case_id, method, wid)).fetchall()
                ok = any(wells.get(r['well_id'], {}).get('state') == 'complete' for r in repl)
            complete = complete and ok
        issues = [dict(r) for r in con.execute(
            'SELECT well_id, code, detail FROM pipette_issue WHERE case_id=? AND method=? ORDER BY id',
            (case_id, method))]
        return {'wells': wells, 'reservoirs': reservoirs, 'complete': complete, 'issues': issues}
    finally:
        con.close()


# ---------------------------------------------------------------- 编排

def load_robot(db_path, case_id):
    con = connect(db_path)
    ensure_schema(con)
    try:
        contract = _contract(con, case_id)
    finally:
        con.close()
    return SimRobot(contract)


def _apply_contract_fault(db_path, case_id, method, robot):
    con = connect(db_path)
    ensure_schema(con)
    try:
        contract = _contract(con, case_id)
        fault = contract.get('fault_injection')
        if not fault:
            return
        if fault.get('point') != 'after_dispense_before_ack':
            raise PipetteError('unknown_fault_point', str(fault))
        row = con.execute("SELECT id FROM pipette_command WHERE case_id=? AND method=? AND seq=?"
                          " AND kind='dispense' AND well_id=?",
                          (case_id, method, fault['command_index'], fault['well'])).fetchone()
        if not row:
            raise PipetteError('fault_target_missing', str(fault))
        robot.faults.setdefault('drop_ack', set()).add(row['id'])
    finally:
        con.close()


def run_method(db_path, case_id, method, robot=None, inject_fault=False):
    """完整生命周期:计划→执行→(中断时对账恢复)→终验。"""
    robot = robot or load_robot(db_path, case_id)
    plan(db_path, case_id, method)
    if inject_fault:
        _apply_contract_fault(db_path, case_id, method, robot)
    recovery = []
    for _ in range(4):
        try:
            execute(db_path, case_id, method, robot)
            break
        except PipetteInterrupted:
            recovery.extend(recover(db_path, case_id, method, robot))
    else:
        raise PipetteError('recovery_exhausted', case_id)
    summary = finalize(db_path, case_id, method, robot)
    return {'recovery': recovery, 'summary': summary}


# ---------------------------------------------------------------- 报告

def _method_report(con, contract, case_id, method, batch_id):
    targets = {w['id']: w for w in contract['wells']}
    commands = [dict(r) for r in con.execute(
        'SELECT id, seq, kind, reservoir_id, well_id, volume_ul, tip_id, expect_well_ul,'
        ' depends_on, retries, state FROM pipette_command WHERE case_id=? AND method=?'
        ' ORDER BY seq, kind', (case_id, method))]
    wells = {}
    for well in con.execute('SELECT * FROM well_state WHERE case_id=? AND method=? ORDER BY well_id',
                            (case_id, method)):
        wid = well['well_id']
        vol = well['volume_ul']
        mass = well['mass']
        sources = [dict(r) for r in con.execute(
            'SELECT reservoir_id, volume_ul, tip_id, id AS command_id FROM pipette_command'
            " WHERE case_id=? AND method=? AND well_id=? AND kind='dispense'"
            " AND state IN ('acked','reconciled') ORDER BY seq", (case_id, method, wid))]
        readbacks = [dict(r) for r in con.execute(
            'SELECT context, measured_ul FROM well_readback WHERE case_id=? AND method=? AND well_id=?'
            ' ORDER BY id', (case_id, method, wid))]
        target = targets.get(_root_well(con, case_id, method, wid))
        wells[wid] = {
            'volume_ul': vol, 'mass': mass,
            'concentration': (mass / vol if vol > EPS else 0.0),
            'state': well['state'], 'replaces': well['replaces'], 'batch_id': batch_id,
            'target_ul': target['target_ul'] if target else None,
            'target_concentration': target['target_concentration'] if target else None,
            'sources': sources, 'readbacks': readbacks}
    balances = _balances(con, case_id, method, contract)
    reservoirs = {}
    for r in contract['reservoirs']:
        rid = r['id']
        ledger = [dict(x) for x in con.execute(
            'SELECT command_id, delta_ul, delta_mass, balance_ul, balance_mass FROM reservoir_ledger'
            ' WHERE case_id=? AND method=? AND reservoir_id=? ORDER BY id', (case_id, method, rid))]
        reservoirs[rid] = {'concentration': float(r['concentration']),
                           'initial_ul': float(r['available_ul']),
                           'remaining_ul': balances[rid]['ul'],
                           'remaining_mass': balances[rid]['mass'], 'ledger': ledger}
    tips = [dict(r) for r in con.execute(
        'SELECT tip_id, state, reservoir_id, concentration FROM tip_ledger'
        ' WHERE case_id=? AND method=? ORDER BY tip_id', (case_id, method))]
    issues = [dict(r) for r in con.execute(
        'SELECT well_id, code, detail FROM pipette_issue WHERE case_id=? AND method=? ORDER BY id',
        (case_id, method))]
    return {'commands': commands, 'wells': wells, 'reservoirs': reservoirs,
            'tips': tips, 'issues': issues}


def _markdown(data):
    lines = [f"# 质控板制备审计 {data['case_id']}", '',
             f"- 批次: {data['batch_id']} · 板: {data['plate_id']} · 机器人: {data['robot_id']}",
             f"- 合同哈希: {data['contract_sha256']}",
             f"- 消耗日志: {data['journal']['entries']} 条 · 校验 {'通过' if data['journal']['verified'] else '失败'}"
             f" · 链头 {data['journal']['head_sha256'] or '—'}", '']
    for method, m in data['methods'].items():
        lines += [f'## 方法 {method}', '', '### 板图(实测)', '',
                  '| 孔位 | 目标 µL@浓度 | 实测 µL | 溶质 | 浓度 | 状态 | 来源 |',
                  '|---|---|---|---|---|---|---|']
        for wid, w in m['wells'].items():
            src = '; '.join(f"{s['reservoir_id']} {s['volume_ul']:g}µL({s['tip_id']})"
                            for s in w['sources']) or '—'
            lines.append(f"| {wid} | {w['target_ul']:g}@{w['target_concentration']:g} | {w['volume_ul']:g}"
                         f" | {w['mass']:g} | {w['concentration']:g} | {w['state']} | {src} |")
        lines += ['', '### 试剂库存账', '', '| 容器 | 浓度 | 初始 µL | 剩余 µL | 剩余溶质 |',
                  '|---|---|---|---|---|']
        for rid, r in m['reservoirs'].items():
            lines.append(f"| {rid} | {r['concentration']:g} | {r['initial_ul']:g}"
                         f" | {r['remaining_ul']:g} | {r['remaining_mass']:g} |")
        lines += ['', '### 命令序列', '', '| # | 动作 | 源容器 | 目标孔 | 体积 µL | 枪头 | 状态 |',
                  '|---|---|---|---|---|---|---|']
        for c in m['commands']:
            lines.append(f"| {c['seq']} | {c['kind']} | {c['reservoir_id'] or '—'}"
                         f" | {c['well_id'] or '—'} | {c['volume_ul']:g} | {c['tip_id']} | {c['state']} |")
        lines += ['', '### 枪头去向', '', '| 枪头 | 状态 | 接触容器 |', '|---|---|---|']
        for t in m['tips']:
            lines.append(f"| {t['tip_id']} | {t['state']} | {t['reservoir_id'] or '—'} |")
        lines += ['', '### 问题', '']
        if m['issues']:
            for i in m['issues']:
                suffix = f"(孔 {i['well_id']})" if i['well_id'] else ''
                lines.append(f"- {i['code']}: {i['detail']}{suffix}")
        else:
            lines.append('- 无')
        lines.append('')
    return '\n'.join(lines)


def report(db_path, case_id, out):
    """输出 JSON 机器审计与 Markdown 板图;内容确定,不含时间戳。"""
    con = connect(db_path)
    ensure_schema(con)
    try:
        case = con.execute('SELECT * FROM pipette_case WHERE id=?', (case_id,)).fetchone()
        if not case:
            raise PipetteError('unknown_case', case_id)
        contract = json.loads(case['contract_json'])
        head = con.execute('SELECT sha256 FROM robot_journal ORDER BY seq DESC LIMIT 1').fetchone()
        data = {
            'case_id': case_id,
            'batch_id': case['batch_id'],
            'plate_id': contract.get('plate_id'),
            'robot_id': contract['robot']['id'],
            'contract_sha256': case['contract_sha256'],
            'journal': {'entries': con.execute('SELECT count(*) c FROM robot_journal').fetchone()['c'],
                        'verified': verify_journal(con),
                        'head_sha256': head['sha256'] if head else None},
            'methods': {},
        }
        for row in con.execute('SELECT DISTINCT method FROM pipette_command WHERE case_id=?'
                               ' ORDER BY method', (case_id,)):
            data['methods'][row['method']] = _method_report(
                con, contract, case_id, row['method'], case['batch_id'])
        out = Path(out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.with_suffix('.json').write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        out.with_suffix('.md').write_text(_markdown(data), encoding='utf-8')
        return out.with_suffix('.json')
    finally:
        con.close()
