import sqlite3
from pathlib import Path

SCHEMA="""
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS batch(id TEXT PRIMARY KEY, expiry TEXT NOT NULL, max_out_minutes INTEGER NOT NULL, min_temp REAL NOT NULL, max_temp REAL NOT NULL);
CREATE TABLE IF NOT EXISTS sample(id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batch(id), replacement_of TEXT REFERENCES sample(id));
CREATE TABLE IF NOT EXISTS source(id INTEGER PRIMARY KEY, path TEXT NOT NULL, sha256 TEXT NOT NULL UNIQUE, raw BLOB NOT NULL DEFAULT X'');
CREATE TABLE IF NOT EXISTS event(id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL REFERENCES source(id), line_no INTEGER NOT NULL, device TEXT NOT NULL, seq INTEGER NOT NULL, sample_id TEXT, kind TEXT NOT NULL, ts TEXT NOT NULL, actor TEXT, seal TEXT, value REAL, payload TEXT NOT NULL, sha256 TEXT NOT NULL, UNIQUE(source_id,line_no));
CREATE TABLE IF NOT EXISTS issue(id INTEGER PRIMARY KEY, sample_id TEXT, event_id INTEGER REFERENCES event(id), code TEXT NOT NULL, detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS run(id TEXT PRIMARY KEY, rules_version TEXT NOT NULL, source_digest TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS evidence(id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES run(id), subject_id TEXT NOT NULL, source_id INTEGER REFERENCES source(id), object_kind TEXT NOT NULL, object_id INTEGER NOT NULL, detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS decision(sample_id TEXT PRIMARY KEY REFERENCES sample(id), result TEXT NOT NULL, reasons TEXT NOT NULL, event_ids TEXT NOT NULL, rules_version TEXT NOT NULL, run_id TEXT NOT NULL REFERENCES run(id));
CREATE TABLE IF NOT EXISTS fridge_case(id TEXT PRIMARY KEY,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS fridge_feed(id INTEGER PRIMARY KEY, case_id TEXT, sha256 TEXT NOT NULL UNIQUE, path TEXT NOT NULL, raw BLOB NOT NULL DEFAULT X'', status TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS fridge_plan(id TEXT PRIMARY KEY, case_id TEXT NOT NULL REFERENCES fridge_case(id), contract TEXT NOT NULL, plan_version INTEGER NOT NULL, status TEXT NOT NULL, basis TEXT NOT NULL, summary TEXT NOT NULL, evidence TEXT NOT NULL, supersedes TEXT, created_at TEXT NOT NULL, UNIQUE(case_id,contract,plan_version));
CREATE TABLE IF NOT EXISTS fridge_command(id INTEGER PRIMARY KEY, plan_id TEXT NOT NULL REFERENCES fridge_plan(id), period INTEGER NOT NULL, zone_id TEXT, action TEXT NOT NULL, status TEXT NOT NULL, tariff_cents INTEGER NOT NULL, energy_cents INTEGER NOT NULL, UNIQUE(plan_id,period));
CREATE TABLE IF NOT EXISTS fridge_period_state(id INTEGER PRIMARY KEY, plan_id TEXT NOT NULL REFERENCES fridge_plan(id), period INTEGER NOT NULL, zone_id TEXT NOT NULL, temp_before_c REAL NOT NULL, temp_after_c REAL NOT NULL, reading_c REAL, exposure_increment_c REAL NOT NULL, cumulative_exposure_c REAL NOT NULL, tariff_cents INTEGER NOT NULL, energy_cents INTEGER NOT NULL, compressor_zone TEXT, UNIQUE(plan_id,period,zone_id));
CREATE TABLE IF NOT EXISTS fridge_hold(id INTEGER PRIMARY KEY, plan_id TEXT, case_id TEXT NOT NULL, reason TEXT NOT NULL, detail TEXT NOT NULL, created_at TEXT NOT NULL, cleared INTEGER NOT NULL DEFAULT 0);
"""

def connect(path):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    con=sqlite3.connect(path);con.row_factory=sqlite3.Row;con.executescript(SCHEMA)
    return con
