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
CREATE TABLE IF NOT EXISTS catalog_case(id TEXT PRIMARY KEY, payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS catalog_offer(id INTEGER PRIMARY KEY, case_id TEXT NOT NULL, supplier TEXT NOT NULL, page TEXT NOT NULL, page_sha256 TEXT NOT NULL, maker_code TEXT NOT NULL, gtin TEXT NOT NULL, name TEXT NOT NULL, quantity INTEGER NOT NULL, unit TEXT NOT NULL, amount_cents INTEGER NOT NULL, storage TEXT NOT NULL, revision INTEGER NOT NULL, canonical_unit TEXT NOT NULL, unit_price_cents REAL NOT NULL, offer_sha256 TEXT NOT NULL, UNIQUE(case_id, offer_sha256));
CREATE TABLE IF NOT EXISTS catalog_issue(id INTEGER PRIMARY KEY, case_id TEXT NOT NULL, code TEXT NOT NULL, detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS catalog_version(id TEXT PRIMARY KEY, case_id TEXT NOT NULL, target TEXT NOT NULL, seq INTEGER NOT NULL, strategy TEXT NOT NULL, source_set TEXT NOT NULL, status TEXT NOT NULL, payload TEXT NOT NULL, diff TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS lims_publication(id INTEGER PRIMARY KEY, case_id TEXT NOT NULL, version_id TEXT NOT NULL, nonce TEXT NOT NULL, acked INTEGER NOT NULL DEFAULT 0, receipt TEXT NOT NULL DEFAULT '', UNIQUE(case_id, nonce));
"""

def connect(path):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    con=sqlite3.connect(path);con.row_factory=sqlite3.Row;con.executescript(SCHEMA)
    return con
