import sqlite3
from pathlib import Path

SCHEMA="""
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS batch(id TEXT PRIMARY KEY, expiry TEXT NOT NULL, max_out_minutes INTEGER NOT NULL, min_temp REAL NOT NULL, max_temp REAL NOT NULL);
CREATE TABLE IF NOT EXISTS sample(id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batch(id), replacement_of TEXT REFERENCES sample(id));
CREATE TABLE IF NOT EXISTS source(id INTEGER PRIMARY KEY, path TEXT NOT NULL, sha256 TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS event(id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL REFERENCES source(id), line_no INTEGER NOT NULL, device TEXT NOT NULL, seq INTEGER NOT NULL, sample_id TEXT, kind TEXT NOT NULL, ts TEXT NOT NULL, actor TEXT, seal TEXT, value REAL, payload TEXT NOT NULL, sha256 TEXT NOT NULL, UNIQUE(source_id,line_no));
CREATE TABLE IF NOT EXISTS issue(id INTEGER PRIMARY KEY, sample_id TEXT, event_id INTEGER REFERENCES event(id), code TEXT NOT NULL, detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS decision(sample_id TEXT PRIMARY KEY REFERENCES sample(id), result TEXT NOT NULL, reasons TEXT NOT NULL, event_ids TEXT NOT NULL, rules_version TEXT NOT NULL);
"""

def connect(path):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    con=sqlite3.connect(path);con.row_factory=sqlite3.Row;con.executescript(SCHEMA)
    return con
