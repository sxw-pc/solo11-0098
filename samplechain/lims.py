"""本地模拟 LIMS 目录接收端：原子应用版本、严格版本栅栏、nonce 幂等。

独立 SQLite 文件模拟“远端”：已接受的版本不随采录台本地库回滚而消失，
采录台须通过 status() 核验远端最终状态。faults 用于测试注入
（partial_ack：未提交且回执否认；drop_ack：已提交但回执丢失）。
"""
import hashlib
import json
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS lims_version(target TEXT PRIMARY KEY, seq INTEGER NOT NULL, version_id TEXT NOT NULL, nonce TEXT NOT NULL, catalog TEXT NOT NULL, catalog_sha256 TEXT NOT NULL, receipt TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS lims_nonce(target TEXT NOT NULL, nonce TEXT NOT NULL, version_id TEXT NOT NULL, receipt TEXT NOT NULL, PRIMARY KEY(target, nonce));
"""


class LostAck(Exception):
    """版本已在 LIMS 落库，但 ACK 回执在传输中丢失。"""

    def __init__(self, receipt):
        super().__init__('lims_ack_lost')
        self.receipt = receipt


class StaleVersion(Exception):
    """提交的 seq 不高于 LIMS 当前版本且版本内容不同。"""


class VersionGap(Exception):
    """提交的 seq 跳过了一个版本，栅栏拒绝。"""


class NonceConflict(Exception):
    """同一 nonce 被用于两个不同的版本。"""


class MockLims:
    def __init__(self, path, faults=()):
        self.path = str(path)
        self.faults = list(faults)
        con = sqlite3.connect(self.path)
        con.executescript(SCHEMA)
        con.commit()
        con.close()

    def _connect(self):
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        return con

    def status(self, target):
        con = self._connect()
        try:
            row = con.execute('SELECT * FROM lims_version WHERE target=?', (target,)).fetchone()
            return dict(row) if row else None
        finally:
            con.close()

    def apply(self, target, version_id, seq, nonce, catalog):
        con = self._connect()
        con.isolation_level = None
        try:
            con.execute('BEGIN IMMEDIATE')
            seen = con.execute('SELECT * FROM lims_nonce WHERE target=? AND nonce=?', (target, nonce)).fetchone()
            if seen:
                con.execute('COMMIT')
                if seen['version_id'] != version_id:
                    raise NonceConflict(nonce)
                return json.loads(seen['receipt'])
            cur = con.execute('SELECT * FROM lims_version WHERE target=?', (target,)).fetchone()
            if cur:
                if seq <= cur['seq']:
                    if cur['version_id'] == version_id:
                        receipt = dict(json.loads(cur['receipt']), duplicate=True)
                        con.execute('INSERT INTO lims_nonce VALUES (?,?,?,?)',
                                    (target, nonce, version_id, json.dumps(receipt, sort_keys=True)))
                        con.execute('COMMIT')
                        return receipt
                    con.execute('ROLLBACK')
                    raise StaleVersion(f"{target}:{seq}<={cur['seq']}")
                if seq > cur['seq'] + 1:
                    con.execute('ROLLBACK')
                    raise VersionGap(f"{target}:{seq}>{cur['seq']}+1")
            elif seq != 1:
                con.execute('ROLLBACK')
                raise VersionGap(f'{target}:first={seq}')
            if 'partial_ack' in self.faults:
                self.faults.remove('partial_ack')
                con.execute('COMMIT')
                return {'ack': False, 'reason': 'partial_ack', 'target': target, 'seq': seq,
                        'version_id': version_id, 'nonce': nonce}
            products = len(json.loads(catalog)['products'])
            digest = hashlib.sha256(catalog.encode()).hexdigest()
            receipt = {'ack': True, 'target': target, 'seq': seq, 'version_id': version_id,
                       'products': products, 'catalog_sha256': digest, 'nonce': nonce}
            blob = json.dumps(receipt, sort_keys=True)
            con.execute('INSERT OR REPLACE INTO lims_version VALUES (?,?,?,?,?,?,?)',
                        (target, seq, version_id, nonce, catalog, digest, blob))
            con.execute('INSERT INTO lims_nonce VALUES (?,?,?,?)', (target, nonce, version_id, blob))
            con.execute('COMMIT')
            if 'drop_ack' in self.faults:
                self.faults.remove('drop_ack')
                raise LostAck(receipt)
            return receipt
        except Exception:
            try:
                con.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            raise
        finally:
            con.close()
