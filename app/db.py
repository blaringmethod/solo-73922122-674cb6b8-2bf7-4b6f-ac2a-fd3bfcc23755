"""SQLite 连接、建表与一次性迁移。

表
--
- scans          扫码明细 (每次扫码一行, scan_id 唯一)
- flow_index     流向索引 (件/日期/方向 -> ref_count), 支撑 /flows 统计
- flow_events    查证依据明细: 每条实际相邻流转一行, 主键为"后一次扫码"
- flow_endpoints 查证索引: 每个 (件, 日期, 方向) 的首尾两次流转及两端扫码
- schema_meta    迁移标记

旧库 (已有 scans / flow_index, 没有查证表) 在首次启动时自动**补建一次**
flow_events 与 flow_endpoints, 并按 scans 重算 flow_index; 之后只在增/改/撤
的新旧相邻位置做增量维护。补建在单个事务内完成, 失败则保留原结果。

写入一律走单连接 + threading.Lock, 配合事务保证
"扫码明细、查证依据、流向索引一同生效, 失败保留原状"。
"""
from __future__ import annotations

import os
import sqlite3
import threading

DB_PATH = os.environ.get("SCANFLOW_DB_PATH", "/data/scanflow.db")

_write_lock = threading.Lock()
_conn: sqlite3.Connection | None = None

# 旧库补建标记: 存在即表示 flow_events / flow_endpoints 已按 scans 建好。
PROOF_BACKFILL_KEY = "proof_backfill_v1"
PROOF_BACKFILL_VALUE = "done"


def get_conn() -> sqlite3.Connection:
    """返回进程内唯一的 SQLite 连接 (启用 WAL, 外键)。"""
    global _conn
    if _conn is None:
        os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        _conn = conn
    return _conn


def write_lock() -> threading.Lock:
    return _write_lock


SCHEMA = """
CREATE TABLE IF NOT EXISTS scans (
    scan_id   TEXT PRIMARY KEY,              -- 每次扫码的唯一编号
    part_id   TEXT NOT NULL,                 -- 工件号
    station   TEXT NOT NULL,                 -- 工位
    ts_utc    TEXT NOT NULL,                 -- UTC 时刻, 固定格式 YYYY-MM-DDTHH:MM:SS.ffffffZ
    day_utc   TEXT NOT NULL                  -- UTC 日期 YYYY-MM-DD (扫描归属日期)
);

-- 同件扫码按 (时刻, 编号) 排序, 该索引同时服务于前后相邻扫码的定位
CREATE INDEX IF NOT EXISTS idx_scans_part_order
    ON scans (part_id, ts_utc, scan_id);

CREATE TABLE IF NOT EXISTS flow_index (
    part_id      TEXT NOT NULL,
    day_utc      TEXT NOT NULL,              -- 归入后一次扫码的 UTC 日期
    from_station TEXT NOT NULL,
    to_station   TEXT NOT NULL,
    ref_count    INTEGER NOT NULL DEFAULT 1, -- 该件当天该流向出现的次数
    PRIMARY KEY (part_id, day_utc, from_station, to_station)
);

CREATE INDEX IF NOT EXISTS idx_flow_day
    ON flow_index (day_utc);

-- 查证依据明细: 每次实际相邻流转一行。
-- 主键取"后一次扫码"(每次扫码最多只有一个前驱), 因此该表行数 = 实际流转次数。
-- 末尾两列的顺序索引用于 O(log n) 取某方向的首/末事件, 替换失效首尾时
-- 无需回扫整件扫码。
CREATE TABLE IF NOT EXISTS flow_events (
    next_scan_id TEXT PRIMARY KEY,
    part_id      TEXT NOT NULL,
    day_utc      TEXT NOT NULL,              -- 后一次扫码的 UTC 日期
    from_station TEXT NOT NULL,
    to_station   TEXT NOT NULL,
    prev_scan_id TEXT NOT NULL,              -- 前一次 (来源) 扫码
    prev_ts_utc  TEXT NOT NULL,
    next_ts_utc  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_flow_events_group
    ON flow_events (part_id, day_utc, from_station, to_station,
                    next_ts_utc, next_scan_id);

-- 查证索引: 每个 (件, 日期, 方向) 一行, 直接给出首尾两次流转的两端扫码。
-- 查询只读本表 (+ 同主键的 flow_index), 每方向 1 行, 与同向流转次数无关。
CREATE TABLE IF NOT EXISTS flow_endpoints (
    part_id             TEXT NOT NULL,
    day_utc             TEXT NOT NULL,
    from_station        TEXT NOT NULL,
    to_station          TEXT NOT NULL,
    first_prev_scan_id  TEXT NOT NULL,
    first_prev_ts_utc   TEXT NOT NULL,
    first_next_scan_id  TEXT NOT NULL,
    first_next_ts_utc   TEXT NOT NULL,
    last_prev_scan_id   TEXT NOT NULL,
    last_prev_ts_utc    TEXT NOT NULL,
    last_next_scan_id   TEXT NOT NULL,
    last_next_ts_utc    TEXT NOT NULL,
    PRIMARY KEY (part_id, day_utc, from_station, to_station)
);

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def _already_backfilled(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = ?", (PROOF_BACKFILL_KEY,)
    ).fetchone()
    return row is not None and row["value"] == PROOF_BACKFILL_VALUE


def _rebuild_derived(conn: sqlite3.Connection) -> None:
    """按 scans 全量重算派生结构 (仅用于一次性补建; 调用方需已开事务)。"""
    conn.execute("DELETE FROM flow_events")
    conn.execute("DELETE FROM flow_endpoints")
    conn.execute("DELETE FROM flow_index")

    rows = conn.execute(
        "SELECT scan_id, part_id, station, ts_utc, day_utc FROM scans"
        " ORDER BY part_id, ts_utc, scan_id"
    ).fetchall()

    prev: sqlite3.Row | None = None
    for row in rows:
        # 只有同件、且相邻扫码工位不同才产生一条流转, 归入后一次扫码的日期
        if (prev is not None and prev["part_id"] == row["part_id"]
                and prev["station"] != row["station"]):
            conn.execute(
                "INSERT INTO flow_events (next_scan_id, part_id, day_utc,"
                " from_station, to_station, prev_scan_id, prev_ts_utc, next_ts_utc)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (row["scan_id"], row["part_id"], row["day_utc"],
                 prev["station"], row["station"], prev["scan_id"],
                 prev["ts_utc"], row["ts_utc"]),
            )
            conn.execute(
                "INSERT INTO flow_index (part_id, day_utc, from_station,"
                " to_station, ref_count) VALUES (?, ?, ?, ?, 1)"
                " ON CONFLICT(part_id, day_utc, from_station, to_station)"
                " DO UPDATE SET ref_count = ref_count + 1",
                (row["part_id"], row["day_utc"], prev["station"], row["station"]),
            )
        prev = row

    # 按索引顺序扫描 flow_events, 每组的首行/末行即首尾流转
    firsts: dict[tuple, sqlite3.Row] = {}
    lasts: dict[tuple, sqlite3.Row] = {}
    for ev in conn.execute(
        "SELECT * FROM flow_events ORDER BY part_id, day_utc, from_station,"
        " to_station, next_ts_utc, next_scan_id"
    ):
        key = (ev["part_id"], ev["day_utc"], ev["from_station"], ev["to_station"])
        firsts.setdefault(key, ev)
        lasts[key] = ev

    for key, first in firsts.items():
        last = lasts[key]
        conn.execute(
            "INSERT INTO flow_endpoints (part_id, day_utc, from_station,"
            " to_station, first_prev_scan_id, first_prev_ts_utc,"
            " first_next_scan_id, first_next_ts_utc, last_prev_scan_id,"
            " last_prev_ts_utc, last_next_scan_id, last_next_ts_utc)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (key[0], key[1], key[2], key[3],
             first["prev_scan_id"], first["prev_ts_utc"],
             first["next_scan_id"], first["next_ts_utc"],
             last["prev_scan_id"], last["prev_ts_utc"],
             last["next_scan_id"], last["next_ts_utc"]),
        )


def init_db() -> None:
    """建表; 旧库首次启动时在同一事务内补建查证依据。"""
    conn = get_conn()
    with _write_lock:
        conn.executescript(SCHEMA)
        conn.commit()
        conn.execute("BEGIN IMMEDIATE")
        try:
            if not _already_backfilled(conn):
                _rebuild_derived(conn)
                conn.execute(
                    "INSERT INTO schema_meta (key, value) VALUES (?, ?)"
                    " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (PROOF_BACKFILL_KEY, PROOF_BACKFILL_VALUE),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
