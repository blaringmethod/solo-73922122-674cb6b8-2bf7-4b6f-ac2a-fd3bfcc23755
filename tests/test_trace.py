"""查证依据/首尾指针的单元测试。

覆盖:
- 首/末按 (后一次扫码时刻, 编号) 定义, 前一次扫码可早于查询日;
- 补登/改工位/改时刻/撤回后, 即使某方向次数不变, 首尾也指向当前实际相邻扫码;
- 旧库一次性补建查证依据;
- 查询某方向读取的记录数不随同向流转次数增长 (EXPLAIN QUERY PLAN);
- 写入失败时查证依据与流向索引一同回滚。

运行: .venv/bin/python -m pytest tests/test_trace.py -q
"""
from __future__ import annotations

import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import db, services  # noqa: E402


@pytest.fixture()
def conn(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(db, "DB_PATH", db_path)
    monkeypatch.setattr(db, "_conn", None)
    db.init_db()
    c = db.get_conn()
    yield c
    c.close()
    monkeypatch.setattr(db, "_conn", None)


def _expected(conn: sqlite3.Connection) -> tuple[dict[tuple, int], dict[tuple, list]]:
    """全量重算: 统计索引期望 + 每个 (件,天,方向) 的流转明细列表。"""
    rows = conn.execute(
        "SELECT scan_id, part_id, station, ts_utc, day_utc FROM scans"
        " ORDER BY part_id, ts_utc, scan_id"
    ).fetchall()
    by_part: dict[str, list] = {}
    for r in rows:
        by_part.setdefault(r["part_id"], []).append(r)
    counts: dict[tuple, int] = {}
    events: dict[tuple, list] = {}
    for scans in by_part.values():
        for a, b in zip(scans, scans[1:]):
            if a["station"] == b["station"]:
                continue
            key = (b["part_id"], b["day_utc"], a["station"], b["station"])
            counts[key] = counts.get(key, 0) + 1
            events.setdefault(key, []).append(
                (a["scan_id"], a["ts_utc"], b["scan_id"], b["ts_utc"]))
    return counts, events


def _oracle(conn: sqlite3.Connection) -> None:
    """核对 flow_index / flow_events / flow_endpoints 三张表。"""
    counts, events = _expected(conn)

    actual_counts = {
        (r["part_id"], r["day_utc"], r["from_station"], r["to_station"]):
            r["ref_count"]
        for r in conn.execute("SELECT * FROM flow_index")
    }
    assert actual_counts == counts, "流向索引与全量重算不一致"

    actual_events: dict[tuple, list] = {}
    for r in conn.execute("SELECT * FROM flow_events"):
        key = (r["part_id"], r["day_utc"], r["from_station"], r["to_station"])
        actual_events.setdefault(key, []).append(
            (r["prev_scan_id"], r["prev_ts_utc"], r["next_scan_id"], r["next_ts_utc"]))
    for lst in actual_events.values():
        lst.sort(key=lambda x: (x[3], x[2]))
    for lst in events.values():
        lst.sort(key=lambda x: (x[3], x[2]))
    assert actual_events == events, "查证明细与全量重算不一致"

    expected_endpoints = {}
    for key, lst in events.items():
        if not lst:
            continue
        expected_endpoints[key] = (lst[0], lst[-1])
    actual_endpoints = {}
    for r in conn.execute("SELECT * FROM flow_endpoints"):
        key = (r["part_id"], r["day_utc"], r["from_station"], r["to_station"])
        actual_endpoints[key] = (
            (r["first_prev_scan_id"], r["first_prev_ts_utc"],
             r["first_next_scan_id"], r["first_next_ts_utc"]),
            (r["last_prev_scan_id"], r["last_prev_ts_utc"],
             r["last_next_scan_id"], r["last_next_ts_utc"]),
        )
    assert actual_endpoints == expected_endpoints, "首尾指针与全量重算不一致"


def _trace(part_id: str, day: str, frm: str, to: str) -> dict:
    res = services.query_flow_trace(part_id, day, frm, to)
    assert len(res["directions"]) == 1, res
    d = res["directions"][0]
    return {
        "count": d["count"],
        "first": (d["first"]["prev_scan_id"], d["first"]["prev_ts_utc"],
                  d["first"]["next_scan_id"], d["first"]["next_ts_utc"]),
        "last": (d["last"]["prev_scan_id"], d["last"]["prev_ts_utc"],
                 d["last"]["next_scan_id"], d["last"]["next_ts_utc"]),
    }


def test_trace_returns_count_and_first_last(conn):
    services.add_scan("a1", "P1", "A", "2026-11-24T08:00:00Z")
    services.add_scan("b1", "P1", "B", "2026-11-24T09:00:00Z")
    services.add_scan("c1", "P1", "C", "2026-11-24T10:00:00Z")
    services.add_scan("a2", "P1", "A", "2026-11-24T11:00:00Z")
    services.add_scan("b2", "P1", "B", "2026-11-24T12:00:00Z")
    _oracle(conn)
    d = _trace("P1", "2026-11-24", "A", "B")
    assert d["count"] == 2
    assert d["first"] == (
        "a1", "2026-11-24T08:00:00.000000Z",
        "b1", "2026-11-24T09:00:00.000000Z")
    assert d["last"] == (
        "a2", "2026-11-24T11:00:00.000000Z",
        "b2", "2026-11-24T12:00:00.000000Z")


def test_previous_scan_may_precede_query_day(conn):
    # 前一次扫码 11-24 23:50，后一次 11-25 00:10: 查 11-25 时前一次可早于查询日
    services.add_scan("n1", "P1", "A", "2026-11-24T23:50:00Z")
    services.add_scan("n2", "P1", "B", "2026-11-25T00:10:00Z")
    _oracle(conn)
    d = _trace("P1", "2026-11-25", "A", "B")
    assert d["count"] == 1
    assert d["first"][1] == "2026-11-24T23:50:00.000000Z"
    assert d["first"][3] == "2026-11-25T00:10:00.000000Z"
    assert d["first"] == d["last"]
    # 查询日按后一次扫码归属: 11-24 没有该方向
    assert services.query_flow_trace("P1", "2026-11-24")["directions"] == []


def test_first_last_ordered_by_next_scan_not_prev(conn):
    # 同一后一次扫码日期组内, 后一次时刻相同则按后一次扫码编号定序
    services.add_scan("p1", "P1", "A", "2026-11-24T09:00:00Z")   # 前一次较早
    services.add_scan("x1", "P1", "B", "2026-11-24T10:00:00Z")
    services.add_scan("a2", "P1", "C", "2026-11-24T11:00:00Z")
    # 构造两个同方向 A->B? 用同后一次时刻不同编号: 先 A->B, 再经 C 回 A 再 B
    services.add_scan("x2", "P1", "B", "2026-11-24T12:00:00Z")
    services.add_scan("c2", "P1", "C", "2026-11-24T12:00:00Z")
    services.add_scan("a3", "P1", "A", "2026-11-24T13:00:00Z")
    services.add_scan("b3", "P1", "B", "2026-11-24T14:00:00Z")
    _oracle(conn)


def test_backfill_moves_first_source_without_changing_count(conn):
    """补登后次数不变, 首笔来源改变 (演示的核心场景)。"""
    # A(08) -> B(09) [A->B #1], C(10), A(11) -> B(12) [A->B #2]
    services.add_scan("a1", "P1", "A", "2026-11-24T08:00:00Z")
    services.add_scan("b1", "P1", "B", "2026-11-24T09:00:00Z")
    services.add_scan("c1", "P1", "C", "2026-11-24T10:00:00Z")
    services.add_scan("a2", "P1", "A", "2026-11-24T11:00:00Z")
    services.add_scan("b2", "P1", "B", "2026-11-24T12:00:00Z")
    _oracle(conn)
    before = _trace("P1", "2026-11-24", "A", "B")
    assert before["count"] == 2
    assert before["first"][0] == "a1"
    assert before["last"][0] == "a2"

    # 在 a1 与 b1 之间补登同站 A 的 a0: 首笔 A->B 的来源由 a1 换成 a0,
    # 次数仍为 2, 首尾指针指向当前实际相邻的扫码
    services.add_scan("a0", "P1", "A", "2026-11-24T08:30:00Z")
    _oracle(conn)
    after = _trace("P1", "2026-11-24", "A", "B")
    assert after["count"] == 2, "补登不应改变该方向的流转次数"
    assert after["first"] == (
        "a0", "2026-11-24T08:30:00.000000Z",
        "b1", "2026-11-24T09:00:00.000000Z")
    assert after["last"][0] == "a2"


def test_update_and_withdraw_repair_endpoints_even_if_count_unchanged(conn):
    # A(08) -> B(09) -> B(10) -> C(11): A->B 仅 1 笔, 目的地是 b1
    services.add_scan("a1", "P1", "A", "2026-11-24T08:00:00Z")
    services.add_scan("b1", "P1", "B", "2026-11-24T09:00:00Z")
    services.add_scan("b2", "P1", "B", "2026-11-24T10:00:00Z")
    services.add_scan("c1", "P1", "C", "2026-11-24T11:00:00Z")
    _oracle(conn)
    d = _trace("P1", "2026-11-24", "A", "B")
    assert d["count"] == 1
    assert d["first"][2] == "b1"

    # 改时刻: b1 移到 b2 之后 -> 相邻关系变为 a1->b2, 次数仍为 1, 末笔指向 b2
    services.update_scan("b1", station=None, ts_raw="2026-11-24T10:30:00Z")
    _oracle(conn)
    d = _trace("P1", "2026-11-24", "A", "B")
    assert d["count"] == 1
    assert d["first"][2] == "b2" and d["last"][2] == "b2"

    # 撤回 b2 -> A->B 的相邻目的地回退到 b1, 次数仍为 1
    services.withdraw_scan("b2")
    _oracle(conn)
    d = _trace("P1", "2026-11-24", "A", "B")
    assert d["count"] == 1
    assert d["first"][2] == "b1" and d["first"] == d["last"]
    assert d["first"] == (
        "a1", "2026-11-24T08:00:00.000000Z",
        "b1", "2026-11-24T10:30:00.000000Z")


def test_withdraw_restores_endpoints(conn):
    services.add_scan("a1", "P1", "A", "2026-11-24T08:00:00Z")
    services.add_scan("b1", "P1", "B", "2026-11-24T09:00:00Z")
    services.add_scan("a2", "P1", "A", "2026-11-24T10:00:00Z")
    services.add_scan("b2", "P1", "B", "2026-11-24T11:00:00Z")
    _oracle(conn)
    # 撤掉中间扫描 a2: A->B 只剩 a1->b1，b2 的前驱变为 b1 (同站, 无流转)
    services.withdraw_scan("a2")
    _oracle(conn)
    d = _trace("P1", "2026-11-24", "A", "B")
    assert d["count"] == 1
    assert d["first"] == (
        "a1", "2026-11-24T08:00:00.000000Z",
        "b1", "2026-11-24T09:00:00.000000Z")
    assert d["first"] == d["last"]
    res = services.query_flow_trace("P1", "2026-11-24")
    assert [(x["from_station"], x["to_station"]) for x in res["directions"]] == [
        ("A", "B")]


def _measure_trace_work(conn: sqlite3.Connection, part_id: str) -> int:
    """用 SQLite 进度回调统计一次查证读取的工作量 (VM 指令数)。"""
    steps = {"n": 0}

    def handler() -> int:
        steps["n"] += 1
        return 0

    conn.set_progress_handler(handler, 1)
    try:
        res = services.query_flow_trace(part_id, "2026-11-24", "A", "B")
    finally:
        conn.set_progress_handler(None, 0)
    assert len(res["directions"]) == 1
    return steps["n"]


def test_trace_reads_constant_rows_per_direction(conn):
    """查询某方向的工作量不随同向流转次数增长。"""
    from datetime import datetime, timedelta, timezone

    def ring(part: str, times: int) -> None:
        # 每次: A -> B -> C, 使 A->B 相邻流转恰好出现 times 次
        start = datetime(2026, 11, 24, 0, 0, tzinfo=timezone.utc)
        for k in range(times):
            for offset, station in enumerate(("A", "B", "C")):
                ts = start + timedelta(minutes=k * 3 + offset)
                services.add_scan(f"{part}-{k}-{station}", part, station,
                                  ts.strftime("%Y-%m-%dT%H:%M:%SZ"))

    ring("FEW", 2)
    ring("MANY", 200)
    _oracle(conn)
    assert _trace("MANY", "2026-11-24", "A", "B")["count"] == 200
    assert _trace("FEW", "2026-11-24", "A", "B")["count"] == 2

    few = _measure_trace_work(conn, "FEW")
    many = _measure_trace_work(conn, "MANY")
    # 索引定位深度只随 log(n) 变化; 若回扫/聚合 200 条会显著放大
    assert many <= few * 2 + 10, (few, many)


def test_trace_uses_index_only(conn):
    """首尾指针查询只走端点表主键与事件表组内索引, 不回扫整件扫码。"""
    for k in range(50):
        services.add_scan(f"s{k}", "P1", "A" if k % 2 == 0 else "B",
                          f"2026-11-24T08:{k:02d}:00Z")
    _oracle(conn)

    def plan(sql: str, params: tuple) -> str:
        rows = conn.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()
        return " ".join(str(r["detail"]) for r in rows)

    events_plan = plan(
        "SELECT * FROM flow_events"
        " WHERE part_id = ? AND day_utc = ? AND from_station = ? AND to_station = ?"
        " ORDER BY next_ts_utc ASC, next_scan_id ASC LIMIT 1",
        ("P1", "2026-11-24", "A", "B"),
    )
    assert "idx_flow_events_group" in events_plan, events_plan
    assert "TEMP B-TREE" not in events_plan.upper(), events_plan

    query_plan = plan(
        "SELECT * FROM flow_endpoints WHERE part_id = ? AND day_utc = ?"
        " AND from_station = ? AND to_station = ?",
        ("P1", "2026-11-24", "A", "B"),
    )
    assert "SEARCH flow_endpoints" in query_plan, query_plan
    assert "SCAN flow_endpoints" not in query_plan, query_plan

    # 完整查证 SQL 只走端点表主键 + 统计索引主键, 不扫任何明细表
    trace_plan = plan(
        "SELECT e.from_station, e.to_station, i.ref_count"
        " FROM flow_endpoints e JOIN flow_index i"
        " ON i.part_id = e.part_id AND i.day_utc = e.day_utc"
        " AND i.from_station = e.from_station AND i.to_station = e.to_station"
        " WHERE e.part_id = ? AND e.day_utc = ?"
        " AND e.from_station = ? AND e.to_station = ?",
        ("P1", "2026-11-24", "A", "B"),
    )
    assert trace_plan.count("SEARCH") == 2, trace_plan
    assert "SCAN " not in trace_plan, trace_plan


def test_failure_rolls_back_proof_and_index(conn, monkeypatch):
    services.add_scan("s1", "P1", "A", "2026-11-24T08:00:00Z")
    services.add_scan("s3", "P1", "C", "2026-11-24T12:00:00Z")

    def snapshot() -> tuple:
        return (
            conn.execute("SELECT COUNT(*) FROM scans").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM flow_index").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM flow_events").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM flow_endpoints").fetchone()[0],
        )

    before = snapshot()
    real_refresh = services._refresh_endpoints

    def flaky_refresh(*args, **kwargs):
        raise RuntimeError("simulated proof failure")

    monkeypatch.setattr(services, "_refresh_endpoints", flaky_refresh)
    with pytest.raises(RuntimeError):
        services.add_scan("s2", "P1", "B", "2026-11-24T10:00:00Z")
    monkeypatch.setattr(services, "_refresh_endpoints", real_refresh)

    assert snapshot() == before
    _oracle(conn)


def test_invalid_trace_params(conn):
    with pytest.raises(ValueError):
        services.query_flow_trace("P1", "2026-13-01")
    with pytest.raises(ValueError):
        services.query_flow_trace("P1", "2026-11-24", "A", None)


def test_randomized_incremental_matches_oracle(conn):
    """随机补登/改时刻/改工位/撤回, 每步后三张表都与全量重算一致。"""
    import random
    from datetime import datetime, timedelta, timezone

    rng = random.Random(20261124)
    stations = ["A", "B", "C", "D"]
    base = datetime(2026, 11, 24, 0, 0, tzinfo=timezone.utc)
    scans: list[dict] = []
    next_id = 0

    def ts(day_offset: int, minute: int) -> str:
        return (base + timedelta(days=day_offset, minutes=minute)).strftime(
            "%Y-%m-%dT%H:%M:%SZ")

    for step in range(300):
        action = rng.choices(
            ["add", "restation", "retime", "withdraw"], weights=[4, 2, 2, 2])[0]
        if action == "add" or not scans:
            next_id += 1
            sid = f"r{next_id}"
            body = {
                "scan_id": sid, "part_id": rng.choice(["P1", "P2"]),
                "station": rng.choice(stations),
                "ts": ts(rng.randint(0, 2), rng.randint(0, 600)),
            }
            services.add_scan(body["scan_id"], body["part_id"],
                              body["station"], body["ts"])
            scans.append(body)
        else:
            target = rng.choice(scans)
            if action == "withdraw":
                services.withdraw_scan(target["scan_id"])
                scans.remove(target)
            elif action == "restation":
                station = rng.choice([s for s in stations if s != target["station"]])
                services.update_scan(target["scan_id"], station=station, ts_raw=None)
                target["station"] = station
            else:
                new_ts = ts(rng.randint(0, 2), rng.randint(0, 600))
                services.update_scan(target["scan_id"], station=None, ts_raw=new_ts)
                target["ts"] = new_ts
        _oracle(conn)

    # 查询结果也与重算一致
    for part in ("P1", "P2"):
        for day in ("2026-11-24", "2026-11-25", "2026-11-26"):
            res = services.query_flow_trace(part, day)
            _, events = _expected(conn)
            got = {(d["from_station"], d["to_station"]): d
                   for d in res["directions"]}
            want = {k[2:]: v for k, v in events.items()
                    if k[0] == part and k[1] == day}
            assert set(got) == set(want), (part, day, got.keys(), want.keys())
            for direction, evs in want.items():
                d = got[direction]
                assert d["count"] == len(evs)
                assert (d["first"]["prev_scan_id"], d["first"]["prev_ts_utc"],
                        d["first"]["next_scan_id"], d["first"]["next_ts_utc"]) == evs[0]
                assert (d["last"]["next_scan_id"], d["last"]["next_ts_utc"]) == (
                    evs[-1][2], evs[-1][3])


def test_legacy_db_backfill_once(tmp_path, monkeypatch):
    """旧库只有 scans/flow_index, 启动时补建查证依据; 再次启动不重复。"""
    db_path = str(tmp_path / "legacy.db")
    raw = sqlite3.connect(db_path)
    raw.executescript("""
        CREATE TABLE scans (
            scan_id TEXT PRIMARY KEY, part_id TEXT NOT NULL,
            station TEXT NOT NULL, ts_utc TEXT NOT NULL, day_utc TEXT NOT NULL);
        CREATE INDEX idx_scans_part_order ON scans (part_id, ts_utc, scan_id);
        CREATE TABLE flow_index (
            part_id TEXT NOT NULL, day_utc TEXT NOT NULL,
            from_station TEXT NOT NULL, to_station TEXT NOT NULL,
            ref_count INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (part_id, day_utc, from_station, to_station));
    """)
    data = [
        ("a1", "P1", "A", "2026-11-24T08:00:00.000000Z", "2026-11-24"),
        ("b1", "P1", "B", "2026-11-24T09:00:00.000000Z", "2026-11-24"),
        ("c1", "P1", "C", "2026-11-24T10:00:00.000000Z", "2026-11-24"),
        ("c2", "P1", "C", "2026-11-24T11:00:00.000000Z", "2026-11-24"),
        ("d1", "P1", "D", "2026-11-25T00:10:00.000000Z", "2026-11-25"),
    ]
    raw.executemany("INSERT INTO scans VALUES (?, ?, ?, ?, ?)", data)
    raw.commit()
    raw.close()

    monkeypatch.setattr(db, "DB_PATH", db_path)
    monkeypatch.setattr(db, "_conn", None)
    db.init_db()
    conn = db.get_conn()
    try:
        _oracle(conn)
        d = _trace("P1", "2026-11-24", "A", "B")
        assert d["count"] == 1 and d["first"][0] == "a1"
        # 首尾指针存在, 且下一次启动不再重建 (这里直接调用一次 init_db 验证幂等)
        db.init_db()
        _oracle(conn)
    finally:
        conn.close()
        monkeypatch.setattr(db, "_conn", None)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
