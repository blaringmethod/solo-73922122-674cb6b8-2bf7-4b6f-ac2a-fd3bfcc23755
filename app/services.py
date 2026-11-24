"""核心业务逻辑: 扫码增/改/撤 + 流向索引与查证依据的增量维护。

规则
----
同件扫码按 (ts_utc, scan_id) 排序。相邻两次扫码工位不同, 才记录一条
前站 -> 后站的流向, 归入"后一次扫码"的 UTC 日期 (day_utc)。

派生结构
--------
- flow_index     统计索引: (件, 日期, 方向) -> ref_count, 供 /flows 使用。
- flow_events    查证依据明细: 每条实际相邻流转一行, 主键为后一次扫码
                 next_scan_id (每次扫码至多一个前驱, 故主键天然唯一)。
- flow_endpoints 查证索引: (件, 日期, 方向) -> 首/末两次流转及两端扫码。
                 由 flow_events 的 (group, next_ts, next_scan_id) 索引取首/末
                 各一次, 不需要回扫整件扫码。

增量维护
--------
更新时只查"改动位置"前后的相邻扫码:
- 新增: 取前驱 prev / 后继 next, 删除旧边 prev->next, 加入 prev->new 与 new->next;
- 撤回: 取 prev / next, 删除 prev->old 与 old->next, 加入 prev->next;
- 修改: 按旧位置先撤一次, 再按新位置插一次。
每次增删边只刷新受影响的 (件, 日期, 方向) 的首尾指针: 两次索引定位
(ORDER BY ... LIMIT 1) 即可, 与同向流转次数无关, 也不回扫整件扫码。

写入在一个事务内完成。扫码明细、查证依据 (flow_events/flow_endpoints) 与
流向索引 (flow_index) 一同提交, 任一步失败整体回滚, 保留原结果。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any

from .db import get_conn, write_lock

TS_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"
DAY_FMT = "%Y-%m-%d"


class ConflictError(Exception):
    """scan_id 已存在。"""


class NotFoundError(Exception):
    """scan_id 不存在。"""


def parse_ts(raw: str) -> datetime:
    """解析 ISO8601 时刻并归一化为 UTC。

    接受 'Z' 结尾或带偏移量的 ISO 字符串; 无时区信息时按 UTC 处理。
    """
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"无法解析时刻: {raw!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_ts(dt: datetime) -> str:
    return dt.strftime(TS_FMT)


def day_of(dt: datetime) -> str:
    return dt.strftime(DAY_FMT)


def _validate_day(value: str, field: str) -> str:
    try:
        return datetime.strptime(value, DAY_FMT).strftime(DAY_FMT)
    except ValueError as exc:
        raise ValueError(f"{field} 必须是 YYYY-MM-DD 格式: {value!r}") from exc


# ---------------------------------------------------------------- 相邻扫码

def _get_scan(conn: sqlite3.Connection, scan_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM scans WHERE scan_id = ?", (scan_id,)
    ).fetchone()


def _prev_row(
    conn: sqlite3.Connection, part_id: str, ts_utc: str, scan_id: str
) -> sqlite3.Row | None:
    """同件中排序严格在 (ts_utc, scan_id) 之前的最近一条。

    用行值比较而非 `ts < ? OR (ts = ? AND id < ?)`: 后者会退化为
    MULTI-INDEX OR + 临时排序, 随同件扫码数线性增长; 行值比较可直接
    走 (part_id, ts_utc, scan_id) 索引, 只读 1 行。
    """
    return conn.execute(
        """
        SELECT * FROM scans
        WHERE part_id = ? AND (ts_utc, scan_id) < (?, ?)
        ORDER BY ts_utc DESC, scan_id DESC
        LIMIT 1
        """,
        (part_id, ts_utc, scan_id),
    ).fetchone()


def _next_row(
    conn: sqlite3.Connection, part_id: str, ts_utc: str, scan_id: str
) -> sqlite3.Row | None:
    """同件中排序严格在 (ts_utc, scan_id) 之后的最近一条 (同 _prev_row)。"""
    return conn.execute(
        """
        SELECT * FROM scans
        WHERE part_id = ? AND (ts_utc, scan_id) > (?, ?)
        ORDER BY ts_utc ASC, scan_id ASC
        LIMIT 1
        """,
        (part_id, ts_utc, scan_id),
    ).fetchone()


# ---------------------------------------------------------------- 首尾指针

def _refresh_endpoints(conn: sqlite3.Connection, part_id: str, day_utc: str,
                       from_station: str, to_station: str) -> None:
    """按 flow_events 的组内顺序索引刷新该方向的首/末流转。

    两次 `ORDER BY ... LIMIT 1` 索引定位即可; 不扫描整件扫码, 读取量与
    同向流转次数无关。
    """
    key = (part_id, day_utc, from_station, to_station)
    first = conn.execute(
        "SELECT * FROM flow_events WHERE part_id = ? AND day_utc = ?"
        " AND from_station = ? AND to_station = ?"
        " ORDER BY next_ts_utc ASC, next_scan_id ASC LIMIT 1",
        key,
    ).fetchone()
    if first is None:
        conn.execute(
            "DELETE FROM flow_endpoints WHERE part_id = ? AND day_utc = ?"
            " AND from_station = ? AND to_station = ?",
            key,
        )
        return
    last = conn.execute(
        "SELECT * FROM flow_events WHERE part_id = ? AND day_utc = ?"
        " AND from_station = ? AND to_station = ?"
        " ORDER BY next_ts_utc DESC, next_scan_id DESC LIMIT 1",
        key,
    ).fetchone()
    conn.execute(
        """
        INSERT INTO flow_endpoints (
            part_id, day_utc, from_station, to_station,
            first_prev_scan_id, first_prev_ts_utc,
            first_next_scan_id, first_next_ts_utc,
            last_prev_scan_id, last_prev_ts_utc,
            last_next_scan_id, last_next_ts_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(part_id, day_utc, from_station, to_station) DO UPDATE SET
            first_prev_scan_id = excluded.first_prev_scan_id,
            first_prev_ts_utc  = excluded.first_prev_ts_utc,
            first_next_scan_id = excluded.first_next_scan_id,
            first_next_ts_utc  = excluded.first_next_ts_utc,
            last_prev_scan_id  = excluded.last_prev_scan_id,
            last_prev_ts_utc   = excluded.last_prev_ts_utc,
            last_next_scan_id  = excluded.last_next_scan_id,
            last_next_ts_utc   = excluded.last_next_ts_utc
        """,
        (part_id, day_utc, from_station, to_station,
         first["prev_scan_id"], first["prev_ts_utc"],
         first["next_scan_id"], first["next_ts_utc"],
         last["prev_scan_id"], last["prev_ts_utc"],
         last["next_scan_id"], last["next_ts_utc"]),
    )


# ---------------------------------------------------------------- 流向边维护

def _add_edge(conn: sqlite3.Connection, a: sqlite3.Row | None,
              b: sqlite3.Row | None) -> None:
    """加入一条 a -> b 的流向边 (b 为后一次扫码)。

    相邻两次工位相同则不产生流向。查证明细以 b 的扫码编号为键,
    统计索引按主键去重只累加 ref_count, 随后刷新该方向首尾指针。
    """
    if a is None or b is None:
        return
    if a["station"] == b["station"]:
        return
    conn.execute(
        """
        INSERT INTO flow_events (next_scan_id, part_id, day_utc, from_station,
            to_station, prev_scan_id, prev_ts_utc, next_ts_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (b["scan_id"], b["part_id"], b["day_utc"], a["station"],
         b["station"], a["scan_id"], a["ts_utc"], b["ts_utc"]),
    )
    conn.execute(
        """
        INSERT INTO flow_index (part_id, day_utc, from_station, to_station, ref_count)
        VALUES (?, ?, ?, ?, 1)
        ON CONFLICT(part_id, day_utc, from_station, to_station)
        DO UPDATE SET ref_count = ref_count + 1
        """,
        (b["part_id"], b["day_utc"], a["station"], b["station"]),
    )
    _refresh_endpoints(conn, b["part_id"], b["day_utc"],
                       a["station"], b["station"])


def _remove_edge(conn: sqlite3.Connection, a: sqlite3.Row | None,
                 b: sqlite3.Row | None) -> None:
    """移除一条 a -> b 的流向边的一次引用。

    删掉 b 对应的查证明细行、统计索引引用数归零即删行, 再刷新首尾指针。
    """
    if a is None or b is None:
        return
    if a["station"] == b["station"]:
        return
    cur = conn.execute(
        "DELETE FROM flow_events WHERE next_scan_id = ?"
        " AND from_station = ? AND to_station = ?",
        (b["scan_id"], a["station"], b["station"]),
    )
    if cur.rowcount == 0:
        raise RuntimeError(
            f"查证依据不一致, 缺少流转: part={b['part_id']} "
            f"next={b['scan_id']} {a['station']}->{b['station']}"
        )
    cur = conn.execute(
        """
        UPDATE flow_index
        SET ref_count = ref_count - 1
        WHERE part_id = ? AND day_utc = ?
          AND from_station = ? AND to_station = ? AND ref_count > 0
        """,
        (b["part_id"], b["day_utc"], a["station"], b["station"]),
    )
    if cur.rowcount == 0:
        raise RuntimeError(
            f"流向索引不一致, 缺少边: part={b['part_id']} day={b['day_utc']} "
            f"{a['station']}->{b['station']}"
        )
    conn.execute(
        """
        DELETE FROM flow_index
        WHERE part_id = ? AND day_utc = ?
          AND from_station = ? AND to_station = ? AND ref_count = 0
        """,
        (b["part_id"], b["day_utc"], a["station"], b["station"]),
    )
    _refresh_endpoints(conn, b["part_id"], b["day_utc"],
                       a["station"], b["station"])


def _insert_scan_row(conn: sqlite3.Connection, scan_id: str, part_id: str,
                     station: str, ts_utc: str, day_utc: str) -> None:
    """在已开启的事务内插入一条扫码并增量更新流向 (只碰前后相邻)。"""
    prev = _prev_row(conn, part_id, ts_utc, scan_id)
    nxt = _next_row(conn, part_id, ts_utc, scan_id)

    # 新边把旧的 prev->next 邻接关系替换成 prev->new 与 new->next
    _remove_edge(conn, prev, nxt)
    conn.execute(
        "INSERT INTO scans (scan_id, part_id, ts_utc, day_utc, station)"
        " VALUES (?, ?, ?, ?, ?)",
        (scan_id, part_id, ts_utc, day_utc, station),
    )
    new_row = _get_scan(conn, scan_id)
    _add_edge(conn, prev, new_row)
    _add_edge(conn, new_row, nxt)


def _delete_scan_row(conn: sqlite3.Connection, scan_id: str) -> sqlite3.Row:
    """在已开启的事务内撤回一条扫码并增量修复流向。"""
    old = _get_scan(conn, scan_id)
    if old is None:
        raise NotFoundError(scan_id)
    prev = _prev_row(conn, old["part_id"], old["ts_utc"], old["scan_id"])
    nxt = _next_row(conn, old["part_id"], old["ts_utc"], old["scan_id"])

    _remove_edge(conn, prev, old)
    _remove_edge(conn, old, nxt)
    conn.execute("DELETE FROM scans WHERE scan_id = ?", (scan_id,))
    # old 被删后 prev 与 nxt 成为新的相邻扫码
    _add_edge(conn, prev, nxt)
    return old


# ---------------------------------------------------------------- 公开写接口

def add_scan(scan_id: str, part_id: str, station: str, ts_raw: str) -> dict[str, Any]:
    dt = parse_ts(ts_raw)
    ts_utc, day_utc = format_ts(dt), day_of(dt)
    conn = get_conn()
    with write_lock():
        try:
            conn.execute("BEGIN IMMEDIATE")
            if _get_scan(conn, scan_id) is not None:
                raise ConflictError(scan_id)
            _insert_scan_row(conn, scan_id, part_id, station, ts_utc, day_utc)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return {
        "scan_id": scan_id, "part_id": part_id, "station": station,
        "ts_utc": ts_utc, "day_utc": day_utc,
    }


def update_scan(scan_id: str, station: str | None, ts_raw: str | None) -> dict[str, Any]:
    """改时刻 / 改工位。仅传需要修改的字段。

    实现为同事务内"按旧位置撤回 + 按新位置插入", 每步都只查相邻扫码。
    """
    new_dt = parse_ts(ts_raw) if ts_raw is not None else None
    conn = get_conn()
    with write_lock():
        try:
            conn.execute("BEGIN IMMEDIATE")
            old = _get_scan(conn, scan_id)
            if old is None:
                raise NotFoundError(scan_id)

            part_id = old["part_id"]
            new_station = station if station is not None else old["station"]
            if new_dt is None:
                new_ts, new_day = old["ts_utc"], old["day_utc"]
            else:
                new_ts, new_day = format_ts(new_dt), day_of(new_dt)

            _delete_scan_row(conn, scan_id)
            _insert_scan_row(conn, scan_id, part_id, new_station, new_ts, new_day)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return {
        "scan_id": scan_id, "part_id": part_id, "station": new_station,
        "ts_utc": new_ts, "day_utc": new_day,
    }


def withdraw_scan(scan_id: str) -> dict[str, Any]:
    conn = get_conn()
    with write_lock():
        try:
            conn.execute("BEGIN IMMEDIATE")
            old = _delete_scan_row(conn, scan_id)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return {
        "scan_id": old["scan_id"], "part_id": old["part_id"],
        "station": old["station"], "ts_utc": old["ts_utc"],
        "day_utc": old["day_utc"], "withdrawn": True,
    }


# ---------------------------------------------------------------- 查询 (只读索引)

def query_flows(start_day: str, end_day: str) -> list[dict[str, Any]]:
    """按 UTC 日期区间统计各流向的工件数 (口径与改造前一致)。

    只读 flow_index: 同件在区间内走同一流向只算一次 (主键天然去重,
    同日重复走同一流向时索引行数不随次数增长)。跨天则各天分别计一次。
    """
    start_day = _validate_day(start_day, "start_day")
    end_day = _validate_day(end_day, "end_day")
    if start_day > end_day:
        raise ValueError("start_day 不能晚于 end_day")
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT from_station, to_station,
               COUNT(DISTINCT part_id) AS part_count,
               SUM(ref_count) AS event_count
        FROM flow_index
        WHERE day_utc BETWEEN ? AND ?
        GROUP BY from_station, to_station
        ORDER BY from_station, to_station
        """,
        (start_day, end_day),
    ).fetchall()
    return [
        {
            "from_station": r["from_station"],
            "to_station": r["to_station"],
            "part_count": r["part_count"],
            "event_count": r["event_count"],
        }
        for r in rows
    ]


def query_flow_trace(part_id: str, day_utc: str,
                     from_station: str | None = None,
                     to_station: str | None = None) -> dict[str, Any]:
    """查证: 某工件某 UTC 日期各方向的次数与首/末两次流转的原始扫码。

    只读 flow_endpoints 与 flow_index 的同主键行: 每个方向各读 1~2 行,
    读取量不随同向流转次数增长。首/末按 (后一次扫码时刻, 后一次扫码编号)
    定义; 前一次扫码允许早于查询日 (跨天流转)。
    """
    day_utc = _validate_day(day_utc, "day_utc")
    if (from_station is None) != (to_station is None):
        raise ValueError("from_station 与 to_station 需同时提供或同时省略")
    sql = """
        SELECT e.from_station, e.to_station, i.ref_count,
               e.first_prev_scan_id, e.first_prev_ts_utc,
               e.first_next_scan_id, e.first_next_ts_utc,
               e.last_prev_scan_id, e.last_prev_ts_utc,
               e.last_next_scan_id, e.last_next_ts_utc
        FROM flow_endpoints e
        JOIN flow_index i
          ON i.part_id = e.part_id AND i.day_utc = e.day_utc
         AND i.from_station = e.from_station AND i.to_station = e.to_station
        WHERE e.part_id = ? AND e.day_utc = ?
    """
    params: list[Any] = [part_id, day_utc]
    if from_station is not None:
        sql += " AND e.from_station = ? AND e.to_station = ?"
        params += [from_station, to_station]
    sql += " ORDER BY e.from_station, e.to_station"
    rows = get_conn().execute(sql, params).fetchall()
    return {
        "part_id": part_id,
        "day_utc": day_utc,
        "directions": [
            {
                "from_station": r["from_station"],
                "to_station": r["to_station"],
                "count": r["ref_count"],
                "first": {
                    "prev_scan_id": r["first_prev_scan_id"],
                    "prev_ts_utc": r["first_prev_ts_utc"],
                    "next_scan_id": r["first_next_scan_id"],
                    "next_ts_utc": r["first_next_ts_utc"],
                },
                "last": {
                    "prev_scan_id": r["last_prev_scan_id"],
                    "prev_ts_utc": r["last_prev_ts_utc"],
                    "next_scan_id": r["last_next_scan_id"],
                    "next_ts_utc": r["last_next_ts_utc"],
                },
            }
            for r in rows
        ],
    }


def list_scans(part_id: str | None = None) -> list[dict[str, Any]]:
    conn = get_conn()
    if part_id is None:
        rows = conn.execute(
            "SELECT * FROM scans ORDER BY part_id, ts_utc, scan_id"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM scans WHERE part_id = ? ORDER BY ts_utc, scan_id",
            (part_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def list_flow_index(part_id: str | None = None) -> list[dict[str, Any]]:
    """暴露统计索引内容, 便于核对 (只读查询)。"""
    conn = get_conn()
    if part_id is None:
        rows = conn.execute(
            "SELECT * FROM flow_index ORDER BY day_utc, part_id, from_station, to_station"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM flow_index WHERE part_id = ?"
            " ORDER BY day_utc, from_station, to_station",
            (part_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def list_flow_events(part_id: str | None = None,
                     day_utc: str | None = None) -> list[dict[str, Any]]:
    """暴露查证依据明细 (每条实际流转一行), 供人工核对。"""
    conn = get_conn()
    sql = "SELECT * FROM flow_events"
    clauses: list[str] = []
    params: list[Any] = []
    if part_id is not None:
        clauses.append("part_id = ?")
        params.append(part_id)
    if day_utc is not None:
        clauses.append("day_utc = ?")
        params.append(_validate_day(day_utc, "day_utc"))
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += (" ORDER BY part_id, day_utc, from_station, to_station,"
            " next_ts_utc, next_scan_id")
    return [dict(r) for r in conn.execute(sql, params).fetchall()]
