# 返工补登扫码流向核对服务

FastAPI + SQLite。质检员补登漏扫后，系统按修正后的扫码顺序自动维护工位间
**流向索引**，支持按 UTC 日期区间核对每个流向经手的工件数；并可对某工件某
UTC 日期**查证**各方向的首/末两次流转分别来自哪两次原始扫码。

## 规则

1. 每次扫码含：唯一编号 `scan_id`、工件号 `part_id`、工位 `station`、UTC 时刻 `ts_utc`。
2. 同一工件的扫码按 **(UTC 时刻, 扫码编号)** 排序；相邻两次扫码工位不同，
   才产生一条 `前站 -> 后站` 的流向，归入**后一次扫码的 UTC 日期**。
3. 同工位相邻不产生流向。
4. 支持改时刻、改工位、撤回；更新时**只定位改动位置前后的相邻扫码**做增量修复，
   不做全表重算。
5. 扫码明细、查证依据、流向索引在**同一事务**内提交，任一步失败整体回滚，保留原状。
6. 流向索引按 `(工件, 日期, 前站, 后站)` 主键去重，重复出现只累加 `ref_count`：
   - 同件同日重复走同一流向，索引行数不随次数增长；
   - 区间查询只读索引，同件在区间内走同一流向只算一个工件
     （`COUNT(DISTINCT part_id)`），另给 `event_count` 反映实际发生次数。

## 查证

`GET /flow-trace` 对某工件某 UTC 日期按方向返回：

- `count`：该方向的实际流转次数（与 `/flows` 中该件该日的 `event_count` 同口径）；
- `first` / `last`：首、末两次流转各自的**前一次扫码**与**后一次扫码**编号和 UTC 时刻。

首/末按后一次扫码的 `(时刻, 编号)` 定序；前一次扫码可早于查询日（跨天流转，
流向仍归入后一次扫码的日期）。补登、改时刻、改工位、撤回后，即使某方向次数
不变，`first` / `last` 也会重新指向当前**实际相邻**的扫码。

实现上，查证依据存在两张表：

- `flow_events`：每条实际相邻流转一行（主键为后一次扫码编号），
  附 `(工件, 日期, 方向, 后一次时刻, 后一次编号)` 组内顺序索引；
- `flow_endpoints`：每个 `(工件, 日期, 方向)` 一行的首/末指针缓存。

查询只读 `flow_endpoints` 与 `flow_index` 的**同主键各 1 行**，每个方向的读取量
固定，不随同向流转次数增长。增/改/撤只在改动位置的前后相邻处增删边，并对
受影响的 `(工件, 日期, 方向)` 用两次索引定位（`ORDER BY … LIMIT 1`）刷新首尾；
替换失效首尾无需回扫整件扫码。

## 启动

### Docker Compose（交付方式）

```bash
docker compose up --build
# 服务: http://localhost:8000  交互文档: http://localhost:8000/docs
```

SQLite 数据存于命名卷 `scanflow-data`（容器内 `/data/scanflow.db`）。

> **旧库升级**：若库中只有 `scans` / `flow_index`，服务首次启动会在一个事务内
> **一次性补建** `flow_events` 与 `flow_endpoints`（并在 `schema_meta` 记录标记），
> 之后只在增/改/撤时做增量维护。补建中途失败会整体回滚，保留原结果。

### 本机直接运行（无 Docker 时）

```bash
python3 -m venv --without-pip .venv        # 若 ensurepip 缺失, 需先引导 pip
.venv/bin/pip install -r requirements.txt
SCANFLOW_DB_PATH=./data/scanflow.db \
  .venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
```

## API（/docs 可直接录入和查数）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/scans` | 录入扫码（编号重复返回 409） |
| PATCH | `/scans/{scan_id}` | 改时刻 / 改工位（只传要改的字段） |
| DELETE | `/scans/{scan_id}` | 撤回扫码并修复相邻流向 |
| GET | `/scans?part_id=` | 查询扫码明细（按件/时刻/编号排序） |
| GET | `/flows?start_day=&end_day=` | **按日期区间统计各流向工件数（只读索引）** |
| GET | `/flow-trace?part_id=&day_utc=` | **查证某件某日各方向次数 + 首/末两次流转的原始扫码** |
| GET | `/flow-events?part_id=&day_utc=` | 查证依据明细（每条实际流转一行，核对用） |
| GET | `/flow-index?part_id=` | 查看统计索引与引用计数（核对用） |
| GET | `/health` | 健康检查 |

`/flow-trace` 可选 `from_station` 与 `to_station`（需同时提供）只看某一方向。
时刻接受 ISO8601（`...Z` 或带偏移量如 `+08:00`），内部统一转 UTC 存储；
日期参数格式为 `YYYY-MM-DD`。

### 示例

```bash
curl -X POST localhost:8000/scans -H 'Content-Type: application/json' \
  -d '{"scan_id":"S1","part_id":"P1","station":"A","ts_utc":"2026-11-24T08:00:00Z"}'
curl -X PATCH localhost:8000/scans/S1 -H 'Content-Type: application/json' \
  -d '{"station":"B"}'
curl -X DELETE localhost:8000/scans/S1
curl "localhost:8000/flows?start_day=2026-11-24&end_day=2026-11-25"
curl "localhost:8000/flow-trace?part_id=P1&day_utc=2026-11-24"
```

`/flows` 返回：

```json
[
  {"from_station": "A", "to_station": "B", "part_count": 3, "event_count": 5}
]
```

`/flow-trace` 返回：

```json
{
  "part_id": "P1",
  "day_utc": "2026-11-24",
  "directions": [
    {
      "from_station": "A",
      "to_station": "B",
      "count": 2,
      "first": {"prev_scan_id": "A1", "prev_ts_utc": "2026-11-24T08:00:00.000000Z",
                "next_scan_id": "B1", "next_ts_utc": "2026-11-24T09:00:00.000000Z"},
      "last":  {"prev_scan_id": "A2", "prev_ts_utc": "2026-11-24T11:00:00.000000Z",
                "next_scan_id": "B2", "next_ts_utc": "2026-11-24T12:00:00.000000Z"}
    }
  ]
}
```

演示"次数相同但首笔来源改变"（在 `/docs` 的 `/flow-trace` 上可直接复现）：

```bash
# A(08)->B(09), C(10), A(11)->B(12): A->B 两次, 首笔来源 A1
# 补登同站 A 的 A0(08:30) 后: 次数仍为 2, 首笔来源变为 A0
curl -X POST localhost:8000/scans -H 'Content-Type: application/json' \
  -d '{"scan_id":"A0","part_id":"P1","station":"A","ts_utc":"2026-11-24T08:30:00Z"}'
curl "localhost:8000/flow-trace?part_id=P1&day_utc=2026-11-24&from_station=A&to_station=B"
```

## 存储结构

- `scans`：扫码明细，主键 `scan_id`，索引 `(part_id, ts_utc, scan_id)` 支撑相邻定位。
- `flow_index`：流向统计索引，主键 `(part_id, day_utc, from_station, to_station)`，
  `ref_count` 记录该件当天该流向的出现次数。
- `flow_events`：查证依据明细，主键 `next_scan_id`（每次扫码至多一个前驱），
  索引 `(part_id, day_utc, from_station, to_station, next_ts_utc, next_scan_id)`
  用于 O(log n) 取某方向的首/末事件。
- `flow_endpoints`：查证索引，主键 `(part_id, day_utc, from_station, to_station)`，
  直接给出首/末两次流转的两端扫码。
- `schema_meta`：迁移标记（旧库一次性补建）。

## 测试

```bash
.venv/bin/python -m pytest tests/test_service.py tests/test_trace.py -q  # 服务层 + 查证
.venv/bin/python -I tests/e2e_http.py http://127.0.0.1:8000  # HTTP 端到端 + 随机化对照
```

端到端脚本内置独立的全量重算"对照器"，在 120 轮随机增/改/撤后逐行核对
统计索引与每个方向的首/末流转，并验证区间统计的去重语义与"补登后次数不变、
首笔来源改变"的演示场景。
