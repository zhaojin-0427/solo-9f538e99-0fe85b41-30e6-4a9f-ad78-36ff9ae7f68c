# 水质数仓化验表 API

固定站点、UTC 采样时刻，外加至多 8 个可空浮点指标的化验表服务。
技术栈：**FastAPI + PyArrow + SQLite + Parquet 分片**，可通过 Docker Compose 一键启动。

## 1. 核心模型（为什么改名/删名不会串列）

| 概念 | 说明 |
| --- | --- |
| 指标身份 `indicator_id` | 指标的稳定身份。**改名只改名字，不换身份**；删除只是 `active=0`，身份永不复用。 |
| 字段版 `schema_version` | 每次新增/改名/删除都产生一个不可变新版本，版内固化“字段名 → 指标身份”的完整映射。 |
| 分片 shard | 每次 Parquet 上传产生一个分片，物理列名为身份列 `i{indicator_id}`，与当前叫什么名字完全解耦。 |
| 快照 snapshot | 首次查询时固化“当时的字段名集合 + 当时的全部分片”，此后分页、字段变更、上传、重启都不改变它。 |

典型故事（与需求一致）：

1. 建指标 **浓度**（身份 1，字段版 2），上传历史分片 → 物理列 `i1`。
2. **浓度 改名为 硝酸盐**（身份仍是 1，字段版 3）。
3. 再建指标 **浓度**（同名复用 → 身份 2，字段版 4，语义为磷酸盐，不继承任何旧值）。
4. 用**旧字段版 2** 上传：旧版“浓度”映射到身份 1，数据进 `i1`（硝酸盐），不串列。
5. 用**字段版 4** 上传：当前“浓度”映射到身份 2，数据进 `i2`（磷酸盐）。
6. 查询按当前名合读：旧分片没有 `i2` → 磷酸盐列为 `NULL`；新分片没有 `i1` → 硝酸盐列为 `NULL`。

## 2. 启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 服务：http://localhost:8000
# 交互式文档（Swagger）：http://localhost:8000/docs
# OpenAPI：http://localhost:8000/openapi.json
# 健康检查：http://localhost:8000/health
```

数据持久化在命名卷 `water_quality_data`（容器内 `/data`，含 `warehouse.db` 与 `shards/*.parquet`），
重启容器后字段、分片、快照全部保留。

### 本地直接运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 3. API 一览

| 方法/路径 | 作用 |
| --- | --- |
| `GET /schema` | 当前字段版号、固定字段、存续指标（含身份 ID） |
| `GET /schema/versions/{version_id}` | 查看任意字段版的不可变映射（含已删指标） |
| `POST /schema/indicators` | 新增指标（body `{"name": "..."}`）；同名复用得到**新身份** |
| `POST /schema/indicators/rename` | 改名（body `{"old_name": "...", "new_name": "..."}`），身份不变 |
| `DELETE /schema/indicators/{name}` | 删除指标（历史分片保留） |
| `POST /ingest/parquet` | 表单上传：`schema_version`（必填）+ `file`（Parquet） |
| `GET /rows` | 分页查询原始值；首次不带 `snapshot_id` 即建快照，之后带快照号翻页 |
| `GET /snapshots/{snapshot_id}` | 快照元数据（固化的字段名与分片） |
| `GET /calibrations` | 列出校准状态（可按 `station` 过滤） |
| `GET /calibrations/{station}/{indicator_id}` | 读取某站点×指标的当前校准区间与修订号 |
| `PUT /calibrations/{station}/{indicator_id}` | 整体替换校准区间（须带 `expected_revision`） |
| `GET /qualified-rows` | 按站点×指标 ID 分页查询合格数据；首次不带 `snapshot_id` 即固化快照 |
| `GET /qualified-snapshots/{snapshot_id}` | 合格查询快照固化的指标身份、名称与校准区间 |
| `GET /docs` | Swagger UI（可在此维护校准期并对照新旧查询） |

### Parquet 上传规则

* 必含固定列：`station`（非空字符串）、`sampled_at`（UTC 时刻；支持带时区 timestamp、
  ISO-8601 字符串含 `Z`/偏移量、date；无时区值按 UTC 处理）。
* 指标列必须可转 `float64`（整数允许、布尔拒绝、全 NULL 允许），至多 8 个。
* 列名按**指定字段版**裁决：
  * 版内不存在的列（未知列）→ **整次拒绝（409）**；
  * 列映射到已删除指标 → **整次拒绝（409）**；
  * 版内未包含的指标可以缺列（指标本身可空）。
* 空文件、0 行分片、无法解析的 Parquet → 409；字段版不存在 → 404。
* 失败只回滚事务并清理临时文件，**不改动任何已有字段、数据与快照**。

### 查询与快照语义

* `GET /rows?page=1&page_size=100`：首次调用返回 `snapshot_id` 与 `snapshot_created=true`。
* 之后 `GET /rows?snapshot_id=...&page=2&page_size=100` 始终读同一批分片、同一组字段名；
  新字段在旧分片为 `NULL`，同名复用的新身份在旧分片同样为 `NULL`。
* 不带快照号的新查询会生成新快照，反映最新字段与分片。
* 分片内按 `(station, sampled_at)` 稳定排序后分页；`page_size` 上限 10000。
* `/rows` **永远返回原始值**，校准期的存在不改变它。

## 4. 校准有效区间与合格数据（分析供数）

分析供数只使用“校准有效期内”的化验值；期外的化验值被隔离，但原始记录仍完整保留
在分片与 `/rows` 中，随时可审计。

### 4.1 维护校准区间（`/docs` 的「校准期」分组）

```bash
# 读取当前状态（从未设置过时 revision=0、intervals=[]）
curl http://localhost:8000/calibrations/站点A/1

# 一次替换该 (站点, 指标身份) 的全部区间；expected_revision 必须是刚读到的 revision
curl -X PUT http://localhost:8000/calibrations/站点A/1 \
  -H 'Content-Type: application/json' \
  -d '{
        "expected_revision": 0,
        "intervals": [
          {"valid_from": "2026-01-01T00:00:00Z", "valid_to": "2026-02-01T00:00:00Z"},
          {"valid_from": "2026-03-01T00:00:00Z", "valid_to": "2026-04-01T00:00:00Z"}
        ]
      }'
```

* 身份键是 `(station, indicator_id)`，**与指标名无关**：改名不影响校准；
  同名新建的指标是**新身份**，不继承旧校准期。
* 一次 PUT 是**整体替换**，不是增量追加；`intervals: []` 表示清除（仍推进修订号）。
* 区间是 UTC 半开区间 `[valid_from, valid_to)`，要求 `valid_from < valid_to`，
  首尾相接合法，**任何重叠整次拒绝**。
* 修订号 `revision` 是乐观并发控制：PUT 必须提交 `expected_revision`（提交方所见的
  revision；从未设置过为 `0`）。过期或超前提交一律 `409`，**原状态一字不改**。
* 非法时间、空区间、重叠区间同样 `409`，由事务整体回滚。
* 已删除指标不能写入校准（`409`）；历史已有的校准仍可只读查看。
* `GET /calibrations?station=...` 可列出全部校准状态，便于核对。

### 4.2 查询合格数据（`GET /qualified-rows`）

```bash
# 首次查询：station + indicator_id 必填；服务端固化快照并返回 snapshot_id
curl 'http://localhost:8000/qualified-rows?station=站点A&indicator_id=1&page=1&page_size=100'
# 翻页：只带 snapshot_id 即可（站点/指标/区间/分片全部来自快照）
curl 'http://localhost:8000/qualified-rows?snapshot_id=7&page=2&page_size=100'
```

每行返回 `station`、`sampled_at`、`value`、`status`：

| `status` | 含义 |
| --- | --- |
| `calibrated` | 采样时刻在有效校准区间内，且原值非 NULL；`value` 为原值。 |
| `calibrated_null` | 采样时刻在有效校准区间内，但**原值为 NULL**（水样未测）。 |
| `uncalibrated` | 校准期外；默认不返回，仅 `include_outside=true` 审计对照时出现。 |

响应顶层同时给出 `calibration_status`（`configured` / `missing`）：
`missing` 表示查询时该站点×指标**没有任何有效校准区间**，这与“有校准但原值为 NULL”
是两件事，分别由 `calibration_status` 和 `status` 标明。
`include_outside=true` 只用于审计对照，不能把期外值当成合格供数。

**快照固定**：首次查询把分片集合、指标身份与名称、**校准区间副本**一并固化。
此后校准区间更正、指标改名、同名新建、上传、重启都不会改变旧查询（`snapshot_id` 不变即可复现）；
不带 `snapshot_id` 的新查询读取最新完整状态。翻页时若显式重复传 `station`/`indicator_id`，
必须与快照一致（否则 `409`）；`include_outside` 也必须与首次一致。
`GET /qualified-snapshots/{snapshot_id}` 可查看快照固化的区间与分片。

**原始记录仍可查**：同一批数据用 `/rows` 查询仍返回全部原值，包括校准期外的行。

## 5. 并发与一致性

所有字段变更、上传、校准替换与快照创建都在同一把进程写锁 + SQLite `BEGIN IMMEDIATE`
事务内裁决：每个请求看到的都是一个完整的已提交状态，不存在半份结果。

* 校准替换之间：`expected_revision` 保证并发更正只有一个成功，其余 `409`。
* 校准替换与合格查询之间：首次查询把区间复制进快照，交错操作不会把前后两个版本的
  区间混进同一次查询。
* 字段变更与上传交错时，按同一个已提交字段状态裁决，不串列。

## 6. 目录结构

```
app/
  config.py       # 常量与数据目录
  db.py           # SQLite 建表、旧库迁移、立即事务
  timeutil.py     # UTC 微秒解析/格式化
  models.py       # Pydantic 模型与错误类型
  catalog.py      # 指标身份与字段版（新增/改名/删除）
  calibration.py  # 站点×指标 ID 的 UTC 校准区间与修订号
  ingest.py       # Parquet 校验、按字段版映射身份、原子落盘
  query.py        # 快照创建、原始分页合读、合格数据分页
  main.py         # FastAPI 路由与并发控制
tests/            # pytest 自测 + 独立 HTTP 冒烟脚本
Dockerfile
docker-compose.yml
```

## 7. 自测

```bash
pip install -r requirements.txt
pip install pytest httpx
python -m pytest tests/ -v
# 对已运行的服务做独立 HTTP 冒烟：
python tests/manual_smoke.py
```
