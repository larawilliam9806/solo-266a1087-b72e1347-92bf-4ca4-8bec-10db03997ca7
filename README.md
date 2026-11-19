# Buoy Data Lake Compaction Service

单进程浮标数据湖 API：按 UTC 日分区追加上传 CSV（浮标号、UTC 时刻、盐度），
每次上传落为一个 Parquet 文件；支持按日读取全部记录与当前文件数；支持按日
触发后台压实，把启动时已有的若干小文件合并为一份，降低下游扫描的小文件开销。

技术栈：FastAPI + PyArrow + SQLite（WAL），单进程设计。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 服务监听 http://localhost:8000 ，交互文档 http://localhost:8000/docs
```

所有状态（Parquet 数据 + SQLite 元数据）都在 `/data` 卷中，容器重启不丢数据。

### 本地运行

```bash
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 运行测试

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -q
```

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/days/{day}/upload` | 上传 CSV（multipart 文件或 `text/csv` 原始 body），追加为一个 Parquet |
| GET | `/days/{day}/records` | 读取当日全部记录（一致快照），含 `file_count`/`row_count` |
| GET | `/days/{day}/increment` | 增量读取：按入湖顺序分页返回启用后成功上传的记录（游标续读，每页 ≤500 行） |
| GET | `/days/{day}/files` | 当日活跃文件清单与文件数 |
| POST | `/days/{day}/compactions` | 启动后台压实（202）；当日已有压实进行中返回 409 |
| GET | `/days/{day}/compactions` | 当日压实任务列表 |
| GET | `/days/{day}/compactions/{job_id}` | 查询压实任务状态 |
| GET | `/health` | 健康检查 |

`day` 格式为 `YYYY-MM-DD`（UTC 日期分区）。CSV 列为 `buoy_id, ts_utc, salinity`，
表头可选（无表头时按此顺序解析）；时间戳支持 ISO-8601（`Z` 或时区偏移，
裸时间按 UTC 处理）。

### 示例

```bash
# 上传（multipart 或原始 body 均可）
curl -F "file=@obs.csv" http://localhost:8000/days/2026-11-18/upload
curl -X POST -H "Content-Type: text/csv" --data-binary @obs.csv \
     http://localhost:8000/days/2026-11-18/upload

# 读取全部记录与文件数
curl http://localhost:8000/days/2026-11-18/records

# 启动压实并轮询状态
curl -X POST http://localhost:8000/days/2026-11-18/compactions
curl http://localhost:8000/days/2026-11-18/compactions/1
```

### 增量订阅（下游按入湖顺序消费）

`GET /days/{day}/increment?cursor=...&limit=...` 为下游分析程序提供按入湖
（成功登记）顺序的增量读取，响应含 `records`（原始三列
`buoy_id`/`ts_utc`/`salinity`）、`row_count`、`next_cursor`、`has_more`：

- 只包含本功能启用后**成功上传**的行；启用前的历史观测仍通过
  `GET /days/{day}/records` 全量读取，增量流不做回填。
- 同一观测上传几次就交付几次（不去重）；每批上传的行在流中连续、
  按成功登记顺序排列，并发上传之间不交错。
- 每页最多 500 行（`limit` 默认 500，超出按 500 计）；用上一页返回的
  `next_cursor` 作为 `cursor` 续读。翻页期间新上传的行会在后续请求中按
  顺序出现；页面为空时 `next_cursor` 为 `null`，沿用上一次使用的游标继续
  轮询即可。
- 游标即全局入湖序列的高水位：压实、退役文件回收、服务重启都不会改变
  已发游标的含义；停在一批的中间续读也不会漏行或重复。

```bash
# 从头消费当日增量（第一页）
curl http://localhost:8000/days/2026-11-18/increment
# 用上一页返回的 next_cursor 续读
curl "http://localhost:8000/days/2026-11-18/increment?cursor=500&limit=500"
```

## 一致性与可靠性设计

- **上传原子性**：Parquet 先写 `*.tmp-*` 临时文件，`fsync` 后 `rename` 为正式
  文件并 `fsync` 目录，再在 SQLite 登记为 `active`。读者永远看不到半成品文件；
  崩溃最多留下临时碎片，启动恢复时清理。
- **读取一致性**：读取在同一把锁内完成「快照当日 active 文件列表 + 引用计数
  +1」，因此一次读取必然对应完整的切换前或切换后状态，不会出现新旧混杂；
  响应中的 `file_count` 与实际读取的文件集合严格一致。
- **压实切换原子性**：合并输出完整落盘（tmp+fsync+rename）后，「退役输入文件
  + 登记输出文件 + 任务置为 done」在**单个 SQLite 事务**中提交。提交前崩溃
  → 原文件全部仍在，任务重启后续跑；提交后崩溃 → 切换已生效。不存在中间态。
- **压实期间可写可读**：合并只处理任务启动时快照的输入集合；期间追加的文件
  不在集合内，切换时原样保留、全程可读，不会被遗漏或误删。上传、读取均不
  等待压实结束。
- **同日只受理一项压实**：SQLite 部分唯一索引
  `WHERE status='running'` 在数据库层面保证，并发请求第二个返回 409，
  进程重启后约束依然有效。
- **失败保留原数据**：合并或落盘失败 → 任务标记 `failed`（带错误信息），
  输入文件保持 `active`，可修复后重新发起压实。
- **中断恢复**：启动时 `recover()` 先清理退役文件、孤儿 Parquet（未登记的
  上传残留、被取代的压实输出）和 tmp 碎片，再把仍处于 `running` 的任务重新
  投入执行（其输入必然仍全部 active，重跑是安全的）。已提交的上传不受影响。
- **旧文件回收**：退役文件仅在在途读取引用计数归零后才 unlink；回收只针对
  登记表中 `retired` 的文件，`active` 文件（包括压实期间新追加的）永远不会
  被误删。另有后台周期清扫兜底。
- **增量流与登记同事务**：上传的 Parquet 登记与本批 `feed_rows` 增量日志写入
  在**同一个 SQLite 事务**中提交（transactional outbox），二者要么同时可见、
  要么同时不可见；失败上传（CSV 解析失败、落盘失败、登记失败）绝不进入增量
  流。`feed_rows` 只增不改、与 Parquet 文件生命周期完全解耦，增量游标
  （`feed_rows.seq` 高水位）持久化于 SQLite，压实、旧文件回收与进程重启均不
  改变其含义。

## 目录布局

```
data/
  lake_meta.db                 # SQLite 元数据（files 登记表 + compactions 任务表 + feed_rows 增量日志）
  dt=2026-11-18/
    part-<uuid>.parquet        # 上传产生的文件
    compacted-<job>-<id>.parquet  # 压实输出
```

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `./data`（容器内 `/data`） | 数据与元数据目录 |
| `LAKE_GC_INTERVAL_SEC` | `2` | 退役文件周期清扫间隔 |
| `LAKE_COMPACTION_DELAY_SEC` | `0` | 测试/演示钩子：合并前人为延迟，便于观察压实期间的并发读写 |

> 注意：一致性依赖进程内锁 + SQLite 事务，请以单进程运行（不要多 worker
> 水平扩展）。
