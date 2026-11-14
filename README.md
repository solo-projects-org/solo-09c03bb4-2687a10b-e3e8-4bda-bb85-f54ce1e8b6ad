# 湖仓水位观测表 Delta Lake 压实与变更读取 API

用 **FastAPI + PyArrow** 手工实现的 Delta Lake（读协议 v1 / 写协议 v2）小文件压实服务，
以 **双进程** 方式运行，面向一张无分区的水位观测表（非空字符串站号、UTC 秒与水位毫米
均为 int64）。压实把指定版本中的至少两份 Parquet 合成一份，保持原值、重复行、模式与
全部旧快照不变，并在外部写者持续提交的环境下用乐观并发控制（OCC）保证提交安全。

同一服务还提供**逐行变更读取**（`/table/changes`）：按 Delta 提交顺序输出
`dataChange=true` 的新增与删除行，供告警管道补取观测；`dataChange=false` 的压实
不产生任何变更，因此压实后旧观测不会再次触发告警。

## 目录结构

```
.
├── app/
│   ├── delta.py            # Delta 协议：JSON 日志、快照回放、原子提交、压实/OCC
│   ├── change_feed.py      # 逐行变更读取：dataChange 过滤、行级定位、游标分页
│   ├── external_writer.py  # 外部写者：dataChange=true 的追加与删除（独立进程）
│   ├── sample_data.py      # 样例表引导（4 个小文件，幂等、跨进程加锁安全）
│   └── main.py             # FastAPI 路由、Swagger、错误码映射
├── scripts/
│   ├── _demo_common.py
│   ├── demo_readback.py            # 演示 1：压实并读回新旧版本
│   ├── demo_concurrent_append.py   # 演示 2：压实期间外部并发追加
│   ├── demo_concurrent_delete.py   # 演示 3：压实期间外部删除 -> 409 冲突
│   ├── demo_change_feed.py         # 演示 4：压实后补取变更、跨副本续读
│   ├── run_local_servers.sh        # 无 Docker 时本地双进程启动
│   └── run_all_demos.sh
├── tests/test_compaction.py        # 23 个压实端到端/并发/崩溃测试
├── tests/test_change_feed.py       # 30 个变更读取/分页/游标/HTTP 测试
├── Dockerfile
├── docker-compose.yml              # api-1:8001、api-2:8002，共享一个数据卷
├── requirements.txt / requirements-dev.txt
```

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

两个副本共享命名卷 `delta-data` 中的同一张表，任一进程都可发起压实：

| 服务   | 地址              | Swagger UI                |
|--------|-------------------|---------------------------|
| api-1  | http://localhost:8001 | http://localhost:8001/docs |
| api-2  | http://localhost:8002 | http://localhost:8002/docs |

启动时自动创建样例表：版本 0 为 `protocol(1,2)+metaData`，版本 1–4 各追加一个
Parquet（共 24 行、4 个文件）。容器内表路径为 `/data/water_level`。

## API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/health` | 健康检查，返回最新版号 |
| POST | `/compact` | 压实：body `{"version": 4, "files": ["a.parquet", "b.parquet"]}`，`version` 省略时取最新版，`files` 至少 2 个 |
| GET  | `/table/versions` | 全部连续版号 |
| GET  | `/table/snapshot?version=N` | 快照信息（不传为最新，支持时间旅行） |
| GET  | `/table/changes` | **逐行变更读取**（见下节），支持游标分页 |
| GET  | `/table/stats` | 行数、模式、孤儿清理结果 |

`POST /compact` 成功响应包含提交版号与文件数变化：

```json
{
  "committed_version": 5,
  "base_version": 4,
  "files_added": 1,
  "files_removed": 2,
  "output_file": "….parquet",
  "input_files": ["….parquet", "….parquet"],
  "rows": 12,
  "input_bytes": 2168,
  "output_bytes": 1131,
  "attempts": 1
}
```

错误码：400 请求非法（少于两文件、路径逃逸等）、404 版号或文件不存在、
409 并发冲突（所选文件已被他人移除）、503 表暂不可用。

## 逐行变更读取 `GET /table/changes`

告警管道用这个接口从湖仓补取"新增了多少观测、撤销了多少观测"。它直接回放 Delta
日志中 `dataChange=true` 的 `add` / `remove` 动作，**按提交顺序**逐行输出：

* `add` → `insert`，读取该 Parquet 文件的全部行；
* `remove` → `delete`，同样读取被移除文件的内容。因此删除压实产物（压实生成、
  随后又被删除的文件）也能正确输出被撤销的行；
* `dataChange=false` 的压实动作（`OPTIMIZE` 的 add/remove）**完全不产生记录**，
  这类版本只会得到 `has_more=false` 的空页——压小文件不会让旧观测再次告警；
* 不做去重：文件里有两行相同数据，就输出两条记录。

### 记录标识

每条记录带 `record_id = "v{version}-a{action_index}-r{row_index}"`，三个分量分别是
Delta 提交版号、动作在该日志文件中的 0 基位置、行在该 Parquet 文件中的 0 基行号。
接口同时以独立字段返回 `version` / `action_index` / `row_index` / `path` / `action` /
`change_type`。

### 首次请求与续读游标

首次请求传 `start_version`（起始版号）与 `page_size`（每页条数）：

```bash
curl -s "localhost:8001/table/changes?start_version=0&page_size=5"
```

服务在这一刻固定本轮日志上界 `end_version = 当前最新版号`，并返回：

* `records`：本页记录（最多 `page_size` 条）；
* `has_more`：本轮是否还有下一页；
* `next_cursor`（同义字段 `cursor`）：续读游标，本轮结束时为 `null`；
* `next_start_version`：本轮结束后的下一轮起始版号（恒为 `end_version + 1`）；
* `latest_version`：本次请求时观察到的最新版号（可能已大于 `end_version`）。

续读只传游标，**不要再传 `start_version`**（两者同时传会返回 400）：

```bash
curl -s "localhost:8001/table/changes?cursor=<next_cursor>"
```

游标是自签名（HMAC）的无状态令牌，内含固定的 `start_version`、`end_version`、
`page_size` 和精确位置 `(version, action_index, row_index)`：

* **跨副本**：api-1 签发的游标可直接交给 api-2（两副本共享
  `CHANGE_FEED_CURSOR_SECRET`，compose 中默认一致）；
* **重启后**：游标不依赖任何进程内会话，重启后继续读位置不变；
* **同一 Parquet 跨页**：游标精确到行，不会漏行也不会重发；读取时只解码与请求
  窗口相交的 row group，大文件小页也不会退化为整文件重读；
* **本轮固定上界**：续读期间即使有新提交（`latest_version` 变大），本轮的
  `end_version` 也不会改变，新提交留给下一轮（用 `next_start_version` 重新发起）。

### 错误与不完整页

* 400：缺少 `start_version` 且无游标；`start_version` 与游标同传；游标被篡改、
  版本前缀不符或签名不匹配（例如密钥配置不一致）；`page_size` 不在 1..100000。
* 500：被 `dataChange` 动作引用的 Parquet 文件缺失或不可读。此时**不会**把该页
  当作"已读完"返回，而是报错——丢弃会静默丢失新增/撤销的观测。日志在
  本轮上界内消失（日志损坏）同样报错。
* 若 `start_version` 大于当前最新版号（如轮询方传 latest+1），返回空页，
  `next_start_version` 保持为 `start_version`，等该版本提交后再读。

```json
{
  "start_version": 0,
  "end_version": 4,
  "page_size": 5,
  "records": [
    {
      "record_id": "v1-a1-r0",
      "version": 1,
      "action_index": 1,
      "row_index": 0,
      "action": "add",
      "change_type": "insert",
      "path": "1731600001-….parquet",
      "row": {"station": "ST-001", "utc_seconds": 1731600000, "level_mm": 1204}
    }
  ],
  "count": 5,
  "has_more": true,
  "next_cursor": "cf1.….…",
  "cursor": "cf1.….…",
  "next_start_version": 5,
  "latest_version": 4
}
```

### 示例

```bash
# 压实与旧快照时间旅行
curl -s localhost:8001/table/snapshot | jq .active_files
curl -s -X POST localhost:8001/compact -H 'content-type: application/json' \
  -d '{"files": ["<f1>.parquet", "<f2>.parquet"]}'
curl -s "localhost:8001/table/snapshot?version=4"

# 变更读取：首轮 + 续读（可换到 8002）
curl -s "localhost:8001/table/changes?start_version=0&page_size=100"
curl -s "localhost:8002/table/changes?cursor=<next_cursor>"
```

## 演示脚本

脚本同时走 HTTP 与本地表路径（外部写者以独立 OS 进程直接提交 Delta 日志）：

```bash
pip install -r requirements-dev.txt
bash scripts/run_local_servers.sh            # 终端 A：本地起两个 uvicorn
bash scripts/run_all_demos.sh                # 终端 B：依次跑四个演示
```

* **演示 1（读回新旧版）**：压实后新版行数/模式不变、文件数 -2+1；`?version=` 读回
  旧版，文件集与数据完全不变。
* **演示 2（并发追加）**：外部进程在压实进行中连续提交 6 个 `dataChange=true` 追加，
  压实仍成功（无关文件变化不构成冲突），日志保持 0 起连续。
* **演示 3（并发删除）**：外部进程删除所选文件之一，压实提交时复检发现文件失效，
  返回 409 且不复活已删数据；改用仍有效的文件重试即成功。
* **演示 4（变更读取）**：分页读完整日志（每行恰好一次、记录标识唯一）；压实后的
  版本回放为空；删除压实产物后其行以 `delete` 记录出现；本轮进行中发生的新提交
  被固定上界挡住，换到另一副本用游标续读位置不变。

## 协议与正确性保证

**表结构**：无分区；协议固定 reader=1、writer=2；无删除向量；JSON 日志从 0 连续
编号（`_delta_log/00000000000000000000.json` …），每行一个 JSON action。

**数据不变量**

* 压实仅做 `concat`，不去重、不过滤、不改序语义；模式逐字段校验，非空约束校验。
* 新文件先写 `_staging/` 再 **硬链接** 到唯一最终路径（纳秒时间戳+UUID，路径永不复用），
  半成品不会出现在任何快照中、不可查询。
* 被移除的物理 Parquet 不删除，旧快照（时间旅行）始终可读。
* 写盘失败/进程中途退出：已提交快照不受影响；`_staging` 与 `.tmp-*` 孤儿由
  `/table/stats` 或启动时的 `cleanup_staging()` 按 mtime 安全回收。

**并发控制（OCC）**

* 日志提交 = 唯一临时文件 + `os.link` 原子改名：两个进程不可能得到同一版号，
  已提交日志绝不被覆盖或截断。
* 压实在提交前重新加载最新快照：**所选文件全部仍有效才替换**；任一文件已被并发
  移除则报 409 并清理新产物，绝不复活已删数据。
* 仅无关文件的增删不影响压实；日志版号竞争时自动退避重试（上限 25 次）。
* 压实提交的 add/remove 均带 `"dataChange": false`（add 另带 `tags.compaction=true`，
  `commitInfo.operation=OPTIMIZE` 与指标），外部追加/删除为 `dataChange=true`。

## 测试

```bash
python -m pytest tests/ -q
```

覆盖：连续日志与协议号、行/重复行/模式保持、时间旅行、路径校验、跨进程同集/异集
压实竞争、并发删除 409 与不复活、崩溃后快照不变、staging 孤儿清理、双进程并发建表、
临时日志文件忽略，以及 HTTP 层全部状态码映射；另有变更读取测试覆盖 dataChange
过滤、压实不可见、删除读取被移除文件（含压实产物）、重复行、记录标识、多种页大小
下的不漏不重、跨副本/重启游标、游标防篡改、键轮换、缺失文件报错、日志缺失报错，
以及只解码所需 row group。
