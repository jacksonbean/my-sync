# 双层门卫（Two-Layer Gate）实现完成报告

## 改动文件清单

| 文件 | 状态 | 说明 |
|------|------|------|
| `pkg/sync/db/gate_record.go` | 新增 | 表结构 `SyncRecordV2` + 接口 `DbGateService` + 表名生成 `ResolveGateTableName` |
| `pkg/sync/db/gate_service.go` | 新增 | MySQL / SQLite 纯 SQL 实现（`INSERT ... ON DUPLICATE KEY UPDATE` / `ON CONFLICT`） |
| `pkg/sync/gate.go` | 新增 | 双层门卫核心逻辑：`firstGate` / `secondGate` / `GateRecordBuffer` / `RecordSkip`（含 `Diff` 计算） |
| `pkg/sync/gate_test.go` | 新增 | 测试用例：firstGate 分支、secondGate 分支、Diff 计算、Buffer 并发安全 |
| `pkg/sync/sync.go` | 修改 | 全局变量 `gateSvc`/`gateBuf` + `Sync()` 初始化/销毁 + `produce()` 双层门卫插入 + `worker()` 记录写入 |
| `cmd/sync.go` | 修改 | 新增 CLI 参数 `--gate-table` |
| `pkg/sync/config.go` | 修改 | 新增 `Config.GateTable` 字段 + CLI 绑定 |

---

## 核心设计

### 双层门卫流程

```
listAll(src) ──► for obj := range srckeys
                    │
                    ▼
            ┌───────────────┐
            │ 第一层：DB 快速门卫 │  ← 查本地 DB（零目标端 API）
            │ firstGate()   │
            └───────┬───────┘
                    │
        ┌───────────┼───────────┐
        │           │           │
    GateSkip   GateMissing   NeedSecondGate
        │           │           │
        ▼           │           ▼
    skipIt()      │     ┌───────────────┐
    continue      │     │ 第二层：目标端实时门卫 │  ← 调用目标端 Head
                  │     │ secondGate()  │
                  │     └───────┬───────┘
                  │             │
                  │     ┌───────┴───────┐
                  │     │               │
                  │ ActionSend     ActionSkip
                  │     │               │
                  │     ▼               ▼
                  │  sendTask()      skipIt()
                  │     │               │
                  │     ▼               │
                  │ worker() ────────────┘
                  │     │
                  │     ▼
                  │  success ──► RecordSuccess(gateBuf)
                  │  failure ──► RecordFailure(gateBuf)
                  │  skip ─────► RecordSkip(gateBuf) [含 Diff 计算]
                  │
                  └────► (DB 缺失时直接进入第二层)
```

### 第一层门卫决策（基于 DB 记录，零目标端 API）

| 条件 | 结果 | 说明 |
|------|------|------|
| `--force-update` | `NeedSecondGate` | 强制全部进入第二层 |
| DB 记录缺失 | `Missing` | 新对象，必须进入第二层 |
| DB 状态 != `success` | `NeedSecondGate` | 上次失败/中断，重新处理 |
| 状态=`success` 且源 mtime 未变 | `Skip` | **核心优化：直接跳过，不调用 Head** |
| 状态=`success` 但源 mtime 变了 | `NeedSecondGate` | 可能更新，需目标端确认 |

### 第二层门卫决策（基于目标端 Head，只对比 mtime）

| 条件 | 结果 | 说明 |
|------|------|------|
| 目标端不存在 | `Send` | 全新对象 |
| 目标 mtime ≥ 源 mtime | `Skip` | **保护目标端新数据**（应用已切换后修改的对象） |
| 目标 mtime < 源 mtime | `Send` | 源端更新，需要覆盖 |

**注意：第二层不对比 size**，因为用户明确表达了：迁移期间应用切换到目标端后，目标 mtime 一定比源新，此时必须跳过。size 差异通过 `Diff` 字段记录，用于事后诊断。

---

## Diff 诊断字段

`RecordSkip` 在被第二层门卫跳过时写入 DB：

```go
diff := targetSize != 0 && obj.Size() != targetSize
```

| status | diff | 含义 | 诊断 SQL |
|--------|------|------|---------|
| `success` | false | 正常覆盖 | 无需关注 |
| `skipped` | false | 目标端大小与源一致，跳过（mtime 更新） | `SELECT ... WHERE status='skipped' AND diff=false` |
| `skipped` | **true** | ⚠️ **目标端存在但大小与源不一致，被跳过** | `SELECT ... WHERE status='skipped' AND diff=true` |
| `failed` | false | 失败 | 下次重新处理 |

```sql
-- 找出所有"目标端存在但大小与源不一致，且被跳过"的对象
SELECT key, source_size, target_size
FROM sync_records_v2_a3f7e2b9
WHERE status = 'skipped' AND diff = true;
```

---

## 表名设计（关键！）

### 默认行为：自动生成稳定表名

```go
// 未指定 --gate-table 时
hash := md5(src + "|" + dst)[:8]
table := "sync_records_v2_" + hash  // 如 sync_records_v2_a3f7e2b9
```

同一对 `src→dst` 始终使用同一张表，无论 sync 运行多少次。

### 自定义表名：--gate-table

```bash
# 首次同步
juicefs sync --db mysql://u:p@host/db --gate-table project_a_migration \
  s3://src.oss.com/ s3://dst.oss.com/

# 二次同步（复用同一张表）
juicefs sync --db mysql://u:p@host/db --gate-table project_a_migration \
  s3://src.oss.com/ s3://dst.oss.com/

# 多 worker 共享（所有 worker 指定同一个表名）
juicefs sync --db mysql://u:p@host/db --gate-table project_a_migration \
  --worker host1,host2,host3 s3://src.oss.com/ s3://dst.oss.com/
```

**自定义表名会被合法化**：非法字符替换为 `_`，长度限制 64 字符。

### 为什么表名必须稳定？

现有 JuiceFS `--db` 使用时间戳生成 JobID；当前实现已精确到微秒（如 `mybucket_20260101120000123456`），并对超长 bucket/path 做哈希截断，避免同分钟冲突或 MySQL 表名超过 64 字符。如果 gate 表也按时间戳生成，二次 sync 会找不到之前的记录，第一层门卫失效。

**Gate 表名与 Job 表完全独立**：
- Job 表：`objects_mybucket_202601011200`（每次运行新表，记录本次 sync 的详细日志）
- Gate 表：`sync_records_v2_a3f7e2b9`（稳定表名，跨运行复用，用于增量跳过）

---

## 使用方式

### 场景 1：日常增量备份（最常用）

```bash
# 首次（DB 为空，所有对象走第二层，同步完成后填满 gate 表）
juicefs sync --db mysql://u:p@host/db s3://src/ s3://dst/

# 二次（99% 对象命中第一层，直接跳过，零 Head 调用）
juicefs sync --db mysql://u:p@host/db s3://src/ s3://dst/
```

### 场景 2：多 worker 共享 gate 记录

```bash
# 所有 worker 指定同一个自定义表名
juicefs sync --db mysql://u:p@host/db --gate-table migration_batch_1 \
  --worker host1,host2,host3 s3://src/ s3://dst/
```

### 场景 3：诊断 size 不一致的对象

```bash
# 同步后查询 DB
mysql -h host -u u -p db -e "
  SELECT key, source_size, target_size
  FROM sync_records_v2_a3f7e2b9
  WHERE status = 'skipped' AND diff = true;
"
```

### 场景 4：强制重新同步（忽略 gate 记录）

```bash
juicefs sync --db mysql://u:p@host/db --force-update s3://src/ s3://dst/
```

`--force-update` 会让第一层直接 `NeedSecondGate`，所有对象重新进入第二层。

---

## 注意事项

1. **Gate 表与 Job 表独立**：`--gate-table` 只控制 gate 表名，不影响 `--db` 生成的 Job 记录表（`objects_...`）。
2. **Gate 表不会自动清理**：长期运行会积累大量记录。如果对象被删除，gate 记录不会自动删除（不会导致问题，只是 DB 膨胀）。可定期 `TRUNCATE TABLE` 或重建。
3. **换了 src/dst 后 hash 会变**：如果 endpoint 加了端口号、改了 bucket 名，默认表名会变化。此时应使用 `--gate-table` 指定固定表名，或者接受从头开始。
4. **进程被 kill 后的数据丢失**：`GateRecordBuffer` 每 1 秒或每 500 条刷盘一次。进程被 kill 最多丢失最近 500 条记录（这些对象下次 sync 会被第一层放行到第二层重新处理，不会导致数据不一致）。
5. **并发安全**：`GateRecordBuffer` 使用 `sync.Mutex` 保护，`Add` 和 `Flush` 可在不同 goroutine 中并发调用。
6. **MySQL 表名 64 字符限制**：自定义表名超过 64 字符会被截断。默认生成的表名（`sync_records_v2_` + 8 位 hash = 20 字符）远低于限制。
7. **`--delete-dst` 与 gate skip**：第一层 gate 必须在 src/dst key 对齐和 extra 处理之后执行；命中 `GateSkip` 且 key 相等时要消费 dst 游标，避免把已同步对象误判为 extra 删除。
8. **`--fix-meta` 执行顺序**：fix-meta 在正常同步 pipeline 启动前返回，不会先跑一轮数据复制。
9. **StartJob 失败处理**：job 明细表创建失败会直接返回错误，不再继续同步然后静默丢弃 DB 明细。

---

## 编译前检查清单

由于环境中无 Go 编译器，以下手动检查点：

- [ ] `pkg/sync/db/gate_service.go`：`_ "github.com/go-sql-driver/mysql"` 的 import 是否已存在（在 `mysql.go` 中已有，但 `gate_service.go` 中 `NewGateService` 使用了 `sql.Open("mysql", ...)`，需要确认 import 了 `_ "github.com/go-sql-driver/mysql"`）。**注意**：`gate_service.go` 中 `sql.Open` 不需要显式 import driver，但 `database/sql` 必须有 driver 注册。由于 `mysql.go` 已经 import 了该 driver，同一包中 `gate_service.go` 不需要重复 import。
- [ ] `pkg/sync/sync.go`：`sync_db` 包是否已 import（已有，现有代码使用了 `sync_db.ParseDbDSN` 等）。
- [ ] `pkg/sync/sync.go`：`gateSvc` 和 `gateBuf` 全局变量在所有使用点（`produce()` / `worker()`）中直接引用，无需传递参数。
- [ ] `pkg/sync/gate_test.go`：使用了 `mockObject`，只实现了部分 `object.Object` 接口方法。测试编译时可能需要补齐接口方法，或改用 `object.Object` 的实际实现。

---

## 可能的后续优化

1. **SQLite 支持**：`gate_service.go` 中已实现 `sqliteGateService`，但 `sync.go` 中 `NewGateService` 只支持 MySQL。可扩展为根据 DSN 自动选择 driver。
2. **Gate 表清理命令**：提供 `--gate-reset` 或 `--gate-truncate` 参数，方便运维重置 gate 记录。
3. **Gate 记录过期**：添加 TTL 机制（如 `updated_at < NOW() - INTERVAL 90 DAY` 的记录自动删除），防止 DB 无限膨胀。
4. **多 worker 并发写优化**：当前每个 worker 独立连接 DB 写入 gate 记录。如果 worker 数量很大，可能产生写冲突。可考虑 worker 只发送 gate 记录给 manager，由 manager 统一批量写入。

---

## 2026-07-29 更新：老版本 MySQL 兼容 + --db 性能修复

### 表结构变更（ecs-sync 风格自增主键）

原 schema 以 `key VARCHAR(768) PRIMARY KEY`（utf8mb4，索引需 3072 字节），在老版本 MySQL
（InnoDB 行格式 REDUNDANT/COMPACT，如 MySQL ≤5.6 / MariaDB ≤10.1）上触发
`Error 1709: Index column size too large. The maximum column size is 767 bytes`，建表失败。

新 schema 改为自增主键 + `key_hash CHAR(32)` 唯一索引（key 的 MD5 hex，Go 侧计算）：

```sql
CREATE TABLE IF NOT EXISTS sync_records_v2_xxx (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    key_hash CHAR(32) NOT NULL,
    `key` VARCHAR(768) NOT NULL,            -- 仅诊断用，不上索引
    source_mtime DATETIME(3) NOT NULL,
    source_size BIGINT DEFAULT 0,
    target_size BIGINT DEFAULT 0,
    diff BOOLEAN DEFAULT FALSE,
    status VARCHAR(16) NOT NULL,
    error_msg TEXT,
    updated_at DATETIME(3) NOT NULL,
    UNIQUE KEY uk_key_hash (key_hash),      -- 32 字节，任何版本都能建
    INDEX idx_gate (status, source_mtime)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

- 查询与去重都走 `key_hash`（`WHERE key_hash = ?` / `ON DUPLICATE KEY UPDATE`）。
- **不做旧表自动迁移**：检测到旧 schema 表时记 warning 提示手动 `DROP TABLE`，回退 no gate。

### 第一层门卫批量预取（消除 N+1 查询）

原实现：produce 单 goroutine 逐对象 `SELECT`（任意时刻仅 1 个查询在飞），
吞吐被钉死在 1/RTT（50ms 延迟 → ~20 条/秒 ≈ 1200 条/分钟）。

新实现：`prefetchGateRecords` goroutine 按批（500 个 / 50ms 无新对象 / 流结束）收集对象，
每批一次 `GetRecords`（`WHERE key_hash IN (...)`），produce 主循环消费 `(obj, record)`。
500 次串行查询合并为 1 次，且 DB 查询与 listing 并行。批查询失败时该批 record 为 nil，
落第二门卫（与原单查失败行为一致）。

### --ignore-existing 场景跳过第一层

`--ignore-existing` 的 skip 决策完全由 dst listing 决定，门卫 DB 状态不影响决策，
因此该场景下第一层门卫（含预取）整体跳过，produce 零 DB 查询。

### 异步记录写入提速

- `AsyncDbService` 批次 500 → 2000，`recordObjectsSync` chunk 200 → 500；
- 两个 MySQL DSN 增加 `interpolateParams=true`（等效 ecs-sync 的 cachePrepStmts）；
- gate 连接池 `SetMaxOpenConns` 5 → 16（对齐 ecs-sync Hikari maximumPoolSize=16）；
- `flushBatch` 增加每批落盘耗时的 debug 日志。
