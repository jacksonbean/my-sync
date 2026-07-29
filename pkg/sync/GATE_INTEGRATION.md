# 双层门卫与现有 JuiceFS sync 的集成说明

> 本文件描述如何把 `gate.go` + `db/gate_record.go` 集成到现有的 `pkg/sync/sync.go` 和 `cmd/sync.go` 中。
> 修改点尽量克制，只做最小侵入式改动。

---

## 一、新增/修改的文件清单

```
pkg/sync/
  ├── db/
  │   ├── gate_record.go    # 新增：SyncRecordV2 + DbGateService 接口
  │   └── db.go             # 已有：复用其中 MySQL 连接初始化逻辑
  ├── gate.go               # 新增：firstGate / secondGate / GateRecordBuffer
  └── sync.go               # 修改：produce() + worker() + Sync() 入口

cmd/sync.go                  # 修改：新增 --gate-db 参数（可选，复用 --db）
```

---

## 二、cmd/sync.go：新增参数（最小改动）

### 方案 A：复用现有 `--db` 参数（推荐）

如果用户已经配置了 `--db mysql://...`，则自动启用双层门卫模式。
不需要新增参数，但需要在 `doSync()` 中判断：如果 `config.DbDSN != ""` 且是 sync 模式（非 scan-only），则初始化 `DbGateService`。

### 方案 B：新增独立参数（如果担心兼容性问题）

```go
// cmd/sync.go 中 syncStorageFlags() 或 syncActionFlags() 里添加
&cli.StringFlag{
    Name:  "gate-db",
    Usage: "enable two-layer gate with a dedicated DB for sync records (defaults to --db if not set)",
}
```

这里采用**方案 A**，保持 CLI 简洁。

---

## 三、pkg/sync/sync.go：修改点详解

### 修改点 1：Sync() 函数入口 —— 初始化 Gate 服务

在 `Sync()` 函数开头（`config.EnableCheckpoint` 判断之后），增加 `GateRecordBuffer` 的初始化：

```go
// sync.go - Sync() 函数中，约第 2487 行附近

var gateBuf *GateRecordBuffer
var gateSvc sync_db.DbGateService

// 如果启用了 DB 记录且不是 scan-only 模式，初始化双层门卫
if config.DbDSN != "" && !config.Scan && !config.ScanSingle {
    // 复用现有 db 连接
    cfg, err := sync_db.ParseDbDSN(config.DbDSN)
    if err != nil {
        logger.Errorf("Failed to parse gate db url: %v", err)
    } else {
        // 这里需要 DbGateService 的构造函数。如果现有 db 包只提供了 AsyncDbService，
        // 可以在 db 包中新增一个 NewGateService(cfg) 返回 DbGateService 接口。
        svc, err := sync_db.NewGateService(cfg) // 需要新增此函数
        if err != nil {
            logger.Errorf("Failed to init gate db service: %v", err)
        } else {
            gateSvc = svc
            gateBuf = NewGateRecordBuffer(svc, 500, time.Second)
            logger.Infof("Two-layer gate enabled with DB: %s", config.DbDSN)
        }
    }
}
```

> 注意：`sync_db.NewGateService()` 需要你在 `pkg/sync/db` 包中实现，返回 `DbGateService` 接口。可以复用现有的 MySQL 连接初始化逻辑。

---

### 修改点 2：produce() 函数 —— 插入第一层门卫

在 `produce()` 函数的 `srckeys` 遍历循环中插入第一层检查，但**必须先完成 src/dst key 对齐和 extra 处理**，再允许 `GateSkip` 短路。否则 `--delete-dst` 会把“源被 gate 跳过、目标实际存在”的对象误判成 extra 并删除。

正确顺序：

```go
for obj := range srckeys {
    // ... nil / dir / Limit / incrTotal

    // 1) 先按 key 对齐 dstobj，并处理 obj.Key() > dstobj.Key() 的 extra
    if dstobj != nil && obj.Key() > dstobj.Key() {
        // handleExtraObject(...); dstobj = nil
    }
    if dstobj == nil {
        // 消费 dstkeys，直到 obj.Key() <= dstobj.Key()
    }

    // 2) 第一层：DB 快速门卫（gate 记录由预取 goroutine 批量查询，此处零 DB 调用）
    //    --force-update / --ignore-existing 时不启用第一层（后者 skip 决策完全由 dst listing 决定）
    if useFirstGate {
        if firstGate(obj, gobj.rec, config.ForceUpdate) == sync_db.GateSkip {
            if dstobj != nil && obj.Key() == dstobj.Key() {
                dstobj = nil // 关键：消费同 key 的 dst 游标，避免误判 extra
            }
            skipIt(obj)
            continue
        }
    }

    // 3) 再进入原有“目标不存在/同 key 对比”的第二层逻辑
}
```

**关键点**：
- `firstGate` 在 `obj` 和 `dstobj` 对比**之前**执行。
- 如果 `GateSkip`，直接 `skipIt(obj)` + `continue`，**不执行后面的 `Head` 调用**。
- `skipIt(obj)` 函数已经做了 `checkpointMgr.UpdateLastListedKey` 和 `syncDbService` 记录（如果启用了），不需要额外处理。

---

### 修改点 3：produce() 第二层 —— 保留现有逻辑，但可简化

第二层仍然走现有的 `Head` 对比逻辑，但可以**简化判断**。

同时，这里引入 `TargetSize` 的记录：**当目标端存在时（`dstobj != nil`），记录目标端大小，用于事后诊断"目标端存在但 size 与源不一致"的场景**。

找到这段代码（约第 1640 行附近）：

```go
} else { // obj.key == dstobj.key
    if config.IgnoreExisting {
        skipIt(obj)
        dstobj = nil
        continue
    }

    // 记录目标端存在时的大小（诊断字段，不参与同步决策）
    var targetSize int64
    if dstobj != nil {
        targetSize = dstobj.Size()
    }

    // 原有逻辑：
    // if config.ForceUpdate ||
    //     (config.Update && obj.Mtime().Unix() > dstobj.Mtime().Unix()) ||
    //     (!config.Update && obj.Size() != dstobj.Size()) {
    //     sendTask(obj)
    // }

    // ========== 【可选简化】用 secondGate 替换原有逻辑 ==========
    // 如果你的设计明确"只对比 mtime，不看 size"，可以替换为：
    action := secondGate(obj, dstobj)
    if action == ActionSend {
        sendTask(obj)
    } else {
        // 被第二层门卫跳过（目标 mtime 更新），记录目标端大小
        if gateBuf != nil && targetSize != 0 {
            RecordSkip(gateBuf, obj, targetSize)
        }
        skipIt(obj)
    }
    // =========================================================

    dstobj = nil
}
```

> **注意**：如果替换为 `secondGate`，则 `--update` 和 `--force-update` 的语义需要重新定义：
> - `--force-update` 已经在第一层被处理（`forceUpdate=true` → `GateNeedSecondGate`），第二层看到 `forceUpdate` 时应该直接 send。但 `secondGate` 目前不看 `forceUpdate`，所以需要调整：

```go
func secondGate(obj object.Object, dstobj object.Object, forceUpdate bool) SecondGateAction {
    if forceUpdate {
        return ActionSend
    }
    // ... 原有逻辑
}
```

> 或者保留 `--force-update` 在 `produce()` 外层判断，不进第二层直接 send。

---

### 修改点 3.5：produce() 中目标端存在的诊断记录（新增）

`Diff` 字段的设计目的：**明确标记"目标端存在且大小与源不一致"的场景**，不用于同步决策，只用于事后排查。

当第二层门卫发现目标端存在但决定**跳过**（目标 mtime 更新，保护目标端数据）时，记录目标端大小并计算 `diff`：

```go
// 在 produce() 中，当 secondGate 返回 ActionSkip 时：
if gateBuf != nil && targetSize != 0 {
    RecordSkip(gateBuf, obj, targetSize)
}
```

这样 sync 结束后，可以通过 `diff` 字段直接定位：

```sql
-- 找出"目标端存在、被跳过、且大小与源不一致"的对象
SELECT key, source_size, target_size 
FROM sync_records_v2 
WHERE status = 'skipped' AND diff = true;
```

| 状态 | diff | target_size | 含义 |
|------|------|-------------|------|
| `success` | false（默认） | 0 | 正常覆盖（无需诊断） |
| `skipped` | false | 等于 source_size | 目标端大小与源一致，被跳过（mtime 更新） |
| `skipped` | **true** | **不等于 source_size** | ⚠️ **目标端存在、大小与源不一致、被跳过**（目标 mtime 更新，已写入新数据） |
| `failed` | false（默认） | 0 | 失败，无法判断目标状态 |

> 说明：当 `sendTask` 后 `worker` 成功覆盖，会调用 `RecordSuccess(gateBuf, obj, 0)`（target_size 设为 0，因为覆盖后目标端大小等于源大小，不需要额外诊断）。

---

### 修改点 4：worker() 函数 —— 成功/失败时写 DB

在 `worker()` 函数的 `default` 分支（对象复制/校验的主逻辑）中，找到成功和失败的处理点。

#### 成功时（约第 1320-1333 行附近）：

```go
if err == nil {
    // ... 原有 chmod / chown / copied.Increment()
    if syncDbService != nil {
        recordSyncObject(syncDbJobID, key, obj.Size(), time.Now(), sync_db.StatusCopied, "")
    }

    // ========== 【新增】记录到 gate DB ==========
    if gateBuf != nil {
        // 覆盖成功，target_size 传 0（表示已覆盖，无需诊断）
        RecordSuccess(gateBuf, obj, 0)
    }
    // ============================================
} else if errors.Is(err, utils.ErrSkipped) {
    // ... 原有 skip 处理
} else {
    // ... 原有失败处理
    if syncDbService != nil {
        recordSyncObject(syncDbJobID, key, obj.Size(), time.Now(), sync_db.StatusFailed, err.Error())
    }

    // ========== 【新增】记录失败到 gate DB ==========
    if gateBuf != nil {
        // 失败时 target_size 传 0（无法确定目标端当前状态）
        RecordFailure(gateBuf, obj, err, 0)
    }
    // ==============================================
}
```

> 注意：失败的 `RecordFailure` 是为了让下次 sync 时第一层看到这个对象 `status != success`，从而放行到第二层重新处理。

#### 校验失败时（约第 1314-1318 行附近）：

```go
if equal, err = checkSum(src, dst, key, &srcChksum, obj, config); err == nil && !equal {
    err = fmt.Errorf("checksums of copied object %s don't match", key)
    // 校验失败也算失败，需要 RecordFailure
    if gateBuf != nil {
        RecordFailure(gateBuf, obj, err, 0)
    }
}
```

---

### 修改点 4.5：skip 记录（produce 中新增）

除了 `worker()` 中的成功/失败记录，`produce()` 中第二层门卫决定 `skip` 时也需要记录（已在"修改点 3.5"中说明）。

关键区别：
- `produce()` 中 `skip` → 目标端存在，可以记录 `targetSize = dstobj.Size()`（用于诊断）
- `worker()` 中 `success` → 目标端已被覆盖，`targetSize = 0`（无需诊断）
- `worker()` 中 `failure` → 未知目标端状态，`targetSize = 0`

---

### 修改点 5：Sync() 函数结尾 —— 刷盘 gate buffer

在 `syncExitFunc()` 中，或者 `Sync()` 函数的 `wg.Wait()` 之后，确保 gate buffer 的数据全部落盘：

```go
// 在 Sync() 的结尾，wg.Wait() 之后
if gateBuf != nil {
    if err := gateBuf.Close(); err != nil {
        logger.Warnf("Failed to flush gate records: %v", err)
    } else {
        logger.Infof("Gate records flushed to DB")
    }
}
```

---

## 四、pkg/sync/db 包：需要补充的函数

现有 `db.go` 或 `mysql.go` 中，需要新增一个构造 `DbGateService` 的函数，复用现有的数据库连接池。

示例（在 `pkg/sync/db/mysql.go` 或新文件中）：

```go
// NewGateService 从 DbConfig 创建 DbGateService 实现。
// 复用现有的 MySQL 连接，但使用新的表 sync_records_v2。
func NewGateService(cfg *DbConfig) (DbGateService, error) {
    // 复用现有的 gorm 初始化逻辑
    db, err := gorm.Open(mysql.Open(fmt.Sprintf("%s:%s@tcp(%s)/...", cfg.User, cfg.Pass, cfg.Host)), &gorm.Config{})
    if err != nil {
        return nil, err
    }
    return NewGormGateService(db)
}
```

---

## 五、修改后的流程图（带 gate 的完整 produce）

```
listAll(src) ──► for obj := range srckeys
                    │
                    ▼
            ┌───────────────┐
            │ 第一层门卫    │  ← 查 DB (GetRecord)
            │ firstGate()   │
            └───────┬───────┘
                    │
        ┌───────────┼───────────┐
        │           │           │
    GateSkip   GateMissing   NeedSecondGate
        │           │           │
        ▼           │           ▼
    skipIt()      │     ┌───────────────┐
    continue      │     │ 第二阶段：    │  ← 目标端 Head
                  │     │ 第二层门卫    │
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
                  │
                  └────► (DB 不存在时，直接进入第二层)
```

---

## 六、兼容性说明

| 场景 | 行为 | TargetSize 记录 |
|------|------|----------------|
| 未配置 `--db` | `gateSvc == nil`，所有对象直接走原有第二层逻辑，无行为变化 | 无记录 |
| 配置了 `--db` 但 DB 是空的 | 所有对象 `GateMissing`，走原有第二层，sync 完成后 DB 逐渐填满 | 首次同步 target_size=0 |
| 配置了 `--db` 且 DB 有记录 | 命中第一层，大部分对象直接跳过，仅变化对象进入第二层 | 跳过时不记录（第一层直接跳），send 覆盖后 target_size=0 |
| `--force-update` | 第一层直接 `GateNeedSecondGate`，所有对象重新进入第二层 | 覆盖后 target_size=0 |
| `--scan` / `--scan-single` | 不启用 gate（`!config.Scan && !config.ScanSingle`），纯扫描模式行为不变 | 无记录 |
| 进程中途被 kill | 已成功的对象已写入 gateBuf（最多延迟 1s + 500 条缓冲），下次 sync 第一层能识别 | 可能丢失最近 500 条 |
| 目标端被外部修改 | 第二层 `secondGate` 发现目标 mtime 更新 → 跳过，保护目标端数据 | **记录 target_size，可用于诊断** |
| 目标端存在但 size 不同 | 第二层只对比 mtime，send 或 skip 不取决于 size | **skip 时记录 target_size，SQL 可查出 size 不一致** |

---

## 七、待办清单

- [ ] 在 `pkg/sync/db` 中实现 `NewGateService()` 构造函数
- [ ] 在 `pkg/sync/db` 中实现 `GormGateService`（或复用现有 SQL 连接）
- [ ] 修改 `cmd/sync.go`：确认 `--db` 参数在 sync 模式下自动触发 gate
- [ ] 修改 `pkg/sync/sync.go`：
  - [ ] `Sync()` 中初始化 `gateSvc` + `gateBuf`
  - [ ] `produce()` 中插入 `firstGate` 检查
  - [ ] `produce()` 中可选替换原有 Head 对比为 `secondGate`
  - [ ] `produce()` 中 skip 时调用 `RecordSkip(gateBuf, obj, targetSize)`
  - [ ] `worker()` 中成功/失败时调用 `RecordSuccess(gateBuf, obj, 0)` / `RecordFailure(gateBuf, obj, err, 0)`
  - [ ] `Sync()` 结尾 flush `gateBuf`
- [ ] 测试：首次 sync（DB 空）→ 第二次 sync（DB 命中）→ 修改目标端 → 第三次 sync（保护目标端 + 诊断 SQL）
- [ ] 编写诊断 SQL：查询 `status='skipped' AND target_size != 0 AND source_size != target_size`