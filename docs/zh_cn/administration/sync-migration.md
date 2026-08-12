# JuiceFS Sync 迁移逻辑与实现说明

> 本文基于当前工作区代码整理，重点描述本分支增强后的 `juicefs sync`：两层 gate、MySQL 记录、checkpoint、scan/scan-single、fix-meta 与重试语义。上游 JuiceFS 的通用 sync 文档仍适用，但本文以当前代码为准。

## 1. 迁移逻辑图

```mermaid
flowchart TD
    A["juicefs sync SRC DST"] --> B["解析 flags / selection rules<br/>cmd/sync.go"]
    B --> C["创建 src/dst ObjectStorage<br/>并设置 concurrent / limiter"]
    C --> D{模式分支}

    D -- "--scan-single" --> SS["只 List 源桶<br/>记录 key/size/mtime/storage_class<br/>到 single_scan 或 CSV"]
    D -- "--scan" --> SC["流式对比 src/dst<br/>记录 matches/differs/missing/extra<br/>到 scan_sync 或 CSV"]
    D -- "--fix-meta" --> FM["遍历 src<br/>同 size 且 Content-Type 不同<br/>用 Get + PutWithMeta 修 dst metadata"]
    D -- 正常同步 --> N["正常迁移流程"]

    N --> DB{"配置了 --db ?"}
    DB -- 是 --> DB1["启动 AsyncDbService<br/>创建 sync_jobs / juicefs_sync 表"]
    DB1 --> GT["初始化两层 gate<br/>表: --gate-table 或 sync_records_v2_md5(src|dst)[:8]"]
    DB -- 否 --> CK
    GT --> CK{"EnableCheckpoint ?"}
    CK -- 是 --> CK1["加载 checkpoint<br/>恢复 PrefixState / pending / failed / multipart / delay-del"]
    CK -- 否 --> L
    CK1 --> L["并发 List src 与 dst"]

    L --> P["produce 归并 srckeys/dstkeys<br/>按 key 字典序对齐"]
    P --> F{"命中过滤规则 ?<br/>include/exclude/size/age/time"}
    F -- 是 --> X["excluded++"]
    F -- 否 --> AL["src/dst key 字典序对齐<br/>先处理 dst extra / delete-dst"]
    AL --> G1{"第一层 gate<br/>status=success 且 src.mtime 未变 ?"}
    G1 -- 是 --> SKIP1["skipIt<br/>零 dst Head 调用<br/>若 key 相等则消费 dst 游标"]
    G1 -- 否/无记录/失败/skipped/mtime 变化 --> CMP{"dst 同 key 存在 ?"}

    CMP -- 不存在 --> SEND["sendTask<br/>新对象 copy"]
    CMP -- 存在 --> G2{"第二层 gate<br/>实时比较 src/dst"}
    G2 -- "force-update" --> SEND
    G2 -- "dst.mtime >= src.mtime" --> SKIP2["ActionSkip<br/>保护 dst 新数据<br/>RecordSkip(diff=size 不等)"]
    G2 -- "dst.mtime < src.mtime" --> SEND

    SEND --> W["worker 执行任务<br/>copy / checksum / perms / delete-src 标记"]
    W --> R{结果}
    R -- 成功 --> RS["copied++<br/>RecordSuccess -> gate buffer<br/>写 sync object record"]
    R -- 校验相等跳过 --> RK["skipped++"]
    R -- 失败 --> RF["failed++<br/>RecordFailure -> gate buffer<br/>达到 MaxFailure 则退出"]

    P --> ED{"--delete-dst 且 dst 多出对象 ?"}
    ED -- 是 --> DELD["删除 dst extra<br/>目录进入 dstDelayDel"]
    W --> DS{"--delete-src / --delete-src-after ?"}
    DS -- 是 --> DELS["删除 src 已处理对象<br/>目录进入 srcDelayDel"]

    RS --> END
    RK --> END
    RF --> END
    SKIP1 --> END
    SKIP2 --> END
    DELD --> END
    DELS --> END

    END["flush progress / checkpoint / gate buffer / async db"] --> SUM["输出统计<br/>EndJob(completed/failed)"]
    SUM --> DC{"--double-check ?"}
    DC -- 是 --> DCP["第二遍查漏<br/>补拷迁移期间新增/变化对象"]
    DC -- 否 --> DONE["退出"]
    DCP --> DONE
```

## 2. 核心流程

入口是 `cmd/sync.go:doSync` 与 `pkg/sync/sync.go:Sync`。命令行解析后创建源/目的 `object.ObjectStorage`，再根据模式进入正常同步、`--scan`、`--scan-single` 或 `--fix-meta`。

正常同步的主线是：

1. **初始化输出与 DB**：`--output` 创建 CSV；`--db` 解析 DSN，创建 `AsyncDbService`。普通同步使用 `sync_jobs` 和 `juicefs_sync.objects_<job_id>`。
2. **初始化 gate**：只要配置了 `--db` 且不是 scan/scan-single，就启用两层 gate。表名来自 `--gate-table`，否则用 `sync_records_v2_ + md5(src|dst)[:8]`。
3. **恢复 checkpoint**：`--checkpoint` 开启后，会从目的端 checkpoint 对象恢复每个 prefix 的 `LastListedKey`、pending/failed keys、multipart 状态、delay-delete 列表。
4. **双边 List 归并**：`produce` 同时消费 `srckeys` 和 `dstkeys`，按 key 字典序对齐；在 gate 判断之前先处理 `dst` 多出的 extra 对象，保证 `--delete-dst` 只删“源没有”的 key。
5. **第一层 gate**：命中 DB 成功记录且源 mtime 未变，直接 skip，不访问目标端 Head；若当前 key 与 `dstobj` 相等，会同时消费 dst 游标，避免把已同步对象误判成 extra。
6. **第二层 gate**：目标端同 key 存在时实时裁决：目标不存在或 `--force-update` 发送；`dst.mtime >= src.mtime` 跳过以保护目标端新数据；否则发送。
7. **worker 执行**：复制、校验、权限同步、删除源/目标、写 DB 记录和 gate 记录。
8. **收尾**：flush progress、checkpoint、gate buffer、async DB，更新 job 状态；可选 `--double-check` 再跑一遍查漏。

## 3. 两层 gate 语义

代码在 `pkg/sync/gate.go` 与 `pkg/sync/sync.go:produce`。

- **第一层：DB 快速门卫**：`firstGate(obj, record, forceUpdate)`。
  - `record == nil`：缺失，进入第二层。
  - `status != success`：上次失败/中断/被跳过，进入第二层。
  - `status == success && !obj.Mtime().After(record.SourceMtime)`：直接 `GateSkip`，零目标端 API 调用。
  - 源 mtime 变新：进入第二层，让目标端实时状态裁决。
- **第二层：目标端实时门卫**：`secondGate(obj, dstObj, forceUpdate)`。
  - 目标不存在：发送。
  - `--force-update`：发送并覆盖。
  - `dst.mtime >= src.mtime`：跳过，保护目标端被应用新写入的数据。
  - `dst.mtime < src.mtime`：发送。

写入侧：`RecordSuccess/RecordFailure/RecordSkip` 先进 `GateRecordBuffer`，默认 500 条或 1 秒批量 upsert 到 gate 表；正常退出、信号退出都会 flush。`RecordSkip` 只记录诊断信息，`diff=true` 表示目标端存在且 size 与源不一致。

按当前确认语义，gate 表被视为迁移事实来源：如果对象已经成功同步且源 mtime 未变，即使目标端之后被外部删除或改坏，后续增量同步也会直接跳过，不会自愈。需要强制重建时使用 `--force-update` 或清空/更换 gate 表。

## 4. 模式说明

- **正常同步**：默认模式。比较逻辑受 `--update`、`--force-update`、`--existing`、`--ignore-existing`、`--check-*`、`--perms`、`--delete-*` 影响；启用 gate 后，同 key 是否发送由两层 gate 接管。
- **`--scan`**：只对比不复制。源侧流式 List，目标侧 Head 判断 `matches/differs/missing`；第二阶段 List 目标侧找 `extra`。结果写 `scan_jobs`/`scan_sync` 或 CSV。List 结果缺少 Content-Type/metadata 时会 fallback 到源 Head。
- **`--scan-single`**：只扫描单个源桶，不访问目标；通过 ListObjects 记录 key/size/mtime/storage_class 到 `single_scan_jobs`/`single_scan` 或 CSV。
- **`--fix-meta`**：只修同 size 对象的 Content-Type/metadata；在正常同步 pipeline 启动前直接返回，不复制数据；不依赖 S3 self-copy，统一走 `Get` + `PutWithMeta`，避免 COPY directive 造成的假成功。
- **`--double-check`**：正常同步结束后再做一遍查漏，捕捉迁移期间新增或变化的对象。

## 5. 数据表

- 普通同步 job：`sync_jobs.sync_jobs`；对象明细：`juicefs_sync.objects_<job_id>`。job ID 时间戳精确到微秒并对超长 bucket/path 做哈希截断，避免同分钟冲突或 MySQL 表名超长；`StartJob` 失败会直接返回错误，不再继续跑然后静默丢明细。
  - 默认只写入 `copied` 和 `failed` 两种状态的明细，`skipped` 不写入，可显著降低 `--ignore-existing` 场景下的 MySQL 压力。通过 `--db-record-status` 可调整，例如 `--db-record-status=copied,skipped,failed,deleted` 恢复旧行为。
- scan job：`scan_jobs.sync_jobs`；明细：`scan_sync.objects_<job_id>`。
- scan-single job：`single_scan_jobs.sync_jobs`；明细：`single_scan.scan_<job_id>`。
- gate：`NewGateService` 使用 `--db` DSN path 指定的数据库，表为 `--gate-table` 或 `sync_records_v2_<hash>`。因此 DSN 需要带 database，例如 `mysql://user:pass@host:3306/juicefs_gate`。

gate 表关键字段：`id`（自增主键）、`key_hash`（key 的 MD5，唯一索引，用于查询与去重）、`key`、`source_mtime`、`source_size`、`target_size`、`diff`、`status(success/failed/skipped)`、`error_msg`、`updated_at`。

> 注意：自增主键 + `key_hash` 唯一索引的表结构兼容老版本 MySQL（767 字节索引上限）。
> 如果 gate 表是早期版本创建的（以 `key` 为主键），需要手动 `DROP TABLE` 后重新运行，程序检测到旧结构时会提示并回退 no gate。

## 6. checkpoint、信号与失败控制

- checkpoint 保存前会对 `PrefixState`、multipart uploads、delay-delete 列表做深拷贝快照，避免和 producer/worker 并发读写 `json.Marshal` 竞争。
- SIGINT/SIGTERM 会保存 checkpoint，并 flush `syncDbService`、`gateBuf`、CSV，然后退出。
- `--max-failure` 达到阈值会执行收尾并退出；普通失败会写 DB/gate 失败记录，供下一次增量重试。
- `--limit` 使用原子计数，多 producer/worker 并发下不会超扣。

## 7. 使用建议

- 迁移期应用可能切写到目标端：开启 `--db`，依赖第二层 gate 的 `dst.mtime >= src.mtime` 保护。
- 需要强制以源为准覆盖目标：使用 `--force-update`，或清理对应 gate 表后重跑。
- 只想盘点单个桶：用 `--scan-single`；想做源目对比审计：用 `--scan`。
- 跨多次增量同步/多 worker 共享 gate 状态：显式指定同一个 `--gate-table`。
- 大对象 server-side copy 场景要注意当前 `doCopyRange` 会按 range 内 part 并发上传，生产上建议结合 `--concurrent`、带宽限制和对象存储限流观察压力。
