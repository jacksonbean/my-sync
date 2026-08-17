# JuiceFS Sync —— 对象存储同步工具

高性能、多线程的对象存储同步工具，用 Go 编写。支持 40 种存储后端，包括 S3、OSS、Azure Blob、GCS、MinIO、本地文件系统、HDFS、NFS、Ceph 等。

> 本仓库是 JuiceFS 的 **sync-only fork**：只保留 `juicefs sync` 一个命令，
> 不含 `format` / `mount` / `gateway` 等上游功能，并在上游 sync 基础上增强了：
> MySQL 结果记录（`--db`）、两层 gate 增量优化（`--gate-table`）、
> 断点续传（checkpoint）、`--scan` / `--scan-single` 对比扫描、
> `--fix-meta` 元数据修复、`--double-check` 二次查漏等。

## 目录

- [快速开始](#快速开始)
- [前置条件](#前置条件)
- [核心概念](#核心概念)
- [常用场景](#常用场景)
- [完整参数参考](#完整参数参考)
- [MySQL 数据表详解](#mysql-数据表详解)
- [checkpoint、信号与失败控制](#checkpoint信号与失败控制)
- [使用建议](#使用建议)
- [与上游 JuiceFS 的差异](#与上游-juicefs-的差异)
- [已知限制](#已知限制)
- [编译与测试](#编译与测试)
- [开发记录](#开发记录)

## 快速开始

```bash
# 编译
make juicefs

# 全量同步（S3 → MinIO）
juicefs sync s3://mybucket s3://backup

# 增量同步（目标更新的跳过；配合 --db 走两层 gate）
juicefs sync --db mysql://user:pass@host:3306/juicefs_gate s3://mybucket s3://backup

# 只对比不复制（输出 CSV）
juicefs sync --scan --output result.csv s3://mybucket s3://backup

# 单桶盘点（只 List 源桶，输出 CSV）
juicefs sync --scan-single --output inventory.csv s3://mybucket
```

## 前置条件

- **存储 URI**：`[NAME]://[ACCESS_KEY:SECRET_KEY[:TOKEN]@]BUCKET[.ENDPOINT][/PREFIX]`，
  凭证也可通过环境变量传入（如 `ACCESS_KEY` / `SECRET_KEY`，不同后端变量名见 `pkg/sync/config.go` 的 `envList()`）。
- **MySQL（可选）**：使用 `--db` 记录结果时需可连通的 MySQL；DSN 格式
  `mysql://user:pass@host:3306/dbname`。启用 gate 时 **DSN 必须带 database**，
  因为 gate 表建在 DSN path 指定的库中。
- **`--scan` / `--scan-single` 必须搭配 `--db` 或 `--output`**，否则直接报错退出。
- **checkpoint**：启用 `--enable-checkpoint` 后，断点以 JSON 文件形式保存；
  默认存放在**目标端**对象存储（`.juicefs-sync-checkpoint.<md5>.json`，完成后自动删除），
  单机场景可用 `--checkpoint-file DIR` 改存本地目录，避免在目标桶产生临时对象。

## 核心概念

### 迁移逻辑总览

```mermaid
flowchart TD
    A["juicefs sync SRC DST"] --> B["解析 flags / selection rules<br/>cmd/sync.go"]
    B --> C["创建 src/dst ObjectStorage<br/>并设置 concurrent / limiter"]
    C --> D{模式分支}

    D -- "--scan-single" --> SS["只 List 源桶<br/>记录 key/size/storage_class<br/>到 single_scan 或 CSV"]
    D -- "--scan" --> SC["src/dst 双 List 归并对比<br/>零 Head 调用<br/>记录 matches/differs/missing/extra<br/>到 scan_sync 或 CSV"]
    D -- "--fix-meta" --> FM["遍历 src<br/>同 size 且 Content-Type 不同<br/>用 Get + PutWithMeta 修 dst metadata"]
    D -- 正常同步 --> N["正常迁移流程"]

    N --> DB{"配置了 --db ?"}
    DB -- 是 --> DB1["启动 AsyncDbService<br/>创建 sync_jobs / juicefs_sync 库<br/>（含 sync_jobs 表与 objects_* 明细表）"]
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

### 数据流

```
                 ┌──────────────┐
                 │   ListAll    │  ← 每个前缀一个 goroutine
                 └──────┬───────┘
                        │ object.Object 通道
                        ▼
             ┌──────────────────┐
             │     生产者       │  ← 比较源/目标、过滤、排序、gate 决策
             │  (produce/tasks) │
             └──────┬──────────┘
                    │ object.Object 通道（缓冲 10240）
                    ▼
        ┌───────────────────────┐
        │  工作池               │  ← config.Threads 个 goroutine
        │  copyData / checkSum  │
        │  deleteObj / copyPerms│
        └───────────────────────┘
```

### 默认对比决策矩阵（未启用 gate 时）

| 条件 | 动作 |
|------|------|
| 目标端不存在 | 复制（`--existing` 时跳过） |
| 大小相同 | 跳过（除非 `--force-update` 或 `--check-change`） |
| 大小不同 | 复制 |
| 目标更新 | 跳过（`--update` 时；`--force-update` 则覆盖） |
| 目标端多余 + `--delete-dst` | 删除（在 key 对齐后判定） |
| 目标端多余 + 无 `--delete-dst` | 跳过（记入 "extra"） |

### 两层 gate（增量优化）

配置 `--db`（且非 scan 模式）时自动启用。代码在 `pkg/sync/gate.go` 与 `pkg/sync/sync.go` 的 `produce()`。

**第一层（DB 快速门卫）** `firstGate(obj, record, forceUpdate)`：

| DB 记录状态 | 决策 |
|------------|------|
| 记录缺失 | 进入第二层 |
| `status != success`（上次失败/中断/被跳过） | 进入第二层 |
| `status == success` 且源 mtime 未变（按秒精度比较） | 直接 `GateSkip`，**零目标端 API 调用** |
| `status == success` 但源 mtime 变新 | 进入第二层 |

查询由 `prefetchGateRecords` 批量预取（每批 500 个 key 一次 IN 查询，50ms 出批），
避免逐对象 SELECT 的 N+1 开销，且 DB 查询与 listing 并行。

**第二层（目标端实时门卫）** `secondGate(obj, dstObj, forceUpdate)`：

| 条件 | 决策 |
|------|------|
| 目标端不存在 | 发送 |
| `--force-update` | 发送并覆盖 |
| `dst.mtime >= src.mtime` | 跳过，保护目标端被应用新写入的数据 |
| `dst.mtime < src.mtime` | 发送 |

注意：第二层只对比 mtime 不对比 size（启用 gate 后同 key 决策由两层 gate 接管，
`--update` 不再参与）。写入侧 `RecordSuccess/RecordFailure/RecordSkip` 先进入
`GateRecordBuffer`（默认 500 条或 1 秒批量 UPSERT 到 gate 表），正常退出、信号退出都会 flush。
`RecordSkip` 只记录诊断信息：`diff=true` 表示目标端存在且 size 与源不一致。

**关键语义**：gate 表视为迁移事实来源——成功同步且源 mtime 未变的对象，即使目标端
后来被外部删除或改坏，后续增量同步也会直接跳过、不会自愈。需要强制重建时使用
`--force-update`，或清空/更换 gate 表。多 worker / 多次增量共享状态时，显式指定同一个
`--gate-table`。

**gate 表结构**（`sync_records_v2_<hash>`）：`id`（自增主键）、`key_hash`（key 的 MD5，
唯一索引，用于查询与去重）、`key`、`source_mtime`（DATETIME，秒级精度，为兼容老 MySQL
不保留毫秒）、`source_size`、`target_size`、`diff`、`status(success/failed/skipped)`、
`error_msg`、`updated_at`。自增主键 + `key_hash` 唯一索引兼容老版本 MySQL 的 767 字节
索引上限；若 gate 表是早期版本创建的（以 `key` 为主键），需手动 `DROP TABLE` 后重新运行，
程序检测到旧结构时会提示并回退 no gate。

### 四种持久化机制对照

| 机制 | 存到哪里 | 粒度 | 生命周期 | 用途 |
|------|----------|------|----------|------|
| gate 表 | MySQL（`--gate-table` 或 `sync_records_v2_<hash>`） | 每 key 一条（UPSERT） | 跨多次运行复用 | 增量跳过决策（事实来源） |
| job 明细表 | MySQL（`<jobs_db>.sync_jobs` + `<data_db>.objects_<job_id>`） | 每对象一行 | 每次运行一个 job | 迁移结果审计/统计 |
| checkpoint | 目标端对象存储 JSON 文件 | 前缀进度 + pending/failed/multipart | 单次运行（完成后删除） | 断点续传 |
| CSV | `--output` 指定的本地文件 | 每对象一行 | 单次运行 | scan 结果导出/审计 |

三种运行模式对应三组独立数据库：`sync_jobs` + `juicefs_sync`、`scan_jobs` + `scan_sync`、
`single_scan_jobs` + `single_scan`。明细表默认只记录 `copied` / `failed` 两种状态
（`--db-record-status` 可调整，该参数只作用于正常同步；scan 模式始终记录全部状态）。

### 运行模式

| 模式 | 行为 | 访问目标端 |
|------|------|-----------|
| 正常同步（默认） | 复制/删除/校验/权限 | 是 |
| `--scan` | src/dst 双 List 归并对比（零 Head 调用），输出 matches/differs/missing/extra | 只 List |
| `--scan-single` | 只 List 源桶，记录 key/size/storage_class | 否 |
| `--fix-meta` | 同 size 对象修复 Content-Type/metadata（`Get` + `PutWithMeta`），不复制数据 | 是 |

## 常用场景

1. **全量迁移 + 定期增量**

   ```bash
   juicefs sync --db mysql://user:pass@host:3306/juicefs_sync s3://src/ s3://dst/
   ```

   后续重跑同一命令即可增量：第一层 gate 命中直接跳过已同步对象。

   超大规模（几十亿对象）且**只补新文件、不更新已有文件**的增量迁移，
   用 `--ignore-existing` + 本地断点，例如：

   ```bash
   juicefs sync \
     --db mysql://user:pass@host:3306/juicefs_sync \
     --ignore-existing \
     --enable-checkpoint \
     --checkpoint-file /data/jfs-checkpoint \
     --checkpoint-interval 30s \
     --threads 20 --list-threads 5 \
     s3://src-bucket/ s3://dst-bucket/
   ```

   中断后重跑同一命令从本地断点续传；扫描/复制统计查 `sync_jobs.sync_jobs` 汇总行
   （`total_objects` 为扫描数，`copied_objects` 为复制数，明细表只记 copied/failed）。

   nohup 后台运行时同样能看到进度：无 TTY 时每 10 秒向 stderr 输出一行
   `Progress: scanned=... copied=... skipped=...`（在 nohup.out 里，`tail -f nohup.out` 可看），
   同时 `sync_jobs` 汇总行每 10 秒更新实时计数，可随时查库。

2. **迁移期间应用已切写到目标端（双写保护）**

   目标端 mtime 更新的对象会被第二层 gate 跳过，避免覆盖应用新写入的数据；
   需要强制以源为准时加 `--force-update`。

3. **数据盘点与审计**

   ```bash
   juicefs sync --scan --output audit.csv s3://src/ s3://dst/   # 源目对比
   juicefs sync --scan-single --output inventory.csv s3://src/  # 单桶清单
   ```

   只扫某个前缀（字符串前缀匹配，如 `[a_atta` 开头的所有 key）并保留完整 key：

   ```bash
   # 不加尾斜杠 = 字符串前缀匹配：[a_attaXYZ、[a_atta/foo 都会扫到
   juicefs sync --scan-single --full-key --output atta.csv 's3://mybucket/[a_atta'
   ```

   `--full-key` 时 CSV/DB 记录完整 key；默认记录去掉前缀后的相对 key。

4. **修复 Content-Type / 元数据**

   ```bash
   juicefs sync --fix-meta s3://src/ s3://dst/
   ```

   只处理同 size 对象，不依赖 S3 self-copy（避免 COPY 假成功），不复制数据。

5. **多节点分布式同步**

   ```bash
   # 管理节点
   juicefs sync --worker host1,host2 s3://src/ s3://dst/
   ```

6. **加密迁移**

   ```bash
   juicefs sync --encrypt-rsa-key my.pem s3://src/ s3://dst/        # 目标端加密
   juicefs sync --decrypt-rsa-key my.pem s3://src/ s3://dst/        # 源端解密
   ```

   支持 `aes256gcm-rsa`（默认）、`chacha20-rsa`、`sm4gcm`（`--encrypt-algo` / `--decrypt-algo`）。

## 完整参数参考

### 选择过滤（SELECTION）

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--start` / `-s` | — | 起始 key |
| `--end` / `-e` | — | 结束 key |
| `--include` / `--exclude` | — | 基于模式的过滤（可多值） |
| `--match-full-path` | false | 按完整路径匹配过滤规则 |
| `--max-size` / `--min-size` | — | 按大小过滤 |
| `--max-age` / `--min-age` | — | 按对象年龄过滤 |
| `--start-time` / `--end-time` | — | 按修改时间过滤（`2006-01-02 15:04:05`） |
| `--limit` | -1 | 最多处理的对象数（-1 不限，0 不处理） |
| `--update` / `-u` | false | 目标更新则跳过 |
| `--force-update` / `-f` | false | 总是覆盖已有文件 |
| `--existing` | false | 跳过目标端不存在的新文件 |
| `--ignore-existing` | false | 跳过目标端已存在的文件 |
| `--files-from` | — | 从文件读取要同步的 key 列表 |
| `--style` | 自动 | S3 寻址风格：path 或 virtual-host |

### 同步动作（ACTION）

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--dirs` | false | 同步目录条目 |
| `--perms` | false | 保留文件权限 |
| `--preserve-meta` | false | 保留 Content-Type 与用户自定义元数据 |
| `--links` / `-l` | false | 符号链接按符号链接复制 |
| `--inplace` | false | 直接写目标文件（跳过临时文件 + rename） |
| `--delete-src` | false | 删除目标端已存在的源对象 |
| `--delete-src-after` | false | 处理成功后删除源对象 |
| `--delete-dst` | false | 删除目标端多余对象 |
| `--check-all` | false | CRC32C 校验所有文件完整性 |
| `--check-new` | false | 校验新复制文件的完整性 |
| `--check-change` | false | 校验源文件同步期间是否变化 |
| `--max-failure` | -1 | 失败 N 次后中止（-1 不限） |
| `--dry` | false | 试运行，不复制 |
| `--enable-checkpoint` | false | 启用断点续传 |
| `--checkpoint-force-reset` | false | 从头开始并覆盖已有断点 |
| `--checkpoint-interval` | 10s | 断点保存间隔（每隔 N 秒把当前进度快照保存一次） |
| `--checkpoint-file` | — | 把断点存到本地目录 `DIR` 而不是目标桶（单机模式，需配合 `--enable-checkpoint`） |
| `--mountpoint` | — | 卷挂载点（用于跟随符号链接） |

### 数据库与扫描（DB / SCAN）

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--db` | — | MySQL 连接串，记录同步结果；启用 gate 时 DSN 需带 database |
| `--db-record-status` | copied,failed | 明细表只记录指定状态（只作用于正常同步） |
| `--gate-table` | 自动生成 | 两层 gate 记录表名；多 worker / 多次增量共享时显式指定 |
| `--scan` | false | 对比模式（双 List 归并，零 Head 调用） |
| `--scan-single` | false | 单桶扫描模式（只 List 源桶） |
| `--double-check` | false | 同步结束后二次查漏 |
| `--fix-meta` | false | 修复同 size 对象的 Content-Type/metadata，不复制数据 |
| `--output` | — | scan 结果导出 CSV（配合 `--scan` / `--scan-single`） |
| `--full-key` | false | `--scan-single` 结果记录完整 key（含 URL 前缀），默认记录相对前缀的 key |

### 存储与性能（STORAGE）

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--threads` / `-p` | 10 | 并发线程数 |
| `--list-threads` | 1 | 并行 List 线程数 |
| `--list-depth` | 1 | 并行 List 的目录层级 |
| `--bwlimit` | 0 | 带宽上限（Mbps，0 不限） |
| `--traffic-control-url` | — | 全局流量控制服务地址 |
| `--storage-class` | — | 目标端存储类 |
| `--no-https` | false | 不使用 HTTPS |
| `--encrypt-rsa-key` / `--decrypt-rsa-key` | — | 目标端加密 / 源端解密的 RSA/SM2 私钥（PEM） |
| `--encrypt-algo` / `--decrypt-algo` | aes256gcm-rsa | 加密算法：`aes256gcm-rsa` / `chacha20-rsa` / `sm4gcm` |

### 集群与可观测（CLUSTER / METRICS）

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--worker` | — | 工作节点主机列表（逗号分隔） |
| `--manager` | — | 管理节点地址（worker 节点使用） |
| `--manager-addr` | — | 与 worker 通信的本机地址 |
| `--metrics` | — | Prometheus 指标监听地址（如 `127.0.0.1:9567`） |
| `--consul` | — | Consul 注册地址 |

## MySQL 数据表详解

三种运行模式对应三组独立数据库（每组含 job 表 + 对象明细表）：

| 模式 | job 表 | 明细表 |
|------|--------|--------|
| 正常同步 | `sync_jobs.sync_jobs` | `juicefs_sync.objects_<job_id>` |
| `--scan` | `scan_jobs.sync_jobs` | `scan_sync.objects_<job_id>` |
| `--scan-single` | `single_scan_jobs.sync_jobs` | `single_scan.scan_<job_id>` |

- job ID 形如 `<dst_bucket>_<微秒级时间戳>`，超长 bucket/path 会做哈希截断，
  避免同分钟冲突或 MySQL 表名超长（64 字符限制）。
- `StartJob` 失败会**直接返回错误**，不会继续跑然后静默丢明细。
- **运行中可查实时进度**：同步期间每 10 秒会把当前扫描/复制/跳过/失败计数原位更新到
  `sync_jobs.sync_jobs` 汇总行（只改计数，不动 status/end_time），后台运行（nohup）时
  可随时 `SELECT total_objects, copied_objects, skipped_objects FROM sync_jobs.sync_jobs WHERE id='<job_id>'`；
  结束时的 `EndJob` 会写最终值。
- 明细表默认只写入 `copied` 和 `failed` 两种状态，`skipped` 不写入，可显著降低
  `--ignore-existing` 场景下的 MySQL 压力；`--db-record-status` 可调整
  （如 `--db-record-status=copied,skipped,failed,deleted` 恢复旧行为）。
  该参数只作用于正常同步路径；scan/scan-single 模式始终记录全部扫描状态
  （missing/differs/matches/extra），不受过滤。
- gate 表与 job 表的区别：gate 表跨运行复用（UPSERT 每 key 一行），job 明细表每次运行新建。
- gate 表不自动清理；对象明细写入走 `AsyncDbService`（非阻塞 channel + 批量事务 INSERT，
  每 2000 条一批、500 条一个 Exec，flush 失败保留 batch 重试、3 次失败丢弃并告警）。

## checkpoint、信号与失败控制

- **断点续传**（`--enable-checkpoint`）：`CheckpointManager` 把每个 prefix 的
  `LastListedKey`、pending/failed keys、multipart 上传状态、delay-delete 列表存成 JSON。
  默认写入**目标端**对象存储（`.juicefs-sync-checkpoint.<md5>.json`，正常完成后删除）；
  单机迁移建议加 `--checkpoint-file /path/to/dir` 改存本地（原子写：临时文件 + rename），
  换机续跑则必须用目标端存储。配置哈希确保断点与配置匹配，不匹配则从头开始。
- **`--checkpoint-interval`（默认 10s）**：每隔该时长把当前进度快照**覆盖保存**一次，
  信号到达时也会立即保存一次。间隔越大，保存开销（目标端 PUT 或本地磁盘写）越小，
  但中断时最多丢失最近一个间隔的进度；40 亿级同步建议 30s~60s。
- **checkpoint 文件大小与对象总数无关**：只记录在途任务（≈ tasks 缓冲 + 线程数）、
  失败对象、分片上传状态和延迟删除列表，稳态通常几 MB；只有失败对象大量累积
  （`--max-failure` 不限时）或同时在传的超大文件很多时才会到几十 MB。
- checkpoint 保存前对 `PrefixState`、multipart uploads、delay-delete 列表做深拷贝快照，
  避免与 producer/worker 并发读写竞争。
- **信号处理**：SIGINT/SIGTERM 会保存 checkpoint、flush `syncDbService`/`gateBuf`/CSV、
  标记 job failed（`EndJob`），并以 `128+信号值` 退出码退出（SIGINT→130，SIGTERM→143），
  便于外层脚本/编排系统识别「被中断」。
- **失败控制**：`--max-failure` 达到阈值执行收尾并退出；普通失败写 DB/gate 失败记录，
  供下一次增量重试；`--limit` 使用原子计数，多 producer/worker 并发下不会超扣。
- 同步结束存在失败或 lost 对象（已扫描但未被处理）时，job 标记为 `failed`，
  退出码非零并保留 checkpoint 供下次续传。

## 使用建议

- **迁移期应用可能切写到目标端**：开启 `--db`，依赖第二层 gate 的
  `dst.mtime >= src.mtime` 保护目标端新数据。
- **需要强制以源为准覆盖目标**：使用 `--force-update`，或清理对应 gate 表后重跑。
- **只想盘点单个桶**：用 `--scan-single`；**想做源目对比审计**：用 `--scan`。
- **跨多次增量同步 / 多 worker 共享 gate 状态**：显式指定同一个 `--gate-table`。
- **大对象 server-side copy**：`doCopyRange` 会按 range 内 part 并发上传，生产上建议
  结合 `--threads`、带宽限制和对象存储限流观察压力。

## 与上游 JuiceFS 的差异

本 fork 在 94f8fc9（`juicefs sync: preserve Content-Type and user-defined metadata`）基础上持续增强：

- **MySQL 结果记录**：`--db` / `--db-record-status`，三组独立 job 数据库
  （`sync_jobs`、`scan_jobs`、`single_scan_jobs`），每 job 独立明细表。
- **两层 gate**：`--gate-table`，DB 快速门卫 + 目标端实时门卫，增量同步大幅减少 Head 调用。
- **`--scan` 零 Head 对比**：双 List 归并，不再逐对象 Head。
- **`--scan-single`**：单桶盘点，仅 ListObjects。
- **`--fix-meta`**：同 size 元数据修复（`Get` + `PutWithMeta`，不依赖 S3 self-copy）。
- **`--double-check`**：同步后二次查漏。
- 其他：`--preserve-meta`、`--style`、加密算法完善、`--db-record-status` 状态过滤、
  老 MySQL 兼容表结构、panic-free 的 AsyncDbService 关闭等。

上游 JuiceFS 完整产品文档见 <https://juicefs.com/docs/zh/community/introduction>；
上游 sync 教程中的 jfs:// 等特性在本 fork 不适用，一切以本 README 与代码为准。

## 已知限制

- `encrypted.Get()` 将整个密文加载到内存后再解密。
- `chunkedEncrypted.ListAll` 在调用方提前停止读取时可能泄漏 goroutine。
- 全局计数器/状态使用包级别变量（同一进程内不支持多个并发 `Sync()` 调用）。
- gate 表不会自动清理；以 gate 表为事实来源意味着目标端被外部删除后增量同步不自愈
  （需要 `--force-update` 或重建 gate 表）。
- 被第二层 gate 跳过的对象（status=skipped）在后续增量中每轮都会重新走第二层裁决
  并重写 gate 记录（当前设计如此；`--ignore-existing` 模式不经过 gate，不受此影响）。

## 编译与测试

```bash
make juicefs           # 默认编译
make juicefs.lite      # 最小编译（不含 S3/OSS/HDFS 等）
make juicefs.linux     # 交叉编译 Linux（macOS 上）

make test.pkg          # 包测试
make test.cmd          # CLI 集成测试
```

## 开发记录

### 历史修复摘记（2026-06 审查）

- `listAll` 通道缓冲 1000 → `maxResults*3`（适配亿级 listing）
- `config.Limit--` data race → `atomic.AddInt64`
- `flushBatch` 失败丢数据 → 保留 batch 下次重试（3 次后丢弃告警）
- `--dry` + `--max-failure` panic → goroutine 加 `!config.Dry` 保护
- `chunkedEncrypted` metadata 丢失 → 实现 `MetadataPutter` 委托
- `fix-meta` 不再依赖 S3 self-copy（COPY 假成功）→ `Get` + `PutWithMeta`
- 日志泄露 Access Key → `maskStorageURL()` 掩码
- SSH `StrictHostKeyChecking=no` → `accept-new`
- gate 表改为自增主键 + `key_hash` 唯一索引（老 MySQL 767 字节兼容），
  第一层查询改为 500 key/批的 IN 批量预取
- AsyncDbService 关闭 panic 修复（done 信号代替 close(ch)）
- 2026-08 复审修复：dry-run 负 size 入库、lost 对象 job 状态、worker 进程
  EndJob、gate 秒级 mtime 截断、restful/qiniu/dragonfly 响应体泄漏、
  qingstor/ks3/ibmcos/nfs nil 解引用、lost 诊断 100 万 key 封顶、
  信号退出码 128+信号值 + 中断时 EndJob 等（详见提交历史）

### 未实现的功能提案

以下均为提案，**当前未实现**：

- `--verify`：迁移完成后抽样/全量校验 ETag/CRC32C，输出校验报告（复用 scan + checksum，成本低）
- `--notify-webhook`：开始/完成/失败时 POST JSON 到 webhook（耗时、对象数、字节数、错误摘要）
- `--schedule`：内置 cron 定时触发增量同步（建议 `github.com/robfig/cron`）
- `--storage-class-filter`：按存储类过滤
- `--log-format json`：结构化日志输出

### 待评估的优化项（2026-08 复审）

| # | 问题 | 建议 |
|---|------|------|
| 1 | gate 启用后同 key 决策改为纯 mtime 比较，内容变化但 mtime 未变的对象会被跳过 | 接受"mtime 为准"，或在 gate 分支补 size 校验 |
| 2 | 被第二层跳过的对象写 status=skipped，每轮增量重写 gate 记录（diff=false 时语义上已同步） | 可改 diff=false 的 skip 直接写 success，消除重复 UPSERT |
| 3 | AsyncDbService 与 gate 各自独立连接池（20+16） | gate 复用 mysqlService 的 `*sql.DB` |
| 4 | 批量 INSERT chunkSize=500 | 按 `max_allowed_packet` 提升到 1000-2000 减少往返 |
