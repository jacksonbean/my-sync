# JuiceFS Sync

高性能、多线程的对象存储同步工具，用 Go 编写。支持 40+ 存储后端，包括 S3、OSS、Azure Blob、GCS、MinIO、本地文件系统、HDFS、NFS、Ceph 等。

## 架构总览

```
┌─────────────────────────────────────────────────────┐
│                    juicefs sync                       │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │  列表遍历     │  │  生产者      │  │  工作池    │ │
│  │  (ListAll)   │─▶│  (task chan) │─▶│  Worker   │ │
│  └──────────────┘  └──────────────┘  └─────┬──────┘ │
│                                            │         │
│  ┌──────────────┐  ┌──────────────┐        │         │
│  │  断点续传     │  │  带宽限制    │        │         │
│  │  Checkpoint  │  │  Limiter    │        │         │
│  └──────────────┘  └──────────────┘        │         │
│                                            ▼         │
│  ┌──────────────────────────────────────────────────┐│
│  │  对象存储层 (pkg/object)                          ││
│  │  S3 · OSS · Azure · GCS · MinIO · File · HDFS   ││
│  │  + 加密 (AES-256-GCM / ChaCha20 / SM4)           ││
│  │  + 前缀 / 分片 / 缓存 包装层                      ││
│  └──────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────┘
```

## 模块说明

### `cmd/sync.go` — CLI 入口
- 解析 URI 格式的存储地址（`s3://ak:sk@bucket/path/`）
- 创建对应的存储后端、加密包装、前缀包装
- 初始化 Prometheus 指标和 Consul 注册
- 调用 `sync.Sync()` 执行同步

### `pkg/sync/` — 同步引擎

| 文件 | 职责 |
|------|------|
| `sync.go` | 核心同步循环：列表遍历 → 生产者 → 工作池，约 3100 行 |
| `config.go` | 从 CLI 参数解析 `Config`；`LimitDecr()` 原子递减辅助方法 |
| `cluster.go` | 管理/工作节点模式：基于 HTTP 的状态交换、任务分发 |
| `checkpoint.go` | `CheckpointManager`：将断点状态以 JSON 写入对象存储（定时 + 信号保存） |
| `download.go` | `parallelDownloader`：从源端并发分段下载 |
| `db/db.go` | `DbService` 接口 + `AsyncDbService`：非阻塞批量数据库写入 |
| `db/mysql.go` | MySQL 实现：每个任务独立建表，批量 INSERT |

#### 数据流

```
                 ┌──────────────┐
                 │   ListAll    │  ← 每个前缀一个 goroutine
                 └──────┬───────┘
                        │ object.Object 通道
                        ▼
             ┌──────────────────┐
             │     生产者       │  ← 比较源/目标、过滤、排序
             │  (produce/tasks) │
             └──────┬──────────┘
                    │ object.Object 通道（缓冲 10240）
                    ▼
        ┌───────────────────────┐
        │  工作池               │  ← config.Threads 个 goroutine
        │  • copyData (单文件)  │
        │  • copyData (分片)    │
        │  • checkSum           │
        │  • deleteObj          │
        │  • copyPerms          │
        └───────────────────────┘
```

### `pkg/object/` — 存储抽象层

| 文件 | 存储后端 |
|------|----------|
| `interface.go` | `ObjectStorage` 接口（18 个方法：Get/Put/Copy/Delete/Head/List/...） |
| `object_storage.go` | `DefaultObjectStorage` 基类 + `CreateStorage()` 工厂 |
| `s3.go` | AWS S3 + 兼容后端（MinIO、Oracle、OVH） |
| `oss.go` | 阿里云 OSS |
| `cos.go` | 腾讯云 COS |
| `azure.go` | Azure Blob |
| `gs.go` | Google Cloud Storage |
| `obs.go` | 华为云 OBS |
| `bos.go` | 百度云 BOS |
| `tos.go` | 火山引擎 TOS |
| `minio.go` | MinIO |
| `qiniu.go` | 七牛 Kodo |
| `qingstor.go` | 青云 QingStor |
| `ks3.go` | 金山云 KS3 |
| `ibmcos.go` | IBM COS |
| `b2.go` | Backblaze B2 |
| `swift.go` | OpenStack Swift |
| `ceph.go` | Ceph RGW |
| `hdfs.go` | HDFS |
| `nfs.go` | NFS v3 |
| `sftp.go` | SFTP |
| `webdav.go` | WebDAV |
| `file.go` | 本地文件系统 |
| `mem.go` | 内存（测试用） |
| `redis.go` / `tikv.go` / `etcd.go` / `sql.go` | KV / 关系型数据库 |
| `encrypt.go` | 全对象加密（AES-256-GCM-RSA、ChaCha20-RSA、SM4-GCM） |
| `encrypt_chunked.go` | 分块加密（每块 1 MiB 明文）|
| `prefix.go` | `WithPrefix` 包装，实现子目录范围同步 |
| `sharding.go` | 基于 FNV 哈希的分片存储 |
| `checksum.go` | CRC32C 校验和验证 |

#### 加密流程

```
客户端数据
    │
    ▼
chunkedEncrypted.PutWithMeta()  ← MetadataPutter 接口
    │
    ├─ 加密：chunkEncryptReader
    │   ├─ 读取 1 MiB 明文块
    │   ├─ dataEncryptor.Encrypt() → AEAD (AES-256-GCM / ChaCha20-Poly1305 / SM4-GCM)
    │   └─ 前置 [4 字节密文长度][密文]
    │
    └─ 委托给底层 MetadataPutter（S3/OSS 等）
```

### `pkg/acl/` — ACL 管理
- POSIX ACL 规则的编码/解码
- 基于校验和去重的规则缓存
- `Cache` 接口，`sync.RWMutex` 保护

### `pkg/metric/` — Prometheus 指标 + Consul
- 进程 CPU/内存/运行时间 的 gauge 指标
- Consul 服务注册

## 核心概念

### 生产者模式
三种生成任务的方式：
1. **全量扫描** — `startProducer()` 列出某个前缀下的所有对象
2. **文件列表** — `produceFromList()` 从文件读取 key 列表
3. **断点恢复** — `restoreFromCheckpoint()` 从已保存的断点恢复

### 对比逻辑（`produce()`）
对每个源对象：
1. 检查排除/包含规则、大小/时间/期限过滤器
2. 先完成源/目的 key 字典序对齐，并处理目的端 extra（`--delete-dst` 只删除源端没有的 key）
3. 配置 `--db` 时启用两层 gate：第一层命中成功记录且源 mtime 未变则直接跳过；第二层在目标端同 key 时按 mtime 保护目标端新数据
4. 未启用 gate 时按默认决策矩阵：

| 条件 | 动作 |
|------|------|
| 目标端不存在 | 复制 |
| 大小相同 | 跳过（除非 `--force-update` 或 `--check-change`）|
| 大小不同 | 复制 |
| 目标更新 | 跳过（除非 `--force-update`）|
| 目标端多余 + `--delete-dst` | 删除（在 key 对齐后判定，gate 跳过的同 key 对象不会被误删）|
| 目标端多余 + 无 `--delete-dst` | 跳过（记入 "extra"）|

### 断点续传
- `CheckpointManager` 将状态保存为 JSON 到目标存储（`.juicefs-sync-checkpoint.<md5>.json`）
- 记录：每个前缀的遍历进度、已完成 key、失败 key、分片上传状态、待删除队列
- 定时保存（默认 10s）+ 信号保存（SIGINT / SIGTERM）
- 配置哈希确保断点兼容性

### 集群模式
- 管理节点通过 HTTP 将前缀分发给工作节点
- 工作节点上报：复制/跳过/失败数量、已完成 key、分片进度
- 管理节点汇聚统计，处理延迟删除

## 配置参数

主要 CLI 参数：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--threads` / `-p` | 10 | 并发线程数 |
| `--list-threads` | 1 | 并行列表线程数 |
| `--start` / `-s` | — | 起始 key |
| `--end` / `-e` | — | 结束 key |
| `--include` / `--exclude` | — | 基于模式的过滤 |
| `--update` / `-u` | false | 目标更新则跳过 |
| `--force-update` / `-f` | false | 覆盖已有文件 |
| `--delete-src` | false | 同步后删除源端 |
| `--delete-dst` | false | 删除目标端多余文件 |
| `--check-all` | false | 通过 CRC32C 校验完整性 |
| `--check-new` | false | 校验新复制文件 |
| `--preserve-meta` | false | 保留 Content-Type 和元数据 |
| `--enable-checkpoint` | false | 启用断点续传 |
| `--max-failure` | -1 | 失败 N 次后中止（-1 为不限）|
| `--bwlimit` | 0 | 带宽上限（Mbps）|
| `--encrypt-rsa-key` | — | 目标端 RSA/SM2 密钥 |
| `--decrypt-rsa-key` | — | 源端 RSA/SM2 密钥 |
| `--db` | — | MySQL 连接串，用于结果记录；启用 gate 时 DSN 需带 database，例如 `mysql://user:pass@host:3306/juicefs_gate` |
| `--gate-table` | 自动生成 | 指定两层 gate 记录表；默认 `sync_records_v2_<md5(src|dst)[:8]>` |
| `--scan` | false | 仅对比模式 |
| `--scan-single` | false | 单桶扫描（仅 ListObjects）|
| `--dry` | false | 试运行 |
| `--links` / `-l` | false | 保留符号链接 |
| `--perms` | false | 保留文件权限 |
| `--dirs` | false | 包含目录条目 |
| `--fix-meta` | false | 修复 Content-Type/metadata；在正常同步流程前返回，不复制数据 |
| `--double-check` | false | 同步后二次扫描 |

## 存储 URI 格式

```
[NAME]://[ACCESS_KEY:SECRET_KEY[:TOKEN]@]BUCKET[.ENDPOINT][/PREFIX]
```

示例：
```
s3://mybucket.s3.us-east-1.amazonaws.com/prefix/
oss://mybucket.oss-cn-shanghai.aliyuncs.com
file:///mnt/data/
hdfs://namenode:8020/path
```

## 性能相关

- **并行下载**：大于 10 MiB 的对象使用 `parallelDownloader`（并发 Range GET）
- **分片上传**：大于 4 GiB 的对象走分片上传，支持断点续传
- **直接 IO**：本地文件系统间同步支持 `splice(2)` / `copy_file_range(2)`
- **带宽控制**：进程级令牌桶 + 可选全局流量控制服务
- **缓冲区池**：1 MiB IO 缓冲区 + 2 的幂次动态缓冲区池用于分片写入

### 已知限制

- `encrypted.Get()` 将整个密文加载到内存后再解密
- `chunkedEncrypted.ListAll` 在调用方提前停止读取时可能泄漏 goroutine
- 全局计数器使用包级别变量（同一进程内不适用于多个并发同步）

## 编译 & 测试

```bash
# 编译
make juicefs           # 默认编译
make juicefs.lite      # 最小编译（不含 S3/OSS/HDFS 等）
make juicefs.linux     # 交叉编译 Linux（macOS 上）

# 测试
make test.pkg          # 包测试
make test.cmd          # CLI 集成测试
```

## 近期修复记录

以下问题已在本版本中识别并修复：

| 问题 | 严重度 | 修复 |
|------|--------|------|
| `retryFailedObjects` 空实现 | 严重 | 改为 fallthrough 到正常同步，不再直接返回 |
| `withPrefix.Copy` 缺少 prefix | 严重 | dst 和 src 前加上 `p.prefix+` |
| `--dry` + `--max-failure` panic | 严重 | goroutine 已用 `!config.Dry` 保护 |
| `DefaultObjectStorage.Head` 缺 ctx | 严重 | 添加 `ctx context.Context` 参数 |
| `chunkedEncrypted` metadata 丢失 | 严重 | 实现了 `MetadataPutter` 委托 |
| `config.Limit--` data race | 中危 | 改为 `atomic.AddInt64` |
| S3 `Copy` 自拷贝不更新 metadata | 中危 | fix-meta 改为 `Get` + `PutWithMeta`，不再依赖 self-copy |
| `flushBatch` 失败丢数据 | 中危 | 保留 batch 等待下次重试，不清空 |
| `createSyncStorage` 部分泄漏 | 中危 | 在 dst 创建前 `defer object.Shutdown(src)` |
| `fixMetaOnly` 返回值忽略 | 中危 | 检查返回值，失败计数 |
| goroutine 泄漏（pending/maxFailure/stats）| 中危 | 添加 stop channel |
| 日志泄露 Access Key | 中危 | `maskStorageURL()` 辅助函数 |
