# JuiceFS Sync (Enhanced)

基于 [JuiceFS](https://github.com/juicedata/juicefs) `juicefs sync` 命令的增强版，专注于对象存储间的数据迁移。

## 特性

| 特性 | 说明 |
|------|------|
| Content-Type 保留 | 始终从源端 Head 获取，不使用扩展名猜测 |
| 自定义 Metadata | `--preserve-meta` 保留 x-amz-meta-* 等 |
| MySQL 记录 | `--db` 写入数据库，每次任务独立建表 |
| 扫描对比 | `--scan` 比较源和目标，检测 missing/differs/extra |
| 单端扫描 | `--scan-single` ListObjects 遍历单桶 |
| CSV 报告 | `--output report.csv` 导出扫描结果 |
| Web Dashboard | `--dashboard :8080` 实时进度 或独立 `dashboard` 命令 |
| 精简二进制 | 去掉所有非 sync 依赖，86M |

## 命令

### sync — 数据迁移

```bash
# 基础 sync
juicefs sync s3://src/ s3://dst/

# 保留元数据
juicefs sync --preserve-meta s3://src/ s3://dst/

# 记录到 MySQL
juicefs sync --db "mysql://user:pass@host:3306" s3://src/ s3://dst/

# 带实时 dashboard
juicefs sync --dashboard :8080 s3://src/ s3://dst/
```

### scan — 扫描对比（不传输）

```bash
# 对比源和目标
juicefs sync --scan --db "mysql://user:pass@host:3306" s3://src/ s3://dst/

# 导出 CSV
juicefs sync --scan --output report.csv s3://src/ s3://dst/

# CSV + DB 双写
juicefs sync --scan --db "..." --output report.csv s3://src/ s3://dst/
```

### scan-single — 单桶扫描

```bash
juicefs sync --scan-single --db "mysql://user:pass@host:3306" s3://bucket/
juicefs sync --scan-single --output list.csv s3://bucket/folder/
```

### dashboard — Web 看板

```bash
juicefs dashboard --db "mysql://user:pass@host:3306" --port 8080
```

## 参数

| 参数 | 说明 |
|------|------|
| `--preserve-meta` | 保留 Content-Type 和自定义元数据 |
| `--db` | MySQL 连接串 `mysql://user:pass@host:port` |
| `--scan` | 对比模式，检测 missing/differs/extra/matches |
| `--scan-single` | 单端扫描，仅 ListObjects（极快） |
| `--output` | CSV 报告路径 |
| `--dashboard` | 内嵌看板端口，如 `:8080` |

## 数据库架构

自动创建 5 个数据库：

| 数据库 | 用途 |
|--------|------|
| `sync_jobs` | sync 任务记录 |
| `juicefs_sync` | sync 对象明细 (`objects_{job_id}`) |
| `scan_jobs` | scan 任务记录 |
| `scan_sync` | scan 对象明细 (`objects_{job_id}`) |
| `single_scan` | scan-single 对象明细 (`scan_{job_id}`) |

## 支持的后端

AWS S3、MinIO、腾讯云 COS、阿里云 OSS、华为云 OBS、金山云 KS3、百度云 BOS、火山引擎 TOS、IBM COS、青云 QingStor、Google Cloud Storage、Azure Blob、OpenStack Swift

## 编译

```bash
go build -ldflags="-s -w" -o juicefs-sync .
```

## License

基于 JuiceFS Apache License 2.0 修改。
