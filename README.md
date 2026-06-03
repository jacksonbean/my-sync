# JuiceFS Sync (Enhanced)

基于 [JuiceFS](https://github.com/juicedata/juicefs) `juicefs sync` 命令的增强版，专注于对象存储间的数据迁移。

## 新增特性

| 特性 | 原版 | 本仓库 |
|------|------|--------|
| Content-Type | 根据扩展名重写 | 始终从源端保留 |
| 自定义 Metadata | 不同步 | `--preserve-meta` 后同步 |
| Crc32c 校验码 | 写入 metadata | 彻底移除 |
| 数据库记录 | 不支持 | `--db` 写入 MySQL |
| 扫描对比 | 不支持 | `--scan` 只对比不传输 |

## 使用

```bash
# 普通 sync：保留 Content-Type
juicefs sync s3://src-bucket/ s3://dst-bucket/

# 同步时保留 Content-Type 和自定义 Metadata
juicefs sync --preserve-meta s3://src-bucket/ s3://dst-bucket/

# 同步并将结果记录到 MySQL
juicefs sync --db "mysql://user:pass@host:3306/dbname" s3://src-bucket/ s3://dst-bucket/

# 扫描模式：只对比源和目标，不实际传输，结果写入 MySQL
juicefs sync --scan --db "mysql://user:pass@host:3306/dbname" s3://src-bucket/ s3://dst-bucket/
```

## 参数说明

| 参数 | 说明 |
|------|------|
| `--preserve-meta` | 保留源对象的 Content-Type 和 x-amz-meta-* 自定义元数据 |
| `--db` | MySQL 连接串，格式 `mysql://user:pass@host:3306/dbname` |
| `--scan` | 扫描模式：列出源对象，对比目标端，结果写入数据库，不实际拷贝 |

## 数据库表结构

`sync_jobs` — 每次 sync/scan 一条记录：

| 字段 | 说明 |
|------|------|
| id | 任务 ID，格式 `bucket_202606031430` |
| src_url / dst_url | 源和目标地址 |
| start_time / end_time | 开始和结束时间 |
| total / copied / skipped / failed / deleted | 各类对象计数 |
| total_bytes | 拷贝总字节数 |
| status | running / completed / failed |

`sync_objects` — 每个对象一条记录：

| 字段 | 说明 |
|------|------|
| job_id | 关联任务 ID |
| source_key / target_key | 对象路径 |
| size / content_type / metadata_json | 对象属性 |
| status | copied / skipped / failed / deleted / missing / differs / matches |
| error_msg | 错误信息 |
| start_time / end_time | 处理起止时间 |

## 支持的后端

所有 S3 协议对象存储：AWS S3、MinIO、腾讯云 COS、阿里云 OSS、华为云 OBS、金山云 KS3、百度云 BOS、火山引擎 TOS、IBM COS、青云 QingStor、Google Cloud Storage、Azure Blob、OpenStack Swift

## 编译

```bash
go build -o juicefs-sync .
```

## License

基于 JuiceFS Apache License 2.0 修改。
