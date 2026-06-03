# JuiceFS Sync (Enhanced)

基于 [JuiceFS](https://github.com/juicedata/juicefs) `juicefs sync` 命令的增强版，专注于对象存储间的数据迁移，**保留源对象的 Content-Type 和用户自定义 Metadata**。

## 与原版差异

| 特性 | 原版 juicefs sync | 本仓库 |
|------|-------------------|--------|
| Content-Type | 根据文件扩展名重新猜测 (`GuessMimeType`) | 始终从源端 Head 获取并保留 |
| 自定义 Metadata (x-amz-meta-*) | 不同步 | `--preserve-meta` 开启后同步 |
| Crc32c 校验码 | 写入目标端 metadata | 不再写入 |

## 使用

```bash
# 普通 sync：保留 Content-Type
juicefs sync s3://src-bucket/ s3://dst-bucket/

# 同时保留 Content-Type 和自定义 Metadata
juicefs sync --preserve-meta s3://src-bucket/ s3://dst-bucket/
```

## 支持的后端

所有 JuiceFS 支持的 S3 协议对象存储，包括但不限于：

- AWS S3 / MinIO
- 腾讯云 COS
- 阿里云 OSS
- 华为云 OBS
- 金山云 KS3
- 百度云 BOS
- 火山引擎 TOS
- IBM COS
- 青云 QingStor
- Google Cloud Storage
- Azure Blob
- OpenStack Swift

## 编译

```bash
go build -o juicefs-sync .
```

## 实现原理

1. sync 前先 `Head` 源对象，获取 Content-Type 和 Metadata
2. 目标端通过 `PutWithMeta` / `CreateMultipartUploadWithMeta` 传入源端属性
3. 覆盖三种 sync 路径：小文件、大文件磁盘写入、多分片上传
4. `withPrefix` wrapper 透明转发 metadata

## License

基于 JuiceFS Apache License 2.0 修改。
