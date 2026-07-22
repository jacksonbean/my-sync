# JuiceFS Sync 优化建议

> 基于 2026-06-18 Code Review 整理，覆盖代码质量修复与功能增强方向。

---

## 已完成的代码修复

| # | 文件 | 问题 | 修复 |
|---|------|------|------|
| 1 | `pkg/sync/db/gate_service.go` | `sql` 局部变量遮蔽 `database/sql` 包 | 重命名为 `query` |
| 2 | `pkg/sync/db/gate_service.go` | `\%s` 非法 Go 转义 | 改为 `` `%s` `` |
| 3 | `pkg/sync/db/mysql.go` | `SET SESSION` 仅影响连接池中单连接 | 移到事务，每批 INSERT 前执行 |
| 4 | `pkg/sync/sync.go` | `listAll` 通道缓冲过小（1000） | 改为 `maxResults * 3`（3000），适配亿级数据 |
| 5 | `pkg/acl/acl.go` | `IsEmpty` 中 `&` 误改为 `|` | 回滚为 `&` |
| 6 | `pkg/object/object_storage_test.go` | Copy 双重前缀导致 3 个测试失败 | 去掉手动前缀，由 `withPrefix.Copy` 自动处理 |
| 7 | `pkg/sync/gate.go` | 未使用的 `fmt` import | 移除 |
| 8 | `cmd/sync.go` | `defer` 拆分后语义正确 | 确认无问题 |
| 9 | `pkg/sync/checkpoint.go` | JSON Marshal 移到锁外 | 确认安全，性能优化 |
| 10 | `pkg/sync/cluster.go` | SSH `StrictHostKeyChecking=no` → `accept-new` | 安全改进 |
| 11 | `pkg/sync/sync.go` | `fixMetaOnly` reader 关闭逻辑 | 已修复 |
| 12 | `cmd/main.go` | Dashboard 命令移除 | 引用已清理 |

---

## 功能增强建议（优先级排序）

### 1. 数据完整性校验 `--verify`（性价比最高）

**现状**：门卫系统用 mtime 做增量决策，但没有端到端数据校验。亿级迁移完成后，用户需要确认目标端数据完整。

**方案**：sync 完成后增加校验阶段：
- 随机采样 N 个对象，对比源和目标端的 ETag / CRC32C
- 全量对比源和目标 key 列表（复用 `--scan` 逻辑但不写 DB）
- 输出校验报告

**实现成本**：低，复用现有 Head + checksum 模块。

### 2. Webhook 通知 `--notify-webhook`

**现状**：长任务（小时/天级）期间用户无法获知进度和结果。

**方案**：
```bash
juicefs sync --notify-webhook https://hooks.slack.com/xxx s3://src/ s3://dst/
```
- 同步开始/完成/失败时 POST JSON 到指定 URL
- 附带：耗时、对象数、字节数、失败数、错误摘要

**实现成本**：低，`net/http` 即可。

### 3. 定时调度 `--schedule`

**现状**：周期性增量同步需要外部 cron，断点/增量状态散落。

**方案**：
```bash
juicefs sync --schedule "0 2 * * *" --db mysql://... s3://src/ s3://dst/
```
- 进程常驻，按 cron 表达式定时触发 sync
- 与双层门卫天然配合：后续执行走增量
- 建议引入 `github.com/robfig/cron`

### 4. 过滤规则增强

**方案**：
- `--filter-file`：从文件读取 key 白名单
- `--min-age` / `--max-age`：按对象年龄过滤
- `--storage-class-filter`：按存储类过滤

### 5. 结构化日志

**方案**：增加 `--log-format json`，输出每行一个 JSON 对象，方便接入 ELK/Loki。

---

## 当前架构能力评估

```
数据面  ████████████████████████████  完整
  ├─ 40+ 存储后端、加密、分片
  ├─ 元数据保留、Content-Type 传递
  └─ 分片上传/下载、校验和

控制面  ████████████████████████░░  较完整
  ├─ 增量优化：双层门卫
  ├─ 断点续传：CheckpointManager
  ├─ 持久化：MySQL/SQLite 批量写入
  ├─ 扫描对比：scan / scan-single
  ├─ CSV 导出
  └─ 集群模式：管理/工作节点

运维面  ████████████░░░░░░░░░░░░░░  有提升空间
  ├─ Prometheus 指标 + Consul
  ├─ 带宽限制
  └─ ⚠ 缺失：通知、调度、校验闭环
```
