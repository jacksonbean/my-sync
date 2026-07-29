package db

import (
	"crypto/md5"
	"fmt"
	"time"
)

// SyncRecordV2 是简化版双层门卫需要的记录结构。
// 相比现有 ObjectRecord，只保留"是否成功同步过 + 当时源端的 mtime/size"，
// 不记录完整的传输/校验中间状态，因为 JuiceFS sync 目前没有在 DB 中写中间状态。
// 新增 Diff 字段：标记目标端存在且大小与源不一致的情况，仅在 status="skipped" 时有意义。
//   - 当 status="skipped" 且 Diff=true 时：目标端存在，且目标大小 ≠ 源大小（被第二层门卫跳过保护）。
//   - 当 status="skipped" 且 Diff=false 时：目标端存在，且目标大小 = 源大小（被第二层门卫跳过保护）。
//   - 其他 status 时 Diff 无意义，保持默认 false。
//
// 建表参考（MySQL，ecs-sync 风格的自增主键 + key_hash 唯一索引）：
//
//	CREATE TABLE IF NOT EXISTS sync_records_v2 (
//	  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
//	  key_hash CHAR(32) NOT NULL,
//	  key VARCHAR(768) NOT NULL,
//	  source_mtime DATETIME(3) NOT NULL,
//	  source_size BIGINT DEFAULT 0,
//	  target_size BIGINT DEFAULT 0,
//	  diff BOOLEAN DEFAULT FALSE,
//	  status VARCHAR(16) NOT NULL,
//	  error_msg TEXT,
//	  updated_at DATETIME(3) NOT NULL,
//	  UNIQUE KEY uk_key_hash (key_hash),
//	  INDEX idx_gate (status, source_mtime)
//	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
//
// 为什么用 key_hash 而不是直接给 key 上索引：老版本 MySQL（InnoDB 行格式为
// REDUNDANT/COMPACT，如 MySQL ≤5.6 / MariaDB ≤10.1）索引列上限 767 字节，
// utf8mb4 的 VARCHAR(768) 当索引需要 3072 字节，会报 Error 1709。
// key_hash 是定长 32 字节的 MD5 hex，任何版本都能建唯一索引，
// 同时承担①按键快速查询 ②ON DUPLICATE KEY UPDATE 去重触发的职责。
type SyncRecordV2 struct {
	Key         string    // 对象 key
	SourceMtime time.Time // 上次成功同步时源端的 mtime
	SourceSize  int64     // 上次成功同步时源端的 size（辅助字段，不参与决策）
	TargetSize  int64     // 最近一次发现目标端存在时目标端的大小（诊断字段）
	Diff        bool      // 目标端存在且大小与源不一致（默认 false，仅 skip 时可能为 true）
	Status      string    // "success" | "failed" | "skipped"
	ErrorMsg    string    // 失败原因（可选）
	UpdatedAt   time.Time // 本记录更新时间
}

func (SyncRecordV2) TableName() string {
	return "sync_records_v2"
}

// KeyHash 返回对象 key 的 MD5 hex（32 字节定长），作为表中的 key_hash 列值。
// 查询与写入都必须通过它，而不是直接对 key 建索引（兼容老版本 MySQL 的 767 字节索引上限）。
func KeyHash(key string) string {
	sum := md5.Sum([]byte(key))
	return fmt.Sprintf("%x", sum)
}

// GateResult 是第一层门卫的返回决策。
type GateResult int

const (
	GateSkip           GateResult = iota // 第一层直接跳过，不进第二层
	GateNeedSecondGate                   // 第一层放行，需要第二层目标端 Head 确认
	GateMissing                          // DB 记录缺失（等同于 NeedSecondGate，语义上更明确）
)

// DbGateService 是第一层门卫的数据层接口。
// 使用原生 database/sql，不引入 gorm。
// 实现方需要负责 CREATE TABLE 和连接池管理。
type DbGateService interface {
	// GetRecord 查询单条记录；不存在时返回 nil, nil。
	GetRecord(key string) (*SyncRecordV2, error)

	// GetRecords 批量查询记录（一次 IN 查询替代多次单查）。
	// 返回 map 中只包含存在的记录；key 为原始对象 key。
	GetRecords(keys []string) (map[string]*SyncRecordV2, error)

	// SaveRecord 保存或更新单条记录（UPSERT）。
	SaveRecord(rec *SyncRecordV2) error

	// BatchSaveRecords 批量写入，用于 sync 过程中缓冲刷盘。
	// 建议实现为 INSERT ... ON DUPLICATE KEY UPDATE（MySQL）或 REPLACE INTO（SQLite）。
	BatchSaveRecords(recs []*SyncRecordV2) error

	// Close 释放底层连接池。由 NewGateService 独立创建连接时应当调用；
	// 当复用外部 *sql.DB 构造时（如 NewMySQLGateService/NewSQLiteGateService 接收已存在的
	// 连接），调用方需自行决定是否关闭共享连接，此方法仅在独立连接场景下安全关闭。
	Close() error
}

// -------------------- 占位：实现将由子 Agent 完成 --------------------
// 具体实现（mysqlGateService / sqliteGateService）在 gate_service.go 中编写。
// 需要复用 mysql.go 中的 *sql.DB 连接，或独立创建。
// 注意：本项目 go.mod 中已包含 github.com/go-sql-driver/mysql 和 mattn/go-sqlite3，
// 不需要额外引入 gorm。

// ResolveGateTableName 解析 gate 表名。
// 规则：
//  1. 如果 userTable 非空，使用用户指定的表名（需合法化）。
//  2. 如果 userTable 为空，自动生成：sync_records_v2_ + md5(src|dst)[:8]。
//
// 合法化：将非法字符替换为下划线，确保长度不超过 64（MySQL 表名限制）。
func ResolveGateTableName(userTable, src, dst string) string {
	if userTable != "" {
		clean := make([]rune, 0, len(userTable))
		for _, r := range userTable {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
				clean = append(clean, r)
			} else {
				clean = append(clean, '_')
			}
		}
		result := string(clean)
		if len(result) > 64 {
			result = result[:64]
		}
		return result
	}

	h := md5.New()
	fmt.Fprintf(h, "%s|%s", src, dst)
	hash := fmt.Sprintf("%x", h.Sum(nil))[:8]
	return "sync_records_v2_" + hash
}
