package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// mysqlGateService 是 DbGateService 的 MySQL 实现，使用原生 database/sql。
type mysqlGateService struct {
	db    *sql.DB
	table string
}

// NewMySQLGateService 创建 MySQL gate 服务。
// db: 已初始化的 *sql.DB 连接（复用 mysqlService 的连接）。
// table: gate 表名（由 ResolveGateTableName 生成）。
func NewMySQLGateService(db *sql.DB, table string) (DbGateService, error) {
	svc := &mysqlGateService{db: db, table: table}
	if err := svc.createTable(); err != nil {
		return nil, fmt.Errorf("create gate table %s: %w", table, err)
	}
	return svc, nil
}

func (s *mysqlGateService) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *mysqlGateService) createTable() error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS `+"`%s`"+` (
		`+"`key`"+` VARCHAR(768) PRIMARY KEY,
		source_mtime DATETIME(3) NOT NULL,
		source_size BIGINT DEFAULT 0,
		target_size BIGINT DEFAULT 0,
		diff BOOLEAN DEFAULT FALSE,
		status VARCHAR(16) NOT NULL,
		error_msg TEXT,
		updated_at DATETIME(3) NOT NULL,
		INDEX idx_gate (status, source_mtime)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, s.table)
	_, err := s.db.Exec(query)
	return err
}

func (s *mysqlGateService) GetRecord(key string) (*SyncRecordV2, error) {
	query := fmt.Sprintf("SELECT `key`, source_mtime, source_size, target_size, diff, status, error_msg, updated_at\n\t\tFROM `%s` WHERE `key` = ?", s.table)
	row := s.db.QueryRow(query, key)

	var rec SyncRecordV2
	var diffInt int
	var errorMsg sql.NullString
	err := row.Scan(&rec.Key, &rec.SourceMtime, &rec.SourceSize, &rec.TargetSize, &diffInt, &rec.Status, &errorMsg, &rec.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rec.Diff = diffInt != 0
	if errorMsg.Valid {
		rec.ErrorMsg = errorMsg.String
	}
	return &rec, nil
}

func (s *mysqlGateService) SaveRecord(rec *SyncRecordV2) error {
	rec.UpdatedAt = time.Now()
	return s.upsertOne(rec)
}

func (s *mysqlGateService) BatchSaveRecords(recs []*SyncRecordV2) error {
	if len(recs) == 0 {
		return nil
	}
	now := time.Now()
	for _, rec := range recs {
		rec.UpdatedAt = now
	}
	return s.upsertBatch(recs)
}

func (s *mysqlGateService) upsertOne(rec *SyncRecordV2) error {
	query := fmt.Sprintf("INSERT INTO `%s` (`key`, source_mtime, source_size, target_size, diff, status, error_msg, updated_at)\n\t\tVALUES (?, ?, ?, ?, ?, ?, ?, ?)\n\t\tON DUPLICATE KEY UPDATE\n\t\t\tsource_mtime = VALUES(source_mtime),\n\t\t\tsource_size = VALUES(source_size),\n\t\t\ttarget_size = VALUES(target_size),\n\t\t\tdiff = VALUES(diff),\n\t\t\tstatus = VALUES(status),\n\t\t\terror_msg = VALUES(error_msg),\n\t\t\tupdated_at = VALUES(updated_at)", s.table)
	_, err := s.db.Exec(query, rec.Key, rec.SourceMtime, rec.SourceSize, rec.TargetSize, boolToInt(rec.Diff), rec.Status, rec.ErrorMsg, rec.UpdatedAt)
	return err
}

func (s *mysqlGateService) upsertBatch(recs []*SyncRecordV2) error {
	const chunkSize = 500
	for i := 0; i < len(recs); i += chunkSize {
		end := i + chunkSize
		if end > len(recs) {
			end = len(recs)
		}
		batch := recs[i:end]
		if err := s.upsertChunk(batch); err != nil {
			return fmt.Errorf("batch upsert chunk %d-%d: %w", i, end, err)
		}
	}
	return nil
}

func (s *mysqlGateService) upsertChunk(recs []*SyncRecordV2) error {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO `%s` (`key`, source_mtime, source_size, target_size, diff, status, error_msg, updated_at) VALUES ", s.table))
	args := make([]interface{}, 0, len(recs)*8)
	for j, rec := range recs {
		if j > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?, ?, ?, ?, ?, ?, ?, ?)")
		args = append(args, rec.Key, rec.SourceMtime, rec.SourceSize, rec.TargetSize, boolToInt(rec.Diff), rec.Status, rec.ErrorMsg, rec.UpdatedAt)
	}
	sb.WriteString(` ON DUPLICATE KEY UPDATE
		source_mtime = VALUES(source_mtime),
		source_size = VALUES(source_size),
		target_size = VALUES(target_size),
		diff = VALUES(diff),
		status = VALUES(status),
		error_msg = VALUES(error_msg),
		updated_at = VALUES(updated_at)`)
	_, err := s.db.Exec(sb.String(), args...)
	return err
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// NewGateService 从 DSN 创建 DbGateService（MySQL 或 SQLite）。
// 独立创建数据库连接，不依赖现有的 AsyncDbService 连接。
// dsn: 数据库连接字符串（如 mysql://user:pass@host:port/db）。
// table: gate 表名（由 ResolveGateTableName 生成）。
func NewGateService(dsn string, table string) (DbGateService, error) {
	cfg, err := ParseDbDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse gate dsn: %w", err)
	}
	if cfg.Driver != "mysql" {
		return nil, fmt.Errorf("unsupported gate db driver: %s", cfg.Driver)
	}
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", cfg.User, cfg.Pass, cfg.Host, cfg.DBName)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, fmt.Errorf("open gate db: %w", err)
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping gate db: %w", err)
	}
	return NewMySQLGateService(db, table)
}

// sqliteGateService 使用 SQLite 实现 DbGateService。
// 复用 SQLite 支持（go.mod 中已有 github.com/mattn/go-sqlite3）。
type sqliteGateService struct {
	db    *sql.DB
	table string
}

// NewSQLiteGateService 创建 SQLite gate 服务。
func NewSQLiteGateService(db *sql.DB, table string) (DbGateService, error) {
	svc := &sqliteGateService{db: db, table: table}
	if err := svc.createTable(); err != nil {
		return nil, fmt.Errorf("create sqlite gate table %s: %w", table, err)
	}
	return svc, nil
}

func (s *sqliteGateService) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *sqliteGateService) createTable() error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		"key" TEXT PRIMARY KEY,
		source_mtime DATETIME NOT NULL,
		source_size INTEGER DEFAULT 0,
		target_size INTEGER DEFAULT 0,
		diff INTEGER DEFAULT 0,
		status TEXT NOT NULL,
		error_msg TEXT,
		updated_at DATETIME NOT NULL
	)`, s.table)
	_, err := s.db.Exec(query)
	if err != nil {
		return err
	}
	// 创建索引
	idxQuery := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_gate ON %s (status, source_mtime)`, s.table, s.table)
	_, err = s.db.Exec(idxQuery)
	return err
}

func (s *sqliteGateService) GetRecord(key string) (*SyncRecordV2, error) {
	query := fmt.Sprintf("SELECT \"key\", source_mtime, source_size, target_size, diff, status, error_msg, updated_at\n\t\tFROM %s WHERE \"key\" = ?", s.table)
	row := s.db.QueryRow(query, key)

	var rec SyncRecordV2
	var diffInt int
	var errorMsg sql.NullString
	err := row.Scan(&rec.Key, &rec.SourceMtime, &rec.SourceSize, &rec.TargetSize, &diffInt, &rec.Status, &errorMsg, &rec.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rec.Diff = diffInt != 0
	if errorMsg.Valid {
		rec.ErrorMsg = errorMsg.String
	}
	return &rec, nil
}

func (s *sqliteGateService) SaveRecord(rec *SyncRecordV2) error {
	rec.UpdatedAt = time.Now()
	return s.upsertOne(rec)
}

func (s *sqliteGateService) BatchSaveRecords(recs []*SyncRecordV2) error {
	if len(recs) == 0 {
		return nil
	}
	now := time.Now()
	for _, rec := range recs {
		rec.UpdatedAt = now
	}
	return s.upsertBatch(recs)
}

func (s *sqliteGateService) upsertOne(rec *SyncRecordV2) error {
	query := fmt.Sprintf(`INSERT INTO %s ("key", source_mtime, source_size, target_size, diff, status, error_msg, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT("key") DO UPDATE SET
			source_mtime = excluded.source_mtime,
			source_size = excluded.source_size,
			target_size = excluded.target_size,
			diff = excluded.diff,
			status = excluded.status,
			error_msg = excluded.error_msg,
			updated_at = excluded.updated_at`, s.table)
	_, err := s.db.Exec(query, rec.Key, rec.SourceMtime, rec.SourceSize, rec.TargetSize, boolToInt(rec.Diff), rec.Status, rec.ErrorMsg, rec.UpdatedAt)
	return err
}

func (s *sqliteGateService) upsertBatch(recs []*SyncRecordV2) error {
	const chunkSize = 500
	for i := 0; i < len(recs); i += chunkSize {
		end := i + chunkSize
		if end > len(recs) {
			end = len(recs)
		}
		batch := recs[i:end]
		if err := s.upsertChunk(batch); err != nil {
			return fmt.Errorf("sqlite batch upsert chunk %d-%d: %w", i, end, err)
		}
	}
	return nil
}

func (s *sqliteGateService) upsertChunk(recs []*SyncRecordV2) error {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s (\"key\", source_mtime, source_size, target_size, diff, status, error_msg, updated_at) VALUES ", s.table))
	args := make([]interface{}, 0, len(recs)*8)
	for j, rec := range recs {
		if j > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?, ?, ?, ?, ?, ?, ?, ?)")
		args = append(args, rec.Key, rec.SourceMtime, rec.SourceSize, rec.TargetSize, boolToInt(rec.Diff), rec.Status, rec.ErrorMsg, rec.UpdatedAt)
	}
	sb.WriteString(` ON CONFLICT("key") DO UPDATE SET
		source_mtime = excluded.source_mtime,
		source_size = excluded.source_size,
		target_size = excluded.target_size,
		diff = excluded.diff,
		status = excluded.status,
		error_msg = excluded.error_msg,
		updated_at = excluded.updated_at`)
	_, err := s.db.Exec(sb.String(), args...)
	return err
}
