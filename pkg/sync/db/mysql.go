package db

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// mysqlService implements DbService backed by MySQL.
type mysqlService struct {
	db          *sql.DB
	objectsTable string // per-job table name, set by StartJob
	mu          sync.Mutex
}

// NewMySQLService creates a new MySQL-backed DbService.
func NewMySQLService(dsn string) (DbService, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(3)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("mysql ping failed: %w", err)
	}

	svc := &mysqlService{db: db}
	if err := svc.createJobsTable(); err != nil {
		db.Close()
		return nil, err
	}
	return svc, nil
}

func (s *mysqlService) createJobsTable() error {
	jobsSQL := `CREATE TABLE IF NOT EXISTS sync_jobs (
		id VARCHAR(128) PRIMARY KEY,
		src_url TEXT NOT NULL,
		dst_url TEXT NOT NULL,
		start_time DATETIME(3),
		end_time DATETIME(3),
		total_objects BIGINT DEFAULT 0,
		copied_objects BIGINT DEFAULT 0,
		skipped_objects BIGINT DEFAULT 0,
		failed_objects BIGINT DEFAULT 0,
		deleted_objects BIGINT DEFAULT 0,
		total_bytes BIGINT DEFAULT 0,
		status VARCHAR(16) DEFAULT 'running'
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
	if _, err := s.db.Exec(jobsSQL); err != nil {
		return fmt.Errorf("create sync_jobs: %w", err)
	}
	return nil
}

func (s *mysqlService) createObjectsTable(tableName string) error {
	// Sanitize table name
	tableName = strings.ReplaceAll(tableName, "-", "_")
	tableName = strings.ReplaceAll(tableName, ".", "_")
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		source_key VARCHAR(2048) NOT NULL,
		target_key VARCHAR(2048),
		size BIGINT DEFAULT 0,
		content_type VARCHAR(256),
		metadata_json TEXT,
		status VARCHAR(16) NOT NULL,
		error_msg TEXT,
		start_time DATETIME(3),
		end_time DATETIME(3),
		INDEX idx_status (status)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, tableName)
	_, err := s.db.Exec(sql)
	return err
}

func (s *mysqlService) StartJob(job JobInfo) error {
	// Insert job record
	jobsSQL := `INSERT INTO sync_jobs
		(id, src_url, dst_url, start_time, end_time, total_objects, copied_objects, skipped_objects, failed_objects, deleted_objects, total_bytes, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	var endTime interface{}
	if job.EndTime.IsZero() {
		endTime = nil
	} else {
		endTime = job.EndTime
	}
	if _, err := s.db.Exec(jobsSQL,
		job.ID, job.SrcURL, job.DstURL, job.StartTime, endTime,
		job.TotalObjects, job.CopiedObjects, job.SkippedObjects, job.FailedObjects, job.DeletedObjects, job.TotalBytes,
		string(job.Status)); err != nil {
		return err
	}

	// Create per-job objects table
	tableName := "objects_" + strings.ReplaceAll(strings.ReplaceAll(job.ID, "-", "_"), ".", "_")
	if err := s.createObjectsTable(tableName); err != nil {
		return fmt.Errorf("create objects table %s: %w", tableName, err)
	}
	s.mu.Lock()
	s.objectsTable = tableName
	s.mu.Unlock()
	return nil
}

func (s *mysqlService) RecordObject(rec ObjectRecord) error {
	s.mu.Lock()
	table := s.objectsTable
	s.mu.Unlock()
	if table == "" {
		return fmt.Errorf("no active job")
	}
	objectsSQL := fmt.Sprintf(`INSERT INTO %s
		(source_key, target_key, size, content_type, metadata_json, status, error_msg, start_time, end_time)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, table)
	_, err := s.db.Exec(objectsSQL,
		rec.SourceKey, rec.TargetKey, rec.Size,
		rec.ContentType, rec.Metadata, string(rec.Status), rec.ErrorMsg,
		rec.StartTime, rec.EndTime)
	return err
}

func (s *mysqlService) EndJob(jobID string, job JobInfo) error {
	sql := `UPDATE sync_jobs SET status = ?, end_time = ?, total_objects = ?, copied_objects = ?, skipped_objects = ?, failed_objects = ?, deleted_objects = ?, total_bytes = ? WHERE id = ?`
	_, err := s.db.Exec(sql, string(job.Status), job.EndTime,
		job.TotalObjects, job.CopiedObjects, job.SkippedObjects,
		job.FailedObjects, job.DeletedObjects, job.TotalBytes, jobID)
	return err
}

func (s *mysqlService) Close() error {
	return s.db.Close()
}

var _ DbService = (*mysqlService)(nil)
var _ DbService = (*AsyncDbService)(nil)
