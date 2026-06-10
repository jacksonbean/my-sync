package db

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	dbSyncJobs   = "sync_jobs"
	dbSyncData   = "juicefs_sync"
	dbScanJobs   = "scan_jobs"
	dbScanData   = "scan_sync"
	dbSingleScan = "single_scan"
)

// mysqlService implements DbService backed by MySQL with separate databases for jobs and data.
type mysqlService struct {
	db           *sql.DB
	host         string
	user         string
	pass         string
	isScan       bool
	isSingleScan bool
	objectsTable string // per-job table name, set by StartJob
	mu           sync.Mutex
}

// NewMySQLService creates a new MySQL-backed DbService.
func NewMySQLService(cfg *DbConfig, isScan, isSingleScan bool) (DbService, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&loc=Local", cfg.User, cfg.Pass, cfg.Host)
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

	svc := &mysqlService{db: db, host: cfg.Host, user: cfg.User, pass: cfg.Pass, isScan: isScan, isSingleScan: isSingleScan}
	if err := svc.createDatabases(); err != nil {
		db.Close()
		return nil, err
	}
	return svc, nil
}

func (s *mysqlService) createDatabases() error {
	dbs := []string{dbSyncJobs, dbSyncData, dbScanJobs, dbScanData, dbSingleScan}
	for _, name := range dbs {
		if _, err := s.db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", name)); err != nil {
			return fmt.Errorf("create database %s: %w", name, err)
		}
	}
	return nil
}

func (s *mysqlService) jobsDB() string {
	if s.isScan {
		return dbScanJobs
	}
	return dbSyncJobs
}

func (s *mysqlService) dataDB() string {
	if s.isSingleScan {
		return dbSingleScan
	}
	if s.isScan {
		return dbScanData
	}
	return dbSyncData
}

func (s *mysqlService) createJobsTable() error {
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS `+"`%s`"+`.`+"`sync_jobs`"+` (
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
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, s.jobsDB())
	_, err := s.db.Exec(sql)
	return err
}

func (s *mysqlService) createSingleScanTable(tableName string) error {
	tableName = strings.ReplaceAll(tableName, "-", "_")
	tableName = strings.ReplaceAll(tableName, ".", "_")
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS `+"`%s`"+`.`+"`%s`"+` (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		source_key VARCHAR(2048) NOT NULL,
		size BIGINT DEFAULT 0,
		last_modified DATETIME(3),
		storage_class VARCHAR(64),
		INDEX idx_key (source_key(768))
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, s.dataDB(), tableName)
	_, err := s.db.Exec(sql)
	return err
}

func (s *mysqlService) createObjectsTable(tableName string) error {
	tableName = strings.ReplaceAll(tableName, "-", "_")
	tableName = strings.ReplaceAll(tableName, ".", "_")
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS `+"`%s`"+`.`+"`%s`"+` (
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
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, s.dataDB(), tableName)
	_, err := s.db.Exec(sql)
	return err
}

func (s *mysqlService) StartJob(job JobInfo) error {
	// Single scan: also insert a job record into sync_jobs for history
	if s.isSingleScan {
		if err := s.createJobsTable(); err != nil {
			return fmt.Errorf("create jobs table: %w", err)
		}
		// Insert into scan_jobs so it appears in history (separate from sync)
		jobsSQL := fmt.Sprintf(`INSERT INTO `+"`%s`"+`.`+"`sync_jobs`"+`
			(id, src_url, dst_url, start_time, end_time, total_objects, copied_objects, skipped_objects, failed_objects, deleted_objects, total_bytes, status)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, dbScanJobs)
		var endTime interface{}
		if job.EndTime.IsZero() {
			endTime = nil
		} else {
			endTime = job.EndTime
		}
		_, _ = s.db.Exec(jobsSQL,
			job.ID, job.SrcURL, job.DstURL, job.StartTime, endTime,
			job.TotalObjects, job.CopiedObjects, job.SkippedObjects, job.FailedObjects, job.DeletedObjects, job.TotalBytes,
			string(job.Status))

		tableName := "scan_" + strings.ReplaceAll(strings.ReplaceAll(job.ID, "-", "_"), ".", "_")
		if err := s.createSingleScanTable(tableName); err != nil {
			return fmt.Errorf("create scan table %s: %w", tableName, err)
		}
		s.mu.Lock()
		s.objectsTable = tableName
		s.mu.Unlock()
		return nil
	}

	if err := s.createJobsTable(); err != nil {
		return fmt.Errorf("create jobs table: %w", err)
	}

	jobsSQL := fmt.Sprintf(`INSERT INTO `+"`%s`"+`.`+"`sync_jobs`"+`
		(id, src_url, dst_url, start_time, end_time, total_objects, copied_objects, skipped_objects, failed_objects, deleted_objects, total_bytes, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, s.jobsDB())
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

	// Single scan: simplified schema (key, size, last_modified, storage_class)
	if s.isSingleScan {
		objectsSQL := fmt.Sprintf(`INSERT INTO `+"`%s`"+`.`+"`%s`"+`
			(source_key, size, last_modified, storage_class)
			VALUES (?, ?, ?, ?)`, s.dataDB(), table)
		_, err := s.db.Exec(objectsSQL,
			rec.SourceKey, rec.Size, rec.EndTime, rec.ContentType)
		return err
	}

	objectsSQL := fmt.Sprintf(`INSERT INTO `+"`%s`"+`.`+"`%s`"+`
		(source_key, target_key, size, content_type, metadata_json, status, error_msg, start_time, end_time)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, s.dataDB(), table)
	_, err := s.db.Exec(objectsSQL,
		rec.SourceKey, rec.TargetKey, rec.Size,
		rec.ContentType, rec.Metadata, string(rec.Status), rec.ErrorMsg,
		rec.StartTime, rec.EndTime)
	return err
}

func (s *mysqlService) EndJob(jobID string, job JobInfo) error {
	sql := fmt.Sprintf(`UPDATE `+"`%s`"+`.`+"`sync_jobs`"+` SET status = ?, end_time = ?, total_objects = ?, copied_objects = ?, skipped_objects = ?, failed_objects = ?, deleted_objects = ?, total_bytes = ? WHERE id = ?`, s.jobsDB())
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
