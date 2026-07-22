package db

import (
	"crypto/md5"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/juicedata/juicefs/pkg/utils"
)

var logger = utils.GetLogger("juicefs")

// ObjectStatus is the sync result for a single object.
type ObjectStatus string

const (
	StatusCopied  ObjectStatus = "copied"
	StatusSkipped ObjectStatus = "skipped"
	StatusFailed  ObjectStatus = "failed"
	StatusDeleted ObjectStatus = "deleted"
	StatusMissing ObjectStatus = "missing" // scan: not on destination
	StatusDiffers ObjectStatus = "differs" // scan: size differs
	StatusMatches ObjectStatus = "matches" // scan: identical on both sides
	StatusExtra   ObjectStatus = "extra"   // scan: on destination but not source
)

// JobStatus is the overall sync job status.
type JobStatus string

const (
	JobRunning   JobStatus = "running"
	JobCompleted JobStatus = "completed"
	JobFailed    JobStatus = "failed"
)

// JobInfo holds summary info for a sync job.
type JobInfo struct {
	ID             string // dest_bucket_20260603143015123456
	SrcURL         string
	DstURL         string
	StartTime      time.Time
	EndTime        time.Time
	TotalObjects   int64
	CopiedObjects  int64
	SkippedObjects int64
	FailedObjects  int64
	DeletedObjects int64
	TotalBytes     int64
	Status         JobStatus
}

// ObjectRecord holds the sync result for a single object.
type ObjectRecord struct {
	JobID       string
	SourceKey   string
	TargetKey   string
	Size        int64
	ContentType string
	Metadata    string // JSON
	Status      ObjectStatus
	ErrorMsg    string
	StartTime   time.Time
	EndTime     time.Time
}

// DbConfig holds parsed database connection info.
type DbConfig struct {
	Driver string
	Host   string // host:port
	User   string
	Pass   string
	DBName string // database name from the URL path (e.g. mysql://user:pass@host/db)
}

// GenerateJobID creates a job ID from the destination bucket name and current time.
// The timestamp includes microseconds to avoid collisions between consecutive runs
// started within the same minute.
func GenerateJobID(dstURL string, t time.Time) string {
	jobTime := t.Format("20060102150405") + fmt.Sprintf("%06d", t.Nanosecond()/1000)
	name := ""
	if u, err := url.Parse(dstURL); err == nil && u.Host != "" {
		name = u.Host
		if idx := strings.Index(name, "."); idx > 0 {
			name = name[:idx]
		}
		if path := strings.Trim(u.Path, "/"); path != "" {
			name += "_" + strings.ReplaceAll(path, "/", "_")
		}
	} else {
		trimmed := strings.TrimPrefix(dstURL, "s3://")
		trimmed = strings.TrimPrefix(trimmed, "cos://")
		trimmed = strings.TrimPrefix(trimmed, "oss://")
		trimmed = strings.TrimSuffix(trimmed, "/")
		parts := strings.Split(trimmed, "/")
		name = parts[0]
		if idx := strings.Index(name, "."); idx > 0 {
			name = name[:idx]
		}
	}
	name = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, name)
	name = strings.Trim(name, "_")
	if name == "" {
		name = "job"
	}
	// MySQL table names are limited to 64 chars. StartJob prefixes the ID with
	// "objects_", so keep the ID comfortably below that limit even for local
	// paths or long bucket/endpoint combinations.
	if len(name) > 32 {
		hash := fmt.Sprintf("%x", md5.Sum([]byte(dstURL)))
		name = name[:16] + "_" + hash[:8]
	}
	return fmt.Sprintf("%s_%s", name, jobTime)
}

// DbService is the interface for recording sync results.
type DbService interface {
	StartJob(job JobInfo) error
	RecordObject(rec ObjectRecord) error
	RecordObjects(recs []ObjectRecord) error
	EndJob(jobID string, job JobInfo) error
	Close() error
}

// ParseDbDSN extracts connection info from a URL string.
// Supported format: mysql://user:pass@host:port
func ParseDbDSN(raw string) (*DbConfig, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid db url: %w", err)
	}
	driver := strings.ToLower(u.Scheme)
	if driver != "mysql" {
		return nil, fmt.Errorf("unsupported db driver: %s", driver)
	}
	pass, _ := u.User.Password()
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":3306"
	}
	dbName := strings.Trim(u.Path, "/")
	return &DbConfig{
		Driver: driver,
		Host:   host,
		User:   u.User.Username(),
		Pass:   pass,
		DBName: dbName,
	}, nil
}

// channelSize is the buffer size for the object record channel.
const channelSize = 50000

// batchSize is the number of records to accumulate before flushing to DB.
// For scan-single (millions of objects), a larger batch reduces round trips.
const batchSize = 500

// flushInterval is the max time between batch flushes.
const flushInterval = time.Second

// AsyncDbService wraps a DbService with a buffered channel, batch writes, and non-blocking sends.
type AsyncDbService struct {
	DbService
	ch           chan ObjectRecord
	wg           sync.WaitGroup
	closed       bool
	mu           sync.Mutex
	errors       []error
	batch        []ObjectRecord
	flushRetries int
}

// NewAsyncDbService creates an AsyncDbService that buffers and batch-writes object records.
func NewAsyncDbService(svc DbService) *AsyncDbService {
	a := &AsyncDbService{
		DbService: svc,
		ch:        make(chan ObjectRecord, channelSize),
		batch:     make([]ObjectRecord, 0, batchSize),
	}
	a.wg.Add(1)
	go a.worker()
	return a
}

func (a *AsyncDbService) worker() {
	defer a.wg.Done()
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for {
		select {
		case rec, ok := <-a.ch:
			if !ok {
				// Final flush: no more records will arrive, so retry the remaining
				// batch immediately instead of waiting for a future tick that will
				// never come. flushBatch drops the batch after 3 failed attempts.
				for len(a.batch) > 0 {
					a.flushBatch()
				}
				return
			}
			a.batch = append(a.batch, rec)
			if len(a.batch) >= batchSize {
				a.flushBatch()
			}
		case <-ticker.C:
			if len(a.batch) > 0 {
				a.flushBatch()
			}
		}
	}
}

func (a *AsyncDbService) flushBatch() {
	if len(a.batch) == 0 {
		return
	}
	if err := a.DbService.RecordObjects(a.batch); err != nil {
		a.mu.Lock()
		a.errors = append(a.errors, err)
		a.flushRetries++
		retries := a.flushRetries
		a.mu.Unlock()
		if retries >= 3 {
			logger.Warnf("Dropping %d records after %d failed attempts: %s", len(a.batch), retries, err)
			a.batch = a.batch[:0]
			a.mu.Lock()
			a.flushRetries = 0
			a.mu.Unlock()
		} else {
			logger.Errorf("Failed to batch record %d objects: %s, will retry on next flush", len(a.batch), err)
		}
		return
	}
	a.mu.Lock()
	a.flushRetries = 0
	a.mu.Unlock()
	a.batch = a.batch[:0]
}

func sendSafe(ch chan ObjectRecord, rec ObjectRecord) (sent bool) {
	// The select with a default never panics on a closed (or nil) channel,
	// so no recover is needed. Swallowing arbitrary panics here would mask
	// real programming errors.
	select {
	case ch <- rec:
		return true
	default:
		return false
	}
}

// RecordObject sends an object record to the async channel (non-blocking).
// Returns an error if the record was dropped due to a full channel.
func (a *AsyncDbService) RecordObject(rec ObjectRecord) error {
	if !sendSafe(a.ch, rec) {
		a.mu.Lock()
		a.errors = append(a.errors, fmt.Errorf("RecordObject dropped: %s", rec.SourceKey))
		a.mu.Unlock()
		return fmt.Errorf("record dropped (channel full): %s", rec.SourceKey)
	}
	return nil
}

// Close flushes remaining records and shuts down the worker.
func (a *AsyncDbService) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	close(a.ch)
	a.mu.Unlock()
	a.wg.Wait()
	a.mu.Lock()
	errCount := len(a.errors)
	a.mu.Unlock()
	if errCount > 0 {
		logger.Warnf("DB write errors during sync: %d (check logs for details)", errCount)
	}
	return nil
}

// Errors returns any errors that occurred during async writes.
func (a *AsyncDbService) Errors() []error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.errors
}
