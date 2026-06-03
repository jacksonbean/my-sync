package db

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
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
	StatusDiffers ObjectStatus = "differs" // scan: size/mtime differs
	StatusMatches ObjectStatus = "matches" // scan: identical on both sides
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
	ID             string    // dest_bucket_202606031430
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

// GenerateJobID creates a job ID from the destination bucket name and current time.
func GenerateJobID(dstURL string, t time.Time) string {
	// Extract bucket from URL like "s3://127.0.0.1:9001/bucket-b/"
	u, err := url.Parse(dstURL)
	if err != nil {
		// Fallback: use trimmed URL
		dstURL = strings.TrimPrefix(dstURL, "s3://")
		dstURL = strings.TrimPrefix(dstURL, "cos://")
		dstURL = strings.TrimPrefix(dstURL, "oss://")
		dstURL = strings.TrimSuffix(dstURL, "/")
		parts := strings.Split(dstURL, "/")
		bucket := parts[0]
		if idx := strings.Index(bucket, "."); idx > 0 {
			bucket = bucket[:idx]
		}
		return fmt.Sprintf("%s_%s", bucket, t.Format("200601021504"))
	}
	// Extract bucket from host
	host := u.Host
	if idx := strings.Index(host, "."); idx > 0 {
		host = host[:idx]
	}
	// Also include path prefix if present
	path := strings.Trim(u.Path, "/")
	if path != "" {
		host = host + "_" + strings.ReplaceAll(path, "/", "_")
	}
	return fmt.Sprintf("%s_%s", host, t.Format("200601021504"))
}

// DbService is the interface for recording sync results.
type DbService interface {
	StartJob(job JobInfo) error
	RecordObject(rec ObjectRecord) error
	EndJob(jobID string, job JobInfo) error
	Close() error
}

// ParseDbDSN extracts the database driver name and DSN from a connection string.
// Supported formats:
//
//	mysql://user:pass@host:port/dbname
func ParseDbDSN(raw string) (driver, dsn string, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", fmt.Errorf("invalid db url: %w", err)
	}
	driver = strings.ToLower(u.Scheme)
	switch driver {
	case "mysql":
		pass, _ := u.User.Password()
		host := u.Host
		if !strings.Contains(host, ":") {
			host += ":3306"
		}
		path := strings.TrimPrefix(u.Path, "/")
		dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
			u.User.Username(), pass, host, path)
	default:
		return "", "", fmt.Errorf("unsupported db driver: %s", driver)
	}
	return
}

// channelSize is the buffer size for the object record channel.
const channelSize = 2048

// batchSize is the number of records to accumulate before flushing to DB.
const batchSize = 100

// flushInterval is the max time between batch flushes.
const flushInterval = time.Second

// AsyncDbService wraps a DbService with a buffered channel, batch writes, and non-blocking sends.
type AsyncDbService struct {
	DbService
	ch      chan ObjectRecord
	wg      sync.WaitGroup
	done    chan struct{}
	closed  bool
	mu      sync.Mutex
	errors  []error
	dropped int64 // count of records dropped due to channel full
	batch   []ObjectRecord
}

// NewAsyncDbService creates an AsyncDbService that buffers and batch-writes object records.
func NewAsyncDbService(svc DbService) *AsyncDbService {
	a := &AsyncDbService{
		DbService: svc,
		ch:        make(chan ObjectRecord, channelSize),
		done:      make(chan struct{}),
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
				// Channel closed, flush remaining
				a.flushBatch()
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
	for _, rec := range a.batch {
		if err := a.DbService.RecordObject(rec); err != nil {
			a.mu.Lock()
			a.errors = append(a.errors, err)
			a.mu.Unlock()
			logger.Errorf("Failed to record object %s: %s", rec.SourceKey, err)
		}
	}
	a.batch = a.batch[:0]
}

// RecordObject sends an object record to the async channel (non-blocking).
// Drops the record if the channel is full (does NOT block sync).
func (a *AsyncDbService) RecordObject(rec ObjectRecord) error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return fmt.Errorf("service closed")
	}
	a.mu.Unlock()
	select {
	case a.ch <- rec:
	default:
		n := atomic.AddInt64(&a.dropped, 1)
		logger.Warnf("DB channel full, dropped record for %s (total dropped: %d)", rec.SourceKey, n)
	}
	return nil
}

// Close flushes remaining records and shuts down the worker.
func (a *AsyncDbService) Close() error {
	a.mu.Lock()
	if !a.closed {
		a.closed = true
		close(a.ch)
	}
	a.mu.Unlock()
	a.wg.Wait()
	close(a.done)
	if n := atomic.LoadInt64(&a.dropped); n > 0 {
		logger.Warnf("Total db records dropped: %d", n)
	}
	return nil
}

// Errors returns any errors that occurred during async writes.
func (a *AsyncDbService) Errors() []error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.errors
}
