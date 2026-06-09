package db

import (
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
	ID             string // dest_bucket_202606031430
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
}

// GenerateJobID creates a job ID from the destination bucket name and current time.
func GenerateJobID(dstURL string, t time.Time) string {
	u, err := url.Parse(dstURL)
	if err != nil {
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
	host := u.Host
	if idx := strings.Index(host, "."); idx > 0 {
		host = host[:idx]
	}
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
	return &DbConfig{
		Driver: driver,
		Host:   host,
		User:   u.User.Username(),
		Pass:   pass,
	}, nil
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
	ch     chan ObjectRecord
	wg     sync.WaitGroup
	closed bool
	mu     sync.Mutex
	errors []error
	batch  []ObjectRecord
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

func sendSafe(ch chan ObjectRecord, rec ObjectRecord) (sent bool) {
	defer func() { recover() }()
	select {
	case ch <- rec:
		return true
	default:
		return false
	}
}

// RecordObject sends an object record to the async channel (non-blocking).
func (a *AsyncDbService) RecordObject(rec ObjectRecord) error {
	if !sendSafe(a.ch, rec) {
		a.mu.Lock()
		a.errors = append(a.errors, fmt.Errorf("RecordObject dropped: %s", rec.SourceKey))
		a.mu.Unlock()
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
