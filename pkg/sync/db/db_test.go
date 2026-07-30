package db

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// mockDbService 记录所有批次，用于验证 AsyncDbService 行为。
type mockDbService struct {
	mu      sync.Mutex
	records []ObjectRecord
}

func (m *mockDbService) StartJob(job JobInfo) error { return nil }
func (m *mockDbService) RecordObject(rec ObjectRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, rec)
	return nil
}
func (m *mockDbService) RecordObjects(recs []ObjectRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, recs...)
	return nil
}
func (m *mockDbService) EndJob(jobID string, job JobInfo) error { return nil }
func (m *mockDbService) Close() error                           { return nil }

func (m *mockDbService) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.records)
}

// TestAsyncDbServiceFlushOnClose 验证 Close 会排空并刷出所有已入队记录。
func TestAsyncDbServiceFlushOnClose(t *testing.T) {
	mock := &mockDbService{}
	a := NewAsyncDbService(mock)

	const n = 1200 // 跨越多个 batch（batchSize=2000 时也在一个 batch 内，但会触发定时 flush）
	for i := 0; i < n; i++ {
		if err := a.RecordObject(ObjectRecord{SourceKey: fmt.Sprintf("key_%d", i)}); err != nil {
			t.Fatalf("RecordObject failed: %v", err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if got := mock.count(); got != n {
		t.Errorf("expected %d records flushed, got %d", n, got)
	}
	// Close 幂等
	if err := a.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
}

// TestAsyncDbServiceCloseWhileSending 回归测试：Close 与并发 RecordObject 共存时不能 panic。
// 原实现 Close 直接 close(ch)，并发发送会 panic: send on closed channel
// （如 SaveOnSignal 在 worker 仍在运行时调用 Close 的场景）。
func TestAsyncDbServiceCloseWhileSending(t *testing.T) {
	mock := &mockDbService{}
	a := NewAsyncDbService(mock)

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				// channel 满时返回错误是允许的（记录被丢弃），但不能 panic
				_ = a.RecordObject(ObjectRecord{SourceKey: fmt.Sprintf("g%d_key_%d", g, i)})
			}
		}(g)
	}

	time.Sleep(5 * time.Millisecond) // 确保 Close 与并发发送重叠
	if err := a.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	wg.Wait() // 若 sendSafe panic，测试进程直接崩溃失败

	// Close 后再发送不应 panic（记录被静默丢弃）
	_ = a.RecordObject(ObjectRecord{SourceKey: "after_close"})
}
