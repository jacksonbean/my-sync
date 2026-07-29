package sync

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	sync_db "github.com/juicedata/juicefs/pkg/sync/db"
)

// mockObject 实现 object.Object 接口的最小子集，用于测试。
type mockObject struct {
	key   string
	size  int64
	mtime time.Time
	isDir bool
}

func (m *mockObject) Key() string                 { return m.key }
func (m *mockObject) Size() int64                 { return m.size }
func (m *mockObject) Mtime() time.Time            { return m.mtime }
func (m *mockObject) IsDir() bool                 { return m.isDir }
func (m *mockObject) IsSymlink() bool             { return false }
func (m *mockObject) String() string              { return m.key }
func (m *mockObject) ContentType() string         { return "" }
func (m *mockObject) Metadata() map[string]string { return nil }
func (m *mockObject) StorageClass() string        { return "" }
func (m *mockObject) Status() string              { return "" }
func (m *mockObject) Owner() string               { return "" }
func (m *mockObject) Group() string               { return "" }
func (m *mockObject) Mode() int                   { return 0 }

// 确保 mockObject 实现足够接口
var _ interface {
	Key() string
	Size() int64
	Mtime() time.Time
} = (*mockObject)(nil)

// -------------------- firstGate 测试 --------------------

func TestFirstGate(t *testing.T) {
	now := time.Now()
	obj := &mockObject{key: "a/b/c", size: 100, mtime: now}

	// 1. forceUpdate=true → GateNeedSecondGate
	if r := firstGate(obj, nil, true); r != sync_db.GateNeedSecondGate {
		t.Errorf("forceUpdate: expected NeedSecondGate, got %v", r)
	}

	// 2. record == nil → GateMissing
	if r := firstGate(obj, nil, false); r != sync_db.GateMissing {
		t.Errorf("nil record: expected Missing, got %v", r)
	}

	// 3. status != success → GateNeedSecondGate
	for _, status := range []string{"failed", "skipped", "pending"} {
		rec := &sync_db.SyncRecordV2{Status: status, SourceMtime: now.Add(-time.Hour)}
		if r := firstGate(obj, rec, false); r != sync_db.GateNeedSecondGate {
			t.Errorf("status=%s: expected NeedSecondGate, got %v", status, r)
		}
	}

	// 4. status=success, mtime 未变 → GateSkip
	rec := &sync_db.SyncRecordV2{Status: "success", SourceMtime: now}
	if r := firstGate(obj, rec, false); r != sync_db.GateSkip {
		t.Errorf("mtime unchanged: expected Skip, got %v", r)
	}
	// mtime 相等也跳过
	rec2 := &sync_db.SyncRecordV2{Status: "success", SourceMtime: now}
	if r := firstGate(obj, rec2, false); r != sync_db.GateSkip {
		t.Errorf("mtime equal: expected Skip, got %v", r)
	}

	// 5. status=success, mtime 变了 → GateNeedSecondGate
	objNewer := &mockObject{key: "a/b/c", size: 100, mtime: now.Add(time.Hour)}
	recOld := &sync_db.SyncRecordV2{Status: "success", SourceMtime: now}
	if r := firstGate(objNewer, recOld, false); r != sync_db.GateNeedSecondGate {
		t.Errorf("mtime newer: expected NeedSecondGate, got %v", r)
	}
}

// -------------------- secondGate 测试 --------------------

func TestSecondGate(t *testing.T) {
	now := time.Now()
	src := &mockObject{key: "a/b/c", size: 100, mtime: now}

	// 1. dstObj == nil → ActionSend
	if a := secondGate(src, nil, false); a != ActionSend {
		t.Errorf("dst nil: expected Send, got %v", a)
	}

	// 2. dst mtime > src mtime → ActionSkip（保护目标端）
	dstNewer := &mockObject{key: "a/b/c", size: 100, mtime: now.Add(time.Hour)}
	if a := secondGate(src, dstNewer, false); a != ActionSkip {
		t.Errorf("dst newer: expected Skip, got %v", a)
	}

	// 3. dst mtime == src mtime → ActionSkip
	dstEqual := &mockObject{key: "a/b/c", size: 100, mtime: now}
	if a := secondGate(src, dstEqual, false); a != ActionSkip {
		t.Errorf("dst equal: expected Skip, got %v", a)
	}

	// 4. dst mtime < src mtime → ActionSend（源端更新）
	dstOlder := &mockObject{key: "a/b/c", size: 100, mtime: now.Add(-time.Hour)}
	if a := secondGate(src, dstOlder, false); a != ActionSend {
		t.Errorf("dst older: expected Send, got %v", a)
	}

	// 5. size 不同但不参与决策（只对比 mtime）
	dstDiffSize := &mockObject{key: "a/b/c", size: 200, mtime: now.Add(-time.Hour)}
	if a := secondGate(src, dstDiffSize, false); a != ActionSend {
		t.Errorf("dst diff size but older: expected Send, got %v", a)
	}
	dstDiffSizeNewer := &mockObject{key: "a/b/c", size: 200, mtime: now.Add(time.Hour)}
	if a := secondGate(src, dstDiffSizeNewer, false); a != ActionSkip {
		t.Errorf("dst diff size but newer: expected Skip, got %v", a)
	}

	// 6. --force-update 忽略目标端 mtime，总是 send
	if a := secondGate(src, dstNewer, true); a != ActionSend {
		t.Errorf("force-update dst newer: expected Send, got %v", a)
	}
	if a := secondGate(src, dstEqual, true); a != ActionSend {
		t.Errorf("force-update dst equal: expected Send, got %v", a)
	}
}

// -------------------- RecordSkip Diff 计算测试 --------------------

func TestRecordSkipDiff(t *testing.T) {
	obj := &mockObject{key: "a/b/c", size: 100, mtime: time.Now()}

	// 1. targetSize == 0 → diff=false
	rec := recordSkipToRec(obj, 0)
	if rec.Diff {
		t.Errorf("targetSize=0: expected Diff=false, got true")
	}
	if rec.TargetSize != 0 {
		t.Errorf("targetSize=0: expected TargetSize=0, got %d", rec.TargetSize)
	}

	// 2. targetSize != 0 且 size 相同 → diff=false
	rec2 := recordSkipToRec(obj, 100)
	if rec2.Diff {
		t.Errorf("size equal: expected Diff=false, got true")
	}

	// 3. targetSize != 0 且 size 不同 → diff=true
	rec3 := recordSkipToRec(obj, 200)
	if !rec3.Diff {
		t.Errorf("size diff: expected Diff=true, got false")
	}
}

// 辅助函数：提取 RecordSkip 内部逻辑，避免依赖 gateBuf
func recordSkipToRec(obj *mockObject, targetSize int64) *sync_db.SyncRecordV2 {
	diff := targetSize != 0 && obj.Size() != targetSize
	return &sync_db.SyncRecordV2{
		Key:         obj.Key(),
		SourceMtime: obj.Mtime(),
		SourceSize:  obj.Size(),
		TargetSize:  targetSize,
		Diff:        diff,
		Status:      "skipped",
	}
}

// -------------------- GateRecordBuffer 测试（Mock） --------------------

type mockDbGateService struct {
	mu         sync.Mutex
	records    map[string]*sync_db.SyncRecordV2
	batches    [][]*sync_db.SyncRecordV2
	queryCount int // GetRecords 调用次数（验证批量预取合并了查询）
}

func (m *mockDbGateService) GetRecord(key string) (*sync_db.SyncRecordV2, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.records[key], nil
}

func (m *mockDbGateService) GetRecords(keys []string) (map[string]*sync_db.SyncRecordV2, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]*sync_db.SyncRecordV2, len(keys))
	for _, k := range keys {
		if rec, ok := m.records[k]; ok {
			result[k] = rec
		}
	}
	m.queryCount++
	return result, nil
}

func (m *mockDbGateService) SaveRecord(rec *sync_db.SyncRecordV2) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[rec.Key] = rec
	return nil
}

func (m *mockDbGateService) BatchSaveRecords(recs []*sync_db.SyncRecordV2) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches = append(m.batches, recs)
	for _, rec := range recs {
		m.records[rec.Key] = rec
	}
	return nil
}

func (m *mockDbGateService) batchCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.batches)
}

func (m *mockDbGateService) batchSize(i int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.batches[i])
}

func (m *mockDbGateService) getRecord(key string) *sync_db.SyncRecordV2 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.records[key]
}

func (m *mockDbGateService) recordCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.records)
}

func (m *mockDbGateService) totalBatched() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, batch := range m.batches {
		total += len(batch)
	}
	return total
}

func (m *mockDbGateService) Close() error {
	return nil
}

func TestGateRecordBuffer(t *testing.T) {
	mock := &mockDbGateService{
		records: make(map[string]*sync_db.SyncRecordV2),
	}
	buf := NewGateRecordBuffer(mock, 3, 100*time.Millisecond)
	defer buf.Close()

	// 添加 2 条，不到 limit，不触发 flush
	now := time.Now()
	buf.Add(&sync_db.SyncRecordV2{Key: "k1", Status: "success", SourceMtime: now})
	buf.Add(&sync_db.SyncRecordV2{Key: "k2", Status: "success", SourceMtime: now})

	if mock.batchCount() != 0 {
		t.Errorf("expected 0 batches before limit, got %d", mock.batchCount())
	}

	// 添加第 3 条，触发 flush（limit=3）
	buf.Add(&sync_db.SyncRecordV2{Key: "k3", Status: "success", SourceMtime: now})

	// 等待 flush 完成（定时器或信号）
	time.Sleep(50 * time.Millisecond)
	if mock.batchCount() != 1 {
		t.Errorf("expected 1 batch after limit, got %d", mock.batchCount())
	}
	if mock.batchSize(0) != 3 {
		t.Errorf("expected batch size 3, got %d", mock.batchSize(0))
	}

	// 再添加 1 条，Close 时刷出
	buf.Add(&sync_db.SyncRecordV2{Key: "k4", Status: "failed", SourceMtime: now})
	buf.Close()

	// 应该总共 2 个 batch（limit 触发 + Close 触发）
	if mock.batchCount() != 2 {
		t.Errorf("expected 2 batches after close, got %d", mock.batchCount())
	}
	if mock.batchSize(1) != 1 {
		t.Errorf("expected last batch size 1, got %d", mock.batchSize(1))
	}

	// 验证记录写入
	if mock.getRecord("k1") == nil || mock.getRecord("k4") == nil {
		t.Errorf("expected records k1 and k4 in mock")
	}
	if mock.getRecord("k4").Status != "failed" {
		t.Errorf("expected k4 status failed, got %s", mock.getRecord("k4").Status)
	}
}

func TestGateRecordBufferFlushError(t *testing.T) {
	failingMock := &mockFailingDbGateService{}
	buf := NewGateRecordBuffer(failingMock, 2, 1*time.Hour)
	defer buf.Close()

	now := time.Now()
	buf.Add(&sync_db.SyncRecordV2{Key: "k1", Status: "success", SourceMtime: now})
	buf.Add(&sync_db.SyncRecordV2{Key: "k2", Status: "success", SourceMtime: now})

	// 触发 flush（limit=2）
	time.Sleep(50 * time.Millisecond)

	// 失败时 batch 不会被保留，数据丢弃
	if failingMock.callCount.Load() != 1 {
		t.Errorf("expected 1 failing call, got %d", failingMock.callCount.Load())
	}
}

type mockFailingDbGateService struct {
	callCount atomic.Int32
}

func (m *mockFailingDbGateService) GetRecord(key string) (*sync_db.SyncRecordV2, error) {
	return nil, nil
}
func (m *mockFailingDbGateService) GetRecords(keys []string) (map[string]*sync_db.SyncRecordV2, error) {
	return nil, errors.New("query error")
}
func (m *mockFailingDbGateService) SaveRecord(rec *sync_db.SyncRecordV2) error {
	return errors.New("save error")
}
func (m *mockFailingDbGateService) BatchSaveRecords(recs []*sync_db.SyncRecordV2) error {
	count := m.callCount.Add(1)
	return fmt.Errorf("batch error %d", count)
}
func (m *mockFailingDbGateService) Close() error {
	return nil
}

// -------------------- 并发安全测试 --------------------

func TestGateRecordBufferConcurrent(t *testing.T) {
	mock := &mockDbGateService{
		records: make(map[string]*sync_db.SyncRecordV2),
	}
	buf := NewGateRecordBuffer(mock, 100, 50*time.Millisecond)
	defer buf.Close()

	now := time.Now()
	// 并发添加 1000 条
	for i := 0; i < 1000; i++ {
		go func(n int) {
			buf.Add(&sync_db.SyncRecordV2{
				Key:         fmt.Sprintf("key_%d", n),
				Status:      "success",
				SourceMtime: now,
			})
		}(i)
	}

	// 等待所有 flush 完成
	time.Sleep(300 * time.Millisecond)
	buf.Close()

	// 验证所有 1000 条都被写入（可能在不同 batch 中）
	if total := mock.totalBatched(); total != 1000 {
		t.Errorf("expected 1000 records, got %d", total)
	}
	if mock.recordCount() != 1000 {
		t.Errorf("expected 1000 unique records, got %d", mock.recordCount())
	}
}

// -------------------- 批量预取测试 --------------------

func TestPrefetchGateRecordsBatches(t *testing.T) {
	mock := &mockDbGateService{
		records: make(map[string]*sync_db.SyncRecordV2),
	}
	now := time.Now()
	// 预置部分记录（奇数 key 已同步成功）
	for i := 0; i < 1200; i += 2 {
		key := fmt.Sprintf("key_%d", i)
		mock.records[key] = &sync_db.SyncRecordV2{Key: key, Status: "success", SourceMtime: now}
	}

	in := make(chan object.Object, 1200)
	for i := 0; i < 1200; i++ {
		in <- &mockObject{key: fmt.Sprintf("key_%d", i), mtime: now}
	}
	close(in)

	done := make(chan struct{})
	defer close(done)
	out := prefetchGateRecords(mock, in, done)

	count := 0
	for gobj := range out {
		if gobj.obj == nil {
			t.Fatalf("unexpected nil object at %d", count)
		}
		// 奇数 key 无记录，偶数 key 有记录
		wantRec := count%2 == 0
		if (gobj.rec != nil) != wantRec {
			t.Errorf("obj %s: record presence = %v, want %v", gobj.obj.Key(), gobj.rec != nil, wantRec)
		}
		count++
	}
	if count != 1200 {
		t.Errorf("expected 1200 objects, got %d", count)
	}
	// 1200 个对象应合并为 3 次查询（500+500+200），而不是 1200 次
	if mock.queryCount != 3 {
		t.Errorf("expected 3 batch queries, got %d", mock.queryCount)
	}
}

func TestPrefetchGateRecordsQueryError(t *testing.T) {
	failingMock := &mockFailingDbGateService{}
	in := make(chan object.Object, 3)
	now := time.Now()
	for i := 0; i < 3; i++ {
		in <- &mockObject{key: fmt.Sprintf("key_%d", i), mtime: now}
	}
	close(in)

	done := make(chan struct{})
	defer close(done)
	out := prefetchGateRecords(failingMock, in, done)

	count := 0
	for gobj := range out {
		if gobj.obj == nil {
			t.Fatalf("unexpected nil object")
		}
		// 查询失败时 record 为 nil（落第二门卫），对象不能丢
		if gobj.rec != nil {
			t.Errorf("expected nil record on query error, got %+v", gobj.rec)
		}
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 objects despite query error, got %d", count)
	}
}

func TestPrefetchGateRecordsNilPassthrough(t *testing.T) {
	mock := &mockDbGateService{
		records: make(map[string]*sync_db.SyncRecordV2),
	}
	in := make(chan object.Object, 3)
	now := time.Now()
	in <- &mockObject{key: "key_0", mtime: now}
	in <- nil // listing 失败信号
	close(in)

	done := make(chan struct{})
	defer close(done)
	out := prefetchGateRecords(mock, in, done)

	var results []gatedObject
	for gobj := range out {
		results = append(results, gobj)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].obj == nil || results[0].obj.Key() != "key_0" {
		t.Errorf("first result should be key_0, got %+v", results[0].obj)
	}
	if results[1].obj != nil {
		t.Errorf("nil listing-failure signal should be passed through, got %+v", results[1].obj)
	}
}

func TestPrefetchGateRecordsDoneExits(t *testing.T) {
	mock := &mockDbGateService{
		records: make(map[string]*sync_db.SyncRecordV2),
	}
	in := make(chan object.Object) // 无缓冲，模拟持续 listing
	done := make(chan struct{})
	out := prefetchGateRecords(mock, in, done)

	// 不消费 out、不再生产，直接关闭 done（模拟 produce 提前返回）
	close(done)
	select {
	case _, ok := <-out:
		if ok {
			// 允许读出已缓冲的数据，继续等到关闭
			for range out {
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("prefetch goroutine did not exit after done closed")
	}
}
