package sync

import (
	"sync"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	sync_db "github.com/juicedata/juicefs/pkg/sync/db"
)

// -------------------- 第一层：DB 快速门卫 --------------------

// firstGate 根据 DB 记录快速判断对象是否需要进入第二层。
// 返回值：
//   - GateSkip:          对象已同步成功且源 mtime 未变 → 直接跳过（零目标端 API 调用）
//   - GateNeedSecondGate: 需要调用目标端 Head 做最终确认
//   - GateMissing:        语义同 NeedSecondGate，表示 DB 记录缺失
func firstGate(obj object.Object, record *sync_db.SyncRecordV2, forceUpdate bool) sync_db.GateResult {
	if forceUpdate {
		return sync_db.GateNeedSecondGate
	}

	if record == nil {
		return sync_db.GateMissing
	}

	// 状态不是 success（上次失败或中断）→ 必须重新进入第二层
	if record.Status != "success" {
		logger.Debugf("firstGate: %s status=%s, need second gate", obj.Key(), record.Status)
		return sync_db.GateNeedSecondGate
	}

	// 源 mtime 未变 → 直接跳过（核心优化点：这里不调用目标端 Head）
	if !obj.Mtime().After(record.SourceMtime) {
		logger.Debugf("firstGate: %s mtime unchanged (%s), skip", obj.Key(), record.SourceMtime.Format(time.RFC3339))
		return sync_db.GateSkip
	}

	// mtime 变了 → 放行到第二层，让目标端实时状态做最终裁决
	logger.Debugf("firstGate: %s mtime changed (db=%s obj=%s), need second gate",
		obj.Key(), record.SourceMtime.Format(time.RFC3339), obj.Mtime().Format(time.RFC3339))
	return sync_db.GateNeedSecondGate
}

// -------------------- 第二层：目标端实时门卫 --------------------

// SecondGateAction 是第二层的最终决策。
type SecondGateAction int

const (
	ActionSend SecondGateAction = iota // 发送同步任务
	ActionSkip                         // 跳过（保护目标端新数据）
)

// secondGate 根据目标端实时 Head 结果做最终裁决。
// 设计原则：
//  1. 目标端不存在 → 必须 send（全新对象）
//  2. forceUpdate=true → 必须 send（--force-update 强制覆盖目标端）
//  3. 目标端 mtime >= 源 mtime → 跳过（保护目标端可能被应用修改后的新数据）
//  4. 目标端 mtime < 源 mtime → send（源端更新，需要覆盖）
//
// 注意：这里不对比 size，只对比 mtime。因为用户明确表达了：
// "如果迁移期间应用已经切到目标端并修改了对象，目标 mtime 一定比源新，此时必须跳过"。
// size 仅作为辅助字段写入 DB，不参与决策。
func secondGate(obj object.Object, dstObj object.Object, forceUpdate bool) SecondGateAction {
	if dstObj == nil {
		logger.Debugf("secondGate: %s not exists on dst, send", obj.Key())
		return ActionSend
	}

	if forceUpdate {
		logger.Debugf("secondGate: %s force-update, send (overwrite dst)", obj.Key())
		return ActionSend
	}

	srcMtime := obj.Mtime()
	dstMtime := dstObj.Mtime()

	if dstMtime.After(srcMtime) || dstMtime.Equal(srcMtime) {
		logger.Debugf("secondGate: %s dst mtime(%s) >= src mtime(%s), skip (protect dst)",
			obj.Key(), dstMtime.Format(time.RFC3339), srcMtime.Format(time.RFC3339))
		return ActionSkip
	}

	logger.Debugf("secondGate: %s dst mtime(%s) < src mtime(%s), send",
		obj.Key(), dstMtime.Format(time.RFC3339), srcMtime.Format(time.RFC3339))
	return ActionSend
}

// -------------------- 第一层门卫批量预取 --------------------

// gatedObject 是预取阶段的输出：对象 + 其 gate 记录（不存在或查询失败时为 nil）。
type gatedObject struct {
	obj object.Object
	rec *sync_db.SyncRecordV2
}

// prefetchGateRecords 从 in 按批收集对象，每批一次 GetRecords（IN 查询），
// 把 (obj, record) 送入返回的 channel。相比逐对象 SELECT（N+1 查询，吞吐被钉死在
// 1/RTT），批量预取把 500 次串行查询合并为 1 次，且 DB 查询与 listing 并行。
//
// 出批条件：满 batchSize 个 / flushAfter 内无新对象 / in 关闭（强制出批）。
// 查询失败时记 warning，该批 record 传 nil（落第二门卫，与单查失败行为一致）。
// done 关闭时 goroutine 退出（produce 提前返回时不泄漏）。
func prefetchGateRecords(svc sync_db.DbGateService, in <-chan object.Object, done <-chan struct{}) <-chan gatedObject {
	const batchSize = 500
	const flushAfter = 50 * time.Millisecond
	out := make(chan gatedObject, batchSize)
	go func() {
		defer close(out)
		var objs []object.Object
		// flush 把当前批查询并推送结果；返回 false 表示 done 关闭应退出。
		flush := func() bool {
			if len(objs) == 0 {
				return true
			}
			keys := make([]string, len(objs))
			for i, o := range objs {
				keys[i] = o.Key()
			}
			recs, err := svc.GetRecords(keys)
			if err != nil {
				logger.Warnf("gate prefetch: batch query failed (%d keys), fall back to second gate: %s", len(keys), err)
				recs = nil
			}
			for _, o := range objs {
				select {
				case out <- gatedObject{obj: o, rec: recs[o.Key()]}:
				case <-done:
					return false
				}
			}
			objs = objs[:0]
			return true
		}
	batchLoop:
		for {
			// 收集一批：满 batchSize 或 flushAfter 超时或流结束
			timer := time.NewTimer(flushAfter)
			for len(objs) < batchSize {
				select {
				case obj, ok := <-in:
					if !ok { // 流结束：强制出批后退出
						timer.Stop()
						flush()
						return
					}
					if obj == nil { // listing 失败信号：先出批，再原样转发
						timer.Stop()
						if !flush() {
							return
						}
						select {
						case out <- gatedObject{obj: nil}:
						case <-done:
						}
						return
					}
					objs = append(objs, obj)
				case <-timer.C:
					if !flush() {
						return
					}
					continue batchLoop
				case <-done:
					timer.Stop()
					return
				}
			}
			timer.Stop()
			if !flush() {
				return
			}
		}
	}()
	return out
}

// passThroughObjects 直接把 object channel 包装成 gatedObject channel（rec 恒为 nil），
// 用于门卫查询被禁用的场景，使 produce 主循环只需处理一种类型。
func passThroughObjects(in <-chan object.Object, done <-chan struct{}) <-chan gatedObject {
	out := make(chan gatedObject, 1000)
	go func() {
		defer close(out)
		for obj := range in {
			select {
			case out <- gatedObject{obj: obj}:
			case <-done:
				return
			}
		}
	}()
	return out
}

// -------------------- 批量 DB 写入缓冲 --------------------

// GateRecordBuffer 在 sync 过程中缓冲成功记录，批量写入 DB，避免逐条写拖慢同步。
// 线程安全：Add 和 Flush 可在不同 goroutine 中并发调用。
type GateRecordBuffer struct {
	svc     sync_db.DbGateService
	mu      sync.Mutex
	batch   []*sync_db.SyncRecordV2
	limit   int
	flushCh chan struct{}
	done    chan struct{}
	stopped chan struct{}
	closed  bool
}

// NewGateRecordBuffer 创建缓冲写入器。
// limit: 批量大小（建议 500），flushInterval: 最大刷盘间隔（建议 1s）。
func NewGateRecordBuffer(svc sync_db.DbGateService, limit int, flushInterval time.Duration) *GateRecordBuffer {
	b := &GateRecordBuffer{
		svc:     svc,
		batch:   make([]*sync_db.SyncRecordV2, 0, limit),
		limit:   limit,
		flushCh: make(chan struct{}, 1),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go b.periodicFlush(flushInterval)
	return b
}

func (b *GateRecordBuffer) periodicFlush(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer close(b.stopped)
	for {
		select {
		case <-ticker.C:
			b.Flush()
		case <-b.flushCh:
			b.Flush()
		case <-b.done:
			return
		}
	}
}

// Add 添加一条记录到缓冲。线程安全。
func (b *GateRecordBuffer) Add(rec *sync_db.SyncRecordV2) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.batch = append(b.batch, rec)
	shouldSignal := len(b.batch) >= b.limit
	b.mu.Unlock()

	if shouldSignal {
		select {
		case b.flushCh <- struct{}{}:
		default:
		}
	}
}

// Flush 强制刷盘。线程安全。
// 失败时记录日志，不再将数据塞回 buffer（避免无限堆积和竞争条件）。
func (b *GateRecordBuffer) Flush() error {
	b.mu.Lock()
	if len(b.batch) == 0 {
		b.mu.Unlock()
		return nil
	}
	batch := b.batch
	b.batch = make([]*sync_db.SyncRecordV2, 0, b.limit)
	b.mu.Unlock()

	if err := b.svc.BatchSaveRecords(batch); err != nil {
		logger.Warnf("GateRecordBuffer flush failed (dropped %d records): %s", len(batch), err)
		return err
	}
	return nil
}

// Close 关闭缓冲，刷完剩余数据。线程安全。
func (b *GateRecordBuffer) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.mu.Unlock()

	close(b.done)
	<-b.stopped // 等待 periodicFlush goroutine 退出，避免与最终 Flush 并发
	return b.Flush()
}

// -------------------- 与现有 worker 的集成：记录成功状态 --------------------

// RecordSuccess 在对象成功同步后，构造一条 DB 记录并加入缓冲。
// targetSize 记录目标端存在时的大小（诊断用）；若目标端不存在或未知，传 0。
// 调用点：在 worker() 中成功复制/校验后调用。
func RecordSuccess(buf *GateRecordBuffer, obj object.Object, targetSize int64) {
	if buf == nil {
		return
	}
	buf.Add(&sync_db.SyncRecordV2{
		Key:         obj.Key(),
		SourceMtime: obj.Mtime(),
		SourceSize:  obj.Size(),
		TargetSize:  targetSize,
		Status:      "success",
	})
}

// RecordFailure 在对象失败时，构造一条失败记录并加入缓冲。
// 作用：下次 sync 时第一层看到 status != success，会放行到第二层重新处理。
func RecordFailure(buf *GateRecordBuffer, obj object.Object, err error, targetSize int64) {
	if buf == nil {
		return
	}
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
		if len(errMsg) > 2048 {
			errMsg = errMsg[:2048]
		}
	}
	buf.Add(&sync_db.SyncRecordV2{
		Key:         obj.Key(),
		SourceMtime: obj.Mtime(),
		SourceSize:  obj.Size(),
		TargetSize:  targetSize,
		Status:      "failed",
		ErrorMsg:    errMsg,
	})
}

// RecordSkip 在对象被第二层门卫跳过时记录。
// 主要用于记录"目标端存在且 size 与源不一致"的诊断场景。
// 当第二层门卫发现目标端 mtime 比源新，决定跳过保护目标端数据时调用。
// targetSize 必须传入目标端实际 Head 得到的大小。
// diff 会自动计算：当 targetSize != 0 且 obj.Size() != targetSize 时设为 true。
func RecordSkip(buf *GateRecordBuffer, obj object.Object, targetSize int64) {
	if buf == nil {
		return
	}
	diff := targetSize != 0 && obj.Size() != targetSize
	buf.Add(&sync_db.SyncRecordV2{
		Key:         obj.Key(),
		SourceMtime: obj.Mtime(),
		SourceSize:  obj.Size(),
		TargetSize:  targetSize,
		Diff:        diff,
		Status:      "skipped",
	})
}

// -------------------- 兼容函数 --------------------

// RecordSuccessLegacy 兼容旧签名（不带 targetSize），在 worker 中可直接替换。
func RecordSuccessLegacy(buf *GateRecordBuffer, obj object.Object) {
	RecordSuccess(buf, obj, 0)
}
