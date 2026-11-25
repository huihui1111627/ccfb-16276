package buffer

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"catalogbuffer/internal/disk"
)

// AccessInput is one query reference to a page.
type AccessInput struct {
	TxID    string
	QueryID string
	// QueryType records the class that caused the fetch: hotspot/scan/random.
	QueryType string
	AtMS      int64

	// Write mutates the cached page and marks it dirty.
	Write    bool
	WriteTag int64
}

// AccessResult reports the outcome of one reference.
type AccessResult struct {
	Block        disk.Block
	Hit          bool
	Duplicate    bool
	GhostRevived bool
	PrefetchHit  bool
	DiskRead     bool
	EvictedPage  disk.PageID
}

// WriteObserver is invoked while the pool lock is held after a logical write
// commits. It gives tests a race-free oracle of the latest acknowledged write.
type WriteObserver func(id disk.PageID, b disk.Block, lsn int64)

type txPageKey struct {
	tx string
	id disk.PageID
}

type ghostInfo struct {
	History     []int64
	EvictedAt   int64
	FetchClass  string
	AccessCount int
	Segment     int
}

type ghostQueueItem struct {
	id        disk.PageID
	evictedAt int64
}

type seqState struct {
	LastID disk.PageID
	Stride disk.PageID
	Run    int
	LastAt int64
}

// Pool is the simulated buffer pool. All exported state access is safe for
// concurrent query goroutines.
type Pool struct {
	mu  sync.Mutex
	cfg Config

	disk     *disk.Disk
	observer WriteObserver
	nowFunc  func() time.Time

	pages         map[disk.PageID]*Frame
	victims       victimHeap
	flushQueue    flushHeap
	prefetchQueue prefetchHeap

	dedup map[txPageKey]int64

	ghost      map[disk.PageID]*ghostInfo
	ghostOrder []ghostQueueItem

	seq map[string]*seqState

	usage map[string]*TypeUsage

	metrics Metrics
	lsn     int64

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(cfg Config, d *disk.Disk, observer WriteObserver) *Pool {
	cfg = cfg.Normalize()
	p := &Pool{
		cfg:           cfg,
		disk:          d,
		observer:      observer,
		nowFunc:       time.Now,
		pages:         make(map[disk.PageID]*Frame, cfg.Capacity),
		victims:       make(victimHeap, 0, cfg.Capacity),
		flushQueue:    make(flushHeap, 0),
		prefetchQueue: make(prefetchHeap, 0),
		dedup:         make(map[txPageKey]int64),
		ghost:         make(map[disk.PageID]*ghostInfo),
		seq:           make(map[string]*seqState),
		usage:         make(map[string]*TypeUsage),
	}
	p.metrics.CapacityTarget = cfg.Capacity
	return p
}

func (p *Pool) nowMS(input int64) int64 {
	if input > 0 {
		return input
	}
	return p.nowFunc().UnixMilli()
}

func (p *Pool) Config() Config {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cfg
}

// SetNowFunc overrides the wall clock. Production code leaves it as time.Now.
func (p *Pool) SetNowFunc(fn func() time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if fn != nil {
		p.nowFunc = fn
	}
}

func (p *Pool) classUsageLocked(class string) *TypeUsage {
	if class == "" {
		class = "unknown"
	}
	u := p.usage[class]
	if u == nil {
		u = &TypeUsage{QueryType: class}
		p.usage[class] = u
	}
	return u
}

// Access reads or writes a page through the buffer pool.
func (p *Pool) Access(id disk.PageID, in AccessInput) (AccessResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.nowMS(in.AtMS)
	class := in.QueryType
	if class == "" {
		class = "unknown"
	}

	existing, cached := p.pages[id]
	res := AccessResult{Hit: cached}
	p.metrics.Requests++
	if cached {
		p.metrics.Hits++
		if existing.Prefetch && !existing.PrefetchUsed {
			res.PrefetchHit = true
			p.metrics.PrefetchHits++
			p.markPrefetchUsedLocked(existing)
		}
	} else {
		p.metrics.Misses++
	}

	// Rule 2: collapse repeated references from one transaction/page in a
	// short window. Data writes still apply even when policy access is folded.
	duplicate := false
	if in.TxID != "" {
		key := txPageKey{tx: in.TxID, id: id}
		last, ok := p.dedup[key]
		if ok && now-last <= p.cfg.DedupWindowMS {
			duplicate = true
			res.Duplicate = true
			p.metrics.DuplicateTxAccesses++
		}
		// Slide the short burst window as adjacent references arrive.
		p.dedup[key] = now
	}

	if !duplicate {
		p.metrics.PolicyAccesses++
		if cached {
			p.metrics.PolicyHits++
		} else {
			p.metrics.PolicyMisses++
		}
	}

	f := existing
	if !cached {
		evicted, err := p.makeRoomLocked(now, "demand")
		if err != nil {
			return res, err
		}
		res.EvictedPage = evicted

		blk, err := p.disk.Read(id)
		if err != nil {
			return res, err
		}
		p.metrics.DiskReads++
		res.DiskRead = true

		f = &Frame{
			ID:            id,
			Block:         blk,
			InsertedAt:    now,
			FetchClass:    class,
			Segment:       SegmentOld,
			victimIndex:   -1,
			flushIndex:    -1,
			prefetchIndex: -1,
		}

		if g, ok := p.ghost[id]; ok && now-g.EvictedAt <= p.cfg.GhostTTLMS {
			f.History = append([]int64(nil), g.History...)
			f.AccessCount = g.AccessCount
			// Segment is deliberately reset to old: the page must re-earn
			// young status after re-entering memory.
			res.GhostRevived = true
			p.metrics.GhostRevivals++
			delete(p.ghost, id)
		} else if ok {
			delete(p.ghost, id)
		}

		p.pages[id] = f
		p.refreshPriorityLocked(f)
		heap.Push(&p.victims, f)
		p.classUsageLocked(class).addFrame(f)
		p.metrics.Frames = len(p.pages)

		if !duplicate {
			p.recordAcceptedAccessLocked(f, now)
		} else {
			p.refreshPriorityLocked(f)
			heap.Fix(&p.victims, f.victimIndex)
		}
	} else if !duplicate {
		p.recordAcceptedAccessLocked(f, now)
	}

	if in.Write {
		p.applyWriteLocked(f, now, in.WriteTag)
	}

	// Sequential detection is based on real query references, not internal
	// prefetch insertions.
	if in.QueryID != "" || in.TxID != "" {
		streamKey := in.QueryID
		if streamKey == "" {
			streamKey = in.TxID
		}
		p.observeSequentialLocked(streamKey, id, now, class)
	}

	res.Block = f.Block
	return res, nil
}

func (p *Pool) markPrefetchUsedLocked(f *Frame) {
	if !f.PrefetchUsed {
		f.PrefetchUsed = true
		p.metrics.PrefetchUsed++
		if u := p.usage[f.FetchClass]; u != nil {
			u.UnusedPrefetch--
		}
		if f.Prefetch && f.prefetchIndex >= 0 {
			heap.Remove(&p.prefetchQueue, f.prefetchIndex)
		}
		p.refreshPriorityLocked(f)
		if f.victimIndex >= 0 {
			heap.Fix(&p.victims, f.victimIndex)
		}
	}
}

func (p *Pool) recordAcceptedAccessLocked(f *Frame, now int64) {
	f.AccessCount++
	f.LastAccess = now
	f.History = append(f.History, now)
	if limit := p.cfg.StoredHistoryLimit; limit > 0 && len(f.History) > limit {
		// Copy the retained tail so the beginning of the backing array can be
		// released and frame memory stays bounded.
		kept := make([]int64, limit)
		copy(kept, f.History[len(f.History)-limit:])
		f.History = kept
	}

	// Rule 4: a page starts in old. An access after sufficient old-segment
	// residency is the only thing that promotes it to young.
	if f.Segment == SegmentOld && now-f.InsertedAt >= p.cfg.OldMinResidencyMS {
		f.Segment = SegmentYoung
	}

	p.refreshPriorityLocked(f)
	if f.victimIndex >= 0 {
		heap.Fix(&p.victims, f.victimIndex)
	}
}

func (p *Pool) applyWriteLocked(f *Frame, now int64, tag int64) {
	p.lsn++
	b := &f.Block
	b.Version++
	b.WriteLSN = p.lsn

	// Encode enough distinct values to catch torn writes, stale copies, and
	// page-id mix-ups in the final byte-for-byte verification.
	for off := 0; off+8 <= 32; off += 8 {
		var v int64
		switch off {
		case 0:
			v = p.lsn
		case 8:
			v = tag
		case 16:
			v = int64(f.ID)
		default:
			v = b.Version
		}
		for i := 0; i < 8; i++ {
			b.Data[off+i] = byte(v >> (8 * i))
		}
	}
	for i := 32; i < disk.PageSize; i++ {
		b.Data[i] = byte(p.lsn*131 + tag*17 + int64(f.ID)*31 + int64(i)*7)
	}
	b.Seal()

	f.WriteLSN = p.lsn
	if !f.Dirty {
		f.Dirty = true
		f.DirtySince = now
		p.metrics.DirtyFrames++
		if u := p.usage[f.FetchClass]; u != nil {
			u.DirtyFrames++
		}
		p.enqueueFlushLocked(f)
	} else if f.FlushQueued && f.flushIndex >= 0 {
		// WriteLSN is a tie-break key in the flush heap; repeated writes must
		// restore its heap invariant.
		heap.Fix(&p.flushQueue, f.flushIndex)
	}
	if p.observer != nil {
		p.observer(f.ID, f.Block, p.lsn)
	}
}

func (p *Pool) enqueueFlushLocked(f *Frame) {
	if !f.FlushQueued {
		f.FlushQueued = true
		heap.Push(&p.flushQueue, f)
		p.metrics.FlushQueue = p.flushQueue.Len()
	}
}

// flushFrameLocked persists one dirty frame. The simulated disk is in-memory
// and goroutine-safe; its accounted latency is exposed via disk.Stats.
func (p *Pool) flushFrameLocked(f *Frame) error {
	if !f.Dirty {
		return nil
	}
	blk := f.Block
	if err := p.disk.Write(blk); err != nil {
		return err
	}
	if f.FlushQueued && f.flushIndex >= 0 {
		heap.Remove(&p.flushQueue, f.flushIndex)
	}
	f.Dirty = false
	f.FlushQueued = false
	f.flushIndex = -1
	p.metrics.DirtyFrames--
	p.metrics.FlushWrites++
	p.metrics.DiskWrites++
	if u := p.usage[f.FetchClass]; u != nil {
		u.DirtyFrames--
	}
	p.metrics.FlushQueue = p.flushQueue.Len()
	return nil
}

func (p *Pool) makeRoomLocked(now int64, reason string) (disk.PageID, error) {
	return p.makeRoomForLocked(now, 1, reason)
}

func (p *Pool) makeRoomForLocked(now int64, extra int, reason string) (disk.PageID, error) {
	if p.cfg.Capacity <= 0 {
		return -1, errors.New("buffer pool capacity must be positive")
	}
	if extra <= 0 {
		return -1, nil
	}

	evictions := 0
	if len(p.pages) > p.cfg.Capacity {
		// A runtime shrink is already pending. Admission is one-for-one (or
		// batch-for-batch for prefetch planning); it must not drain the whole
		// overage in one miss. ShrinkStep/background loop reclaims gradually.
		evictions = extra
	} else {
		evictions = len(p.pages) + extra - p.cfg.Capacity
	}
	if evictions < 0 {
		evictions = 0
	}
	if evictions > p.victims.Len() {
		evictions = p.victims.Len()
	}

	var evicted disk.PageID = -1
	for i := 0; i < evictions; i++ {
		id, err := p.evictOneLocked(now, reason)
		if err != nil {
			return -1, err
		}
		evicted = id
	}
	return evicted, nil
}

func (p *Pool) evictOneLocked(now int64, reason string) (disk.PageID, error) {
	if p.victims.Len() == 0 {
		return -1, errors.New("cannot evict: victim heap is empty")
	}
	f := p.victims[0]
	return p.evictFrameLocked(f, now, reason)
}

func (p *Pool) evictFrameLocked(f *Frame, now int64, reason string) (disk.PageID, error) {
	id := f.ID
	if f.Dirty {
		if err := p.flushFrameLocked(f); err != nil {
			return -1, fmt.Errorf("flush dirty page %d before eviction: %w", id, err)
		}
		p.metrics.DirtyEvictions++
	}

	if f.victimIndex >= 0 {
		heap.Remove(&p.victims, f.victimIndex)
	}
	if f.Prefetch && f.prefetchIndex >= 0 {
		heap.Remove(&p.prefetchQueue, f.prefetchIndex)
	}
	if f.Prefetch && !f.PrefetchUsed {
		p.metrics.PrefetchWasted++
	}

	// Rule 3: keep only the metadata/history, not the data page.
	if p.cfg.GhostTTLMS > 0 {
		hist := append([]int64(nil), f.History...)
		p.ghost[id] = &ghostInfo{
			History:     hist,
			EvictedAt:   now,
			FetchClass:  f.FetchClass,
			AccessCount: f.AccessCount,
			Segment:     f.Segment,
		}
		p.ghostOrder = append(p.ghostOrder, ghostQueueItem{id: id, evictedAt: now})
		p.trimGhostsLocked(now)
	}

	p.classUsageLocked(f.FetchClass).removeFrame(f)
	delete(p.pages, id)
	p.metrics.Evictions++
	if reason == "shrink" {
		p.metrics.ShrinkEvictions++
	}
	p.metrics.Frames = len(p.pages)
	p.metrics.GhostEntries = len(p.ghost)
	return id, nil
}

// Metrics returns a consistent counters snapshot.
func (p *Pool) Metrics() Metrics {
	p.mu.Lock()
	defer p.mu.Unlock()
	m := p.metrics
	m.Frames = len(p.pages)
	m.GhostEntries = len(p.ghost)
	m.DirtyFrames = p.countDirtyLocked()
	m.FlushQueue = p.flushQueue.Len()
	m.CapacityTarget = p.cfg.Capacity
	m.CurrentLSN = p.lsn
	return m
}

func (p *Pool) countDirtyLocked() int {
	n := 0
	for _, f := range p.pages {
		if f.Dirty {
			n++
		}
	}
	return n
}

// FlushAll writes every dirty page. It is used during orderly shutdown and
// final consistency verification.
func (p *Pool) FlushAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var firstErr error
	for _, f := range p.pages {
		if f.Dirty {
			if err := p.flushFrameLocked(f); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Close stops background work and flushes all dirty pages.
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.cancel != nil {
		p.cancel()
	}
	p.mu.Unlock()
	p.wg.Wait()
	return p.FlushAll()
}

// ShrinkStep evicts at most batch pages while above the configured target.
// It is split out so deterministic tests can advance shrinking without sleeping.
func (p *Pool) ShrinkStep(now int64, batch int) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if batch <= 0 {
		batch = p.cfg.ShrinkBatch
	}
	n := 0
	for n < batch && len(p.pages) > p.cfg.Capacity {
		if _, err := p.evictOneLocked(now, "shrink"); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// FlushBatch writes at most n oldest dirty/queued pages.
func (p *Pool) FlushBatch(n int) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n <= 0 {
		n = p.cfg.FlushBatch
	}
	flushed := 0
	for flushed < n && p.flushQueue.Len() > 0 {
		f := p.flushQueue[0]
		if err := p.flushFrameLocked(f); err != nil {
			return flushed, err
		}
		flushed++
		p.metrics.BackgroundFlush++
	}
	return flushed, nil
}

// Maintenance expires ghosts, transaction dedup keys, stale sequential
// detectors, and early-reclaims expired unused prefetches.
func (p *Pool) Maintenance(now int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reapPrefetchesLocked(now)
	p.expireGhostsLocked(now)
	p.expireDedupLocked(now)
	p.expireSeqLocked(now)
}

func (p *Pool) expireDedupLocked(now int64) {
	// Map sizes are bounded by active transaction/page bursts in the simulator.
	// Periodic cleanup prevents unbounded growth in the long-running server.
	for k, t := range p.dedup {
		if now-t > p.cfg.DedupWindowMS*4+1_000 {
			delete(p.dedup, k)
		}
	}
}

func (p *Pool) expireSeqLocked(now int64) {
	for k, st := range p.seq {
		if now-st.LastAt > 30_000 {
			delete(p.seq, k)
		}
	}
}
