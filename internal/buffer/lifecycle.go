package buffer

import (
	"container/heap"
	"context"
	"time"
)

// BackgroundOptions controls the low-priority maintenance loops.
type BackgroundOptions struct {
	FlushInterval       time.Duration
	ShrinkInterval      time.Duration
	MaintenanceInterval time.Duration
}

func DefaultBackgroundOptions() BackgroundOptions {
	return BackgroundOptions{
		FlushInterval:       250 * time.Millisecond,
		ShrinkInterval:      120 * time.Millisecond,
		MaintenanceInterval: 500 * time.Millisecond,
	}
}

// Start launches flushing, gradual shrinking, and metadata maintenance.
// Close cancels the loops and performs a final flush.
func (p *Pool) Start(parent context.Context, opt BackgroundOptions) {
	if opt.FlushInterval <= 0 {
		opt.FlushInterval = 250 * time.Millisecond
	}
	if opt.ShrinkInterval <= 0 {
		opt.ShrinkInterval = 120 * time.Millisecond
	}
	if opt.MaintenanceInterval <= 0 {
		opt.MaintenanceInterval = 500 * time.Millisecond
	}

	ctx, cancel := context.WithCancel(parent)
	p.mu.Lock()
	p.cancel = cancel
	p.mu.Unlock()

	p.wg.Add(3)
	go func() {
		defer p.wg.Done()
		t := time.NewTicker(opt.FlushInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_, _ = p.FlushBatch(p.cfg.FlushBatch)
			}
		}
	}()

	go func() {
		defer p.wg.Done()
		t := time.NewTicker(opt.ShrinkInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				p.mu.Lock()
				batch := p.cfg.ShrinkBatch
				now := p.nowMS(0)
				over := len(p.pages) > p.cfg.Capacity
				p.mu.Unlock()
				if over {
					_, _ = p.ShrinkStep(now, batch)
				}
			}
		}
	}()

	go func() {
		defer p.wg.Done()
		t := time.NewTicker(opt.MaintenanceInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				p.mu.Lock()
				now := p.nowFunc().UnixMilli()
				p.mu.Unlock()
				p.Maintenance(now)
			}
		}
	}()
}

// SetCapacity changes the target pool size. When shrinking, frames are not
// removed in one jump: the shrink loop and future misses apply the configured
// victim rule in batches.
func (p *Pool) SetCapacity(n int) Config {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n < 1 {
		n = 1
	}
	p.cfg.Capacity = n
	p.metrics.CapacityTarget = n
	return p.cfg
}

// ApplyConfigPatch applies only fields present in patch. Changing K rebuilds
// the bounded indexed heap once; ordinary evictions use O(log N) updates/peeks.
func (p *Pool) ApplyConfigPatch(patch ConfigPatch) Config {
	p.mu.Lock()
	defer p.mu.Unlock()

	if patch.Capacity != nil {
		p.cfg.Capacity = *patch.Capacity
	}
	if patch.K != nil {
		p.cfg.K = *patch.K
	}
	if patch.DedupWindowMS != nil {
		p.cfg.DedupWindowMS = *patch.DedupWindowMS
	}
	if patch.GhostTTLMS != nil {
		p.cfg.GhostTTLMS = *patch.GhostTTLMS
	}
	if patch.GhostMax != nil {
		p.cfg.GhostMax = *patch.GhostMax
	}
	if patch.OldMinResidencyMS != nil {
		p.cfg.OldMinResidencyMS = *patch.OldMinResidencyMS
	}
	if patch.SequentialThreshold != nil {
		p.cfg.SequentialThreshold = *patch.SequentialThreshold
	}
	if patch.PrefetchDepth != nil {
		p.cfg.PrefetchDepth = *patch.PrefetchDepth
	}
	if patch.PrefetchUnusedTTLMS != nil {
		p.cfg.PrefetchUnusedTTLMS = *patch.PrefetchUnusedTTLMS
	}
	if patch.ShrinkBatch != nil {
		p.cfg.ShrinkBatch = *patch.ShrinkBatch
	}
	if patch.FlushBatch != nil {
		p.cfg.FlushBatch = *patch.FlushBatch
	}
	if patch.RecentHistoryCount != nil {
		p.cfg.RecentHistoryCount = *patch.RecentHistoryCount
	}
	if patch.StoredHistoryLimit != nil {
		p.cfg.StoredHistoryLimit = *patch.StoredHistoryLimit
	}

	p.cfg = p.cfg.Normalize()
	p.metrics.CapacityTarget = p.cfg.Capacity

	for _, f := range p.pages {
		if len(f.History) > p.cfg.StoredHistoryLimit {
			kept := make([]int64, p.cfg.StoredHistoryLimit)
			copy(kept, f.History[len(f.History)-p.cfg.StoredHistoryLimit:])
			f.History = kept
		}
		p.refreshPriorityLocked(f)
	}
	// One O(N) rebuild only after K/config changes, never per eviction.
	heap.Init(&p.victims)
	for i, f := range p.victims {
		f.victimIndex = i
	}
	return p.cfg
}
