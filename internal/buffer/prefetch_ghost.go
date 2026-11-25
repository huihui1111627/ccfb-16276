package buffer

import (
	"container/heap"

	"catalogbuffer/internal/disk"
)

func absPageDelta(x disk.PageID) disk.PageID {
	if x < 0 {
		return -x
	}
	return x
}

func (p *Pool) observeSequentialLocked(key string, id disk.PageID, now int64, class string) {
	if key == "" {
		key = class
	}
	st := p.seq[key]
	if st == nil {
		p.seq[key] = &seqState{LastID: id, Run: 1, LastAt: now}
		return
	}

	delta := id - st.LastID
	if delta != 0 && absPageDelta(delta) <= 64 {
		if delta == st.Stride {
			st.Run++
		} else {
			// Two observations define a candidate stride; the next matching
			// one reaches the default threshold of three consecutive pages.
			st.Stride = delta
			st.Run = 2
		}
	} else {
		st.Stride = 0
		st.Run = 1
	}
	st.LastID = id
	st.LastAt = now

	if st.Run >= p.cfg.SequentialThreshold && p.cfg.PrefetchDepth > 0 {
		p.prefetchAheadLocked(st, id, now, class)
	}
}

func (p *Pool) prefetchAheadLocked(st *seqState, id disk.PageID, now int64, class string) {
	if st.Stride == 0 || p.cfg.PrefetchDepth <= 0 || p.cfg.Capacity < 2 {
		return
	}
	// Never plan a batch large enough to evict the currently active page and
	// then run the heap dry on an extreme runtime capacity/depth combination.
	depth := p.cfg.PrefetchDepth
	if maxDepth := p.cfg.Capacity - 1; depth > maxDepth {
		depth = maxDepth
	}

	type planned struct {
		id       disk.PageID
		distance int
	}
	missing := make([]planned, 0, depth)
	for j := 1; j <= depth; j++ {
		pid := id + st.Stride*disk.PageID(j)
		if pid < 0 || int(pid) >= p.disk.PageCount() {
			break
		}
		if f, ok := p.pages[pid]; ok {
			if f.Prefetch && !f.PrefetchUsed && f.PrefetchDistance != j {
				f.PrefetchDistance = j
				p.refreshPriorityLocked(f)
				heap.Fix(&p.victims, f.victimIndex)
			}
			continue
		}
		missing = append(missing, planned{id: pid, distance: j})
	}
	if len(missing) == 0 {
		return
	}
	if _, err := p.makeRoomForLocked(now, len(missing), "prefetch"); err != nil {
		return
	}
	for _, m := range missing {
		if _, ok := p.pages[m.id]; ok {
			continue
		}
		p.insertPrefetchLocked(m.id, now, class, m.distance)
	}
}

func (p *Pool) insertPrefetchLocked(id disk.PageID, now int64, class string, distance int) {
	blk, err := p.disk.Read(id)
	if err != nil {
		return
	}
	p.metrics.DiskReads++

	f := &Frame{
		ID:               id,
		Block:            blk,
		InsertedAt:       now,
		FetchClass:       class,
		Segment:          SegmentOld,
		Prefetch:         true,
		PrefetchAt:       now,
		PrefetchDistance: distance,
		victimIndex:      -1,
		flushIndex:       -1,
		prefetchIndex:    -1,
	}
	if g, ok := p.ghost[id]; ok {
		if p.cfg.GhostTTLMS > 0 && now-g.EvictedAt <= p.cfg.GhostTTLMS {
			// History is carried in the frame. Until a query uses this prefetch,
			// its zero-reference priority still makes it an early victim.
			f.History = append([]int64(nil), g.History...)
			f.AccessCount = g.AccessCount
			p.metrics.GhostRevivals++
		}
		// Expired or TTL-disabled ghosts must not feed ancient history into K.
		delete(p.ghost, id)
	}

	p.pages[id] = f
	p.refreshPriorityLocked(f)
	heap.Push(&p.victims, f)
	heap.Push(&p.prefetchQueue, f)
	p.classUsageLocked(class).addFrame(f)
	p.metrics.Frames = len(p.pages)
	p.metrics.PrefetchIssued++
}

// reapPrefetchesLocked uses the dedicated prefetch time heap. It reclaims only
// expired never-used prefetches and costs O(number reaped log N), not O(N).
func (p *Pool) reapPrefetchesLocked(now int64) {
	ttl := p.cfg.PrefetchUnusedTTLMS
	for p.prefetchQueue.Len() > 0 {
		f := p.prefetchQueue[0]
		if f.PrefetchUsed || !f.Prefetch {
			if f.prefetchIndex >= 0 {
				heap.Remove(&p.prefetchQueue, f.prefetchIndex)
			}
			continue
		}
		if ttl > 0 && now-f.PrefetchAt <= ttl {
			return
		}
		if _, err := p.evictFrameLocked(f, now, "prefetch-ttl"); err != nil {
			return
		}
		p.metrics.PrefetchReaped++
	}
}

func (p *Pool) expireGhostsLocked(now int64) {
	ttl := p.cfg.GhostTTLMS
	for len(p.ghostOrder) > 0 {
		item := p.ghostOrder[0]
		g, ok := p.ghost[item.id]
		if !ok || g.EvictedAt != item.evictedAt {
			p.ghostOrder = p.ghostOrder[1:]
			continue
		}
		if ttl > 0 && now-item.evictedAt <= ttl {
			return
		}
		p.ghostOrder = p.ghostOrder[1:]
		delete(p.ghost, item.id)
		p.metrics.GhostExpirations++
	}
	p.metrics.GhostEntries = len(p.ghost)
}

func (p *Pool) trimGhostsLocked(now int64) {
	p.expireGhostsLocked(now)
	maxGhost := p.cfg.GhostMax
	for len(p.ghost) > maxGhost && len(p.ghostOrder) > 0 {
		item := p.ghostOrder[0]
		p.ghostOrder = p.ghostOrder[1:]
		if g, ok := p.ghost[item.id]; ok && g.EvictedAt == item.evictedAt {
			delete(p.ghost, item.id)
			p.metrics.GhostExpirations++
		}
	}
	p.metrics.GhostEntries = len(p.ghost)
}

// Candidate returns the current policy-selected victim and a human reason.
func (p *Pool) Candidate(now int64) (disk.PageID, *Frame, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.victims.Len() == 0 {
		return -1, nil, "池子为空"
	}
	f := p.victims[0]
	p.refreshPriorityLocked(f)
	reason := p.candidateReasonLocked(f, now)
	return f.ID, f, reason
}

func (p *Pool) candidateReasonLocked(f *Frame, now int64) string {
	age := func(t int64) int64 {
		if now <= 0 {
			now = p.nowFunc().UnixMilli()
		}
		return maxInt64(0, now-t)
	}
	if f.Prefetch && !f.PrefetchUsed {
		return "未使用预读页，0 次有效访问进入 <K 优先层；预读于 " +
			formatAge(candidateAge(f.PrefetchAt, now)) + " 前，TTL 到期会快速回收"
	}
	switch f.PriorityTier {
	case 1:
		return "有效访问 " + itoa(f.AccessCount) + " 次 < K=" + itoa(p.cfg.K) +
			"，首次访问 " + formatAge(age(f.firstAccessOrInsert())) + " 前"
	default:
		key := f.History[len(f.History)-p.cfg.K]
		return "按倒数第 " + itoa(p.cfg.K) + " 次访问排序；该次访问在 " +
			formatAge(age(key)) + " 前"
	}
}

func candidateAge(t, now int64) int64 {
	if now <= 0 {
		return 0
	}
	return maxInt64(0, now-t)
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func formatAge(ms int64) string {
	switch {
	case ms >= 60_000:
		return itoa64(ms/1000) + "s"
	case ms >= 1000:
		return itoa64(ms/1000) + "." + itoa64((ms%1000)/100) + "s"
	default:
		return itoa64(ms) + "ms"
	}
}

func itoa(i int) string { return itoa64(int64(i)) }
func itoa64(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
