package buffer

import (
	"catalogbuffer/internal/disk"
)

const (
	SegmentOld   = 0
	SegmentYoung = 1
)

func SegmentName(s int) string {
	if s == SegmentYoung {
		return "young"
	}
	return "old"
}

// Frame is one cached data page and all replacement metadata.
type Frame struct {
	ID    disk.PageID
	Block disk.Block

	// History contains accepted (de-duplicated) access timestamps, ascending.
	History []int64

	AccessCount int
	InsertedAt  int64
	LastAccess  int64

	Segment int

	// FetchClass is the query class that first brought the page into cache.
	FetchClass string

	Prefetch     bool
	PrefetchUsed bool
	PrefetchAt   int64
	// PrefetchDistance is the ahead-offset when issued; it only breaks ties
	// inside one identical-timestamp prefetch batch.
	PrefetchDistance int

	Dirty       bool
	DirtySince  int64
	WriteLSN    int64
	FlushQueued bool

	// Cached heap priority. Tier 1 is fewer than K accepted references
	// (including never-used prefetches), tier 2 is a normal LRU-K key.
	PriorityTier int
	PriorityKey  int64

	victimIndex   int
	flushIndex    int
	prefetchIndex int
}

func (f *Frame) firstAccessOrInsert() int64 {
	if len(f.History) > 0 {
		return f.History[0]
	}
	return f.InsertedAt
}

// refreshPriorityLocked recalculates the O(1) ordering key. The frame's heap
// position must be fixed by the caller when its key changes.
func (p *Pool) refreshPriorityLocked(f *Frame) {
	switch {
	case f.Prefetch && !f.PrefetchUsed:
		// A never-used prefetch has zero accepted references. Put it in the
		// same "less than K" tier, keyed by prefetch time. Older ordinary
		// one-reference scan-tail pages can still correctly lose first.
		f.PriorityTier = 1
		f.PriorityKey = f.PrefetchAt
	case len(f.History) < p.cfg.K:
		f.PriorityTier = 1
		f.PriorityKey = f.firstAccessOrInsert()
	default:
		// For K=2 and history [a,b,c,d], index len-K = 2 gives c,
		// the second-most-recent / second-to-last access.
		f.PriorityTier = 2
		f.PriorityKey = f.History[len(f.History)-p.cfg.K]
	}
}

// victimHeap is a bounded-size indexed heap: exactly one entry per frame.
// Peek is O(1), key updates are O(log N), and no full scan/sort is needed.
type victimHeap []*Frame

func (h victimHeap) Len() int { return len(h) }
func (h victimHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	if a.PriorityTier != b.PriorityTier {
		return a.PriorityTier < b.PriorityTier
	}
	if a.PriorityKey != b.PriorityKey {
		return a.PriorityKey < b.PriorityKey
	}
	if a.Prefetch && !a.PrefetchUsed && b.Prefetch && !b.PrefetchUsed &&
		a.PrefetchDistance != b.PrefetchDistance {
		// Same issue time: recycle the farthest-ahead prediction first.
		return a.PrefetchDistance > b.PrefetchDistance
	}
	// Deterministic tie-breaking; old segment loses before young segment.
	if a.Segment != b.Segment {
		return a.Segment < b.Segment
	}
	return a.ID < b.ID
}
func (h victimHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].victimIndex = i
	h[j].victimIndex = j
}
func (h *victimHeap) Push(x any) {
	f := x.(*Frame)
	f.victimIndex = len(*h)
	*h = append(*h, f)
}
func (h *victimHeap) Pop() any {
	old := *h
	n := len(old)
	f := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	f.victimIndex = -1
	return f
}

// flushHeap orders dirty pages by the oldest time they have remained dirty.
type flushHeap []*Frame

func (h flushHeap) Len() int { return len(h) }
func (h flushHeap) Less(i, j int) bool {
	if h[i].DirtySince != h[j].DirtySince {
		return h[i].DirtySince < h[j].DirtySince
	}
	if h[i].WriteLSN != h[j].WriteLSN {
		return h[i].WriteLSN < h[j].WriteLSN
	}
	return h[i].ID < h[j].ID
}
func (h flushHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].flushIndex = i
	h[j].flushIndex = j
}
func (h *flushHeap) Push(x any) {
	f := x.(*Frame)
	f.flushIndex = len(*h)
	*h = append(*h, f)
}
func (h *flushHeap) Pop() any {
	old := *h
	n := len(old)
	f := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	f.flushIndex = -1
	f.FlushQueued = false
	return f
}

// prefetchHeap is a time-ordered auxiliary index used only for prompt TTL
// reclamation of never-used prefetches. It keeps that maintenance O(expired).
type prefetchHeap []*Frame

func (h prefetchHeap) Len() int { return len(h) }
func (h prefetchHeap) Less(i, j int) bool {
	if h[i].PrefetchAt != h[j].PrefetchAt {
		return h[i].PrefetchAt < h[j].PrefetchAt
	}
	return h[i].ID < h[j].ID
}
func (h prefetchHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].prefetchIndex = i
	h[j].prefetchIndex = j
}
func (h *prefetchHeap) Push(x any) {
	f := x.(*Frame)
	f.prefetchIndex = len(*h)
	*h = append(*h, f)
}
func (h *prefetchHeap) Pop() any {
	old := *h
	n := len(old)
	f := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	f.prefetchIndex = -1
	return f
}
