package replace

import (
	"container/heap"

	"catalogbuffer/internal/disk"
)

// Sample is one point on a hit-rate curve.
type Sample struct {
	X              int     `json:"x"`
	CumulativeRate float64 `json:"cumulativeRate"`
	WindowRate     float64 `json:"windowRate"`
}

// Result is an offline replacement-policy evaluation.
type Result struct {
	Name       string   `json:"name"`
	Capacity   int      `json:"capacity"`
	References int      `json:"references"`
	Hits       int      `json:"hits"`
	Misses     int      `json:"misses"`
	HitRate    float64  `json:"hitRate"`
	Series     []Sample `json:"series"`
}

type seriesBuilder struct {
	step     int
	lastX    int
	lastHits int
}

func newSeriesBuilder(n int) *seriesBuilder {
	step := n / 120
	if step < 1 {
		step = 1
	}
	return &seriesBuilder{step: step}
}

func (b *seriesBuilder) observe(x, totalN, hits int, out *[]Sample) {
	if x%b.step != 0 && x != totalN {
		return
	}
	windowRefs := x - b.lastX
	windowHits := hits - b.lastHits
	windowRate := 0.0
	if windowRefs > 0 {
		windowRate = float64(windowHits) / float64(windowRefs)
	}
	*out = append(*out, Sample{
		X:              x,
		CumulativeRate: float64(hits) / float64(x),
		WindowRate:     windowRate,
	})
	b.lastX = x
	b.lastHits = hits
}

func finish(name string, capacity, n, hits int, series []Sample) Result {
	denom := n
	if denom == 0 {
		denom = 1
	}
	return Result{
		Name:       name,
		Capacity:   capacity,
		References: n,
		Hits:       hits,
		Misses:     n - hits,
		HitRate:    float64(hits) / float64(denom),
		Series:     series,
	}
}

// Reference is one de-duplicated replacement-policy reference.
type Reference struct {
	Page disk.PageID
	AtMS int64
}

// Pages is a convenience adapter for callers without timestamps.
func Pages(ids []disk.PageID) []Reference {
	out := make([]Reference, len(ids))
	for i, id := range ids {
		out[i] = Reference{Page: id, AtMS: int64(i + 1)}
	}
	return out
}

// LRU evaluates ordinary least-recently-used replacement.
func LRU(refs []Reference, capacity int) Result {
	if capacity < 1 {
		capacity = 1
	}
	q := newLRUQueue()
	hits := 0
	series := make([]Sample, 0, len(refs)/80+2)
	sb := newSeriesBuilder(len(refs))
	for i, ref := range refs {
		id := ref.Page
		if e := q.lookup(id); e != nil {
			hits++
			q.moveFront(e)
		} else {
			if q.len() >= capacity {
				q.evictOldest()
			}
			q.insertFront(id)
		}
		sb.observe(i+1, len(refs), hits, &series)
	}
	return finish("LRU 最近最少使用", capacity, len(refs), hits, series)
}

// Clock evaluates the second-chance/clock algorithm.
func Clock(refs []Reference, capacity int) Result {
	if capacity < 1 {
		capacity = 1
	}
	type slot struct {
		id        disk.PageID
		reference bool
	}
	slots := make([]slot, capacity)
	index := make(map[disk.PageID]int, capacity)
	hand := 0
	hits := 0
	series := make([]Sample, 0, len(refs)/80+2)
	sb := newSeriesBuilder(len(refs))

	for i, ref := range refs {
		id := ref.Page
		if pos, ok := index[id]; ok {
			hits++
			slots[pos].reference = true
		} else {
			if len(index) >= capacity {
				for slots[hand].reference {
					slots[hand].reference = false
					hand = (hand + 1) % capacity
				}
				delete(index, slots[hand].id)
			}
			slots[hand] = slot{id: id, reference: true}
			index[id] = hand
			hand = (hand + 1) % capacity
		}
		sb.observe(i+1, len(refs), hits, &series)
	}
	return finish("Clock 时钟算法", capacity, len(refs), hits, series)
}

type optItem struct {
	id    disk.PageID
	next  int
	index int
}

type optHeap []*optItem

func (h optHeap) Len() int { return len(h) }
func (h optHeap) Less(i, j int) bool {
	if h[i].next != h[j].next {
		// Max-heap: furthest next use is evicted. n means never used again.
		return h[i].next > h[j].next
	}
	return h[i].id < h[j].id
}
func (h optHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *optHeap) Push(x any) {
	it := x.(*optItem)
	it.index = len(*h)
	*h = append(*h, it)
}
func (h *optHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	it.index = -1
	*h = old[:n-1]
	return it
}

// OPT evaluates Bélády's algorithm using knowledge of every future reference.
// It is the offline upper bound for demand-paging replacement at this capacity.
func OPT(refs []Reference, capacity int) Result {
	if capacity < 1 {
		capacity = 1
	}
	n := len(refs)
	next := make([]int, n)
	last := make(map[disk.PageID]int, n)
	for i := n - 1; i >= 0; i-- {
		id := refs[i].Page
		if j, ok := last[id]; ok {
			next[i] = j
		} else {
			next[i] = n
		}
		last[id] = i
	}

	h := &optHeap{}
	items := make(map[disk.PageID]*optItem, capacity)
	hits := 0
	series := make([]Sample, 0, n/80+2)
	sb := newSeriesBuilder(n)

	for i, ref := range refs {
		id := ref.Page
		future := next[i]
		if it, ok := items[id]; ok {
			hits++
			it.next = future
			heap.Fix(h, it.index)
		} else {
			if len(items) >= capacity {
				victim := heap.Pop(h).(*optItem)
				delete(items, victim.id)
			}
			it := &optItem{id: id, next: future}
			heap.Push(h, it)
			items[id] = it
		}
		sb.observe(i+1, n, hits, &series)
	}
	return finish("OPT 预知未来最优上界", capacity, n, hits, series)
}

type lrukItem struct {
	id      disk.PageID
	history []int64
	tier    int
	key     int64
	index   int
}

type lrukHeap []*lrukItem

func (h lrukHeap) Len() int { return len(h) }
func (h lrukHeap) Less(i, j int) bool {
	if h[i].tier != h[j].tier {
		return h[i].tier < h[j].tier
	}
	if h[i].key != h[j].key {
		return h[i].key < h[j].key
	}
	return h[i].id < h[j].id
}
func (h lrukHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *lrukHeap) Push(x any) {
	it := x.(*lrukItem)
	it.index = len(*h)
	*h = append(*h, it)
}
func (h *lrukHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	it.index = -1
	*h = old[:n-1]
	return it
}

// LRUK evaluates the K-th-last-reference policy without ghost/segment extras.
func LRUK(refs []Reference, capacity, k int) Result {
	if capacity < 1 {
		capacity = 1
	}
	if k < 1 {
		k = 1
	}
	h := &lrukHeap{}
	items := make(map[disk.PageID]*lrukItem, capacity)
	hits := 0
	series := make([]Sample, 0, len(refs)/80+2)
	sb := newSeriesBuilder(len(refs))

	refresh := func(it *lrukItem) {
		if len(it.history) < k {
			it.tier = 1
			it.key = it.history[0]
		} else {
			it.tier = 2
			// History is capped to the last k entries, so index zero is the
			// K-th most recent / K-th-to-last reference.
			it.key = it.history[0]
		}
	}

	for i, ref := range refs {
		id := ref.Page
		ts := ref.AtMS
		if ts <= 0 {
			ts = int64(i + 1)
		}
		if it, ok := items[id]; ok {
			hits++
			it.history = append(it.history, ts)
			if len(it.history) > k {
				kept := make([]int64, k)
				copy(kept, it.history[len(it.history)-k:])
				it.history = kept
			}
			refresh(it)
			heap.Fix(h, it.index)
		} else {
			if len(items) >= capacity {
				v := heap.Pop(h).(*lrukItem)
				delete(items, v.id)
			}
			it := &lrukItem{id: id, history: []int64{ts}}
			refresh(it)
			heap.Push(h, it)
			items[id] = it
		}
		sb.observe(i+1, len(refs), hits, &series)
	}
	name := "LRU-K 倒数第" + itoa(k) + "次访问"
	return finish(name, capacity, len(refs), hits, series)
}

func itoa(k int) string {
	if k == 0 {
		return "0"
	}
	var b [12]byte
	pos := len(b)
	for k > 0 {
		pos--
		b[pos] = byte('0' + k%10)
		k /= 10
	}
	return string(b[pos:])
}
