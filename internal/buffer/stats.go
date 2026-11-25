package buffer

import "catalogbuffer/internal/disk"

// Metrics contains cumulative pool counters.
type Metrics struct {
	CapacityTarget int `json:"capacityTarget"`
	Frames         int `json:"frames"`

	Requests uint64 `json:"requests"`
	Hits     uint64 `json:"hits"`
	Misses   uint64 `json:"misses"`

	// PolicyAccesses excludes same-transaction duplicates collapsed by rule 2.
	PolicyAccesses      uint64 `json:"policyAccesses"`
	DuplicateTxAccesses uint64 `json:"duplicateTxAccesses"`
	PolicyHits          uint64 `json:"policyHits"`
	PolicyMisses        uint64 `json:"policyMisses"`

	Evictions       uint64 `json:"evictions"`
	DirtyEvictions  uint64 `json:"dirtyEvictions"`
	ShrinkEvictions uint64 `json:"shrinkEvictions"`

	DiskReads       uint64 `json:"diskReads"`
	DiskWrites      uint64 `json:"diskWrites"`
	FlushWrites     uint64 `json:"flushWrites"`
	BackgroundFlush uint64 `json:"backgroundFlush"`

	GhostEntries     int    `json:"ghostEntries"`
	GhostRevivals    uint64 `json:"ghostRevivals"`
	GhostExpirations uint64 `json:"ghostExpirations"`

	PrefetchIssued uint64 `json:"prefetchIssued"`
	PrefetchHits   uint64 `json:"prefetchHits"`
	PrefetchUsed   uint64 `json:"prefetchUsed"`
	PrefetchWasted uint64 `json:"prefetchWasted"`
	PrefetchReaped uint64 `json:"prefetchReaped"`

	DirtyFrames int `json:"dirtyFrames"`
	FlushQueue  int `json:"flushQueue"`

	CurrentLSN int64 `json:"currentLsn"`
}

// HitRate returns the physical-request hit ratio.
func (m Metrics) HitRate() float64 {
	if m.Requests == 0 {
		return 0
	}
	return float64(m.Hits) / float64(m.Requests)
}

// PolicyHitRate returns hit ratio after transaction-local deduplication.
func (m Metrics) PolicyHitRate() float64 {
	if m.PolicyAccesses == 0 {
		return 0
	}
	return float64(m.PolicyHits) / float64(m.PolicyAccesses)
}

// TypeUsage reports pool occupancy attributed to one originating query class.
type TypeUsage struct {
	QueryType      string `json:"queryType"`
	Frames         int    `json:"frames"`
	Bytes          int    `json:"bytes"`
	DirtyFrames    int    `json:"dirtyFrames"`
	PrefetchFrames int    `json:"prefetchFrames"`
	UnusedPrefetch int    `json:"unusedPrefetch"`
}

func (u *TypeUsage) addFrame(f *Frame) {
	u.Frames++
	u.Bytes += disk.PageSize
	if f.Dirty {
		u.DirtyFrames++
	}
	if f.Prefetch {
		u.PrefetchFrames++
		if !f.PrefetchUsed {
			u.UnusedPrefetch++
		}
	}
}

func (u *TypeUsage) removeFrame(f *Frame) {
	u.Frames--
	u.Bytes -= disk.PageSize
	if f.Dirty {
		u.DirtyFrames--
	}
	if f.Prefetch {
		u.PrefetchFrames--
		if !f.PrefetchUsed {
			u.UnusedPrefetch--
		}
	}
}
