package buffer

import "time"

// Config contains all tunable buffer-pool policy settings.
// Time intervals are stored in milliseconds so deterministic virtual clocks
// and the browser API use the same representation.
type Config struct {
	// Capacity is the target maximum number of cached frames.
	Capacity int `json:"capacity"`

	// K is the LRU-K parameter. Eviction uses the K-th most recent access.
	K int `json:"k"`

	// DedupWindowMS collapses repeated references from the same transaction
	// to the same page when they are no farther apart than this window.
	DedupWindowMS int64 `json:"dedupWindowMs"`

	// GhostTTLMS retains evicted-page histories for this long.
	GhostTTLMS int64 `json:"ghostTtlMs"`
	GhostMax   int   `json:"ghostMax"`

	// OldMinResidencyMS is the minimum time a freshly inserted page must
	// spend in the old segment before a later access may promote it.
	OldMinResidencyMS int64 `json:"oldMinResidencyMs"`

	// SequentialThreshold is the consecutive-run length that arms prefetch.
	SequentialThreshold int `json:"sequentialThreshold"`
	// PrefetchDepth is the number of following pages to keep warmed.
	PrefetchDepth int `json:"prefetchDepth"`
	// PrefetchUnusedTTLMS aggressively reclaims never-used prefetch frames.
	PrefetchUnusedTTLMS int64 `json:"prefetchUnusedTtlMs"`

	// ShrinkBatch is the number of victim frames reclaimed per shrink tick.
	ShrinkBatch int `json:"shrinkBatch"`
	// FlushBatch is the maximum dirty pages written per background tick.
	FlushBatch int `json:"flushBatch"`

	// RecentHistoryCount controls how many access timestamps are exposed.
	RecentHistoryCount int `json:"recentHistoryCount"`
	// StoredHistoryLimit bounds per-frame memory. K must not exceed it.
	StoredHistoryLimit int `json:"storedHistoryLimit"`
}

// ConfigPatch uses pointers so omitted fields stay unchanged while an explicit
// JSON zero can disable a time window/TTL.
type ConfigPatch struct {
	Capacity            *int   `json:"capacity"`
	K                   *int   `json:"k"`
	DedupWindowMS       *int64 `json:"dedupWindowMs"`
	GhostTTLMS          *int64 `json:"ghostTtlMs"`
	GhostMax            *int   `json:"ghostMax"`
	OldMinResidencyMS   *int64 `json:"oldMinResidencyMs"`
	SequentialThreshold *int   `json:"sequentialThreshold"`
	PrefetchDepth       *int   `json:"prefetchDepth"`
	PrefetchUnusedTTLMS *int64 `json:"prefetchUnusedTtlMs"`
	ShrinkBatch         *int   `json:"shrinkBatch"`
	FlushBatch          *int   `json:"flushBatch"`
	RecentHistoryCount  *int   `json:"recentHistoryCount"`
	StoredHistoryLimit  *int   `json:"storedHistoryLimit"`
}

func DefaultConfig() Config {
	return Config{
		Capacity:            360,
		K:                   2,
		DedupWindowMS:       80,
		GhostTTLMS:          20_000,
		GhostMax:            8_192,
		OldMinResidencyMS:   1_500,
		SequentialThreshold: 3,
		PrefetchDepth:       3,
		PrefetchUnusedTTLMS: 1_200,
		ShrinkBatch:         12,
		FlushBatch:          32,
		RecentHistoryCount:  8,
		StoredHistoryLimit:  32,
	}
}

func (c Config) Normalize() Config {
	d := DefaultConfig()
	if c.Capacity < 1 {
		c.Capacity = d.Capacity
	}
	if c.K < 1 {
		c.K = 1
	}
	if c.K > c.StoredHistoryLimit && c.StoredHistoryLimit > 0 {
		c.K = c.StoredHistoryLimit
	}
	if c.DedupWindowMS < 0 {
		c.DedupWindowMS = 0
	}
	if c.GhostTTLMS < 0 {
		c.GhostTTLMS = 0
	}
	if c.GhostMax <= 0 {
		c.GhostMax = d.GhostMax
	}
	if c.OldMinResidencyMS < 0 {
		c.OldMinResidencyMS = 0
	}
	if c.SequentialThreshold < 2 {
		c.SequentialThreshold = 2
	}
	if c.PrefetchDepth < 0 {
		c.PrefetchDepth = 0
	}
	if c.PrefetchUnusedTTLMS < 0 {
		c.PrefetchUnusedTTLMS = 0
	}
	if c.ShrinkBatch < 1 {
		c.ShrinkBatch = 1
	}
	if c.FlushBatch < 1 {
		c.FlushBatch = 1
	}
	if c.RecentHistoryCount < 1 {
		c.RecentHistoryCount = d.RecentHistoryCount
	}
	if c.StoredHistoryLimit < c.K {
		c.StoredHistoryLimit = c.K
	}
	if c.RecentHistoryCount > c.StoredHistoryLimit {
		c.RecentHistoryCount = c.StoredHistoryLimit
	}
	return c
}

func msDuration(v int64) time.Duration { return time.Duration(v) * time.Millisecond }
