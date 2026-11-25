package sim

import (
	"sort"

	"catalogbuffer/internal/buffer"
	"catalogbuffer/internal/replace"
	"catalogbuffer/internal/workload"
)

// Comparison is the offline policy comparison report.
type Comparison struct {
	Seed            int64            `json:"seed"`
	PageCount       int              `json:"pageCount"`
	Capacity        int              `json:"capacity"`
	K               int              `json:"k"`
	RawEvents       int              `json:"rawEvents"`
	EffectiveRefs   int              `json:"effectiveRefs"`
	DedupWindowMS   int64            `json:"dedupWindowMs"`
	Results         []replace.Result `json:"results"`
	OPTHitRate      float64          `json:"optHitRate"`
	LRUHitRate      float64          `json:"lruHitRate"`
	ClockHitRate    float64          `json:"clockHitRate"`
	LRUKHitRate     float64          `json:"lrukHitRate"`
	OPTUpperBoundOK bool             `json:"optUpperBoundOk"`
}

// CompareStrategies generates the fixed trace and evaluates OPT, LRU, Clock,
// and LRU-K on the same de-duplicated reference string and capacity.
func CompareStrategies(w workload.Config, poolCfg buffer.Config) Comparison {
	requestedK := poolCfg.K
	if requestedK < 1 {
		requestedK = 1
	}
	w = w.Normalize()
	poolCfg = poolCfg.Normalize()
	trace := workload.GenerateTrace(w)
	refs := workload.EffectiveReferences(trace, poolCfg.DedupWindowMS)

	opt := replace.OPT(refs, poolCfg.Capacity)
	lru := replace.LRU(refs, poolCfg.Capacity)
	clock := replace.Clock(refs, poolCfg.Capacity)
	// The offline policy keeps exactly K timestamps and does not need the live
	// frame's bounded StoredHistoryLimit clamp.
	lruk := replace.LRUK(refs, poolCfg.Capacity, requestedK)
	results := []replace.Result{opt, lruk, lru, clock}

	ok := true
	for _, r := range results[1:] {
		// Floating-point tolerance for independently accumulated ratios.
		if r.HitRate > opt.HitRate+1e-12 {
			ok = false
		}
	}

	return Comparison{
		Seed:            w.Seed,
		PageCount:       w.PageCount,
		Capacity:        poolCfg.Capacity,
		K:               requestedK,
		RawEvents:       len(trace),
		EffectiveRefs:   len(refs),
		DedupWindowMS:   poolCfg.DedupWindowMS,
		Results:         results,
		OPTHitRate:      opt.HitRate,
		LRUHitRate:      lru.HitRate,
		ClockHitRate:    clock.HitRate,
		LRUKHitRate:     lruk.HitRate,
		OPTUpperBoundOK: ok,
	}
}

// SortedResultsByHitRate is a small helper for CLI output.
func (c Comparison) SortedResultsByHitRate() []replace.Result {
	out := append([]replace.Result(nil), c.Results...)
	sort.Slice(out, func(i, j int) bool { return out[i].HitRate > out[j].HitRate })
	return out
}
