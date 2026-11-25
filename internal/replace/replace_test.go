package replace

import (
	"testing"

	"catalogbuffer/internal/disk"
)

func TestOPTNeverBelowDemandPoliciesOnRepeatedPattern(t *testing.T) {
	ids := []disk.PageID{1, 2, 3, 4, 1, 2, 5, 1, 2, 3, 4, 5, 2, 1, 4, 3, 2, 1}
	refs := Pages(ids)
	for _, cap := range []int{2, 3, 4} {
		opt := OPT(refs, cap)
		lru := LRU(refs, cap)
		clock := Clock(refs, cap)
		lruk := LRUK(refs, cap, 2)
		if lru.HitRate > opt.HitRate || clock.HitRate > opt.HitRate || lruk.HitRate > opt.HitRate {
			t.Fatalf("capacity %d: opt=%.3f lru=%.3f clock=%.3f lruk=%.3f",
				cap, opt.HitRate, lru.HitRate, clock.HitRate, lruk.HitRate)
		}
		if len(opt.Series) < 2 || len(lru.Series) != len(opt.Series) {
			t.Fatalf("series should be sampled consistently: opt=%d lru=%d", len(opt.Series), len(lru.Series))
		}
	}
}
