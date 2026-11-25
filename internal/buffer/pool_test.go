package buffer

import (
	"testing"

	"catalogbuffer/internal/disk"
)

func testPool(t *testing.T, pages, cap int, mutate func(*Config)) *Pool {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Capacity = cap
	cfg.PrefetchDepth = 0
	cfg.GhostTTLMS = 10_000
	cfg.DedupWindowMS = 0
	cfg.OldMinResidencyMS = 100
	if mutate != nil {
		mutate(&cfg)
	}
	d := disk.New(pages, 42, disk.Config{})
	return New(cfg, d, nil)
}

func access(t *testing.T, p *Pool, id int, at int64, in ...AccessInput) AccessResult {
	t.Helper()
	input := AccessInput{AtMS: at, QueryID: "q", QueryType: "test"}
	if len(in) > 0 {
		input = in[0]
		if input.AtMS == 0 {
			input.AtMS = at
		}
	}
	res, err := p.Access(disk.PageID(id), input)
	if err != nil {
		t.Fatalf("access page %d: %v", id, err)
	}
	return res
}

func TestLRUKPrioritizesFewerThanKAndKthTimestamp(t *testing.T) {
	p := testPool(t, 16, 3, func(c *Config) { c.K = 2 })

	access(t, p, 1, 1)
	access(t, p, 2, 2)
	access(t, p, 3, 3)
	access(t, p, 1, 10)

	id, _, _ := p.Candidate(100)
	if id != 2 {
		t.Fatalf("fewer-than-K page 2 should be candidate, got %d", id)
	}

	access(t, p, 2, 11)
	id, _, _ = p.Candidate(100)
	if id != 3 {
		t.Fatalf("page 3 remains one-access and should be candidate, got %d", id)
	}

	res := access(t, p, 4, 12)
	if res.EvictedPage != 3 {
		t.Fatalf("miss should evict page 3, got %d", res.EvictedPage)
	}
	if _, ok := p.pages[3]; ok {
		t.Fatal("page 3 still resident after eviction")
	}
}

func TestSameTransactionShortRepeatsCollapse(t *testing.T) {
	p := testPool(t, 8, 4, func(c *Config) { c.DedupWindowMS = 50 })
	in := AccessInput{TxID: "tx-1", QueryID: "q-dedup", QueryType: "hotspot"}

	r1 := access(t, p, 7, 100, in)
	r2 := access(t, p, 7, 120, in)
	r3 := access(t, p, 7, 160, in)
	r4 := access(t, p, 7, 211, in)

	if r1.Duplicate {
		t.Fatal("first reference must be accepted")
	}
	if !r2.Duplicate || !r3.Duplicate {
		t.Fatal("adjacent references inside sliding interval must be duplicates")
	}
	if r4.Duplicate {
		t.Fatal("reference after interval should be accepted")
	}
	f := p.pages[7]
	if f.AccessCount != 2 || len(f.History) != 2 {
		t.Fatalf("expected two policy accesses, got count=%d history=%v", f.AccessCount, f.History)
	}
	if p.metrics.DuplicateTxAccesses != 2 {
		t.Fatalf("duplicate metric=%d", p.metrics.DuplicateTxAccesses)
	}
}

func TestEvictedGhostHistoryIsReused(t *testing.T) {
	p := testPool(t, 8, 2, func(c *Config) {
		c.K = 2
		c.GhostTTLMS = 1_000
	})

	access(t, p, 1, 1)
	access(t, p, 1, 2)
	access(t, p, 2, 3)
	access(t, p, 2, 4)
	res := access(t, p, 3, 5)
	if res.EvictedPage != 1 {
		t.Fatalf("oldest second-to-last reference should evict page 1, got %d", res.EvictedPage)
	}
	if _, ok := p.ghost[1]; !ok {
		t.Fatal("evicted page history should be in ghost table")
	}

	res = access(t, p, 1, 10)
	if !res.GhostRevived {
		t.Fatal("quick read-back should revive ghost history")
	}
	f := p.pages[1]
	want := []int64{1, 2, 10}
	if len(f.History) != len(want) {
		t.Fatalf("history=%v, want %v", f.History, want)
	}
	for i := range want {
		if f.History[i] != want[i] {
			t.Fatalf("history=%v, want %v", f.History, want)
		}
	}
	if f.AccessCount != 3 {
		t.Fatalf("restored count plus new access = 3, got %d", f.AccessCount)
	}
}

func TestOldYoungPromotionRequiresResidencyAndLaterAccess(t *testing.T) {
	p := testPool(t, 8, 4, nil)
	access(t, p, 1, 1)
	if p.pages[1].Segment != SegmentOld {
		t.Fatal("new page must enter old segment")
	}
	access(t, p, 1, 50)
	if p.pages[1].Segment != SegmentOld {
		t.Fatal("access before minimum old residency must not promote")
	}
	access(t, p, 1, 101)
	if p.pages[1].Segment != SegmentYoung {
		t.Fatal("later access after minimum residency must promote to young")
	}
}

func TestSequentialPatternPrefetchesAndTTLReclaimsUnused(t *testing.T) {
	p := testPool(t, 32, 12, func(c *Config) {
		c.SequentialThreshold = 3
		c.PrefetchDepth = 3
		c.PrefetchUnusedTTLMS = 100
	})

	access(t, p, 0, 1)
	access(t, p, 1, 2)
	access(t, p, 2, 3)
	for _, id := range []disk.PageID{3, 4, 5} {
		if _, ok := p.pages[id]; !ok {
			t.Fatalf("sequential pattern should prefetch page %d", id)
		}
	}

	res := access(t, p, 3, 4)
	if !res.Hit || !res.PrefetchHit {
		t.Fatalf("page 3 should be a prefetch hit: %+v", res)
	}

	p.Maintenance(500)
	for _, id := range []disk.PageID{4, 5} {
		if _, ok := p.pages[id]; ok {
			t.Fatalf("unused expired prefetch page %d should be reclaimed", id)
		}
	}
}

func TestCapacityShrinkIsGradual(t *testing.T) {
	p := testPool(t, 128, 10, func(c *Config) { c.ShrinkBatch = 2 })
	for i := 0; i < 10; i++ {
		access(t, p, i, int64(i+1))
	}
	p.SetCapacity(5)
	if len(p.pages) != 10 {
		t.Fatalf("shrink must not immediately remove pages, got %d", len(p.pages))
	}
	n, err := p.ShrinkStep(1_000, 0)
	if err != nil || n != 2 || len(p.pages) != 8 {
		t.Fatalf("first shrink step n=%d len=%d err=%v", n, len(p.pages), err)
	}
	_, _ = p.ShrinkStep(1_001, 0)
	if len(p.pages) != 6 {
		t.Fatalf("second step should leave 6, got %d", len(p.pages))
	}
	_, _ = p.ShrinkStep(1_002, 0)
	if len(p.pages) != 5 {
		t.Fatalf("eventual target size 5, got %d", len(p.pages))
	}
}

func TestQueryTypeUsageAttribution(t *testing.T) {
	p := testPool(t, 32, 8, nil)
	access(t, p, 1, 1, AccessInput{AtMS: 1, QueryType: "hotspot"})
	access(t, p, 2, 2, AccessInput{AtMS: 2, QueryType: "scan"})
	access(t, p, 3, 3, AccessInput{AtMS: 3, QueryType: "random"})
	st := p.State(100)
	seen := map[string]int{}
	for _, u := range st.Usage {
		seen[u.QueryType] = u.Frames
	}
	if seen["hotspot"] != 1 || seen["scan"] != 1 || seen["random"] != 1 {
		t.Fatalf("usage attribution wrong: %#v", seen)
	}
}

func TestDemandMissDuringShrinkDoesNotDrainOverage(t *testing.T) {
	p := testPool(t, 128, 10, func(c *Config) { c.ShrinkBatch = 3 })
	for i := 0; i < 10; i++ {
		access(t, p, i, int64(i+1))
	}
	p.SetCapacity(4)

	// While over target, admission is one-for-one, not an immediate drain of
	// all six excess frames in this single miss.
	res := access(t, p, 100, 1_000)
	if res.EvictedPage < 0 {
		t.Fatal("miss should still evict one frame for admission")
	}
	if len(p.pages) != 10 {
		t.Fatalf("one-for-one admission should preserve pending overage, got %d", len(p.pages))
	}

	n, _ := p.ShrinkStep(1_001, 3)
	if n != 3 || len(p.pages) != 7 {
		t.Fatalf("gradual shrink step n=%d len=%d", n, len(p.pages))
	}
}

func TestFlushHeapFixesRewrittenLSNAndMetricsExposeLSN(t *testing.T) {
	p := testPool(t, 16, 4, nil)
	write := func(id int, at int64) {
		access(t, p, id, at, AccessInput{AtMS: at, QueryType: "hotspot", Write: true})
	}
	write(1, 100) // LSN 1
	write(2, 100) // LSN 2
	write(1, 100) // LSN 3, same DirtySince and heap tie key changes

	if p.Metrics().CurrentLSN != 3 {
		t.Fatalf("CurrentLSN=%d", p.Metrics().CurrentLSN)
	}
	if p.flushQueue.Len() != 2 {
		t.Fatalf("flush queue len=%d", p.flushQueue.Len())
	}
	if root := p.flushQueue[0].ID; root != 2 {
		t.Fatalf("oldest LSN page 2 should be flush root, got %d", root)
	}
	for i := 1; i < p.flushQueue.Len(); i++ {
		parent := (i - 1) / 2
		if p.flushQueue.Less(i, parent) {
			t.Fatalf("flush heap invariant violated at index %d", i)
		}
	}
}

func TestPrefetchGhostTTLAndExtremeDepthDoNotEmptyPool(t *testing.T) {
	p := testPool(t, 32, 2, func(c *Config) { c.GhostTTLMS = 100 })
	access(t, p, 1, 1)
	access(t, p, 1, 2)
	access(t, p, 2, 3)
	access(t, p, 2, 4)
	access(t, p, 3, 5) // evicts page 1 into ghost at t=5

	before := p.metrics.GhostRevivals
	p.insertPrefetchLocked(1, 5_000, "scan", 1)
	f := p.pages[1]
	if f == nil {
		t.Fatal("prefetch should insert page")
	}
	if len(f.History) != 0 {
		t.Fatalf("expired ghost history must not be restored: %v", f.History)
	}
	if p.metrics.GhostRevivals != before {
		t.Fatal("expired ghost should not count as revival")
	}

	small := testPool(t, 32, 1, func(c *Config) {
		c.SequentialThreshold = 3
		c.PrefetchDepth = 100
	})
	access(t, small, 0, 1)
	access(t, small, 1, 2)
	access(t, small, 2, 3)
	if len(small.pages) != 1 {
		t.Fatalf("capacity=1 pool must remain non-empty, got %d pages", len(small.pages))
	}
}
