package buffer

import (
	"testing"
	"time"

	"catalogbuffer/internal/disk"
)

const benchmarkPages = 300_000

func newLargeBenchPool(b testing.TB, extraDisk int) (*Pool, *disk.Disk) {
	b.Helper()
	d := disk.New(benchmarkPages+extraDisk, 7, disk.Config{})
	cfg := DefaultConfig()
	cfg.Capacity = benchmarkPages
	cfg.PrefetchDepth = 0
	cfg.GhostTTLMS = 0
	cfg.DedupWindowMS = 0
	p := New(cfg, d, nil)
	for i := 0; i < benchmarkPages; i++ {
		_, err := p.Access(disk.PageID(i), AccessInput{AtMS: int64(i + 1), QueryType: "bench"})
		if err != nil {
			b.Fatal(err)
		}
	}
	if p.victims.Len() != benchmarkPages {
		b.Fatalf("seed heap size=%d", p.victims.Len())
	}
	return p, d
}

// BenchmarkCandidatePeek300k measures selecting the eviction candidate. The
// indexed heap root makes this O(1), independent of scanning/sorting 300k.
func BenchmarkCandidatePeek300k(b *testing.B) {
	p, _ := newLargeBenchPool(b, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, _, _ := p.Candidate(int64(benchmarkPages + i))
		if id < 0 {
			b.Fatal("no candidate")
		}
	}
}

// BenchmarkAccessHeapUpdate300k measures the O(log N) priority update on a hit.
func BenchmarkAccessHeapUpdate300k(b *testing.B) {
	p, _ := newLargeBenchPool(b, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := disk.PageID(i % benchmarkPages)
		_, err := p.Access(id, AccessInput{AtMS: int64(benchmarkPages + i), QueryType: "bench"})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TestEvictInsertLatency300k performs 1,000 real miss eviction+insert paths and
// reports average latency. It does not assert a rigid timing threshold.
func TestEvictInsertLatency300k(t *testing.T) {
	const rounds = 1000
	p, _ := newLargeBenchPool(t, rounds+1)
	start := time.Now()
	for i := 0; i < rounds; i++ {
		id := disk.PageID(benchmarkPages + i)
		res, err := p.Access(id, AccessInput{AtMS: int64(benchmarkPages + i + 1), QueryType: "bench"})
		if err != nil {
			t.Fatal(err)
		}
		if res.EvictedPage < 0 {
			t.Fatal("full 300k pool miss should evict")
		}
	}
	elapsed := time.Since(start)
	t.Logf("evict+insert at %d pages: %s total, %s/op over %d rounds",
		benchmarkPages, elapsed, elapsed/rounds, rounds)
	if p.victims.Len() != benchmarkPages {
		t.Fatalf("size changed: %d", p.victims.Len())
	}
}
