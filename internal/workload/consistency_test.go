package workload

import (
	"sync"
	"testing"

	"catalogbuffer/internal/buffer"
	"catalogbuffer/internal/disk"
)

func TestConcurrentWritesStayByteConsistent(t *testing.T) {
	const pageCount = 512
	d := disk.New(pageCount, 99, disk.Config{})
	oracle := NewOracle()
	cfg := buffer.DefaultConfig()
	cfg.Capacity = 96
	cfg.PrefetchDepth = 2
	cfg.ShrinkBatch = 4
	p := buffer.New(cfg, d, oracle.Observe)

	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				id := disk.PageID((w*37 + i*13) % pageCount)
				_, err := p.Access(id, buffer.AccessInput{
					TxID:      "concurrent-tx",
					QueryID:   "concurrent-q",
					QueryType: ClassHotspot,
					AtMS:      int64(i + w*1000 + 1),
					Write:     true,
					WriteTag:  int64(w*10000 + i),
				})
				if err != nil {
					t.Errorf("access: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	report := oracle.Verify(d)
	if !report.OK {
		t.Fatalf("consistency failed: %+v", report.Mismatches)
	}
	if report.CheckedWrites == 0 {
		t.Fatal("expected written pages")
	}
	clean := p.CheckCleanPagesAgainstDisk()
	if !clean.OK {
		t.Fatalf("clean frame invariant failed: %v", clean.Mismatches)
	}
}
