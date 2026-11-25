package sim

import (
	"fmt"

	"catalogbuffer/internal/buffer"
	"catalogbuffer/internal/disk"
	"catalogbuffer/internal/workload"
)

type ReadThroughReport struct {
	OK         bool     `json:"ok"`
	Pages      int      `json:"pages"`
	Mismatches []string `json:"mismatches"`
}

type VerificationReport struct {
	OK          bool                       `json:"ok"`
	Workload    workload.Config            `json:"workload"`
	Config      buffer.Config              `json:"config"`
	Comparison  Comparison                 `json:"comparison"`
	Consistency workload.ConsistencyReport `json:"consistency"`
	CleanCheck  buffer.CleanCheckResult    `json:"cleanCheck"`
	ReadThrough ReadThroughReport          `json:"readThrough"`
	PoolMetrics buffer.Metrics             `json:"poolMetrics"`
	DiskStats   disk.Stats                 `json:"diskStats"`
	Errors      []string                   `json:"errors"`
}

// RunFullVerification replays the fixed-seed mixed trace, flushes all writes,
// compares every last logical write byte-for-byte with disk, and then reads
// every page back through a fresh small pool.
func RunFullVerification(events int) (*VerificationReport, error) {
	w := workload.DefaultConfig()
	if events > 0 {
		w.Events = events
	}
	cfg := buffer.DefaultConfig()

	d := disk.New(w.PageCount, w.Seed, disk.Config{})
	oracle := workload.NewOracle()
	p := buffer.New(cfg, d, oracle.Observe)

	trace := workload.GenerateTrace(w)
	var errs []string
	for _, e := range trace {
		_, err := p.Access(e.PageID, buffer.AccessInput{
			TxID:      e.TxID,
			QueryID:   e.QueryID,
			QueryType: e.Class,
			AtMS:      e.AtMS,
			Write:     e.Write,
			WriteTag:  e.WriteTag,
		})
		if err != nil {
			errs = append(errs, fmt.Sprintf("event %d page %d: %v", e.Seq, e.PageID, err))
			if len(errs) >= 20 {
				break
			}
		}
		if e.Seq%512 == 0 {
			p.Maintenance(e.AtMS)
		}
	}

	if err := p.FlushAll(); err != nil {
		errs = append(errs, "flush all: "+err.Error())
	}
	clean := p.CheckCleanPagesAgainstDisk()
	consistency := oracle.Verify(d)
	readThrough := verifyReadThrough(d)
	metrics := p.Metrics()
	diskStats := d.Stats()
	if err := p.Close(); err != nil {
		errs = append(errs, "close: "+err.Error())
	}

	comparison := CompareStrategies(w, cfg)
	ok := len(errs) == 0 && consistency.OK && clean.OK && readThrough.OK && comparison.OPTUpperBoundOK
	return &VerificationReport{
		OK:          ok,
		Workload:    w,
		Config:      cfg,
		Comparison:  comparison,
		Consistency: consistency,
		CleanCheck:  clean,
		ReadThrough: readThrough,
		PoolMetrics: metrics,
		DiskStats:   diskStats,
		Errors:      errs,
	}, nil
}

func verifyReadThrough(d *disk.Disk) ReadThroughReport {
	cfg := buffer.DefaultConfig()
	cfg.Capacity = 96
	cfg.PrefetchDepth = 0
	cfg.GhostTTLMS = 0
	rp := buffer.New(cfg, d, nil)
	rep := ReadThroughReport{OK: true, Mismatches: []string{}}

	for i := 0; i < d.PageCount(); i++ {
		id := disk.PageID(i)
		want, err := d.Read(id)
		if err != nil {
			rep.OK = false
			rep.Mismatches = append(rep.Mismatches, err.Error())
			continue
		}
		got, err := rp.Access(id, buffer.AccessInput{
			QueryID:   fmt.Sprintf("verify-%d", i),
			QueryType: "verification",
			AtMS:      int64(i + 1),
		})
		if err != nil {
			rep.OK = false
			rep.Mismatches = append(rep.Mismatches, err.Error())
			continue
		}
		rep.Pages++
		if !disk.ExactEqual(want, got.Block) {
			rep.OK = false
			rep.Mismatches = append(rep.Mismatches, fmt.Sprintf("read-through page %d mismatch", id))
			if len(rep.Mismatches) >= 50 {
				break
			}
		}
	}
	if err := rp.Close(); err != nil {
		rep.OK = false
		rep.Mismatches = append(rep.Mismatches, err.Error())
	}
	return rep
}
