package workload

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"catalogbuffer/internal/buffer"
	"catalogbuffer/internal/disk"
)

// LiveConfig controls the real concurrent query runner used by the web demo.
type LiveConfig struct {
	Seed           int64
	PageCount      int
	HotPages       int
	Workers        int
	WritePermille  int
	OpPauseMin     time.Duration
	OpPauseMax     time.Duration
	QueryPause     time.Duration
	ScanFirstDelay time.Duration
	ScanInterval   time.Duration
	ScanPagePause  time.Duration
}

func DefaultLiveConfig() LiveConfig {
	return LiveConfig{
		Seed:           DefaultConfig().Seed,
		PageCount:      300_000,
		HotPages:       128,
		Workers:        8,
		WritePermille:  DefaultConfig().WritePermille,
		OpPauseMin:     1 * time.Millisecond,
		OpPauseMax:     7 * time.Millisecond,
		QueryPause:     18 * time.Millisecond,
		ScanFirstDelay: 1500 * time.Millisecond,
		ScanInterval:   18 * time.Second,
		ScanPagePause:  350 * time.Microsecond,
	}
}

type LiveRunner struct {
	cfg     LiveConfig
	pool    *buffer.Pool
	queryID atomic.Uint64
	wg      sync.WaitGroup
}

func StartLive(ctx context.Context, p *buffer.Pool, cfg LiveConfig) *LiveRunner {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	r := &LiveRunner{cfg: cfg, pool: p}
	r.wg.Add(cfg.Workers + 1)
	for w := 0; w < cfg.Workers; w++ {
		go r.worker(ctx, w)
	}
	go r.periodicScans(ctx)
	return r
}

func (r *LiveRunner) Wait() { r.wg.Wait() }

func (r *LiveRunner) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (r *LiveRunner) randomPause(rng *rand.Rand) time.Duration {
	lo := int64(r.cfg.OpPauseMin)
	hi := int64(r.cfg.OpPauseMax)
	if hi <= lo {
		return r.cfg.OpPauseMin
	}
	return time.Duration(lo + rng.Int63n(hi-lo+1))
}

func (r *LiveRunner) worker(ctx context.Context, worker int) {
	defer r.wg.Done()
	rng := rand.New(rand.NewSource(r.cfg.Seed + int64(worker)*7919 + 101))

	for ctx.Err() == nil {
		qid := r.queryID.Add(1)
		class := choosePointClass(rng)
		txID := fmt.Sprintf("w%d-tx-%d", worker, qid)
		queryID := fmt.Sprintf("w%d-q-%d", worker, qid)
		ops := 2 + rng.Intn(9)
		if class == ClassRandom {
			ops = 1 + rng.Intn(4)
		}

		last := disk.PageID(-1)
		for op := 0; op < ops; op++ {
			var id disk.PageID
			switch class {
			case ClassHotspot:
				id = hotPage(rng, r.cfg.HotPages)
			case ClassMixed:
				id = mixedPage(rng, Config{HotPages: r.cfg.HotPages, PageCount: r.cfg.PageCount})
			default:
				id = disk.PageID(rng.Intn(r.cfg.PageCount))
			}
			if last >= 0 && class == ClassHotspot && rng.Intn(100) < 28 {
				id = last
			}
			last = id

			write := false
			if class == ClassHotspot {
				write = rng.Intn(1000) < r.cfg.WritePermille*2
			} else {
				write = rng.Intn(1000) < r.cfg.WritePermille/3
			}

			_, err := r.pool.Access(id, buffer.AccessInput{
				TxID:      txID,
				QueryID:   queryID,
				QueryType: class,
				Write:     write,
				WriteTag:  int64(qid*100 + uint64(op)),
			})
			if err != nil {
				// The simulated IDs are always valid; cancellation/shutdown can
				// simply stop this worker.
				return
			}
			if !r.sleep(ctx, r.randomPause(rng)) {
				return
			}
		}

		pause := r.cfg.QueryPause + time.Duration(rng.Intn(25))*time.Millisecond
		if !r.sleep(ctx, pause) {
			return
		}
	}
}

func (r *LiveRunner) periodicScans(ctx context.Context) {
	defer r.wg.Done()
	scanNo := uint64(0)

	if !r.sleep(ctx, r.cfg.ScanFirstDelay) {
		return
	}
	for ctx.Err() == nil {
		scanNo++
		qid := fmt.Sprintf("periodic-full-scan-%d", scanNo)
		txID := fmt.Sprintf("monthly-stats-tx-%d", scanNo)
		for id := disk.PageID(0); int(id) < r.cfg.PageCount; id++ {
			if ctx.Err() != nil {
				return
			}
			_, err := r.pool.Access(id, buffer.AccessInput{
				TxID:      txID,
				QueryID:   qid,
				QueryType: ClassScan,
			})
			if err != nil {
				return
			}
			if r.cfg.ScanPagePause > 0 && int(id)%8 == 0 {
				if !r.sleep(ctx, r.cfg.ScanPagePause) {
					return
				}
			}
		}
		if !r.sleep(ctx, r.cfg.ScanInterval) {
			return
		}
	}
}
