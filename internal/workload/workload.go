package workload

import (
	"fmt"
	"math"
	"math/rand"

	"catalogbuffer/internal/disk"
	"catalogbuffer/internal/replace"
)

const (
	ClassHotspot = "hotspot"
	ClassScan    = "scan"
	ClassRandom  = "random"
	ClassMixed   = "mixed"
)

// Event is one deterministic reference in the generated workload.
type Event struct {
	Seq      int         `json:"seq"`
	AtMS     int64       `json:"atMs"`
	TxID     string      `json:"txId"`
	QueryID  string      `json:"queryId"`
	Class    string      `json:"class"`
	PageID   disk.PageID `json:"pageId"`
	Write    bool        `json:"write"`
	WriteTag int64       `json:"writeTag"`
}

// Config controls deterministic trace generation.
type Config struct {
	Seed          int64 `json:"seed"`
	PageCount     int   `json:"pageCount"`
	HotPages      int   `json:"hotPages"`
	Events        int   `json:"events"`
	ScanPeriodMS  int64 `json:"scanPeriodMs"`
	WritePermille int   `json:"writePermille"`
}

func DefaultConfig() Config {
	return Config{
		Seed:          20261126,
		PageCount:     5000,
		HotPages:      96,
		Events:        72000,
		ScanPeriodMS:  24000,
		WritePermille: 90,
	}
}

func (c Config) Normalize() Config {
	d := DefaultConfig()
	if c.Seed == 0 {
		c.Seed = d.Seed
	}
	if c.PageCount < 16 {
		c.PageCount = d.PageCount
	}
	if c.HotPages <= 0 || c.HotPages >= c.PageCount {
		c.HotPages = minInt(96, c.PageCount/8)
	}
	if c.Events < 100 {
		c.Events = d.Events
	}
	if c.ScanPeriodMS <= 0 {
		c.ScanPeriodMS = d.ScanPeriodMS
	}
	if c.WritePermille < 0 {
		c.WritePermille = 0
	}
	if c.WritePermille > 1000 {
		c.WritePermille = 1000
	}
	return c
}

type activeScan struct {
	queryID string
	txID    string
	next    disk.PageID
}

// GenerateTrace creates a fixed-seed mixture of hot-row queries, random point
// lookups, middle-range queries, and periodic full-table scans. Scan events are
// interleaved with short point-query bursts to model concurrent queries.
func GenerateTrace(cfg Config) []Event {
	cfg = cfg.Normalize()
	rng := rand.New(rand.NewSource(cfg.Seed))
	events := make([]Event, 0, cfg.Events)

	now := int64(1_000)
	nextScanAt := now + 700
	var scan *activeScan
	querySeq := 0
	scanSeq := 0

	appendEvent := func(at int64, tx, qid, class string, id disk.PageID, write bool, tag int64) {
		if len(events) >= cfg.Events {
			return
		}
		events = append(events, Event{
			Seq:      len(events) + 1,
			AtMS:     at,
			TxID:     tx,
			QueryID:  qid,
			Class:    class,
			PageID:   id,
			Write:    write,
			WriteTag: tag,
		})
	}

	for len(events) < cfg.Events {
		now += 1 + rng.Int63n(4)

		if scan == nil && now >= nextScanAt {
			scanSeq++
			scan = &activeScan{
				queryID: fmt.Sprintf("full-scan-%d", scanSeq),
				txID:    fmt.Sprintf("stats-tx-%d", scanSeq),
				next:    0,
			}
		}

		// Service the active scan on most scheduler steps, while non-scan
		// steps emit short hot/random query bursts.
		if scan != nil && rng.Intn(100) < 72 {
			appendEvent(now, scan.txID, scan.queryID, ClassScan, scan.next, false, 0)
			scan.next++
			if int(scan.next) >= cfg.PageCount {
				scan = nil
				nextScanAt = now + cfg.ScanPeriodMS
			}
			continue
		}

		querySeq++
		class := choosePointClass(rng)
		txID := fmt.Sprintf("tx-%d", querySeq)
		queryID := fmt.Sprintf("q-%d", querySeq)
		ops := 2 + rng.Intn(9)
		if class == ClassRandom {
			ops = 1 + rng.Intn(4)
		}

		lastPage := disk.PageID(-1)
		for op := 0; op < ops && len(events) < cfg.Events; op++ {
			now += 1 + rng.Int63n(7)
			var id disk.PageID
			switch class {
			case ClassHotspot:
				id = hotPage(rng, cfg.HotPages)
			case ClassMixed:
				id = mixedPage(rng, cfg)
			default:
				id = disk.PageID(rng.Intn(cfg.PageCount))
			}

			// Explicit short-transaction repeat pattern for the configurable
			// same-tx/same-page de-duplication rule.
			if lastPage >= 0 && class == ClassHotspot && rng.Intn(100) < 30 {
				id = lastPage
			}
			lastPage = id

			write := false
			if class == ClassHotspot {
				write = rng.Intn(1000) < cfg.WritePermille*2
			} else {
				write = rng.Intn(1000) < cfg.WritePermille/3
			}
			appendEvent(now, txID, queryID, class, id, write, int64(querySeq*100+op))
		}
	}

	return events
}

func choosePointClass(rng *rand.Rand) string {
	v := rng.Intn(100)
	switch {
	case v < 58:
		return ClassHotspot
	case v < 84:
		return ClassRandom
	default:
		return ClassMixed
	}
}

func hotPage(rng *rand.Rand, hotPages int) disk.PageID {
	v := math.Pow(rng.Float64(), 2.2)
	id := int(v * float64(hotPages))
	if id >= hotPages {
		id = hotPages - 1
	}
	return disk.PageID(id)
}

func mixedPage(rng *rand.Rand, cfg Config) disk.PageID {
	lo := cfg.HotPages
	hi := cfg.PageCount / 4
	if hi <= lo {
		hi = cfg.PageCount
	}
	return disk.PageID(lo + rng.Intn(hi-lo))
}

// EffectiveReferences applies the same transaction/page time-window collapse
// used by the live pool before offline replacement comparison.
func EffectiveReferences(events []Event, dedupWindowMS int64) []replace.Reference {
	type key struct {
		tx string
		id disk.PageID
	}
	last := make(map[key]int64)
	out := make([]replace.Reference, 0, len(events))
	for _, e := range events {
		k := key{tx: e.TxID, id: e.PageID}
		prev, ok := last[k]
		duplicate := ok && e.AtMS-prev <= dedupWindowMS
		// Mirror the pool's sliding adjacent-reference window.
		last[k] = e.AtMS
		if duplicate {
			continue
		}
		out = append(out, replace.Reference{Page: e.PageID, AtMS: e.AtMS})
	}
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
