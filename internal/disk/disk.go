package disk

import (
	"errors"
	"fmt"
	"hash/crc64"
	"sort"
	"sync"
	"time"
)

// PageID identifies a fixed-size database page.
type PageID int64

// PageSize is the simulated on-disk payload size.
const PageSize = 96

// Block is the exact representation persisted by the simulated disk.
// Arrays are value types so callers cannot mutate disk state through a
// returned pointer or slice.
type Block struct {
	ID       PageID
	Version  int64
	WriteLSN int64
	Data     [PageSize]byte
	CRC      uint64
}

var crcTable = crc64.MakeTable(crc64.ECMA)

// Seal calculates the checksum that protects the complete block contents.
func (b *Block) Seal() {
	b.CRC = 0
	h := crc64.New(crcTable)
	var num [8]byte
	put := func(v int64) {
		num[0] = byte(v)
		num[1] = byte(v >> 8)
		num[2] = byte(v >> 16)
		num[3] = byte(v >> 24)
		num[4] = byte(v >> 32)
		num[5] = byte(v >> 40)
		num[6] = byte(v >> 48)
		num[7] = byte(v >> 56)
		_, _ = h.Write(num[:])
	}
	put(int64(b.ID))
	put(b.Version)
	put(b.WriteLSN)
	_, _ = h.Write(b.Data[:])
	b.CRC = h.Sum64()
}

// Verify reports whether the checksum matches the block contents.
func (b Block) Verify() bool {
	want := b.CRC
	b.CRC = 0
	h := crc64.New(crcTable)
	var num [8]byte
	put := func(v int64) {
		for i := 0; i < 8; i++ {
			num[i] = byte(v >> (8 * i))
		}
		_, _ = h.Write(num[:])
	}
	put(int64(b.ID))
	put(b.Version)
	put(b.WriteLSN)
	_, _ = h.Write(b.Data[:])
	return want == h.Sum64()
}

// Config controls optional simulated disk service costs. The simulation keeps
// the disk in memory, but accounts for latency without forcing tests to sleep.
type Config struct {
	ReadLatency  time.Duration
	WriteLatency time.Duration
}

// Disk is a goroutine-safe simulated data directory.
type Disk struct {
	mu      sync.RWMutex
	blocks  map[PageID]Block
	pageN   int
	cfg     Config
	reads   uint64
	writes  uint64
	readNS  int64
	writeNS int64
}

// New creates pageCount deterministic, checksum-valid catalog pages.
func New(pageCount int, seed int64, cfg Config) *Disk {
	if pageCount <= 0 {
		pageCount = 1
	}
	d := &Disk{
		blocks: make(map[PageID]Block, pageCount),
		pageN:  pageCount,
		cfg:    cfg,
	}
	for i := 0; i < pageCount; i++ {
		id := PageID(i)
		b := Block{ID: id, Version: 1, WriteLSN: 0}
		for j := 0; j < PageSize; j++ {
			// Deterministic non-uniform initial content. The seed and page id
			// both matter, so corruption cannot be hidden by a repeated pattern.
			b.Data[j] = byte(int(seed>>8) + i*31 + j*17 + (i*j)%251)
		}
		b.Seal()
		d.blocks[id] = b
	}
	return d
}

func (d *Disk) PageCount() int { return d.pageN }

func (d *Disk) valid(id PageID) bool {
	return id >= 0 && int(id) < d.pageN
}

var ErrInvalidPage = errors.New("invalid page id")

// Read returns a value copy of the persisted block.
func (d *Disk) Read(id PageID) (Block, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.valid(id) {
		return Block{}, fmt.Errorf("%w: %d", ErrInvalidPage, id)
	}
	b := d.blocks[id]
	d.reads++
	d.readNS += d.cfg.ReadLatency.Nanoseconds()
	return b, nil
}

// Write persists an exact value copy and verifies its checksum first.
func (d *Disk) Write(b Block) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.valid(b.ID) {
		return fmt.Errorf("%w: %d", ErrInvalidPage, b.ID)
	}
	if !b.Verify() {
		return fmt.Errorf("refusing corrupt write to page %d: checksum mismatch", b.ID)
	}
	d.blocks[b.ID] = b
	d.writes++
	d.writeNS += d.cfg.WriteLatency.Nanoseconds()
	return nil
}

// ExactEqual compares every persisted byte, including metadata and checksum.
func ExactEqual(a, b Block) bool {
	return a == b
}

// Stats is a point-in-time copy of disk counters.
type Stats struct {
	Pages   int     `json:"pages"`
	Reads   uint64  `json:"reads"`
	Writes  uint64  `json:"writes"`
	ReadMS  float64 `json:"readMs"`
	WriteMS float64 `json:"writeMs"`
}

func (d *Disk) Stats() Stats {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return Stats{
		Pages:   d.pageN,
		Reads:   d.reads,
		Writes:  d.writes,
		ReadMS:  float64(d.readNS) / 1e6,
		WriteMS: float64(d.writeNS) / 1e6,
	}
}

// ReadAllForVerification reads every page in stable order. It is intentionally
// linear and is used only by the final correctness check.
func (d *Disk) ReadAllForVerification() ([]Block, error) {
	ids := make([]PageID, 0, d.pageN)
	d.mu.RLock()
	for id := range d.blocks {
		ids = append(ids, id)
	}
	d.mu.RUnlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]Block, 0, len(ids))
	for _, id := range ids {
		b, err := d.Read(id)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}
