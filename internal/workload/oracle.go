package workload

import (
	"fmt"
	"sync"

	"catalogbuffer/internal/disk"
)

// Oracle records the latest committed block for every written page.
type Oracle struct {
	mu     sync.Mutex
	latest map[disk.PageID]disk.Block
	writes uint64
}

func NewOracle() *Oracle {
	return &Oracle{latest: make(map[disk.PageID]disk.Block)}
}

// Observe matches buffer.WriteObserver and is called under pool lock.
// It still uses its own mutex so the verification read path can snapshot safely.
func (o *Oracle) Observe(id disk.PageID, b disk.Block, lsn int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.latest[id] = b
	o.writes++
}

func (o *Oracle) WrittenPages() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.latest)
}

func (o *Oracle) WriteCount() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.writes
}

type ConsistencyReport struct {
	OK                bool     `json:"ok"`
	WrittenPages      int      `json:"writtenPages"`
	WriteObservations uint64   `json:"writeObservations"`
	CheckedDiskPages  int      `json:"checkedDiskPages"`
	CheckedWrites     int      `json:"checkedWrites"`
	Mismatches        []string `json:"mismatches"`
}

// Verify checks every observed last write against the simulated disk and
// validates all disk checksums. Call it after Pool.Close/FlushAll.
func (o *Oracle) Verify(d *disk.Disk) ConsistencyReport {
	o.mu.Lock()
	want := make(map[disk.PageID]disk.Block, len(o.latest))
	for id, b := range o.latest {
		want[id] = b
	}
	writes := o.writes
	o.mu.Unlock()

	rep := ConsistencyReport{
		OK:                true,
		WrittenPages:      len(want),
		WriteObservations: writes,
		Mismatches:        []string{},
	}

	all, err := d.ReadAllForVerification()
	if err != nil {
		rep.OK = false
		rep.Mismatches = append(rep.Mismatches, err.Error())
		return rep
	}
	rep.CheckedDiskPages = len(all)
	onDisk := make(map[disk.PageID]disk.Block, len(all))
	for _, b := range all {
		if !b.Verify() {
			rep.OK = false
			rep.Mismatches = append(rep.Mismatches, fmt.Sprintf("disk page %d checksum invalid", b.ID))
		}
		onDisk[b.ID] = b
	}

	for id, expected := range want {
		rep.CheckedWrites++
		got, err := d.Read(id)
		if err != nil {
			rep.OK = false
			rep.Mismatches = append(rep.Mismatches, err.Error())
			continue
		}
		if !disk.ExactEqual(expected, got) {
			rep.OK = false
			rep.Mismatches = append(rep.Mismatches, fmt.Sprintf(
				"page %d mismatch: expected LSN %d version %d crc %d, disk LSN %d version %d crc %d",
				id, expected.WriteLSN, expected.Version, expected.CRC,
				got.WriteLSN, got.Version, got.CRC))
		}
	}
	if len(rep.Mismatches) > 100 {
		rep.Mismatches = rep.Mismatches[:100]
	}
	return rep
}
