package buffer

import (
	"fmt"
	"sort"

	"catalogbuffer/internal/disk"
)

type FrameView struct {
	ID               disk.PageID `json:"id"`
	QueryType        string      `json:"queryType"`
	Segment          string      `json:"segment"`
	SegmentCode      int         `json:"segmentCode"`
	AccessCount      int         `json:"accessCount"`
	InsertedAtMS     int64       `json:"insertedAtMs"`
	AgeMS            int64       `json:"ageMs"`
	LastAccessMS     int64       `json:"lastAccessMs"`
	LastAccessAgeMS  int64       `json:"lastAccessAgeMs"`
	RecentMS         []int64     `json:"recentMs"`
	RecentAgesMS     []int64     `json:"recentAgesMs"`
	Dirty            bool        `json:"dirty"`
	DirtyAgeMS       int64       `json:"dirtyAgeMs"`
	Prefetch         bool        `json:"prefetch"`
	PrefetchUsed     bool        `json:"prefetchUsed"`
	PrefetchAgeMS    int64       `json:"prefetchAgeMs"`
	WriteLSN         int64       `json:"writeLsn"`
	Version          int64       `json:"version"`
	CRC              uint64      `json:"crc"`
	Candidate        bool        `json:"candidate"`
	PriorityTier     int         `json:"priorityTier"`
	PriorityKeyMS    int64       `json:"priorityKeyMs"`
	PriorityKeyAgeMS int64       `json:"priorityKeyAgeMs"`
}

type CandidateView struct {
	Frame  FrameView `json:"frame"`
	Reason string    `json:"reason"`
}

type FlushJobView struct {
	Position     int         `json:"position"`
	PageID       disk.PageID `json:"pageId"`
	WriteLSN     int64       `json:"writeLsn"`
	DirtySinceMS int64       `json:"dirtySinceMs"`
	DirtyAgeMS   int64       `json:"dirtyAgeMs"`
	QueryType    string      `json:"queryType"`
}

type GhostView struct {
	PageID        disk.PageID `json:"pageId"`
	AccessCount   int         `json:"accessCount"`
	EvictedAtMS   int64       `json:"evictedAtMs"`
	EvictedAgoMS  int64       `json:"evictedAgoMs"`
	HistoryAgesMS []int64     `json:"historyAgesMs"`
	QueryType     string      `json:"queryType"`
}

type State struct {
	NowMS      int64          `json:"nowMs"`
	Config     Config         `json:"config"`
	Metrics    Metrics        `json:"metrics"`
	Disk       disk.Stats     `json:"disk"`
	Candidate  *CandidateView `json:"candidate"`
	Usage      []TypeUsage    `json:"usage"`
	DirtyPages []FrameView    `json:"dirtyPages"`
	FlushQueue []FlushJobView `json:"flushQueue"`
	Ghosts     []GhostView    `json:"ghosts"`
	HeapSize   int            `json:"heapSize"`
}

type FrameQuery struct {
	Offset  int
	Limit   int
	Search  string
	Class   string
	Segment string
}

type FrameList struct {
	Total  int         `json:"total"`
	Offset int         `json:"offset"`
	Limit  int         `json:"limit"`
	Items  []FrameView `json:"items"`
}

func (p *Pool) stateNow(now int64) int64 {
	if now > 0 {
		return now
	}
	return p.nowFunc().UnixMilli()
}

func (p *Pool) frameViewLocked(f *Frame, now int64, recentN int, candidateID disk.PageID) FrameView {
	if recentN <= 0 {
		recentN = p.cfg.RecentHistoryCount
	}
	recent := f.History
	if len(recent) > recentN {
		recent = recent[len(recent)-recentN:]
	}
	recentCopy := append([]int64(nil), recent...)
	ages := make([]int64, len(recentCopy))
	for i, t := range recentCopy {
		ages[i] = maxInt64(0, now-t)
	}
	lastAge := int64(-1)
	if f.LastAccess > 0 {
		lastAge = maxInt64(0, now-f.LastAccess)
	}
	dirtyAge := int64(-1)
	if f.Dirty {
		dirtyAge = maxInt64(0, now-f.DirtySince)
	}
	prefetchAge := int64(-1)
	if f.Prefetch {
		prefetchAge = maxInt64(0, now-f.PrefetchAt)
	}
	keyAge := maxInt64(0, now-f.PriorityKey)
	return FrameView{
		ID:               f.ID,
		QueryType:        f.FetchClass,
		Segment:          SegmentName(f.Segment),
		SegmentCode:      f.Segment,
		AccessCount:      f.AccessCount,
		InsertedAtMS:     f.InsertedAt,
		AgeMS:            maxInt64(0, now-f.InsertedAt),
		LastAccessMS:     f.LastAccess,
		LastAccessAgeMS:  lastAge,
		RecentMS:         recentCopy,
		RecentAgesMS:     ages,
		Dirty:            f.Dirty,
		DirtyAgeMS:       dirtyAge,
		Prefetch:         f.Prefetch,
		PrefetchUsed:     f.PrefetchUsed,
		PrefetchAgeMS:    prefetchAge,
		WriteLSN:         f.WriteLSN,
		Version:          f.Block.Version,
		CRC:              f.Block.CRC,
		Candidate:        f.ID == candidateID,
		PriorityTier:     f.PriorityTier,
		PriorityKeyMS:    f.PriorityKey,
		PriorityKeyAgeMS: keyAge,
	}
}

// State returns the summary shown at the top of the observer page.
func (p *Pool) State(now int64) State {
	p.mu.Lock()
	defer p.mu.Unlock()
	now = p.stateNow(now)

	m := p.metrics
	m.Frames = len(p.pages)
	m.GhostEntries = len(p.ghost)
	m.DirtyFrames = p.countDirtyLocked()
	m.FlushQueue = p.flushQueue.Len()
	m.CapacityTarget = p.cfg.Capacity
	m.CurrentLSN = p.lsn

	var candidateID disk.PageID = -1
	var cand *CandidateView
	if p.victims.Len() > 0 {
		f := p.victims[0]
		p.refreshPriorityLocked(f)
		candidateID = f.ID
		cand = &CandidateView{
			Frame:  p.frameViewLocked(f, now, p.cfg.RecentHistoryCount, candidateID),
			Reason: p.candidateReasonLocked(f, now),
		}
	}

	usage := make([]TypeUsage, 0, len(p.usage))
	for _, u := range p.usage {
		usage = append(usage, *u)
	}
	sort.Slice(usage, func(i, j int) bool {
		if usage[i].Frames != usage[j].Frames {
			return usage[i].Frames > usage[j].Frames
		}
		return usage[i].QueryType < usage[j].QueryType
	})

	dirty := make([]*Frame, 0)
	for _, f := range p.pages {
		if f.Dirty {
			dirty = append(dirty, f)
		}
	}
	sort.Slice(dirty, func(i, j int) bool {
		if dirty[i].DirtySince != dirty[j].DirtySince {
			return dirty[i].DirtySince < dirty[j].DirtySince
		}
		return dirty[i].ID < dirty[j].ID
	})
	dirtyViews := make([]FrameView, 0, minInt(50, len(dirty)))
	for i, f := range dirty {
		if i >= 50 {
			break
		}
		dirtyViews = append(dirtyViews, p.frameViewLocked(f, now, 4, candidateID))
	}
	queue := make([]FlushJobView, 0, len(dirty))
	for i, f := range dirty {
		queue = append(queue, FlushJobView{
			Position:     i + 1,
			PageID:       f.ID,
			WriteLSN:     f.WriteLSN,
			DirtySinceMS: f.DirtySince,
			DirtyAgeMS:   maxInt64(0, now-f.DirtySince),
			QueryType:    f.FetchClass,
		})
	}

	ghosts := make([]GhostView, 0, len(p.ghost))
	for id, g := range p.ghost {
		histAges := make([]int64, 0, len(g.History))
		for _, t := range g.History {
			histAges = append(histAges, maxInt64(0, now-t))
		}
		// Reverse ages to show most recent first.
		for i, j := 0, len(histAges)-1; i < j; i, j = i+1, j-1 {
			histAges[i], histAges[j] = histAges[j], histAges[i]
		}
		ghosts = append(ghosts, GhostView{
			PageID:        id,
			AccessCount:   g.AccessCount,
			EvictedAtMS:   g.EvictedAt,
			EvictedAgoMS:  maxInt64(0, now-g.EvictedAt),
			HistoryAgesMS: histAges,
			QueryType:     g.FetchClass,
		})
	}
	sort.Slice(ghosts, func(i, j int) bool { return ghosts[i].EvictedAgoMS < ghosts[j].EvictedAgoMS })
	if len(ghosts) > 60 {
		ghosts = ghosts[:60]
	}

	return State{
		NowMS:      now,
		Config:     p.cfg,
		Metrics:    m,
		Disk:       p.disk.Stats(),
		Candidate:  cand,
		Usage:      usage,
		DirtyPages: dirtyViews,
		FlushQueue: queue,
		Ghosts:     ghosts,
		HeapSize:   p.victims.Len(),
	}
}

// Frames returns a filterable page of frame views. Every resident page is
// reachable through offset/limit; the observer does not embed hundreds of
// thousands of rows in /api/state.
func (p *Pool) Frames(q FrameQuery) FrameList {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.stateNow(0)
	var candidateID disk.PageID = -1
	if p.victims.Len() > 0 {
		candidateID = p.victims[0].ID
	}

	all := make([]*Frame, 0, len(p.pages))
	for _, f := range p.pages {
		if q.Class != "" && q.Class != "all" && f.FetchClass != q.Class {
			continue
		}
		if q.Segment != "" && q.Segment != "all" && SegmentName(f.Segment) != q.Segment {
			continue
		}
		if q.Search != "" {
			id, err := parsePageID(q.Search)
			if err != nil || f.ID != id {
				continue
			}
		}
		all = append(all, f)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	total := len(all)
	if q.Offset < 0 {
		q.Offset = 0
	}
	if q.Limit <= 0 || q.Limit > 1000 {
		q.Limit = 100
	}
	start := q.Offset
	if start > total {
		start = total
	}
	end := start + q.Limit
	if end > total {
		end = total
	}
	items := make([]FrameView, 0, end-start)
	for _, f := range all[start:end] {
		items = append(items, p.frameViewLocked(f, now, p.cfg.RecentHistoryCount, candidateID))
	}
	return FrameList{Total: total, Offset: start, Limit: q.Limit, Items: items}
}

// CleanCheckResult verifies the invariant that a non-dirty cached page is
// byte-for-byte equal to the last block read from / written to disk.
type CleanCheckResult struct {
	CheckedFrames int      `json:"checkedFrames"`
	DirtySkipped  int      `json:"dirtySkipped"`
	Mismatches    []string `json:"mismatches"`
	OK            bool     `json:"ok"`
}

func (p *Pool) CheckCleanPagesAgainstDisk() CleanCheckResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	res := CleanCheckResult{OK: true}
	for _, f := range p.pages {
		if f.Dirty {
			res.DirtySkipped++
			continue
		}
		res.CheckedFrames++
		got, err := p.disk.Read(f.ID)
		if err != nil {
			res.OK = false
			res.Mismatches = append(res.Mismatches, err.Error())
			continue
		}
		if !disk.ExactEqual(got, f.Block) {
			res.OK = false
			res.Mismatches = append(res.Mismatches,
				fmt.Sprintf("page %d differs between clean cache frame and disk", f.ID))
		}
		if !f.Block.Verify() || !got.Verify() {
			res.OK = false
			res.Mismatches = append(res.Mismatches, fmt.Sprintf("page %d checksum failure", f.ID))
		}
	}
	if len(res.Mismatches) > 0 {
		res.OK = false
		if len(res.Mismatches) > 50 {
			res.Mismatches = res.Mismatches[:50]
		}
	}
	return res
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
