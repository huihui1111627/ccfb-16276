package web

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
	"strconv"
	"sync"
	"time"

	"catalogbuffer/internal/buffer"
	"catalogbuffer/internal/disk"
	"catalogbuffer/internal/sim"
	"catalogbuffer/internal/workload"
)

//go:embed static/*
var staticFS embed.FS

type TimeSample struct {
	AtMS              int64   `json:"atMs"`
	ElapsedMS         int64   `json:"elapsedMs"`
	WindowRequests    uint64  `json:"windowRequests"`
	WindowHits        uint64  `json:"windowHits"`
	WindowHitRate     float64 `json:"windowHitRate"`
	CumulativeHitRate float64 `json:"cumulativeHitRate"`
	Frames            int     `json:"frames"`
	DirtyFrames       int     `json:"dirtyFrames"`
	Evictions         uint64  `json:"evictions"`
	PrefetchIssued    uint64  `json:"prefetchIssued"`
}

type Server struct {
	pool   *buffer.Pool
	disk   *disk.Disk
	oracle *workload.Oracle
	runner *workload.LiveRunner

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	startedAt time.Time

	tsMu    sync.Mutex
	samples []TimeSample

	compareMu sync.Mutex
	compare   sim.Comparison

	verifyMu sync.Mutex
	verify   *sim.VerificationReport
}

func NewServer() (*Server, error) {
	wcfg := workload.DefaultLiveConfig()
	d := disk.New(wcfg.PageCount, wcfg.Seed, disk.Config{})
	oracle := workload.NewOracle()
	cfg := buffer.DefaultConfig()
	p := buffer.New(cfg, d, oracle.Observe)

	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		pool:      p,
		disk:      d,
		oracle:    oracle,
		ctx:       ctx,
		cancel:    cancel,
		startedAt: time.Now(),
		samples:   make([]TimeSample, 0, 360),
		compare:   sim.CompareStrategies(workload.DefaultConfig(), cfg),
	}

	p.Start(ctx, buffer.DefaultBackgroundOptions())
	s.runner = workload.StartLive(ctx, p, wcfg)
	s.startSampler()

	// The initial report makes the correctness panel meaningful immediately.
	report, err := sim.RunFullVerification(36000)
	if err != nil {
		cancel()
		return nil, err
	}
	s.verify = report
	return s, nil
}

func (s *Server) startSampler() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()

		prev := s.pool.Metrics()
		prevAt := time.Now()
		for {
			select {
			case <-s.ctx.Done():
				return
			case now := <-t.C:
				cur := s.pool.Metrics()
				dtMS := now.Sub(prevAt).Milliseconds()
				_ = dtMS
				reqDelta := cur.Requests - prev.Requests
				hitDelta := cur.Hits - prev.Hits
				rate := 0.0
				if reqDelta > 0 {
					rate = float64(hitDelta) / float64(reqDelta)
				}
				s.tsMu.Lock()
				s.samples = append(s.samples, TimeSample{
					AtMS:              now.UnixMilli(),
					ElapsedMS:         now.Sub(s.startedAt).Milliseconds(),
					WindowRequests:    reqDelta,
					WindowHits:        hitDelta,
					WindowHitRate:     rate,
					CumulativeHitRate: cur.HitRate(),
					Frames:            cur.Frames,
					DirtyFrames:       cur.DirtyFrames,
					Evictions:         cur.Evictions,
					PrefetchIssued:    cur.PrefetchIssued,
				})
				if len(s.samples) > 360 {
					s.samples = append([]TimeSample(nil), s.samples[len(s.samples)-360:]...)
				}
				s.tsMu.Unlock()
				prev = cur
				prevAt = now
			}
		}
	}()
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServerFS(staticSub)))
	mux.HandleFunc("GET /", s.handleIndex)
	mux.HandleFunc("GET /api/state", s.handleState)
	mux.HandleFunc("GET /api/frames", s.handleFrames)
	mux.HandleFunc("GET /api/timeseries", s.handleTimeSeries)
	mux.HandleFunc("GET /api/compare", s.handleCompare)
	mux.HandleFunc("POST /api/config", s.handleConfig)
	mux.HandleFunc("POST /api/capacity", s.handleCapacity)
	mux.HandleFunc("GET /api/verify", s.handleVerify)
	mux.HandleFunc("POST /api/maintenance", s.handleMaintenance)
	return mux
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	b, err := staticFS.ReadFile("static/index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(b)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.pool.State(0))
}

func (s *Server) handleFrames(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	offset, _ := strconv.Atoi(q.Get("offset"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	list := s.pool.Frames(buffer.FrameQuery{
		Offset:  offset,
		Limit:   limit,
		Search:  q.Get("q"),
		Class:   q.Get("class"),
		Segment: q.Get("segment"),
	})
	writeJSON(w, list)
}

func (s *Server) handleTimeSeries(w http.ResponseWriter, r *http.Request) {
	s.tsMu.Lock()
	out := append([]TimeSample(nil), s.samples...)
	s.tsMu.Unlock()
	writeJSON(w, out)
}

func (s *Server) handleCompare(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("capacity") == "" && q.Get("k") == "" {
		s.compareMu.Lock()
		cmp := s.compare
		s.compareMu.Unlock()
		writeJSON(w, cmp)
		return
	}
	cfg := buffer.DefaultConfig()
	if n, err := strconv.Atoi(q.Get("capacity")); err == nil && n > 0 {
		cfg.Capacity = n
	}
	if k, err := strconv.Atoi(q.Get("k")); err == nil && k > 0 {
		cfg.K = k
	}
	cmp := sim.CompareStrategies(workload.DefaultConfig(), cfg)
	writeJSON(w, cmp)
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	var patch buffer.ConfigPatch
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	cfg := s.pool.ApplyConfigPatch(patch)
	writeJSON(w, cfg)
}

func (s *Server) handleCapacity(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Capacity int `json:"capacity"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	cfg := s.pool.SetCapacity(body.Capacity)
	writeJSON(w, cfg)
}

func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("live") == "1" {
		writeJSON(w, s.pool.CheckCleanPagesAgainstDisk())
		return
	}
	s.verifyMu.Lock()
	rep := s.verify
	s.verifyMu.Unlock()
	if refresh := r.URL.Query().Get("refresh"); refresh == "1" || rep == nil {
		events := 36000
		if n, err := strconv.Atoi(r.URL.Query().Get("events")); err == nil && n >= 1000 {
			events = n
		}
		fresh, err := sim.RunFullVerification(events)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s.verifyMu.Lock()
		s.verify = fresh
		s.verifyMu.Unlock()
		rep = fresh
	}
	writeJSON(w, rep)
}

func (s *Server) handleMaintenance(w http.ResponseWriter, r *http.Request) {
	s.pool.Maintenance(time.Now().UnixMilli())
	writeJSON(w, map[string]bool{"ok": true})
}

func (s *Server) Close() error {
	s.cancel()
	if s.runner != nil {
		s.runner.Wait()
	}
	s.wg.Wait()
	return s.pool.Close()
}
