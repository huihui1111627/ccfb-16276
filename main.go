package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"catalogbuffer/internal/buffer"
	"catalogbuffer/internal/sim"
	"catalogbuffer/internal/web"
	"catalogbuffer/internal/workload"
)

func main() {
	if len(os.Args) == 1 {
		serveCmd([]string{"-addr", ":8090"})
		return
	}
	switch os.Args[1] {
	case "serve":
		serveCmd(os.Args[2:])
	case "verify":
		verifyCmd(os.Args[2:])
	case "compare":
		compareCmd(os.Args[2:])
	case "help", "-h", "--help":
		printHelp()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		printHelp()
		os.Exit(2)
	}
}

func printHelp() {
	fmt.Println(`catalogbuffer — 邮品目录库缓冲池模拟

用法:
  go run . serve [-addr :8090]   启动负载和观察页面
  go run . verify [-events 72000] [-json]
                                 固定种子回放、OPT/LRU/Clock/LRU-K 对比、字节级一致性校验
  go run . compare [-capacity 360] [-k 2]
                                 只输出离线命中率对比 JSON`)
}

func serveCmd(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8090", "HTTP listen address")
	_ = fs.Parse(args)

	server, err := web.NewServer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "init server: %v\n", err)
		os.Exit(1)
	}
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		fmt.Printf("缓冲池观察台已启动: http://localhost%s\n", *addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "http server: %v\n", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	fmt.Println("\n正在停止并刷出全部脏页…")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	if err := server.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close pool: %v\n", err)
		os.Exit(1)
	}
}

func verifyCmd(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	events := fs.Int("events", 72000, "deterministic trace events")
	asJSON := fs.Bool("json", false, "emit full report as JSON")
	_ = fs.Parse(args)

	report, err := sim.RunFullVerification(*events)
	if err != nil {
		fmt.Fprintf(os.Stderr, "verification failed to run: %v\n", err)
		os.Exit(1)
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report)
	} else {
		printVerification(report)
	}
	if !report.OK {
		os.Exit(1)
	}
}

func compareCmd(args []string) {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	capacity := fs.Int("capacity", 360, "buffer capacity in pages")
	k := fs.Int("k", 2, "LRU-K k")
	_ = fs.Parse(args)
	cfg := buffer.DefaultConfig()
	cfg.Capacity = *capacity
	cfg.K = *k
	cmp := sim.CompareStrategies(workload.DefaultConfig(), cfg)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(cmp)
}

func printVerification(r *sim.VerificationReport) {
	fmt.Println("================ 缓冲池交付校验 ================")
	fmt.Printf("总体: %v\n", okText(r.OK))
	fmt.Printf("负载: seed=%d pages=%d events=%d effectiveRefs=%d\n",
		r.Workload.Seed, r.Workload.PageCount, r.Workload.Events, r.Comparison.EffectiveRefs)
	fmt.Printf("容量=%d K=%d 去重窗口=%dms\n", r.Config.Capacity, r.Config.K, r.Config.DedupWindowMS)
	fmt.Println()
	fmt.Println("命中率对比（同一 trace / 同一容量）:")
	for _, x := range r.Comparison.Results {
		fmt.Printf("  %-28s hits=%7d/%7d  hit-rate=%6.2f%%\n",
			x.Name, x.Hits, x.References, x.HitRate*100)
	}
	fmt.Printf("OPT 上界校验: %v\n", okText(r.Comparison.OPTUpperBoundOK))
	fmt.Println()
	c := r.Consistency
	fmt.Printf("最后写入一致性: %v（写过 %d 页 / %d 次写；磁盘页 %d；比对写入 %d；mismatch=%d）\n",
		okText(c.OK), c.WrittenPages, c.WriteObservations, c.CheckedDiskPages,
		c.CheckedWrites, len(c.Mismatches))
	cc := r.CleanCheck
	fmt.Printf("干净缓存帧=磁盘: %v（checked=%d dirtySkipped=%d mismatch=%d）\n",
		okText(cc.OK), cc.CheckedFrames, cc.DirtySkipped, len(cc.Mismatches))
	fmt.Printf("全新池读回所有磁盘页: %v（pages=%d mismatch=%d）\n",
		okText(r.ReadThrough.OK), r.ReadThrough.Pages, len(r.ReadThrough.Mismatches))
	fmt.Println()
	m := r.PoolMetrics
	fmt.Printf("Pool: requests=%d hits=%d misses=%d evictions=%d dirtyEvictions=%d\n",
		m.Requests, m.Hits, m.Misses, m.Evictions, m.DirtyEvictions)
	fmt.Printf("Prefetch: issued=%d hits=%d used=%d wasted=%d reaped=%d\n",
		m.PrefetchIssued, m.PrefetchHits, m.PrefetchUsed, m.PrefetchWasted, m.PrefetchReaped)
	if !r.OK {
		fmt.Println("\n失败详情:")
		for _, e := range r.Errors {
			fmt.Println("ERROR:", e)
		}
		for _, e := range c.Mismatches {
			fmt.Println("MISMATCH:", e)
		}
		for _, e := range cc.Mismatches {
			fmt.Println("CACHE/DISK:", e)
		}
		for _, e := range r.ReadThrough.Mismatches {
			fmt.Println("READ:", e)
		}
	}
}

func okText(ok bool) string {
	if ok {
		return "✅ 通过"
	}
	return "❌ 失败"
}
