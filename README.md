# 邮品目录库缓冲池模拟（Go）

一个纯 Go 标准库实现的数据库缓冲池模拟器：内存数据页、模拟磁盘、固定种子混合负载、
并发查询、离线替换算法对比，以及一个无需外网 CDN 的内置观察页面。

## 快速开始

```bash
# 启动并发负载和观察台（默认 :8090）
go run . serve -addr :8090
# 浏览器打开 http://127.0.0.1:8090/

# 固定种子全量回放：OPT/LRU/Clock/LRU-K 对比 + 字节级一致性校验
go run . verify -events 72000

# 只看离线对比 JSON
go run . compare -capacity 360 -k 2

# 单元测试 / 竞态测试
go test ./...
go test -race ./...

# 30 万页堆性能
go test ./internal/buffer -run 'TestEvictInsertLatency300k' -v
go test ./internal/buffer -run '^$' \
  -bench 'Benchmark(CandidatePeek|AccessHeapUpdate)300k' -benchtime=2000x
```

## 观察页面包含什么

- KPI：总命中率、去重后有效命中率、帧数/目标容量、磁盘读写、淘汰、脏页、幽灵历史、预读。
- **命中率时间曲线**：每 0.5s 采样窗口命中率和累计命中率。
- **替换方式对比图/表**：同一固定 trace、同一容量下的 OPT、LRU-K、LRU、Clock。
- **当前淘汰候选**：页 ID、候选原因、所属查询类型、老少段、优先级键、最近访问时间。
- **每个缓存页的最近访问时间**：分页表格可按页 ID、查询类型、old/young 段筛选。
- **脏页和刷盘队列**：脏时长、LSN、来源查询类型；淘汰脏页前会先同步写模拟磁盘。
- **按查询类型统计池子占用**：hotspot / scan / random / mixed 的帧数、字节、脏页、预读。
- **幽灵历史表**：淘汰后保留的访问时间链和剩余保留时间。
- **一致性面板**：最后写入 Oracle、干净帧等于磁盘、全新池读回全磁盘三类校验。

## 八条淘汰/缓冲规则的位置

1. **LRU-K**：`internal/buffer/frame.go` 的 `refreshPriorityLocked`。
   满 K 次后按“倒数第 K 次访问时间”排序；不足 K 次的帧进入更高优先淘汰层。
   `K` 可通过页面或 `POST /api/config` 修改。
2. **同事务短间隔重复访问只算一次**：`Pool.Access` 中的 `txPageKey` 去重；
   间隔由 `dedupWindowMs` 配置，采用相邻访问滑动窗口。
3. **淘汰页历史保留**：`ghostInfo` / `ghostOrder`，TTL 为 `ghostTtlMs`；
   TTL 内读回会恢复旧访问时间链。
4. **老段/少段**：新页进入 `old`；在老段停留至少 `oldMinResidencyMs` 后再次被有效访问才升到 `young`。
5. **查询类型归因**：每个 Frame 保存 `FetchClass`，`TypeUsage` 增量统计占用。
6. **几十万页快速选牺牲者**：每个帧在索引最小堆中恰好一项，候选根节点 O(1)，
   访问更新和淘汰删除为 O(log N)；改 K 时才一次性 `heap.Init` 重建。
7. **顺序识别和预读**：按 QueryID/TxID 维护步长与连续次数；识别后预取后续页。
   未使用预读保持零有效访问优先级，并有独立时间堆按 `prefetchUnusedTtlMs` 快速回收。
8. **运行时调容量**：`POST /api/capacity` 只改目标值；后台 shrink loop 按
   `shrinkBatch` 分批执行正常淘汰规则，不一次性清空。

## 负载

固定种子 `20261126`，包含：

- 实时服务模拟 **300,000 个目录页**；可在页面把目标容量热调到几十万，观察索引堆仍快速选候选。
- 离线确定性 trace 使用 5,000 页，便于快速、可重复地跑 OPT 对比和全量一致性校验。
- 邮品热门页的集中热点访问，并显式生成同事务短时间重复页；
- 周期性全表顺序扫描（月底成交统计），会冲刷热门页；
- 全目录均匀随机点查；
- 中间区间 mixed 查询；
- 实时服务默认 8 个并发点查 worker 和 1 个周期扫描 goroutine。

离线 trace 是确定性的；实时 runner 每个 worker 使用派生固定种子，实际调度由 Go 运行时决定，
但一致性 Oracle 不依赖调度顺序。

## OPT 上界

`internal/replace` 中：

- 反向预处理每个页的下一次使用位置；
- OPT 使用“最远下次使用”最大堆；
- LRU 使用 `container/list`；
- Clock 使用引用位和环形指针；
- LRU-K 使用与主池一致的 `<K` 优先层和倒数第 K 次时间键。

所有算法先使用同一个事务/时间窗口折叠 trace，再在相同容量上比较。
校验程序会断言 LRU、Clock、LRU-K 命中率均不超过 OPT（允许浮点误差）。

一次示例结果（固定 72000 events，折叠后 61216 refs，capacity=360，K=2）：

```text
OPT       58.41%
LRU-K     47.27%
LRU       40.45%
Clock     38.87%
```

## 一致性保证

写入时 Pool 在同一把锁内生成单调 LSN 和完整块校验和，并调用 WriteObserver 更新 Oracle。
脏页刷盘或脏页淘汰时把完整 `disk.Block` 值写入模拟磁盘。最终校验：

1. 所有写过的页与 Oracle 中最后一次提交的块逐字段相等；
2. 所有磁盘块 CRC 正确；
3. 所有非脏缓存帧与磁盘当前内容完全相等；
4. 关闭刷盘后，用一个全新小池逐页读回整个磁盘并逐字节比较。

当前固定种子报告为 `mismatch=0`。

## 规模性能示例

Apple M4 Pro 上一次 30 万帧实测：

```text
CandidatePeek300k   ~59 ns/op
AccessHeapUpdate    ~513 ns/op
evict+insert        ~930 ns/op
```

## 主要 HTTP API

```text
GET  /api/state
GET  /api/frames?offset=0&limit=100&q=123&class=scan&segment=old
GET  /api/timeseries
GET  /api/compare?capacity=360&k=2
GET  /api/verify
GET  /api/verify?live=1
POST /api/config
POST /api/capacity
POST /api/maintenance
```

## 目录

```text
main.go                     CLI：serve / verify / compare
internal/disk               模拟磁盘、固定页内容、CRC
internal/buffer             缓冲池、LRU-K 堆、分段、幽灵、预读、刷盘、快照 API
internal/replace            OPT / LRU / Clock / LRU-K
internal/workload           固定 trace、并发 runner、写入 Oracle
internal/sim                离线对比和全量校验编排
internal/web                HTTP API 与嵌入的观察页面
```
