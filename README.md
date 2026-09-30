# telemetry-batch

埋点批量上报：按批次大小与时间窗口出批，缓冲有上限，确认幂等，只用 Go 标准库。

```
go test ./...
go run ./cmd/telemetry-batch --sample size
go run ./cmd/telemetry-batch --sample window
go run ./cmd/telemetry-batch --sample cap
```

## 口径（README 为准）

- **按大小出批**：`Add` 之后缓冲达到 `BatchSize` 就要出批，批次事件按 `AtMs` 升序（同刻按录入顺序）。
- **按时间出批**：`Flush(nowMs)` 只在缓冲非空且最早事件距 `nowMs` 不小于 `FlushMs` 时出批
  （不足 `BatchSize` 也要出），否则返回空。
- **缓冲有上限**：超过 `MaxPending` 丢最旧的事件并把 `Dropped` 加一（不许无界增长）。
- **确认幂等**：`Ack` 第一次返回 `true`，重复确认返回 `false`。
- **查询代价**：`Flush` 不许每次全表扫描缓冲（用头部指针/时间索引），`Scanned` 不随缓冲长度线性增长。
- **规模**：单进程 100 万条事件积压、每秒 10 万次入队与出批，内存 O(MaxPending)。

## 输出契约（不改格式）

```
batches=2 pending=50
early=0 late=1
pending=1000 dropped=2000
first=true second=false
```

## 常量

```
BatchSize  = 100
FlushMs    = 5000
MaxPending = 1000
```

## 目录

```
batch.go                埋点缓冲与出批
cmd/telemetry-batch     命令行入口
batch_test.go           go test 用例
```
