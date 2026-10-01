package telemetry

import (
	"math/rand"
	"testing"
	"time"
)

func TestAddKeepsPending(t *testing.T) {
	buffer := NewBuffer()
	buffer.Add(Event{AtMs: 0, Name: "login"})
	if buffer.Pending() != 1 {
		t.Fatalf("加一条事件应该留在缓冲里：%d", buffer.Pending())
	}
}

func TestCreateBufferHasNoPending(t *testing.T) {
	if NewBuffer().Pending() != 0 {
		t.Fatal("新缓冲应该是空的")
	}
}

func TestAckFirstSucceeds(t *testing.T) {
	if !NewBuffer().Ack("b1") {
		t.Fatal("第一次确认应该成功")
	}
}

func TestConstants(t *testing.T) {
	if BatchSize != 100 || FlushMs != 5000 || MaxPending != 1000 {
		t.Fatal("埋点常量被改了")
	}
}

func TestFlushBatchesBySize(t *testing.T) {
	buffer := NewBuffer()
	for index := 0; index < 250; index++ {
		buffer.Add(Event{AtMs: int64(index), Name: "hit"})
	}
	batches := buffer.Flush(0)
	if len(batches) != 2 {
		t.Fatalf("250 条应该出 2 个整批：%d", len(batches))
	}
	for index, batch := range batches {
		if len(batch.Events) != BatchSize {
			t.Fatalf("整批应该是 %d 条：%d", BatchSize, len(batch.Events))
		}
		if batch.ID != "b"+itoa(index+1) {
			t.Fatalf("批次 ID 应该按出批顺序编号：%s", batch.ID)
		}
	}
	if buffer.Pending() != 50 {
		t.Fatalf("应该剩 50 条：%d", buffer.Pending())
	}
}

func TestBatchEventsSortedByAtMs(t *testing.T) {
	buffer := NewBuffer()
	for index := BatchSize - 1; index >= 0; index-- {
		buffer.Add(Event{AtMs: int64(index % 10), Name: "hit"})
	}
	batches := buffer.Flush(0)
	if len(batches) != 1 {
		t.Fatalf("乱序入队也应该出一个整批：%d", len(batches))
	}
	for index := 1; index < len(batches[0].Events); index++ {
		if batches[0].Events[index-1].AtMs > batches[0].Events[index].AtMs {
			t.Fatal("批次事件应该按 AtMs 升序")
		}
	}
}

func TestSameTimestampKeepsInsertionOrder(t *testing.T) {
	buffer := NewBuffer()
	for index := 0; index < BatchSize; index++ {
		buffer.Add(Event{AtMs: 42, Name: "e" + itoa(index)})
	}
	batches := buffer.Flush(0)
	if len(batches) != 1 {
		t.Fatalf("同刻事件也应该出一个整批：%d", len(batches))
	}
	for index, event := range batches[0].Events {
		if event.Name != "e"+itoa(index) {
			t.Fatalf("同刻事件应该按录入顺序出批：第 %d 条是 %s", index, event.Name)
		}
	}
}

func TestFlushRespectsTimeWindow(t *testing.T) {
	buffer := NewBuffer()
	for index := 0; index < 10; index++ {
		buffer.Add(Event{AtMs: int64(index), Name: "hit"})
	}
	if batches := buffer.Flush(1000); len(batches) != 0 {
		t.Fatalf("最早事件滞留不足 %d 不该出批：%d", FlushMs, len(batches))
	}
	if buffer.Pending() != 10 {
		t.Fatalf("没出批事件应该还在：%d", buffer.Pending())
	}
	batches := buffer.Flush(6000)
	if len(batches) != 1 || len(batches[0].Events) != 10 {
		t.Fatalf("到点不足 %d 也要出批：%v", BatchSize, batches)
	}
	if buffer.Pending() != 0 {
		t.Fatalf("出批后缓冲应该空了：%d", buffer.Pending())
	}
}

func TestFlushTimeWindowBoundary(t *testing.T) {
	buffer := NewBuffer()
	buffer.Add(Event{AtMs: 1000, Name: "hit"})
	if batches := buffer.Flush(1000 + FlushMs - 1); len(batches) != 0 {
		t.Fatalf("滞留 %d 差一毫秒不该出批：%d", FlushMs, len(batches))
	}
	if batches := buffer.Flush(1000 + FlushMs); len(batches) != 1 {
		t.Fatalf("滞留正好 %d 应该出批：%d", FlushMs, len(batches))
	}
}

func TestFlushEmptyReturnsNothing(t *testing.T) {
	if batches := NewBuffer().Flush(1 << 40); len(batches) != 0 {
		t.Fatalf("空缓冲不该出批：%d", len(batches))
	}
}

func TestAddDropsOldestBeyondCap(t *testing.T) {
	buffer := NewBuffer()
	for index := 0; index < 3000; index++ {
		buffer.Add(Event{AtMs: int64(index), Name: "hit"})
	}
	if buffer.Pending() != MaxPending {
		t.Fatalf("缓冲不该超过 %d：%d", MaxPending, buffer.Pending())
	}
	if buffer.Dropped() != 2000 {
		t.Fatalf("应该丢 2000 条最旧事件：%d", buffer.Dropped())
	}
	batches := buffer.Flush(0)
	if len(batches) != MaxPending/BatchSize {
		t.Fatalf("满缓冲应该出 %d 个整批：%d", MaxPending/BatchSize, len(batches))
	}
	if oldest := batches[0].Events[0]; oldest.AtMs != 2000 {
		t.Fatalf("最旧的 2000 条应该被丢掉，剩下最早是 2000：%d", oldest.AtMs)
	}
	dropped := buffer.Dropped()
	buffer.Flush(1 << 40)
	if buffer.Dropped() != dropped {
		t.Fatal("丢弃数只增不减")
	}
}

func TestAckIdempotent(t *testing.T) {
	buffer := NewBuffer()
	buffer.Add(Event{AtMs: 0, Name: "hit"})
	batch := buffer.Flush(FlushMs)[0]
	if !buffer.Ack(batch.ID) {
		t.Fatal("第一次确认应该成功")
	}
	if buffer.Ack(batch.ID) {
		t.Fatal("重复确认应该失败")
	}
	if buffer.Ack(batch.ID) {
		t.Fatal("确认多少次都该失败")
	}
}

func TestScannedDoesNotGrowWithBacklog(t *testing.T) {
	buffer := NewBuffer()
	for index := 0; index < BatchSize-1; index++ {
		buffer.Add(Event{AtMs: int64(index), Name: "hit"})
	}
	for round := 0; round < 100; round++ {
		if batches := buffer.Flush(0); len(batches) != 0 {
			t.Fatal("时间窗口不够不该出批")
		}
	}
	if buffer.Scanned() != 0 {
		t.Fatalf("不出批不该扫描：%d", buffer.Scanned())
	}
	batches := buffer.Flush(FlushMs)
	if len(batches) != 1 || buffer.Scanned() != BatchSize-1 {
		t.Fatalf("扫描数应该只算取走的事件：%d", buffer.Scanned())
	}
	for _, batch := range batches {
		buffer.Ack(batch.ID)
	}
}

func TestDeterministicReplay(t *testing.T) {
	run := func() (int, int, int) {
		buffer := NewBuffer()
		total := 0
		for index := 0; index < 5000; index++ {
			buffer.Add(Event{AtMs: int64(index % 97), Name: "hit"})
			if index%7 == 0 {
				for _, batch := range buffer.Flush(int64(index)) {
					total += len(batch.Events)
					buffer.Ack(batch.ID)
				}
			}
		}
		return total, buffer.Pending(), buffer.Dropped()
	}
	firstTotal, firstPending, firstDropped := run()
	secondTotal, secondPending, secondDropped := run()
	if firstTotal != secondTotal || firstPending != secondPending || firstDropped != secondDropped {
		t.Fatal("同一串入队与出批序列重复执行结果应该相同")
	}
}

func TestBatchOrderFollowsEventTime(t *testing.T) {
	buffer := NewBuffer()
	lastAtMs := int64(-1) << 62
	for clock := int64(0); clock < 4000; clock++ {
		buffer.Add(Event{AtMs: clock, Name: "hit"})
		if clock%13 == 0 {
			for _, batch := range buffer.Flush(clock) {
				for _, event := range batch.Events {
					if event.AtMs < lastAtMs {
						t.Fatal("出批顺序应该与事件时间一致")
					}
					lastAtMs = event.AtMs
				}
			}
		}
	}
}

// naiveBuffer 是「按大小与时间出批 + 上限丢弃 + 幂等确认」的参考实现：
// 有序切片 + 队首丢弃，逻辑直白但慢，专门用来对照堆实现的每一条输出。
type naiveBuffer struct {
	pending []Event
	limit   int
	dropped int
	acked   map[string]bool
	seq     int
}

func newNaiveBuffer(limit int) *naiveBuffer {
	return &naiveBuffer{limit: limit, acked: map[string]bool{}}
}

func (n *naiveBuffer) Add(event Event) {
	n.pending = append(n.pending, event)
	for index := len(n.pending) - 1; index > 0 && n.pending[index-1].AtMs > n.pending[index].AtMs; index-- {
		n.pending[index-1], n.pending[index] = n.pending[index], n.pending[index-1]
	}
	if len(n.pending) > n.limit {
		n.pending = n.pending[1:]
		n.dropped++
	}
}

func (n *naiveBuffer) Flush(nowMs int64) []Batch {
	var batches []Batch
	for len(n.pending) >= BatchSize {
		batches = append(batches, n.emit(BatchSize))
	}
	if len(n.pending) > 0 && nowMs-n.pending[0].AtMs >= FlushMs {
		batches = append(batches, n.emit(len(n.pending)))
	}
	return batches
}

func (n *naiveBuffer) emit(count int) Batch {
	events := append([]Event(nil), n.pending[:count]...)
	n.pending = n.pending[count:]
	n.seq++
	return Batch{ID: "b" + itoa(n.seq), Events: events}
}

func (n *naiveBuffer) Ack(batchID string) bool {
	if n.acked[batchID] {
		return false
	}
	n.acked[batchID] = true
	return true
}

func TestMatchesReferenceOnRandomSequences(t *testing.T) {
	rng := rand.New(rand.NewSource(217))
	limits := []int{64, 128, 250, MaxPending}
	for trial := 0; trial < 200; trial++ {
		limit := limits[trial%len(limits)]
		buffer := newBufferWithLimit(limit)
		reference := newNaiveBuffer(limit)
		clock := rng.Int63n(1000)
		steps := 100 + 2*limit + rng.Intn(limit)
		for step := 0; step < steps; step++ {
			switch rng.Intn(20) {
			case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10:
				age := rng.Int63n(FlushMs)
				if rng.Intn(4) == 0 {
					age = rng.Int63n(4 * FlushMs)
				}
				event := Event{AtMs: clock - age, Name: string(rune('a' + rng.Intn(26)))}
				buffer.Add(event)
				reference.Add(event)
			case 11, 12, 13, 14, 15:
				clock += rng.Int63n(2 * FlushMs)
				got := buffer.Flush(clock)
				want := reference.Flush(clock)
				if len(got) != len(want) {
					t.Fatalf("trial %d step %d: 批次数不同 got=%d want=%d", trial, step, len(got), len(want))
				}
				for index := range want {
					if got[index].ID != want[index].ID {
						t.Fatalf("trial %d step %d: 批次 ID 不同 got=%s want=%s", trial, step, got[index].ID, want[index].ID)
					}
					if len(got[index].Events) != len(want[index].Events) {
						t.Fatalf("trial %d step %d: 批次大小不同 got=%d want=%d", trial, step, len(got[index].Events), len(want[index].Events))
					}
					for eventIndex := range want[index].Events {
						if got[index].Events[eventIndex] != want[index].Events[eventIndex] {
							t.Fatalf("trial %d step %d: 批次事件不同 got=%v want=%v", trial, step, got[index].Events[eventIndex], want[index].Events[eventIndex])
						}
					}
				}
			default:
				id := "b" + itoa(1+rng.Intn(30))
				if got, want := buffer.Ack(id), reference.Ack(id); got != want {
					t.Fatalf("trial %d step %d: 确认结果不同 got=%v want=%v", trial, step, got, want)
				}
			}
			if buffer.Pending() != len(reference.pending) {
				t.Fatalf("trial %d step %d: pending 不同 got=%d want=%d", trial, step, buffer.Pending(), len(reference.pending))
			}
			if buffer.Dropped() != reference.dropped {
				t.Fatalf("trial %d step %d: dropped 不同 got=%d want=%d", trial, step, buffer.Dropped(), reference.dropped)
			}
		}
	}
}

func TestMillionBacklogAndThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("规模测试")
	}
	buffer := newBufferWithLimit(1000000)
	start := time.Now()
	for index := 0; index < 1000000; index++ {
		buffer.Add(Event{AtMs: int64(index), Name: "hit"})
	}
	enqueueElapsed := time.Since(start)
	if buffer.Pending() != 1000000 || buffer.Dropped() != 0 {
		t.Fatalf("百万积压不该丢事件：pending=%d dropped=%d", buffer.Pending(), buffer.Dropped())
	}
	start = time.Now()
	total := 0
	for clock := int64(0); buffer.Pending() > 0; clock += FlushMs {
		for _, batch := range buffer.Flush(clock) {
			total += len(batch.Events)
			buffer.Ack(batch.ID)
		}
	}
	flushElapsed := time.Since(start)
	if total != 1000000 {
		t.Fatalf("出批总数应该是 100 万：%d", total)
	}
	if buffer.Scanned() != 1000000 {
		t.Fatalf("Scanned 不该随缓冲长度线性增长：%d", buffer.Scanned())
	}
	if enqueueElapsed > 10*time.Second || flushElapsed > 10*time.Second {
		t.Fatal("每秒 10 万次入队与出批的门槛达不到")
	}
	t.Logf("百万积压：入队 %v（%.0f 条/秒），出批 %v（%.0f 条/秒）",
		enqueueElapsed, 1e6/enqueueElapsed.Seconds(),
		flushElapsed, 1e6/flushElapsed.Seconds())
}

func BenchmarkEnqueueAndFlush(b *testing.B) {
	buffer := NewBuffer()
	clock := int64(0)
	for index := 0; index < b.N; index++ {
		clock++
		buffer.Add(Event{AtMs: clock, Name: "hit"})
		if index%BatchSize == 0 {
			buffer.Flush(clock)
		}
	}
}
