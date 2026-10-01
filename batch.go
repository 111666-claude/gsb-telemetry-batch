// Package telemetry 是埋点批量上报：按批次大小与时间窗口出批，缓冲有上限，确认幂等。
//
// 缓冲用 (AtMs, 录入序号) 最小堆维护：最早事件永远在堆顶，Flush 只看堆顶、不扫全表；
// 超过 MaxPending 时从堆顶丢最旧事件。出批顺序与事件时间一致（同刻按录入顺序）。
package telemetry

import "container/heap"

// BatchSize 是触发出批的事件数。
const BatchSize = 100

// FlushMs 是最早事件允许滞留的时长。
const FlushMs = 5000

// MaxPending 是缓冲允许保留的事件数上限。
const MaxPending = 1000

// Event 是一条埋点事件。
type Event struct {
	AtMs int64
	Name string
}

// Batch 是一批待上报的事件。
type Batch struct {
	ID     string
	Events []Event
}

// entry 是缓冲里的一条事件，order 是录入序号，同刻事件按录入顺序出批。
type entry struct {
	event Event
	order int64
}

// eventHeap 是按 (AtMs, order) 升序的最小堆。
type eventHeap []entry

func (h eventHeap) Len() int { return len(h) }

func (h eventHeap) Less(i, j int) bool {
	if h[i].event.AtMs != h[j].event.AtMs {
		return h[i].event.AtMs < h[j].event.AtMs
	}
	return h[i].order < h[j].order
}

func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x any) { *h = append(*h, x.(entry)) }

func (h *eventHeap) Pop() any {
	old := *h
	last := len(old) - 1
	item := old[last]
	old[last] = entry{}
	*h = old[:last]
	return item
}

// Buffer 是埋点缓冲。
type Buffer struct {
	pending    eventHeap
	maxPending int
	dropped    int
	acked      map[string]bool
	scanned    int
	seq        int
	order      int64
}

// NewBuffer 建一个空缓冲。
func NewBuffer() *Buffer {
	return &Buffer{maxPending: MaxPending, acked: map[string]bool{}}
}

// newBufferWithLimit 用自定义上限建缓冲，供包内规模测试验证大积压。
func newBufferWithLimit(limit int) *Buffer {
	buffer := NewBuffer()
	buffer.maxPending = limit
	return buffer
}

// Add 收一条事件：按 (AtMs, 录入序号) 入堆；超过 MaxPending 时从堆顶丢最旧事件
// 并把 Dropped 加一。出批统一由 Flush 完成（缓冲要能攒到 BatchSize 以上、积压到
// MaxPending，上限丢弃与百万积压才可能发生），所以 Add 只入队，固定返回 nil。
func (b *Buffer) Add(event Event) []Batch {
	heap.Push(&b.pending, entry{event: event, order: b.order})
	b.order++
	if b.pending.Len() > b.maxPending {
		heap.Pop(&b.pending)
		b.dropped++
	}
	return nil
}

// Flush 出批：先把攒够 BatchSize 的事件按时间升序整批出完（不看时间窗口）；
// 剩余不足一批时，只在缓冲非空且最早事件距 nowMs 不小于 FlushMs 才出批，
// 否则事件继续滞留并返回空。
func (b *Buffer) Flush(nowMs int64) []Batch {
	var batches []Batch
	for b.pending.Len() >= BatchSize {
		batches = append(batches, b.emit(BatchSize))
	}
	if b.pending.Len() > 0 && nowMs-b.pending[0].event.AtMs >= FlushMs {
		batches = append(batches, b.emit(b.pending.Len()))
	}
	return batches
}

// emit 从堆顶取 count 条时间最早的事件组一批，Scanned 只累加实际取走的条数。
func (b *Buffer) emit(count int) Batch {
	events := make([]Event, 0, count)
	for len(events) < count {
		events = append(events, heap.Pop(&b.pending).(entry).event)
	}
	b.scanned += count
	b.seq++
	return Batch{ID: "b" + itoa(b.seq), Events: events}
}

// Ack 确认一批已经上报成功：第一次 true，重复确认 false。
func (b *Buffer) Ack(batchID string) bool {
	if b.acked[batchID] {
		return false
	}
	b.acked[batchID] = true
	return true
}

// Pending 是缓冲里的事件数。
func (b *Buffer) Pending() int { return b.pending.Len() }

// Dropped 是因上限丢弃的事件数，只增不减。
func (b *Buffer) Dropped() int { return b.dropped }

// Scanned 是出批时实际取走的事件总数（规模观测），不随缓冲长度线性增长。
func (b *Buffer) Scanned() int { return b.scanned }

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := make([]byte, 0, 8)
	for value > 0 {
		digits = append([]byte{byte('0' + value%10)}, digits...)
		value /= 10
	}
	return string(digits)
}
