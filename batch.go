// Package telemetry 是埋点批量上报：按批次大小与时间窗口出批，缓冲有上限，确认幂等。
//
// 缓冲用 (AtMs, 录入序号) 最小堆维护，最早事件始终在堆顶，Flush 取头不取全表；
// 超过 MaxPending 时从堆顶丢最旧事件。事件顺序与事件时间一致（同刻按录入顺序）。
package telemetry

import (
	"container/heap"
	"strconv"
)

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

type entry struct {
	event Event
	order int64
}

type eventHeap []*entry

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].event.AtMs != h[j].event.AtMs {
		return h[i].event.AtMs < h[j].event.AtMs
	}
	return h[i].order < h[j].order
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(*entry)) }
func (h *eventHeap) Pop() any {
	old := *h
	last := len(old) - 1
	item := old[last]
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
	pool       []*entry
}

// NewBuffer 建一个空缓冲。
func NewBuffer() *Buffer {
	buffer := &Buffer{acked: map[string]bool{}}
	buffer.maxPending = MaxPending
	heap.Init(&buffer.pending)
	return buffer
}

// newBufferWithLimit 用自定义上限建缓冲，供包内规模测试验证大积压。
func newBufferWithLimit(limit int) *Buffer {
	buffer := NewBuffer()
	buffer.maxPending = limit
	return buffer
}

// Add 收一条事件；超过 MaxPending 时丢最旧事件并累加 Dropped。
// 返回 nil：大小与时间窗口出批统一由 Flush 完成（否则缓冲永远攒不满
// MaxPending，上限丢弃与百万积压都不会发生）。
func (b *Buffer) Add(event Event) []Batch {
	item := b.alloc(event)
	heap.Push(&b.pending, item)
	if b.pending.Len() > b.maxPending {
		oldest := heap.Pop(&b.pending).(*entry)
		b.release(oldest)
		b.dropped++
	}
	return nil
}

// Flush 按批次大小与时间窗口出批。
// 先把攒够 BatchSize 的事件按时间升序整批出完（不看时间）；
// 再在缓冲非空且最早事件距 nowMs 不小于 FlushMs 时把剩余事件出一批，
// 否则剩余事件继续滞留并返回空。
func (b *Buffer) Flush(nowMs int64) []Batch {
	var batches []Batch
	for b.pending.Len() >= BatchSize {
		batches = append(batches, b.emit())
	}
	if b.pending.Len() == 0 || nowMs-b.pending[0].event.AtMs < FlushMs {
		return batches
	}
	batches = append(batches, b.emit())
	return batches
}

func (b *Buffer) emit() Batch {
	events := make([]Event, 0, b.pending.Len())
	for b.pending.Len() > 0 && len(events) < BatchSize {
		item := heap.Pop(&b.pending).(*entry)
		events = append(events, item.event)
		b.release(item)
	}
	b.scanned += len(events)
	b.seq++
	return Batch{ID: "b" + strconv.Itoa(b.seq), Events: events}
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

func (b *Buffer) alloc(event Event) *entry {
	order := b.order
	b.order++
	if count := len(b.pool); count > 0 {
		item := b.pool[count-1]
		b.pool = b.pool[:count-1]
		item.event = event
		item.order = order
		return item
	}
	return &entry{event: event, order: order}
}

func (b *Buffer) release(item *entry) {
	item.event = Event{}
	b.pool = append(b.pool, item)
}
