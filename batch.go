// Package telemetry 是埋点批量上报：按批次大小与时间窗口出批，缓冲有上限，确认幂等。
// 缺陷：缓冲无上限、Add 从不按大小出批、Flush 不看时间窗口、事件不排序、重复确认也返回 true。
package telemetry

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

// Buffer 是埋点缓冲。
type Buffer struct {
	pending []Event
	dropped int
	acked   map[string]bool
	scanned int
	seq     int
}

// NewBuffer 建一个空缓冲。
func NewBuffer() *Buffer {
	return &Buffer{acked: map[string]bool{}}
}

// Add 收一条事件。
// 缺陷：只往切片里追加，既不按 MaxPending 丢弃，也不在攒够 BatchSize 时出批。
func (b *Buffer) Add(event Event) []Batch {
	b.pending = append(b.pending, event)
	return nil
}

// Flush 按时间窗口出批。
// 缺陷：完全不看 nowMs 与事件时间，只要缓冲非空就出一批；出批前还全表扫描。
func (b *Buffer) Flush(nowMs int64) []Batch {
	b.scanned += len(b.pending)
	if len(b.pending) == 0 {
		return nil
	}
	events := append([]Event(nil), b.pending...)
	b.pending = nil
	b.seq++
	return []Batch{{ID: "b" + itoa(b.seq), Events: events}}
}

// Ack 确认一批已经上报成功。
// 缺陷：重复确认也返回 true。
func (b *Buffer) Ack(batchID string) bool {
	b.acked[batchID] = true
	return true
}

// Pending 是缓冲里的事件数。
func (b *Buffer) Pending() int { return len(b.pending) }

// Dropped 是因上限丢弃的事件数。
// 缺陷：从来没被累加过。
func (b *Buffer) Dropped() int { return b.dropped }

// Scanned 是累计扫描事件数（规模观测）。
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
