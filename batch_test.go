package telemetry

import "testing"

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
