// Command telemetry-batch 跑埋点批量样例。
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"example.com/telemetry"
)

// Run 执行一次命令行调用，返回退出码。
func Run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("telemetry-batch", flag.ContinueOnError)
	flags.SetOutput(stderr)
	sample := flags.String("sample", "size", "size / window / cap / ack")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	switch *sample {
	case "size":
		// 攒 250 条。
		buffer := telemetry.NewBuffer()
		batches := 0
		for index := 0; index < 250; index++ {
			batches += len(buffer.Add(telemetry.Event{AtMs: int64(index), Name: "hit"}))
		}
		fmt.Fprintf(stdout, "batches=%d pending=%d\n", batches, buffer.Pending())
	case "window":
		// 十条事件，先在一秒后 flush，再在六秒后 flush。
		buffer := telemetry.NewBuffer()
		for index := 0; index < 10; index++ {
			buffer.Add(telemetry.Event{AtMs: int64(index), Name: "hit"})
		}
		early := len(buffer.Flush(1000))
		late := len(buffer.Flush(6000))
		fmt.Fprintf(stdout, "early=%d late=%d\n", early, late)
	case "cap":
		// 灌三千条。
		buffer := telemetry.NewBuffer()
		for index := 0; index < 3000; index++ {
			buffer.Add(telemetry.Event{AtMs: int64(index), Name: "hit"})
		}
		fmt.Fprintf(stdout, "pending=%d dropped=%d\n", buffer.Pending(), buffer.Dropped())
	case "ack":
		buffer := telemetry.NewBuffer()
		buffer.Add(telemetry.Event{AtMs: 0, Name: "hit"})
		batch := buffer.Flush(6000)[0]
		first := buffer.Ack(batch.ID)
		second := buffer.Ack(batch.ID)
		fmt.Fprintf(stdout, "first=%v second=%v\n", first, second)
	default:
		fmt.Fprintln(stderr, "需要 --sample size|window|cap|ack")
		return 2
	}
	return 0
}

func main() {
	os.Exit(Run(os.Args[1:], os.Stdout, os.Stderr))
}
