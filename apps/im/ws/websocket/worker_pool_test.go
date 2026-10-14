package websocket

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPoolCoreCount(t *testing.T) {
	wp := NewWorkerPool(24, 16, 50)
	defer wp.Stop()
	time.Sleep(20 * time.Millisecond)
	n := wp.RunningWorkers()
	if n != 12 {
		t.Fatalf("core workers want 12 got %d", n)
	}
}

func TestWorkerPoolExtraIdleExit(t *testing.T) {
	wp := newWorkerPool(1, 8, 32, 50)
	defer wp.Stop()

	if wp.coreSize != 1 {
		t.Fatalf("coreSize want 1 got %d", wp.coreSize)
	}

	var started int32
	block := make(chan struct{})
	testRunHook = func() {
		atomic.AddInt32(&started, 1)
		<-block
	}
	t.Cleanup(func() { testRunHook = nil })

	if err := wp.Submit(&Task{Msg: map[string]int{"a": 1}}, time.Second); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt32(&started) < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if atomic.LoadInt32(&started) < 1 {
		t.Fatal("core did not pick task")
	}

	for i := 0; i < 4; i++ {
		if err := wp.Submit(&Task{Msg: map[string]int{"i": i}}, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(30 * time.Millisecond)
	if wp.RunningWorkers() <= 1 {
		t.Fatalf("expected extra workers, got %d", wp.RunningWorkers())
	}

	close(block)
	deadline = time.Now().Add(time.Second)
	for wp.RunningWorkers() != 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := wp.RunningWorkers(); n != 1 {
		t.Fatalf("after idle extras should exit, got %d", n)
	}
}

func TestWorkerPoolStop(t *testing.T) {
	wp := NewWorkerPool(10, 4, 20)
	wp.Stop()
	err := wp.Submit(&Task{Msg: struct{}{}}, 20*time.Millisecond)
	if err == nil {
		t.Fatal("submit after stop want error")
	}
}
