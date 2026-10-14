package websocket

import (
	"context"
	"easy-chat/pkg/metrics"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

const (
	defaultExtraIdle = time.Second

	// 推送池：Marshal + 投 Actor 是短 CPU。常驻按物理核 12，背压扩到逻辑核 24。
	defaultPushPoolCore  = 12
	defaultPushPoolMax   = 24
	defaultPushQueueSize = 10000
	defaultPushIdleMs    = 1000
)

// Task 推送任务
// 由调用方传入，包含要发送的消息内容和目标连接列表
type Task struct {
	Msg   interface{} // 消息内容（通常是结构体，会被序列化为 JSON）
	Conns []*Conn     // 目标连接列表（调用方已过滤不在线的用户）
}

// WorkerPool goroutine 池化工作池
//
//	保底 coreSize 个 worker 一直堵在 taskCh 上，不因空闲退出。
//	突发时 Submit 发现队列仍积压则补 extra worker，最多 poolSize 个。
//	extra 在 execute 之后空等 extraIdle，没有新任务就退出。
//	workPass 只包住 execute，限制同时真正在跑的推送数。
type WorkerPool struct {
	taskCh      chan *Task
	workPass    chan struct{}
	poolSize    int           // extra+core 上限，同时 execute 上限
	coreSize    int           // 常驻 worker 数
	extraIdle   time.Duration // extra 空闲超时
	workerCount int64
	stopped     uint32
	logger      logx.Logger
}

// NewWorkerPool 创建工作池。
//
//	poolSize: extra+core 上限（背压封顶，默认 24）
//	queueSize: 任务队列，挡住 WS/MQ 入口
//	idleMs: extra 空闲退出；<=0 则 1s
//	常驻 core = poolSize/2（24 → 12）；可用 newWorkerPool 指定 core。
func NewWorkerPool(poolSize, queueSize, idleMs int) *WorkerPool {
	core := poolSize / 2
	if core < 1 {
		core = 1
	}
	return newWorkerPool(core, poolSize, queueSize, idleMs)
}

func newWorkerPool(coreSize, poolSize, queueSize, idleMs int) *WorkerPool {
	if poolSize < 1 {
		poolSize = 1
	}
	idle := defaultExtraIdle
	if idleMs > 0 {
		idle = time.Duration(idleMs) * time.Millisecond
	}
	if coreSize < 1 {
		coreSize = 1
	}
	if coreSize > poolSize {
		coreSize = poolSize
	}

	wp := &WorkerPool{
		taskCh:    make(chan *Task, queueSize),
		workPass:  make(chan struct{}, poolSize),
		poolSize:  poolSize,
		coreSize:  coreSize,
		extraIdle: idle,
		logger:    logx.WithContext(context.Background()),
	}

	for i := 0; i < coreSize; i++ {
		atomic.AddInt64(&wp.workerCount, 1)
		go wp.worker(true)
	}

	return wp
}

// testRunHook 仅测试注入：卡住 execute，便于观察 extra 扩缩。
var testRunHook func()

func (wp *WorkerPool) runTask(task *Task) {
	if task == nil {
		return
	}
	if testRunHook != nil {
		testRunHook()
	}
	wp.workPass <- struct{}{}
	wp.execute(task)
	<-wp.workPass
}

// worker core=true 常驻阻塞收任务；core=false 为首包阻塞，之后 extraIdle 内无任务则退出。
func (wp *WorkerPool) worker(core bool) {
	defer atomic.AddInt64(&wp.workerCount, -1)

	if core {
		for {
			task, ok := <-wp.taskCh
			if !ok {
				return
			}
			wp.runTask(task)
		}
	}

	timer := time.NewTimer(wp.extraIdle)
	defer timer.Stop()
	for {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wp.extraIdle)
		select {
		case task, ok := <-wp.taskCh:
			if !ok {
				return
			}
			wp.runTask(task)
		case <-timer.C:
			return
		}
	}
}

func (wp *WorkerPool) execute(task *Task) {
	data, err := json.Marshal(task.Msg)
	if err != nil {
		wp.logger.Errorf("marshal task msg error: %v", err)
		return
	}

	for _, conn := range task.Conns {
		if conn == nil || conn.queue == nil {
			continue
		}
		if err := conn.queue.Submit(data); err != nil {
			wp.logger.Errorf("connqueue submit error: %v", err)
		}
	}
}

// Submit 提交任务到工作池。队列仍积压时才补 extra worker，避免有空闲 core 还扩容。
func (wp *WorkerPool) Submit(task *Task, timeout time.Duration) error {
	if atomic.LoadUint32(&wp.stopped) == 1 {
		return fmt.Errorf("worker pool stopped")
	}

	select {
	case wp.taskCh <- task:
		if len(wp.taskCh) > 0 {
			wp.trySpawnWorker()
		}
		return nil
	case <-time.After(timeout):
		metrics.ErrorTotal.WithLabelValues("worker_pool_submit_timeout").Inc()
		wp.logger.Errorf("submit task timeout after %v, task dropped", timeout)
		return fmt.Errorf("submit task timeout after %v", timeout)
	}
}

func (wp *WorkerPool) trySpawnWorker() {
	for {
		if atomic.LoadUint32(&wp.stopped) == 1 {
			return
		}
		current := atomic.LoadInt64(&wp.workerCount)
		if current >= int64(wp.poolSize) {
			return
		}
		if atomic.CompareAndSwapInt64(&wp.workerCount, current, current+1) {
			go wp.worker(false)
			return
		}
	}
}

func (wp *WorkerPool) RunningWorkers() int64 {
	return atomic.LoadInt64(&wp.workerCount)
}

func (wp *WorkerPool) Stop() {
	if !atomic.CompareAndSwapUint32(&wp.stopped, 0, 1) {
		return
	}
	close(wp.taskCh)
}
