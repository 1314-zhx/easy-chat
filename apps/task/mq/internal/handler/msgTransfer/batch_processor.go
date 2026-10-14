package msgTransfer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"easy-chat/apps/im/immodels"
	"easy-chat/apps/task/mq/mq"
	"easy-chat/apps/task/mq/pkg/retry"
	"easy-chat/pkg/metrics"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	defaultBatchSize      = 100
	defaultBatchTimeout   = 50 * time.Millisecond
	defaultBufferSize     = 256
	occupancyLow          = 0.20 // 占用 < 20% → 单条
	occupancyBatch        = 0.50 // 占用 > 50% → 批插；20%～50% 保持当前模式
	occupancyBackpressure = 0.75 // 占用 ≥ 75% → 入队阻塞反压
	insertModeSingle      = 0
	insertModeBatch       = 1
)

// BatchInsertRetryConfig 批插入重试：首次立即，之后 1s、2s、4s（合计约 6s）
var BatchInsertRetryConfig = retry.RetryConfig{
	MaxRetries:   3,
	InitialDelay: 1 * time.Second,
	MaxDelay:     4 * time.Second,
	BackoffMulti: 2.0,
}

// BatchItem 进入插入缓冲区的一条消息
type BatchItem struct {
	ChatLog *immodels.ChatLog
	Data    *mq.MsgChatTransfer
	MsgId   primitive.ObjectID
	Key       string
	Value     string
	Done      chan error // 容量 1，可选；异步入队时由完成回调通知
	Msg       kafka.Message
	HasCommit bool // 为真时，本条落库成功后才参与位点提交
}

// BatchProcessor 维护 Mongo 插入缓冲区：按占用率滞回切换单条 / 批插入。
type BatchProcessor struct {
	batchSize    int
	timeout      time.Duration
	bufCap       int
	chatLogModel immodels.CustomChatLogModel
	convModel    immodels.ConversationModel
	dlq          retry.DLQProducer
	topic        string
	onComplete   func(item *BatchItem, err error)
	onPersisted  func(msgs []kafka.Message) // 本批落库成功的消息，由调用方异步提交位点

	ch           chan *BatchItem
	pendingRetry []*BatchItem // 落库失败，留在内存里重试，不提交位点
	slots chan struct{} // 容量 = 75% bufCap，满则 Submit 阻塞
	done  chan struct{}
	wg    sync.WaitGroup
}

func NewBatchProcessor(
	chatLogModel immodels.CustomChatLogModel,
	convModel immodels.ConversationModel,
	dlq retry.DLQProducer,
	topic string,
	batchSize int,
	timeout time.Duration,
	onComplete func(item *BatchItem, err error),
) *BatchProcessor {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	if timeout <= 0 {
		timeout = defaultBatchTimeout
	}
	bufCap := defaultBufferSize
	if bufCap < batchSize*2 {
		bufCap = batchSize * 2
	}
	slotN := int(float64(bufCap) * occupancyBackpressure)
	if slotN < batchSize {
		slotN = batchSize
	}
	bp := &BatchProcessor{
		batchSize:    batchSize,
		timeout:      timeout,
		bufCap:       bufCap,
		chatLogModel: chatLogModel,
		convModel:    convModel,
		dlq:          dlq,
		topic:        topic,
		onComplete:   onComplete,
		ch:           make(chan *BatchItem, bufCap),
		slots:        make(chan struct{}, slotN),
		done:         make(chan struct{}),
	}
	bp.wg.Add(1)
	go bp.run()
	return bp
}

// Submit 放入插入缓冲区；占用达到 75% 时阻塞，反压 Kafka 消费。
func (bp *BatchProcessor) Submit(item *BatchItem) error {
	if item.Done == nil {
		item.Done = make(chan error, 1)
	}
	select {
	case bp.slots <- struct{}{}:
	case <-bp.done:
		return fmt.Errorf("batch processor stopped")
	}
	select {
	case bp.ch <- item:
		return nil
	case <-bp.done:
		<-bp.slots
		return fmt.Errorf("batch processor stopped")
	}
}

// SubmitAndWait 入队并等待本条落库结束
func (bp *BatchProcessor) SubmitAndWait(item *BatchItem) error {
	if err := bp.Submit(item); err != nil {
		return err
	}
	return <-item.Done
}

func (bp *BatchProcessor) run() {
	defer bp.wg.Done()

	mode := insertModeSingle
	batch := make([]*BatchItem, 0, bp.batchSize)
	timer := time.NewTimer(time.Hour)
	stopTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	stopTimer()
	timerOn := false

	occupancy := func() float64 {
		n := len(bp.ch) + len(batch)
		if n > bp.bufCap {
			n = bp.bufCap
		}
		return float64(n) / float64(bp.bufCap)
	}

	switchMode := func() {
		occ := occupancy()
		switch {
		case occ < occupancyLow:
			if mode != insertModeSingle {
				logx.Infof("mongo insert mode -> single, occupancy=%.2f", occ)
			}
			mode = insertModeSingle
		case occ > occupancyBatch:
			if mode != insertModeBatch {
				logx.Infof("mongo insert mode -> batch, occupancy=%.2f", occ)
			}
			mode = insertModeBatch
		}
	}

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}
		if timerOn {
			stopTimer()
			timerOn = false
		}
		bp.flush(batch)
		batch = batch[:0]
		switchMode()
	}

	insertOne := func(item *BatchItem) {
		bp.flush([]*BatchItem{item})
		switchMode()
	}

	for {
		if item := bp.popRetry(); item != nil {
			batch = append(batch, item)
			switchMode()
			if mode == insertModeSingle || len(batch) >= bp.batchSize {
				flushBatch()
			} else if !timerOn {
				stopTimer()
				timer.Reset(bp.timeout)
				timerOn = true
			}
			continue
		}

		if mode == insertModeSingle && len(batch) > 0 {
			flushBatch()
			continue
		}

		if mode == insertModeBatch && len(batch) > 0 {
			select {
			case item := <-bp.ch:
				batch = append(batch, item)
				switchMode()
				if mode == insertModeSingle {
					flushBatch()
					continue
				}
				if len(batch) >= bp.batchSize {
					flushBatch()
				}
			case <-timer.C:
				timerOn = false
				flushBatch()
			case <-bp.done:
				bp.drain(batch)
				return
			}
			continue
		}

		select {
		case item := <-bp.ch:
			batch = append(batch, item)
			switchMode()
			if mode == insertModeSingle {
				one := batch[0]
				batch = batch[:0]
				insertOne(one)
				continue
			}
			if len(batch) >= bp.batchSize {
				flushBatch()
				continue
			}
			stopTimer()
			timer.Reset(bp.timeout)
			timerOn = true
		case <-bp.done:
			bp.drain(batch)
			return
		}
	}
}

func (bp *BatchProcessor) popRetry() *BatchItem {
	if len(bp.pendingRetry) == 0 {
		return nil
	}
	item := bp.pendingRetry[0]
	bp.pendingRetry = bp.pendingRetry[1:]
	return item
}

func (bp *BatchProcessor) drain(batch []*BatchItem) {
	if len(bp.pendingRetry) > 0 {
		batch = append(batch, bp.pendingRetry...)
		bp.pendingRetry = nil
	}
	for {
		select {
		case item := <-bp.ch:
			batch = append(batch, item)
		default:
			if len(batch) > 0 {
				bp.flush(batch)
			}
			return
		}
	}
}

func (bp *BatchProcessor) flush(batch []*BatchItem) {
	if len(batch) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	flushStart := time.Now()
	groups := groupByConversation(batch)
	ok := 0
	persisted := make([]kafka.Message, 0, len(batch))
	for _, g := range groups {
		err := retry.Retry(ctx, func() error {
			return bp.persistConvGroup(ctx, g)
		}, BatchInsertRetryConfig)
		if err != nil {
			logx.Errorf("mongo persist conv failed, hold offset, convId=%s size=%d err=%v",
				g[0].ChatLog.ConversationId, len(g), err)
			metrics.MongoOperationTotal.WithLabelValues("batch_insert", "chatlog", "failed").Inc()
			metrics.ErrorTotal.WithLabelValues("batch_mongo").Inc()
			// 失败的留在内存重试。位点停在这条，后面已经落库的也不会越过它提交。
			bp.pendingRetry = append(bp.pendingRetry, g...)
			continue
		}
		ok += len(g)
		for _, item := range g {
			bp.finish(item, nil)
			if item.HasCommit {
				persisted = append(persisted, item.Msg)
			}
		}
	}
	if len(persisted) > 0 && bp.onPersisted != nil {
		bp.onPersisted(persisted)
	}

	latency := time.Since(flushStart).Milliseconds()
	metrics.BatchInsertLatency.Observe(float64(latency))
	metrics.BatchInsertSize.Observe(float64(len(batch)))
	if ok > 0 {
		metrics.MongoOperationTotal.WithLabelValues("batch_insert", "chatlog", "success").Inc()
	}
	logx.Infof("mongo persist done, size=%d ok=%d latency=%dms", len(batch), ok, latency)
}

func groupByConversation(batch []*BatchItem) [][]*BatchItem {
	order := make([]string, 0)
	idx := make(map[string]int)
	for _, item := range batch {
		id := item.ChatLog.ConversationId
		if _, ok := idx[id]; !ok {
			idx[id] = len(order)
			order = append(order, id)
		}
	}
	groups := make([][]*BatchItem, len(order))
	for _, item := range batch {
		i := idx[item.ChatLog.ConversationId]
		groups[i] = append(groups[i], item)
	}
	return groups
}

func (bp *BatchProcessor) persistConvGroup(ctx context.Context, items []*BatchItem) error {
	if len(items) == 0 {
		return nil
	}
	err := bp.persistConvGroupTxn(ctx, items)
	if err != nil && txnUnsupported(err) {
		logx.Errorf("mongo transaction unavailable, fallback IncrSeq+Insert (seq hole possible): %v", err)
		return bp.persistConvGroupPlain(ctx, items)
	}
	return err
}

func (bp *BatchProcessor) persistConvGroupTxn(ctx context.Context, items []*BatchItem) error {
	client := bp.chatLogModel.MongoClient()
	if client == nil {
		return fmt.Errorf("mongo client nil")
	}
	session, err := client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		return nil, bp.assignSeqAndInsert(sessCtx, items)
	})
	return err
}

func (bp *BatchProcessor) persistConvGroupPlain(ctx context.Context, items []*BatchItem) error {
	return bp.assignSeqAndInsert(ctx, items)
}

func (bp *BatchProcessor) assignSeqAndInsert(ctx context.Context, items []*BatchItem) error {
	n := int64(len(items))
	end, err := bp.convModel.IncrSeqBy(ctx, items[0].ChatLog.ConversationId, n)
	if err != nil {
		return err
	}
	start := end - n + 1
	docs := make([]*immodels.ChatLog, n)
	for i, item := range items {
		item.ChatLog.Seq = start + int64(i)
		docs[i] = item.ChatLog
	}
	if n == 1 {
		e := bp.chatLogModel.Insert(ctx, docs[0])
		if mongo.IsDuplicateKeyError(e) {
			return nil
		}
		return e
	}
	e := bp.chatLogModel.InsertMany(ctx, docs)
	if e == nil || !mongo.IsDuplicateKeyError(e) {
		return e
	}
	for _, d := range docs {
		if err := bp.chatLogModel.Insert(ctx, d); err != nil && !mongo.IsDuplicateKeyError(err) {
			return err
		}
	}
	return nil
}

func txnUnsupported(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Transaction numbers are only allowed") ||
		strings.Contains(msg, "not supported") ||
		strings.Contains(msg, "replica set")
}

func (bp *BatchProcessor) finish(item *BatchItem, err error) {
	<-bp.slots
	if bp.onComplete != nil {
		bp.onComplete(item, err)
	}
	if item.Done != nil {
		item.Done <- err
	}
}

func (bp *BatchProcessor) sendDLQ(item *BatchItem, errMsg string) {
	if bp.dlq == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := bp.dlq.SendToDLQ(ctx, bp.topic, item.Key, item.Value, errMsg); e != nil {
			logx.Errorf("send DLQ failed: %v", e)
		}
	}()
}

func (bp *BatchProcessor) Stop() {
	close(bp.done)
	bp.wg.Wait()
}
