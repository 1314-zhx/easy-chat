package msgTransfer

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/logx"
)

// offsetCommitter 按分区记住已经落库的位点，只提交连续成功的那一段。
// 拉取可以走在前面；中间有一条还没落库时，后面的位点不会先交。
type offsetCommitter struct {
	reader *kafka.Reader
	topic  string

	mu    chan struct{}
	parts map[int]*partCommit
}

type partCommit struct {
	next int64
	init bool
	done map[int64]struct{}
}

func newOffsetCommitter(reader *kafka.Reader, topic string) *offsetCommitter {
	return &offsetCommitter{
		reader: reader,
		topic:  topic,
		mu:     make(chan struct{}, 1),
		parts:  make(map[int]*partCommit),
	}
}

func (c *offsetCommitter) lock()   { c.mu <- struct{}{} }
func (c *offsetCommitter) unlock() { <-c.mu }

func (c *offsetCommitter) track(msg kafka.Message) {
	c.lock()
	defer c.unlock()
	st := c.part(msg.Partition)
	if !st.init {
		st.next = msg.Offset
		st.init = true
	}
}

func (c *offsetCommitter) markDone(msgs ...kafka.Message) {
	if len(msgs) == 0 {
		return
	}
	c.lock()
	touched := make(map[int]struct{}, len(msgs))
	for _, msg := range msgs {
		st := c.part(msg.Partition)
		if !st.init {
			st.next = msg.Offset
			st.init = true
		}
		st.done[msg.Offset] = struct{}{}
		touched[msg.Partition] = struct{}{}
	}
	tips := make([]kafka.Message, 0, len(touched))
	for p := range touched {
		st := c.parts[p]
		var tip kafka.Message
		advanced := false
		for {
			if _, ok := st.done[st.next]; !ok {
				break
			}
			delete(st.done, st.next)
			tip = kafka.Message{Topic: c.topic, Partition: p, Offset: st.next}
			st.next++
			advanced = true
		}
		if advanced {
			tips = append(tips, tip)
		}
	}
	c.unlock()

	if len(tips) == 0 {
		return
	}
	if err := commitSync(c.reader, tips...); err != nil {
		logx.Errorf("batch offset commit failed: %v", err)
		return
	}
	for _, tip := range tips {
		logx.Infof("batch offset committed, partition=%d offset=%d", tip.Partition, tip.Offset+1)
	}
}

// commitSync 同步提交位点。Reader 的 CommitInterval 为 0 时，CommitMessages 会等 broker 确认。
func commitSync(r *kafka.Reader, msgs ...kafka.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return r.CommitMessages(ctx, msgs...)
}

func (c *offsetCommitter) part(p int) *partCommit {
	st := c.parts[p]
	if st == nil {
		st = &partCommit{done: make(map[int64]struct{})}
		c.parts[p] = st
	}
	return st
}
