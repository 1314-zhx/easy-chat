package msgTransfer

import (
	"easy-chat/apps/im/ws/ws"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

// groupMsgRead 用于合并同一个会话（群聊）的已读回执。
//
// 设计目标：
// 1. 将短时间内同一个群的多次已读回执聚合到一起，减少推送次数；
// 2. 通过数量阈值和时间窗口双重条件控制发送时机；
// 3. 避免为每个群长期持有 goroutine，空闲后自动清理。
type groupMsgRead struct {
	mu sync.Mutex // 保护以下字段的并发安全

	conversationId string // 当前聚合器对应的群聊会话 ID

	push   *ws.Push      // 当前累计的回执内容
	pushCh chan *ws.Push // 聚合完成后，通过该通道交回给 MsgReadTransfer

	count int // 当前窗口内累计的回执次数

	// pushTime 记录本轮合并窗口的起始时间，便于按时间窗口触发 flush。
	pushTime time.Time

	done     chan struct{}
	stopOnce sync.Once
	wakeCh   chan struct{}
	idleSince time.Time
}

// newGroupMsgRead 创建一个新的群回执合并器，并启动后台协程。
func newGroupMsgRead(push *ws.Push, pushCh chan *ws.Push) *groupMsgRead {
	m := &groupMsgRead{
		conversationId: push.ConversationId,
		push:           push,
		pushCh:         pushCh,
		count:          1, // 第一个 push 进来时，初始计数为 1
		pushTime:       time.Now(),
		done:           make(chan struct{}),
		wakeCh:         make(chan struct{}, 1), // 缓冲 1，避免频繁唤醒时阻塞
	}
	go m.transfer() // 启动后台轮询/flush 协程
	m.notify()      // 立即唤醒一次，尽快进入首次检查
	return m
}

// mergePush 将新的已读回执合并进当前聚合器。
// 这里不立即发送，而是累积到同一个 push 里，等待 flush 条件满足后统一发送。
func (m *groupMsgRead) mergePush(push *ws.Push) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case <-m.done:
		return false
	default:
	}

	m.count++
	m.idleSince = time.Time{}

	if m.push == nil {
		m.push = push
		m.count = 1
		m.pushTime = time.Now()
		m.notifyLocked()
		return true
	}
	if m.push.ReadRecords == nil {
		m.push.ReadRecords = make(map[string]string)
	}
	for msgId, read := range push.ReadRecords {
		m.push.ReadRecords[msgId] = read
	}

	m.notifyLocked()
	return true
}

// transfer 是后台协程主循环，负责根据时间/数量条件决定是否 flush。
func (m *groupMsgRead) transfer() {
	timer := time.NewTimer(GroupMsgReadRecordDelayTime)
	defer timer.Stop()
	for {
		select {
		case <-m.done:
			return
		case <-m.wakeCh:
			m.tryFlush(timer)
		case <-timer.C:
			m.tryFlush(timer)
		}
	}
}

// IsIdle 判断当前合并器是否已进入空闲状态。
// 对外调用时会加锁。
func (m *groupMsgRead) IsIdle() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isIdle()
}

// tryFlush 尝试将当前聚合的数据发送出去。
// 触发条件：
// 1. 数量达到阈值；
// 2. 时间窗口已到；
// 3. 数据太少时延迟再等等；
// 4. 长时间空闲后自动清理。
func (m *groupMsgRead) tryFlush(timer *time.Timer) {
	m.mu.Lock()

	if m.push == nil || m.count == 0 {
		if m.idleSince.IsZero() {
			m.idleSince = time.Now()
			timer.Reset(time.Second)
			m.mu.Unlock()
			return
		}
		if time.Since(m.idleSince) >= 2*time.Second {
			m.stop()
			m.clearLocked()
			m.mu.Unlock()
			return
		}
		timer.Reset(time.Second)
		m.mu.Unlock()
		return
	}

	if m.count >= GroupMsgReadRecordDelayCount {
		push := m.takePushLocked(timer)
		m.mu.Unlock()
		m.emit(push)
		return
	}

	elapsed := time.Since(m.pushTime)
	if elapsed < GroupMsgReadRecordDelayTime {
		timer.Reset(GroupMsgReadRecordDelayTime - elapsed)
		m.mu.Unlock()
		return
	}

	if m.count < GroupMsgReadRecordDelayCount/2 {
		timer.Reset(time.Second)
		m.mu.Unlock()
		return
	}

	push := m.takePushLocked(timer)
	m.mu.Unlock()
	m.emit(push)
}

func (m *groupMsgRead) takePushLocked(timer *time.Timer) *ws.Push {
	push := m.push
	m.push = nil
	m.count = 0
	m.idleSince = time.Time{}
	m.pushTime = time.Now()
	timer.Reset(GroupMsgReadRecordDelayTime)
	return push
}

func (m *groupMsgRead) emit(push *ws.Push) {
	if push == nil {
		return
	}
	logx.Infof("group msg read flush conversation=%s", push.ConversationId)
	select {
	case m.pushCh <- push:
	case <-m.done:
	}
}

// notify 向 wakeCh 发一个非阻塞唤醒信号。
func (m *groupMsgRead) notify() {
	select {
	case m.wakeCh <- struct{}{}:
	default:
	}
}

// notifyLocked 在已持锁的情况下唤醒 transfer 协程。
func (m *groupMsgRead) notifyLocked() {
	select {
	case m.wakeCh <- struct{}{}:
	default:
	}
}

// isIdle 判断合并窗口是否已经结束。
// 这是内部方法，调用前需确保已经持有锁。
func (m *groupMsgRead) isIdle() bool {
	pushTime := m.pushTime
	val := GroupMsgReadRecordDelayTime*2 - time.Since(pushTime)

	// 没有数据，或者超过 2 倍窗口时间未活动，就认为已经空闲。
	if val <= 0 || (m.push == nil && m.count == 0) {
		return true
	}
	return false
}

// clear 关闭 done 通道并清理内部状态。
func (m *groupMsgRead) stop() {
	m.stopOnce.Do(func() {
		close(m.done)
	})
}

func (m *groupMsgRead) clear() {
	m.stop()
	m.clearLocked()
}

// clearLocked 清理内部状态。
// 调用时需确保已经持有锁。
func (m *groupMsgRead) clearLocked() {
	m.push = nil
	m.count = 0
	m.idleSince = time.Time{}
}
