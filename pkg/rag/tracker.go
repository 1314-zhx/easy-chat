package rag

import (
	"strings"
	"sync"
	"time"
)

// TopicKey 话题标识，关联本次对话的主题。
// 用作 map 的键，通过“会话ID + 话题名”来唯一定位一个话题。
type TopicKey struct {
	ConversationId string // 会话 ID
	Topic          string // 话题名称
}

// TopicEntry 单条话题记录。
// 存储某个具体话题的详细状态信息。
type TopicEntry struct {
	Topic       string   // 话题名称
	StartTime   int64    // 该话题首次出现的时间戳
	LastSeen    int64    // 该话题最后一次被提及的时间戳（用于判断话题是否已过期）
	MessageList []string // 归属于该话题的消息列表（记录该话题下的具体对话内容）
}

// ConversationTracker 追踪对话中的话题变化，辅助指代消解。
type ConversationTracker struct {
	mu       sync.RWMutex           // 读写互斥锁，保证在并发环境下读写 topics 的线程安全
	topics   map[string]*TopicEntry // 存储话题的映射表（Key 为 TopicKey 的字符串形式，Value 为话题记录）
	topicTTL time.Duration         // 话题的存活时间（Time To Live），超过该时间未被提及的话题会被视为失效或归档
	ticker   *time.Ticker           // 定时器，定期触发清理过期话题
	stopChan chan struct{}          // 停止信号
}

// NewConversationTracker 创建话题追踪器，默认话题过期时间 30 分钟。
// 内部启动定时器，每 10 分钟自动清理过期话题。
func NewConversationTracker(ttlMinutes int) *ConversationTracker {
	if ttlMinutes <= 0 {
		ttlMinutes = 30
	}
	ct := &ConversationTracker{
		topics:   make(map[string]*TopicEntry),
		topicTTL: time.Duration(ttlMinutes) * time.Minute,
		stopChan: make(chan struct{}),
	}
	go ct.startCleanup()
	return ct
}

// startCleanup 启动后台定时清理协程，每 10 分钟清理一次过期话题。
func (ct *ConversationTracker) startCleanup() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ct.Cleanup()
		case <-ct.stopChan:
			return
		}
	}
}

// Stop 停止定时清理，释放资源。
func (ct *ConversationTracker) Stop() {
	close(ct.stopChan)
}

// topicKey 生成话题缓存的唯一标识键。
// 格式："会话ID::话题名"，用于在 map 中唯一定位一个话题记录。
func (ct *ConversationTracker) topicKey(conversationId, topic string) string {
	return conversationId + "::" + topic
}

// Record 记录一轮对话，推动话题时间戳。
// Record 记录一轮对话（用户提问 + AI 回复），并更新话题状态。
func (ct *ConversationTracker) Record(conversationId, userMsg, aiMsg string) {
	// 1. 加锁：保证并发安全，防止多个协程同时修改 topics 映射表
	ct.mu.Lock()
	defer ct.mu.Unlock()

	// 2. 提取话题：根据用户消息内容，通过关键词匹配识别当前属于哪个话题（如“工作”、“生活”等）
	topic := extractTopic(userMsg)
	if topic == "" {
		topic = "general" // 如果无法识别具体话题，归类为通用话题
	}

	// 3. 生成唯一键：组合“会话ID”和“话题名”，确保不同会话或不同话题之间互不干扰
	key := ct.topicKey(conversationId, topic)
	now := time.Now().Unix()

	// 4. 更新现有话题：如果该话题在当前会话中已经存在
	if entry, ok := ct.topics[key]; ok {
		entry.LastSeen = now // 更新最后活跃时间（用于判断话题是否过期）

		// 限制存储的消息数量，防止内存无限增长（最多存 20 条交互记录）
		// 超出上限时移除最旧的两条，再追加新消息
		if len(entry.MessageList) >= 20 {
			entry.MessageList = entry.MessageList[2:]
		}
		entry.MessageList = append(entry.MessageList, userMsg, aiMsg)
		return
	}

	// 5. 创建新话题：如果是该会话中第一次出现这个话题
	ct.topics[key] = &TopicEntry{
		Topic:       topic,                    // 话题名称
		StartTime:   now,                      // 记录话题开始的时间
		LastSeen:    now,                      // 初始化最后活跃时间
		MessageList: []string{userMsg, aiMsg}, // 初始化消息列表
	}
}

// GetActiveTopic 获取当前最活跃的话题。
// 查找最近 5 分钟内有更新的消息，且是最后更新的那个话题。
// 用于指代消解时确定当前对话的主题上下文。
func (ct *ConversationTracker) GetActiveTopic(conversationId string) string {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	cutoff := time.Now().Add(-5 * time.Minute).Unix()
	var active string
	var latest int64

	for key, entry := range ct.topics {
		if !strings.HasPrefix(key, conversationId+"::") {
			continue
		}
		if entry.LastSeen > cutoff && entry.LastSeen > latest {
			latest = entry.LastSeen
			active = entry.Topic
		}
	}
	return active
}

// GetTopicContext 获取指定话题的对话上下文摘要。
// conversationId: 会话 ID，topic: 话题名称，maxMsgs: 返回的最大消息对数。
// 返回格式化的对话字符串："用户: xxx\nAI: xxx\n用户: xxx\nAI: xxx"。
// 当 maxMsgs > 0 时，限制返回最新 maxMsgs*2 条消息。
func (ct *ConversationTracker) GetTopicContext(conversationId, topic string, maxMsgs int) string {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	key := ct.topicKey(conversationId, topic)
	// 缓存在进程中的话题实体
	entry, ok := ct.topics[key]
	if !ok || len(entry.MessageList) == 0 {
		return ""
	}

	msgs := entry.MessageList
	if maxMsgs > 0 && len(msgs) > maxMsgs*2 {
		// 返回最新的 maxMsgs*2 条消息
		msgs = msgs[len(msgs)-maxMsgs*2:]
	}
	// 为每一条对话增加一个标识，是用户说的还是ai说的
	var lines []string
	for i, m := range msgs {
		role := "用户"
		if i%2 == 1 {
			role = "AI"
		}
		lines = append(lines, role+": "+m)
	}
	return strings.Join(lines, "\n")
}

// Cleanup 清理过期的对话话题记录。
// 遍历所有话题，删除距离上次活跃时间超过 topicTTL 的话题。
// 由后台定时器定期调用，防止内存泄漏。
func (ct *ConversationTracker) Cleanup() {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	cutoff := time.Now().Add(-ct.topicTTL).Unix()
	for key, entry := range ct.topics {
		if entry.LastSeen < cutoff {
			delete(ct.topics, key)
		}
	}
}

// extractTopic 从消息内容中提取话题关键词（规则匹配方式）。
// 维护一个话题-关键词映射表，统计消息中各类关键词的出现次数。
// 返回得分最高的话题名称；若所有话题得分相同或消息过短（≤3字），返回空字符串。
func extractTopic(msg string) string {
	msg = strings.TrimSpace(msg)
	if len([]rune(msg)) <= 3 {
		return ""
	}

	// 话题关键词表
	topicKeywords := map[string][]string{
		"工作": {"工作", "项目", "代码", "bug", "部署", "需求", "开发", "测试", "上线"},
		"代码": {"代码", "函数", "接口", "重构", "算法", "实现", "优化"},
		"聊天": {"聊天", "对话", "消息", "回复"},
		"生活": {"吃饭", "睡觉", "运动", "旅游", "购物", "电影", "音乐"},
		"学习": {"学习", "课程", "教程", "看书", "考试", "读书"},
		"旅行": {"旅行", "旅游", "出行", "订票", "酒店", "景点"},
		"技术": {"技术", "架构", "中间件", "数据库", "缓存", "队列", "微服务"},
	}

	maxScore := 0
	var topTopic string

	msgLower := strings.ToLower(msg)
	for topic, keywords := range topicKeywords {
		score := 0
		for _, kw := range keywords {
			if strings.Contains(msgLower, kw) {
				score++
			}
		}
		if score > maxScore {
			maxScore = score
			topTopic = topic
		}
	}

	return topTopic
}

// MergeTopicContext 将话题上下文注入到检索 query 中。
// 当检测到用户消息含指代词且存在活跃话题时，获取该话题的历史对话片段。
// 返回格式化的上下文字符串："当前话题【话题名】：\n用户: xxx\nAI: xxx"。
// 用于增强语义检索时对指代词的理解能力。
func MergeTopicContext(conversationId, currentMsg string, ct *ConversationTracker) string {
	if ct == nil || !NeedsResolution(currentMsg) {
		return ""
	}

	active := ct.GetActiveTopic(conversationId)
	if active == "" || active == "general" {
		return ""
	}

	ctx := ct.GetTopicContext(conversationId, active, 3)
	if ctx == "" {
		return ""
	}

	return "当前话题【" + active + "】：\n" + ctx
}
