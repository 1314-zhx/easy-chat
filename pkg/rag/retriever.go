package rag

import (
	"context"
	"easy-chat/apps/im/immodels"
	"easy-chat/pkg/qwen"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// RetrieveOptions RAG 检索参数。
type RetrieveOptions struct {
	ConversationId      string  // 会话 ID，用于关联特定的对话上下文
	CurrentMessage      string  // 当前用户输入的提问/消息内容（作为核心检索Query）
	UserId              string  // 用户 ID，可用于数据隔离或个性化检索
	ExcludeSendTime     int64   // 需要排除的时间戳（如：检索时过滤掉该时间点之后的数据，或排除刚发送的重复消息）
	RecentHistoryCount  int     // 检索最近历史对话的条数（用于补充短期上下文）
	TopK                int     // 向量检索返回的最相似文档/片段数量（Top K）
	SimilarityThreshold float64 // 相似度阈值（低于该分数的检索结果将被过滤掉）
	SemanticSearchLimit int64   // 语义搜索的最大限制条数（通常用于在重排或初步筛选阶段限制候选集规模）
	MaxContextChars     int     // 最终输入给大模型（LLM）的上下文最大字符数限制
	QueryRewriteEnabled bool    // 是否开启查询重写（如：将用户的口语化提问改写为更适合检索的关键词）
	RAGEnabled          bool    // 是否开启 RAG 检索增强功能（总开关）
}

// Retriever 混合检索：最近窗口 + 语义 TopK + 用户记忆 + 指代消解 + 话题追踪。
type Retriever struct {
	ChatLogModel immodels.CustomChatLogModel // 聊天记录模型，用于从数据库存取原始的历史对话数据
	VectorModel  immodels.MessageVectorModel // 向量数据模型，用于存取文本对应的向量数据（执行向量相似度检索）
	MemoryModel  immodels.UserMemoryModel    // 用户记忆模型，用于存取用户的长期记忆或画像信息
	Indexer      *Indexer                    // 索引器，负责文本的向量化、建立索引以及底层的向量检索逻辑
	Memory       *MemoryExtractor            // 记忆提取器，用于从历史对话中提炼关键信息、事实或用户偏好
	Tracker      *ConversationTracker        // 对话追踪器，用于管理和追踪当前的对话状态及上下文流转
	Compressor   *Compressor                 // 上下文压缩器，当检索到的内容过长时，负责对其进行精简和摘要
	Qwen         qwen.Config                 // 通义千问（Qwen）大模型的配置（用于调用 LLM 进行语义理解、重写或生成）
}

// NewRetriever 构造 Retriever，自动初始化 Tracker 和 Compressor。
func NewRetriever(
	chatLog immodels.CustomChatLogModel,
	vector immodels.MessageVectorModel,
	memModel immodels.UserMemoryModel,
	indexer *Indexer,
	mem *MemoryExtractor,
	cfg qwen.Config,
	tracker *ConversationTracker,
) *Retriever {
	if tracker == nil {
		tracker = NewConversationTracker(30)
	}
	return &Retriever{
		ChatLogModel: chatLog,
		VectorModel:  vector,
		MemoryModel:  memModel,
		Indexer:      indexer,
		Memory:       mem,
		Tracker:      tracker,
		Compressor:   NewCompressor(200),
		Qwen:         cfg,
	}
}

func (r *Retriever) Retrieve(ctx context.Context, opt RetrieveOptions) (*RetrievedContext, error) {
	if opt.RecentHistoryCount <= 0 {
		opt.RecentHistoryCount = 5
	}
	if opt.TopK <= 0 {
		opt.TopK = 5
	}
	if opt.SemanticSearchLimit <= 0 {
		opt.SemanticSearchLimit = 150
	}
	if opt.MaxContextChars <= 0 {
		opt.MaxContextChars = 6000
	}
	if opt.ExcludeSendTime <= 0 {
		// 默认只检索最近 5 分钟内的消息
		opt.ExcludeSendTime = time.Now().Unix() - 5*60
	}

	// 排除发送时间
	beforeTime := opt.ExcludeSendTime

	// 异步补建向量索引（独立 context，避免 RPC 结束后 context canceled）
	if r.Indexer != nil && opt.ConversationId != "" {
		r.Indexer.EnsureConversationIndexed(r.ChatLogModel, opt.ConversationId, beforeTime, 10)
	}
	// 系统提示词
	var systemParts []string
	// 获取最近的历史对话
	recent := r.fetchRecent(ctx, opt.ConversationId, beforeTime, int64(opt.RecentHistoryCount*2))

	// 同步写入用户记忆，确保本轮即可读到【用户信息】
	if opt.RAGEnabled && opt.UserId != "" {
		mem := r.syncUserMemory(ctx, opt, recent)
		if memText := FormatUserMemory(mem); memText != "" {
			systemParts = append(systemParts, memText)
		}
		if IsPersonalInfoQuestion(opt.CurrentMessage) && mem != nil && mem.Name == "" {
			systemParts = append(systemParts,
				"【提示】用户正在询问其个人信息；若【用户信息】中无记录，请结合下方对话历史回答，勿编造。")
		}
	}
	// 构建最近的历史对话
	merged := r.buildRecentMessages(recent, opt.RecentHistoryCount)

	// 查询语句
	query := opt.CurrentMessage

	// ── 指代消解层（叠加模式）────────────────────────────────────
	// 1. 个人信息查询 → 关键词扩展
	if opt.RAGEnabled && IsPersonalInfoQuestion(query) {
		query = expandQueryForPersonalInfo(query)
	}

	// 2. 含指代词 → 规则展开 + LLM 改写（叠加模式）
	if opt.RAGEnabled && NeedsResolution(query) {
		topicCtx := MergeTopicContext(opt.ConversationId, query, r.Tracker)
		snippet := dialogueSnippet(recent, 3)
		extra := ""
		if topicCtx != "" {
			extra = "\n话题上下文：\n" + topicCtx
		}

		// 步骤1：规则展开（数据增强），将展开结果作为上下文传给 LLM
		expanded := resolveByExpansion(query)

		// 步骤2：LLM 改写（语义消解），传入原始查询 + 历史上下文 + 检索增强词
		// 叠加模式：规则展开的结果作为增强上下文，帮助 LLM 更好地理解指代
		if opt.QueryRewriteEnabled {
			if rewritten, err := RewriteQuery(r.Qwen, query, snippet+extra, expanded); err == nil && strings.TrimSpace(rewritten) != "" {
				query = strings.TrimSpace(rewritten)
			} else if expanded != query {
				// LLM 调用失败时，降级使用规则展开结果
				query = expanded
			}
		} else if expanded != query {
			// LLM 未启用时，直接使用规则展开结果
			query = expanded
		}
	}

	if opt.RAGEnabled && strings.TrimSpace(query) != "" {
		// 用于从历史对话中找出与当前问题最相关的消息。semantic 语义的
		semantic := r.semanticSearch(ctx, opt, query, beforeTime, merged)
		merged = mergeMessages(merged, semantic, opt.RecentHistoryCount+opt.TopK)
	}
	// 构建最近消息
	history := toChatMessages(merged)

	// ── 上下文压缩层 ───────────────────────────────────────────
	// 按字符数进行压缩，并在最近的文本字符。？!,，处截断，保证语义连贯
	if r.Compressor != nil {
		history = r.Compressor.CompressHistory(history, opt.MaxContextChars)
	}

	history, systemParts = applyCharBudget(history, systemParts, opt.MaxContextChars)

	return &RetrievedContext{
		SystemExtras: strings.Join(systemParts, "\n\n"),
		History:      history,
	}, nil
}

// TrackRound 记录一轮对话，推动话题追踪。
// 在 AI 回复后调用，将 userMsg + aiMsg 推进话题追踪器。
func (r *Retriever) TrackRound(conversationId, userMsg, aiMsg string) {
	if r.Tracker == nil || conversationId == "" {
		return
	}
	r.Tracker.Record(conversationId, userMsg, aiMsg)
}

// syncUserMemory 从近期与历史消息中提取并持久化用户画像（同步）。
func (r *Retriever) syncUserMemory(
	ctx context.Context,
	opt RetrieveOptions,
	recent []*immodels.ChatLog,
) *immodels.UserMemory {
	if r.Memory == nil || opt.UserId == "" {
		return nil
	}

	logs := append([]*immodels.ChatLog(nil), recent...)
	if IsPersonalInfoQuestion(opt.CurrentMessage) || len(logs) == 0 {
		profile := r.fetchProfileMessages(ctx, opt.ConversationId, opt.ExcludeSendTime, 50)
		logs = mergeChatLogs(logs, profile)
	}

	mem, err := r.Memory.MergeFromMessages(ctx, opt.UserId, opt.ConversationId, logs)
	if err != nil {
		mem, _ = r.MemoryModel.FindOne(ctx, opt.UserId, opt.ConversationId)
	}
	return mem
}

// fetchProfileMessages 检索用户个人信息相关消息。
// 通过正则匹配识别用户自我介绍（"我叫..."）、偏好（"我喜欢..."）、习惯（"我爱吃..."、"我爱喝..."）等内容。
// 用于在用户询问个人信息时补充背景上下文。
func (r *Retriever) fetchProfileMessages(
	ctx context.Context,
	conversationId string,
	beforeSendTime int64,
	limit int64,
) []*immodels.ChatLog {
	filter := bson.M{
		"conversationId": conversationId,
		"sendId":         bson.M{"$ne": "ai_assistant"},
		"$or": []bson.M{
			{"msgContent": bson.M{"$regex": "我叫", "$options": "i"}},
			{"msgContent": bson.M{"$regex": "我的名字是", "$options": "i"}},
			{"msgContent": bson.M{"$regex": "我喜欢", "$options": "i"}},
			{"msgContent": bson.M{"$regex": "我爱吃", "$options": "i"}},
			{"msgContent": bson.M{"$regex": "我爱喝", "$options": "i"}},
		},
	}
	if beforeSendTime > 0 {
		filter["sendTime"] = bson.M{"$lt": beforeSendTime}
	}
	results, err := r.ChatLogModel.FindWithFilter(ctx, filter, limit, bson.M{"sendTime": -1})
	if err != nil {
		return nil
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].SendTime < results[j].SendTime
	})
	return results
}

// mergeChatLogs 合并两个 ChatLog 列表并去重。
// 根据 SendTime 作为唯一标识，去除重复的消息记录，并按时间正序排列。
func mergeChatLogs(a, b []*immodels.ChatLog) []*immodels.ChatLog {
	seen := make(map[int64]struct{}, len(a)+len(b))
	out := make([]*immodels.ChatLog, 0, len(a)+len(b))
	for _, list := range [][]*immodels.ChatLog{a, b} {
		for _, log := range list {
			if _, ok := seen[log.SendTime]; ok {
				continue
			}
			seen[log.SendTime] = struct{}{}
			out = append(out, log)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].SendTime < out[j].SendTime
	})
	return out
}

// fetchRecent 获取指定会话最近的聊天记录。
// 从数据库中按发送时间倒序获取，然后重新按时间正序排列返回。
// beforeSendTime 用于过滤时间点之前的数据（排除刚发送的重复消息）。
func (r *Retriever) fetchRecent(ctx context.Context, conversationId string, beforeSendTime int64, limit int64) []*immodels.ChatLog {
	history, err := r.ChatLogModel.ListBySendTime(ctx, conversationId, beforeSendTime, 0, limit)
	if err != nil {
		return nil
	}
	sort.Slice(history, func(i, j int) bool {
		return history[i].SendTime < history[j].SendTime
	})
	return history
}

// buildRecentMessages 将数据库的 ChatLog 列表转换为 ScoredMessage 结构。
// 过滤掉工具调用消息（如插件执行结果），只保留普通对话内容。
// max 参数限制返回的最大消息数，优先保留最近的消息。
func (r *Retriever) buildRecentMessages(logs []*immodels.ChatLog, max int) []ScoredMessage {
	var out []ScoredMessage
	for _, log := range logs {
		if IsToolCall(log.MsgContent) {
			continue
		}
		out = append(out, ScoredMessage{
			SendTime: log.SendTime,
			SendId:   log.SendId,
			Content:  log.MsgContent,
			Source:   "recent",
		})
	}
	if len(out) > max {
		out = out[len(out)-max:]
	}
	return out
}

// semanticSearch 执行语义向量检索，找出与当前查询最相关的历史消息。
// 步骤：
//   1. 将查询文本转换为向量表示（使用 embedding 模型）
//   2. 从数据库获取该会话的所有向量数据，并且过滤掉ExcludeSendTime以内的数据，因为history包括了 （上限150条）
//   3. 计算查询向量与历史向量的余弦相似度
//   4. 按相似度降序排列，过滤低于阈值的结果
//   5. 去重（避免与 recent 消息重复），返回 TopK 条最相关的消息
func (r *Retriever) semanticSearch(
	ctx context.Context,
	opt RetrieveOptions,
	query string,
	beforeSendTime int64,
	already []ScoredMessage,
) []ScoredMessage {
	// 创建查询向量
	embedText := "query: " + query
	queryVec, err := qwen.Embed(r.Qwen, embedText)
	if err != nil {
		return nil
	}
	queryVec = Normalize(queryVec)
	// 搜索向量
	vectors, err := r.VectorModel.ListByConversation(ctx, opt.ConversationId, opt.SemanticSearchLimit)
	if err != nil || len(vectors) == 0 {
		return nil
	}
	// 筛选
	seen := make(map[int64]bool)
	for _, m := range already {
		seen[m.SendTime] = true
	}
	// 准备候选集
	type scored struct {
		msg ScoredMessage
		sim float64
	}
	var candidates []scored
	// 筛选向量
	for _, v := range vectors {
		//  跳过不在筛选时间内的
		if beforeSendTime > 0 && v.SendTime >= beforeSendTime {
			continue
		}
		// 跳过工具调用
		if IsToolCall(v.MsgContent) {
			continue
		}
		// 计算余弦相似度
		sim := DotSimilarity(queryVec, v.Embedding)
		// 引入时间衰减：越久远的消息权重越低
		ageHours := math.Max(0, float64(time.Now().UnixNano()-v.SendTime)/float64(time.Hour))
		score := sim * math.Exp(-ageHours/24)
		// 筛选相似度特别低的
		if score < opt.SimilarityThreshold {
			continue
		}
		// 如果发送时间已经存在，则跳过去重
		if seen[v.SendTime] {
			continue
		}
		candidates = append(candidates, scored{
			msg: ScoredMessage{
				SendTime:   v.SendTime,
				SendId:     v.SendId,
				Content:    v.MsgContent,
				Similarity: score,
				Source:     "semantic",
			},
			sim: score,
		})
	}
	// 按照相似度降序
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].sim > candidates[j].sim
	})
	// 筛选 TopK
	limit := opt.TopK
	// 只要相似度最高的
	if limit > len(candidates) {
		limit = len(candidates)
	}
	out := make([]ScoredMessage, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, candidates[i].msg)
	}
	return out
}

// mergeMessages 合并 recent（最近窗口）和 semantic（语义检索）两类消息。
// 使用发送时间作为唯一标识进行去重，然后按时间正序排列。
// 最终返回的消息数量不超过 maxTotal 条。
func mergeMessages(recent, semantic []ScoredMessage, maxTotal int) []ScoredMessage {
	byTime := make(map[int64]ScoredMessage)
	for _, m := range recent {
		byTime[m.SendTime] = m
	}
	for _, m := range semantic {
		if _, ok := byTime[m.SendTime]; !ok {
			byTime[m.SendTime] = m
		}
	}
	merged := make([]ScoredMessage, 0, len(byTime))
	for _, m := range byTime {
		merged = append(merged, m)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].SendTime < merged[j].SendTime
	})
	if len(merged) > maxTotal {
		merged = merged[len(merged)-maxTotal:]
	}
	return merged
}

// toChatMessages 将内部 ScoredMessage 结构转换为 LLM（通义千问）所需的 ChatMessage 格式。
// 根据 SendId 判断角色：ai_assistant → assistant（AI回复），其他 → user（用户消息）。
func toChatMessages(msgs []ScoredMessage) []qwen.ChatMessage {
	out := make([]qwen.ChatMessage, 0, len(msgs))
	for _, m := range msgs {
		role := "user"
		if m.SendId == "ai_assistant" {
			role = "assistant"
		}
		out = append(out, qwen.ChatMessage{Role: role, Content: m.Content})
	}
	return out
}

// dialogueSnippet 提取最近 N 轮对话的文本片段。
// 用于给指代消解或查询改写提供上下文背景。
// 自动过滤掉工具调用消息，按 "用户: xxx\nAI: xxx" 格式拼接。
// dialogueSnippet 将聊天记录切片格式化为对话文本片段。
// logs: 原始聊天记录切片，n: 需要保留的最近对话条数。
func dialogueSnippet(logs []*immodels.ChatLog, n int) string {
	// 如果没有聊天记录，直接返回空字符串
	if len(logs) == 0 {
		return ""
	}

	// 计算截取的起始索引，目的是保留最近的 n 条记录
	start := len(logs) - n
	// 如果总记录数不足 n 条，则从头开始截取（防止索引越界）
	if start < 0 {
		start = 0
	}

	var lines []string
	// 遍历截取后的最近聊天记录
	for _, l := range logs[start:] {
		// 过滤掉 AI 的工具调用指令（通常包含复杂的 JSON，不适合作为上下文展示）
		if IsToolCall(l.MsgContent) {
			continue
		}

		// 根据发送者 ID 判断并设置角色名称
		role := "用户"
		if l.SendId == "ai_assistant" {
			role = "AI"
		}

		// 将单条对话格式化为 "角色: 消息内容" 的形式并加入切片
		lines = append(lines, fmt.Sprintf("%s: %s", role, l.MsgContent))
	}

	// 将所有对话行用换行符拼接成一个完整的字符串返回
	return strings.Join(lines, "\n")
}

// expandQueryForPersonalInfo 扩展个人信息类查询的检索关键词。
// 当用户询问姓名时，扩展为"用户自我介绍 我叫 我的名字是"以提高检索命中率。
// 当用户询问偏好时，扩展为"用户喜好 我喜欢 我爱吃 我爱喝"。
func expandQueryForPersonalInfo(msg string) string {
	if strings.Contains(msg, "叫") || strings.Contains(msg, "名字") || strings.Contains(msg, "是谁") {
		return "用户自我介绍 我叫 我的名字是"
	}
	if strings.Contains(msg, "吃") || strings.Contains(msg, "喜欢") || strings.Contains(msg, "偏好") || strings.Contains(msg, "喝") {
		return "用户喜好 我喜欢 我爱吃 我爱喝"
	}
	return msg
}

// applyCharBudget 根据字符预算裁剪历史消息。
// 遍历历史消息，累加字符数；当超出 maxChars 限制时停止。
// 最后一条超长消息会在句子边界处截断，保证语义连贯。
func applyCharBudget(
	history []qwen.ChatMessage,
	systemParts []string,
	maxChars int,
) ([]qwen.ChatMessage, []string) {
	used := 0
	for _, p := range systemParts {
		used += len([]rune(p))
	}
	trimmed := make([]qwen.ChatMessage, 0, len(history))
	for i, h := range history {
		n := len([]rune(h.Content))
		if used+n > maxChars {
			// 最后一页消息：在句子边界处截断，避免语义中断
			if i == len(history)-1 {
				truncated := truncateWithEllipsis(h.Content, maxChars-used)
				trimmed = append(trimmed, qwen.ChatMessage{Role: h.Role, Content: truncated})
			}
			break
		}
		used += n
		trimmed = append(trimmed, h)
	}
	return trimmed, systemParts
}
