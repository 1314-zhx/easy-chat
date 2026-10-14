package rag

import (
	"context"
	"easy-chat/apps/im/immodels"
	"easy-chat/pkg/qwen"
	"strings"

	"golang.org/x/sync/singleflight"
)

// Indexer 将聊天消息写入向量索引。
type Indexer struct {
	VectorModel immodels.MessageVectorModel
	Qwen        qwen.Config
	sf          singleflight.Group
}
// 通过依赖注入来传入向量模型和qwen配置，而不是硬编码，方便mock注入
func NewIndexer(vm immodels.MessageVectorModel, cfg qwen.Config) *Indexer {
	return &Indexer{VectorModel: vm, Qwen: cfg}
}

// ShouldSkip 判断是否应跳过某条消息，不为其建立向量索引。
// 跳过条件：内容为空字符串、是工具调用消息。
func (idx *Indexer) ShouldSkip(content, sendId string) bool {
	if strings.TrimSpace(content) == "" {
		return true
	}
	return IsToolCall(content)
}

// IndexChatLog 为单条聊天记录建立向量索引（幂等）。
func (idx *Indexer) IndexChatLog(ctx context.Context, log *immodels.ChatLog) error {
	if log == nil || idx.ShouldSkip(log.MsgContent, log.SendId) {
		return nil
	}
	chatLogId := log.ID.Hex()
	exists, err := idx.VectorModel.Exists(ctx, chatLogId)
	if err != nil || exists {
		return err
	}

	text := FormatIndexText(log.SendId, log.MsgContent)
	vec, err := qwen.Embed(idx.Qwen, text)
	if err != nil {
		return err
	}
	vec = Normalize(vec)

	return idx.VectorModel.Upsert(ctx, &immodels.MessageVector{
		ChatLogId:      chatLogId,
		ConversationId: log.ConversationId,
		SendId:         log.SendId,
		MsgContent:     log.MsgContent,
		SendTime:       log.SendTime,
		Embedding:      vec,
	})
}

// EnsureConversationIndexed 异步补建会话内未索引消息（不阻塞 Chat RPC）。
func (idx *Indexer) EnsureConversationIndexed(
	chatLogModel immodels.CustomChatLogModel,
	conversationId string,
	beforeSendTime int64,
	limit int,
) {
	if idx == nil || conversationId == "" {
		return
	}
	if limit <= 0 {
		limit = 10
	}
	go func() {
		_, _, _ = idx.sf.Do("idx:"+conversationId, func() (interface{}, error) {
			ctx := context.Background()
			history, err := chatLogModel.ListBySendTime(ctx, conversationId, beforeSendTime, 0, int64(limit*2))
			if err != nil {
				return nil, err
			}
			indexed := 0
			for _, log := range history {
				if indexed >= limit {
					break
				}
				if idx.ShouldSkip(log.MsgContent, log.SendId) {
					continue
				}
				if err := idx.IndexChatLog(ctx, log); err == nil {
					indexed++
				}
			}
			return nil, nil
		})
	}()
}

// FormatIndexText 格式化待索引的文本，添加角色前缀。
// 用户消息格式："用户: xxx"，AI 助手消息格式："助手: xxx"。
// 前缀有助于向量模型理解对话结构，提升语义检索的准确性。
func FormatIndexText(sendId, content string) string {
	role := "用户"
	if sendId == "ai_assistant" {
		role = "助手"
	}
	return role + ": " + content
}

// IsToolCall 判断消息内容是否为工具调用（插件执行结果）。
// 通过检测 JSON 中是否包含 "action" 或 "function" 字段来识别。
// 工具调用结果通常不适合作为 RAG 检索的上下文。
func IsToolCall(content string) bool {
	return strings.Contains(content, `{"action":`) ||
		strings.Contains(content, `{"function":`)
}
