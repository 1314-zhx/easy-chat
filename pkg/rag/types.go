package rag

import "easy-chat/pkg/qwen"

// RetrievedContext RAG 检索结果。
type RetrievedContext struct {
	SystemExtras string
	History      []qwen.ChatMessage
}

// ScoredMessage 带相似度分数的消息。
type ScoredMessage struct {
	SendTime   int64
	SendId     string
	Content    string
	Similarity float64
	Source     string // recent | semantic
}
