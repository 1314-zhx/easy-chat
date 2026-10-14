package logic

import (
	"context"
	"easy-chat/apps/ai/rpc/internal/svc"
	"easy-chat/apps/im/immodels"
	"easy-chat/pkg/rag"
	"time"
)

type RAGRetriever struct {
	svcCtx    *svc.ServiceContext
	retriever *rag.Retriever
}

func NewRAGRetriever(svcCtx *svc.ServiceContext) *RAGRetriever {
	qwenCfg := svcCtx.QwenConfig()
	mem := rag.NewMemoryExtractor(svcCtx.UserMemoryModel, qwenCfg)
	idx := rag.NewIndexer(svcCtx.MessageVectorModel, qwenCfg)
	retriever := rag.NewRetriever(
		svcCtx.ChatLogModel,
		svcCtx.MessageVectorModel,
		svcCtx.UserMemoryModel,
		idx,
		mem,
		qwenCfg,
		svcCtx.RAGTracker,
	)
	return &RAGRetriever{
		svcCtx:    svcCtx,
		retriever: retriever,
	}
}

// Retrieve 返回多轮历史与系统附加信息。
// excludeSendTime: 纳秒级时间戳，0 表示不限制
func (r *RAGRetriever) Retrieve(
	ctx context.Context,
	conversationId, currentMessage, userId string,
	excludeSendTime int64,
) (*rag.RetrievedContext, error) {
	cfg := r.svcCtx.RuntimeConfig().RAG
	// 时间戳已经是纳秒级，直接使用
	beforeTime := excludeSendTime
	if beforeTime <= 0 {
		beforeTime = time.Now().UnixNano() - 5*60*1000000000 // 默认只检索最近5分钟（纳秒）
	}

	return r.retriever.Retrieve(ctx, rag.RetrieveOptions{
		ConversationId:      conversationId,
		CurrentMessage:      currentMessage,
		UserId:              userId,
		ExcludeSendTime:     beforeTime,
		RecentHistoryCount:  cfg.RecentHistoryCount,
		TopK:                cfg.TopK,
		SimilarityThreshold: cfg.SimilarityThreshold,
		SemanticSearchLimit: int64(cfg.SemanticSearchLimit),
		MaxContextChars:     cfg.MaxContextChars,
		QueryRewriteEnabled: cfg.QueryRewriteEnabled,
		RAGEnabled:          cfg.Enabled,
	})
}

// IndexChatLog 为消息建立向量索引。
func (r *RAGRetriever) IndexChatLog(ctx context.Context, log *immodels.ChatLog) error {
	return r.retriever.Indexer.IndexChatLog(ctx, log)
}

// RefreshMemoryAsync 异步 LLM 提取用户记忆。
func (r *RAGRetriever) RefreshMemoryAsync(userId, conversationId, dialogue string) {
	if !r.svcCtx.RuntimeConfig().RAG.MemoryExtractEnabled {
		return
	}
	go func() {
		_ = r.retriever.Memory.ExtractWithLLM(context.Background(), userId, conversationId, dialogue)
	}()
}

// TrackRound 记录一轮对话，推动话题追踪器。
func (r *RAGRetriever) TrackRound(conversationId, userMsg, aiMsg string) {
	if conversationId == "" {
		return
	}
	r.retriever.TrackRound(conversationId, userMsg, aiMsg)
}
