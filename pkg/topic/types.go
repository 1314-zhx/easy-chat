package topic

import "time"

const (
	PhaseFormal  = "formal"
	PhaseExplore = "explore"

	TopicStatusActive = "active"
	TopicStatusEnded  = "ended"
)

// RoundEvent 一轮完整对话（用户问 + AI 答）完成后的输入。
type RoundEvent struct {
	ConversationId string
	UserId         string
	RoundId        string
	UserMsgId      string
	AiMsgId        string
	UserContent    string
	AiContent      string
}

// TopicDoc Mongo 持久化（无向量）。
type TopicDoc struct {
	TopicId        string
	ConversationId string
	UserId         string
	Name           string
	Summary        string
	SummaryVersion int
	Status         string
	StartedAt      time.Time
	EndedAt        *time.Time
	TotalHitCount  int64
	LastHitAt      time.Time
}

// FormalState 正式主题（Redis）。
type FormalState struct {
	TopicId            string    `json:"topicId"`
	Name               string    `json:"name"`
	Summary            string    `json:"summary"`
	Embedding          []float32 `json:"embedding"`
	MissCount          int       `json:"missCount"`
	RoundsSinceSummary int       `json:"roundsSinceSummary"`
	RoundSummaryBuffer []string  `json:"roundSummaryBuffer"`
	SummaryVersion     int       `json:"summaryVersion"`
}

// CandidateState 候选主题（Redis）。
type CandidateState struct {
	TopicId      string    `json:"topicId"`
	Name         string    `json:"name"`
	Summary      string    `json:"summary"`
	Embedding    []float32 `json:"embedding"`
	HitCount     int       `json:"hitCount"`
	LastHitAt    int64     `json:"lastHitAt"`
	RecentRounds []string  `json:"recentRounds"`
}

// HistoryBoostState 历史高优先级主题（Redis，score>=阈值才入池）。
type HistoryBoostState struct {
	TopicId   string    `json:"topicId"`
	Name      string    `json:"name"`
	Summary   string    `json:"summary"`
	Embedding []float32 `json:"embedding"`
	Score     float64   `json:"score"`
}

// State 会话主题热状态（Redis）。
type State struct {
	ConversationId string             `json:"conversationId"`
	UserId         string             `json:"userId"`
	Phase          string             `json:"phase"`
	Formal         *FormalState       `json:"formal,omitempty"`
	Candidates     []CandidateState   `json:"candidates,omitempty"`
	HistoryBoost   *HistoryBoostState `json:"historyBoost,omitempty"`
	UpdatedAt      int64              `json:"updatedAt"`
}

// RoundResult 单轮处理结果。
type RoundResult struct {
	TopicId       string
	Phase         string
	Action        string // hit_formal | miss_formal | promote | restore | new_candidate | explore_hit
	RewriteSummary bool
}

// SummaryRewriteTask 每 10 轮触发的摘要重写任务。
type SummaryRewriteTask struct {
	ConversationId string
	TopicId        string
	OldSummary     string
	RoundSummaries []string
	SummaryVersion int
}
