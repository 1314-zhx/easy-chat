package topic

import (
	"context"
	"fmt"
	"time"

	"easy-chat/pkg/qwen"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
)

// Engine 主题状态机。
type Engine struct {
	cfg     Config
	store   *RedisStore
	repo    Repository
	qwenCfg qwen.Config
	sf      singleflight.Group
}

func NewEngine(cfg Config, store *RedisStore, repo Repository, qwenCfg qwen.Config) *Engine {
	if cfg.MaxCandidates <= 0 {
		cfg = DefaultConfig()
	}
	return &Engine{
		cfg:     cfg,
		store:   store,
		repo:    repo,
		qwenCfg: qwenCfg,
	}
}

// OnRoundComplete 处理完整一轮（用户+AI）后的主题状态更新。
func (e *Engine) OnRoundComplete(ctx context.Context, ev RoundEvent) (*RoundResult, *SummaryRewriteTask, error) {
	key := ev.ConversationId
	v, err, _ := e.sf.Do(key, func() (interface{}, error) {
		return e.onRound(ctx, ev)
	})
	if err != nil {
		return nil, nil, err
	}
	pair := v.(roundOutcome)
	return pair.result, pair.rewrite, nil
}

type roundOutcome struct {
	result  *RoundResult
	rewrite *SummaryRewriteTask
}

func (e *Engine) onRound(ctx context.Context, ev RoundEvent) (roundOutcome, error) {
	st, err := e.loadState(ctx, ev)
	if err != nil {
		return roundOutcome{}, err
	}

	roundSummary := BuildRoundSummary(ev.UserContent, ev.AiContent)
	roundEmb, err := qwen.Embed(e.qwenCfg, roundSummary)
	if err != nil {
		return roundOutcome{}, fmt.Errorf("embed round summary: %w", err)
	}

	res := &RoundResult{Phase: st.Phase}
	var rewrite *SummaryRewriteTask

	switch st.Phase {
	case PhaseFormal:
		res, rewrite = e.handleFormal(ctx, st, roundSummary, roundEmb, res)
	default:
		st.Phase = PhaseExplore
		res, rewrite = e.handleExplore(ctx, st, roundSummary, roundEmb, res)
	}

	if err := e.store.Save(ctx, st); err != nil {
		return roundOutcome{}, err
	}
	res.TopicId = e.activeTopicId(st)
	return roundOutcome{result: res, rewrite: rewrite}, nil
}

func (e *Engine) handleFormal(ctx context.Context, st *State, roundSummary string, roundEmb []float32, res *RoundResult) (*RoundResult, *SummaryRewriteTask) {
	f := st.Formal
	if f == nil {
		st.Phase = PhaseExplore
		return e.handleExplore(ctx, st, roundSummary, roundEmb, res)
	}

	sim := CosineSimilarity(roundEmb, f.Embedding)
	hit := false

	switch {
	case sim >= e.cfg.FormalHitThreshold:
		hit = true
	case sim >= e.cfg.FormalGrayLow:
		same, err := DisambiguateSameTopic(e.qwenCfg, f.Name, f.Summary, roundSummary)
		if err == nil && same {
			hit = true
		}
	default:
		hit = false
	}

	var rewrite *SummaryRewriteTask
	if hit {
		f.MissCount = 0
		res.Action = "hit_formal"
		_ = e.repo.IncHitCount(ctx, f.TopicId, time.Now())
	} else {
		f.MissCount++
		res.Action = "miss_formal"
		if f.MissCount >= e.cfg.MissLimit {
			_ = e.repo.EndTopic(ctx, f.TopicId, time.Now())
			st.Formal = nil
			st.Phase = PhaseExplore
			e.refreshHistoryBoost(ctx, st)
			res.Phase = PhaseExplore
			res.Action = "enter_explore"
			res2, rw := e.handleExplore(ctx, st, roundSummary, roundEmb, res)
			if rw != nil {
				rewrite = rw
			}
			return res2, rewrite
		}
	}

	f.RoundSummaryBuffer = appendFIFO(f.RoundSummaryBuffer, roundSummary, e.cfg.RoundSummaryBufferSize)
	f.RoundsSinceSummary++
	if f.RoundsSinceSummary >= e.cfg.SummaryRewriteEvery {
		rewrite = &SummaryRewriteTask{
			ConversationId: st.ConversationId,
			TopicId:        f.TopicId,
			OldSummary:     f.Summary,
			RoundSummaries: append([]string(nil), f.RoundSummaryBuffer...),
			SummaryVersion: f.SummaryVersion,
		}
		f.RoundsSinceSummary = 0
	}
	return res, rewrite
}

func (e *Engine) handleExplore(ctx context.Context, st *State, roundSummary string, roundEmb []float32, res *RoundResult) (*RoundResult, *SummaryRewriteTask) {
	res.Phase = PhaseExplore

	if st.HistoryBoost != nil && st.HistoryBoost.Score >= e.cfg.HistoryMinScore {
		if sim := CosineSimilarity(roundEmb, st.HistoryBoost.Embedding); sim >= e.cfg.HistoryRestoreSim {
			e.restoreFormal(ctx, st, st.HistoryBoost.TopicId, st.HistoryBoost.Name, st.HistoryBoost.Summary, roundEmb, roundSummary)
			res.Action = "restore"
			res.Phase = PhaseFormal
			res.TopicId = st.Formal.TopicId
			return res, nil
		}
	}

	bestIdx := -1
	bestSim := 0.0
	for i := range st.Candidates {
		sim := CosineSimilarity(roundEmb, st.Candidates[i].Embedding)
		if sim >= e.cfg.ExploreHitThreshold && sim > bestSim {
			bestSim = sim
			bestIdx = i
		}
	}
	if bestIdx >= 0 {
		c := &st.Candidates[bestIdx]
		c.HitCount++
		c.LastHitAt = time.Now().Unix()
		c.RecentRounds = appendFIFO(c.RecentRounds, roundSummary, 3)
		c.Summary = MergeCandidateSummary(c.RecentRounds)
		if emb, err := qwen.Embed(e.qwenCfg, c.Summary); err == nil {
			c.Embedding = emb
		}
		res.Action = "explore_hit"
		if c.HitCount >= e.cfg.PromoteHitLimit {
			e.promoteCandidate(ctx, st, bestIdx, roundSummary)
			res.Action = "promote"
			res.Phase = PhaseFormal
			res.TopicId = st.Formal.TopicId
		}
		return res, nil
	}

	e.addCandidate(st, roundSummary, roundEmb)
	res.Action = "new_candidate"
	return res, nil
}

func (e *Engine) promoteCandidate(ctx context.Context, st *State, idx int, roundSummary string) {
	c := st.Candidates[idx]
	rounds := append([]string(nil), c.RecentRounds...)
	if roundSummary != "" {
		rounds = appendFIFO(rounds, roundSummary, 3)
	}
	summary, _ := GenerateInitialSummary(e.qwenCfg, c.Name, rounds)
	emb, err := qwen.Embed(e.qwenCfg, summary)
	if err != nil {
		emb = c.Embedding
	}
	now := time.Now()
	topicId := c.TopicId
	if topicId == "" {
		topicId = uuid.NewString()
	}
	doc := &TopicDoc{
		TopicId:        topicId,
		ConversationId: st.ConversationId,
		UserId:         st.UserId,
		Name:           c.Name,
		Summary:        summary,
		SummaryVersion: 1,
		Status:         TopicStatusActive,
		StartedAt:      now,
		TotalHitCount:  int64(c.HitCount),
		LastHitAt:      now,
	}
	_ = e.repo.EndActive(ctx, st.ConversationId, now)
	_ = e.repo.Insert(ctx, doc)

	st.Formal = &FormalState{
		TopicId:            topicId,
		Name:               c.Name,
		Summary:            summary,
		Embedding:          emb,
		MissCount:          0,
		RoundsSinceSummary: 0,
		RoundSummaryBuffer: appendFIFO(nil, MergeCandidateSummary(rounds), e.cfg.RoundSummaryBufferSize),
		SummaryVersion:     1,
	}
	st.Candidates = nil
	st.Phase = PhaseFormal
	st.HistoryBoost = nil
}

func (e *Engine) restoreFormal(ctx context.Context, st *State, topicId, name, summary string, emb []float32, roundSummary string) {
	now := time.Now()
	_ = e.repo.EndActive(ctx, st.ConversationId, now)
	doc := &TopicDoc{
		TopicId:        topicId,
		ConversationId: st.ConversationId,
		UserId:         st.UserId,
		Name:           name,
		Summary:        summary,
		SummaryVersion: 1,
		Status:         TopicStatusActive,
		StartedAt:      now,
		LastHitAt:      now,
		TotalHitCount:  1,
	}
	_ = e.repo.Insert(ctx, doc)

	st.Formal = &FormalState{
		TopicId:            topicId,
		Name:               name,
		Summary:            summary,
		Embedding:          emb,
		MissCount:          0,
		RoundsSinceSummary: 1,
		RoundSummaryBuffer: appendFIFO(nil, roundSummary, e.cfg.RoundSummaryBufferSize),
		SummaryVersion:     1,
	}
	st.Phase = PhaseFormal
	st.Candidates = nil
	st.HistoryBoost = nil
}

func (e *Engine) addCandidate(st *State, roundSummary string, roundEmb []float32) {
	name := CandidateNameFromSummary(roundSummary)
	c := CandidateState{
		TopicId:      uuid.NewString(),
		Name:         name,
		Summary:      roundSummary,
		Embedding:    roundEmb,
		HitCount:     1,
		LastHitAt:    time.Now().Unix(),
		RecentRounds: []string{roundSummary},
	}
	st.Candidates = append(st.Candidates, c)
	if len(st.Candidates) > e.cfg.MaxCandidates {
		evictIdx := 0
		minScore := CandidateEvictScore(st.Candidates[0])
		for i := 1; i < len(st.Candidates); i++ {
			sc := CandidateEvictScore(st.Candidates[i])
			if sc < minScore {
				minScore = sc
				evictIdx = i
			}
		}
		st.Candidates = append(st.Candidates[:evictIdx], st.Candidates[evictIdx+1:]...)
	}
}

func (e *Engine) refreshHistoryBoost(ctx context.Context, st *State) {
	st.HistoryBoost = nil
	if e.repo == nil {
		return
	}
	ended, err := e.repo.ListEnded(ctx, st.ConversationId, 20)
	if err != nil || len(ended) == 0 {
		return
	}
	var maxHit int64
	for _, t := range ended {
		if t.TotalHitCount > maxHit {
			maxHit = t.TotalHitCount
		}
	}
	bestScore := 0.0
	var best *TopicDoc
	for _, t := range ended {
		sc := HistoryScore(t.TotalHitCount, t.LastHitAt, maxHit, e.cfg.HistoryDecayLambda)
		if sc >= e.cfg.HistoryMinScore && sc > bestScore {
			bestScore = sc
			copyDoc := *t
			best = &copyDoc
		}
	}
	if best == nil {
		return
	}
	emb, err := qwen.Embed(e.qwenCfg, best.Summary)
	if err != nil {
		return
	}
	st.HistoryBoost = &HistoryBoostState{
		TopicId:   best.TopicId,
		Name:      best.Name,
		Summary:   best.Summary,
		Embedding: emb,
		Score:     bestScore,
	}
}

func (e *Engine) loadState(ctx context.Context, ev RoundEvent) (*State, error) {
	st, err := e.store.Get(ctx, ev.ConversationId)
	if err != nil {
		return nil, err
	}
	if st != nil {
		return st, nil
	}
	st = &State{
		ConversationId: ev.ConversationId,
		UserId:         ev.UserId,
		Phase:          PhaseExplore,
		Candidates:     make([]CandidateState, 0),
	}
	if e.repo == nil {
		return st, nil
	}
	active, err := e.repo.FindActive(ctx, ev.ConversationId)
	if err != nil {
		return nil, err
	}
	if active != nil {
		emb, err := qwen.Embed(e.qwenCfg, active.Summary)
		if err != nil {
			return st, nil
		}
		st.Phase = PhaseFormal
		st.Formal = &FormalState{
			TopicId:        active.TopicId,
			Name:           active.Name,
			Summary:        active.Summary,
			Embedding:      emb,
			SummaryVersion: active.SummaryVersion,
		}
		return st, nil
	}
	e.refreshHistoryBoost(ctx, st)
	return st, nil
}

func (e *Engine) activeTopicId(st *State) string {
	if st.Formal != nil {
		return st.Formal.TopicId
	}
	return ""
}

// ApplySummaryRewrite 异步摘要重写完成后更新 Redis 与 Mongo。
func (e *Engine) ApplySummaryRewrite(ctx context.Context, task *SummaryRewriteTask) error {
	if task == nil {
		return nil
	}
	newSummary, err := RewriteTopicSummary(e.qwenCfg, task.OldSummary, task.RoundSummaries)
	if err != nil {
		return err
	}
	emb, err := qwen.Embed(e.qwenCfg, newSummary)
	if err != nil {
		return err
	}
	if err := e.store.ApplySummaryRewrite(ctx, task.ConversationId, task.TopicId, newSummary, emb, task.SummaryVersion); err != nil {
		return err
	}
	return e.repo.UpdateSummary(ctx, task.TopicId, newSummary, task.SummaryVersion+1)
}
