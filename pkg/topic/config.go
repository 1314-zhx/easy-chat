package topic

import "time"

// Config 主题状态机配置。
type Config struct {
	FormalHitThreshold    float64
	FormalGrayLow         float64
	ExploreHitThreshold   float64
	MissLimit             int
	PromoteHitLimit       int
	MaxCandidates         int
	RoundSummaryBufferSize int
	SummaryRewriteEvery   int
	HistoryMinScore       float64
	HistoryRestoreSim     float64
	HistoryDecayLambda    float64
	RedisTTL              time.Duration
}

// DefaultConfig 默认配置。
func DefaultConfig() Config {
	return Config{
		FormalHitThreshold:     0.75,
		FormalGrayLow:          0.60,
		ExploreHitThreshold:    0.70,
		MissLimit:              3,
		PromoteHitLimit:        3,
		MaxCandidates:          5,
		RoundSummaryBufferSize: 20,
		SummaryRewriteEvery:    10,
		HistoryMinScore:        0.3,
		HistoryRestoreSim:      0.70,
		HistoryDecayLambda:     0.03,
		RedisTTL:               7 * 24 * time.Hour,
	}
}
