package topic

import (
	"math"
	"time"
)

// HistoryScore 历史主题优先级：命中数 × 时间衰减，归一化到 0~1。
func HistoryScore(totalHit int64, lastHitAt time.Time, maxHit int64, lambda float64) float64 {
	if totalHit <= 0 {
		return 0
	}
	hours := time.Since(lastHitAt).Hours()
	if hours < 0 {
		hours = 0
	}
	decay := math.Exp(-lambda * hours)
	raw := float64(totalHit) * decay
	if maxHit <= 0 {
		maxHit = totalHit
	}
	norm := raw / float64(maxHit)
	if norm > 1 {
		norm = 1
	}
	if norm < 0 {
		norm = 0
	}
	return norm
}

// CandidateEvictScore 候选淘汰分：命中率越高、越 recently 越应保留。
func CandidateEvictScore(c CandidateState) float64 {
	hours := time.Since(time.Unix(c.LastHitAt, 0)).Hours()
	if hours < 0 {
		hours = 0
	}
	return float64(c.HitCount) / (1 + hours)
}
