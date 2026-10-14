package topic

import (
	"testing"
	"time"
)

func TestCosineSimilarity(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{1, 0, 0}
	if got := CosineSimilarity(a, b); got < 0.99 {
		t.Fatalf("expected ~1, got %v", got)
	}
	c := []float32{0, 1, 0}
	if got := CosineSimilarity(a, c); got > 0.01 {
		t.Fatalf("expected ~0, got %v", got)
	}
}

func TestBuildRoundSummary(t *testing.T) {
	s := BuildRoundSummary("你好", "你好，我是小易")
	if s == "" {
		t.Fatal("expected non-empty summary")
	}
}

func TestHistoryScoreMinGate(t *testing.T) {
	old := time.Now().Add(-200 * time.Hour)
	sc := HistoryScore(1, old, 10, 0.03)
	if sc >= 0.3 {
		t.Fatalf("old low-hit topic should score below 0.3, got %v", sc)
	}
}
