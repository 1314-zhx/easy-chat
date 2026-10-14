package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
)

type Message struct {
	ID       string  `json:"id"`
	Role     string  `json:"role"`
	Content  string  `json:"content"`
	AgeHours float64 `json:"age_hours"`
}

type Sample struct {
	ID            string    `json:"id"`
	Scenario      string    `json:"scenario"`
	Query         string    `json:"query"`
	GoldMessageID string    `json:"gold_message_id"`
	Messages      []Message `json:"messages"`
}

type Candidate struct {
	ID    string
	Score float64
}

func tokenize(s string) []string {
	s = strings.ToLower(s)
	repl := strings.NewReplacer(
		"，", " ", "。", " ", "！", " ", "？", " ", "：", " ", ":", " ", "；", " ", ";", " ", ",", " ", ".", " ", "\n", " ", "\t", " ",
	)
	s = repl.Replace(s)
	parts := strings.Fields(s)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func sim(a, b string) float64 {
	aTokens := tokenize(a)
	bTokens := tokenize(b)
	if len(aTokens) == 0 || len(bTokens) == 0 {
		return 0
	}
	set := make(map[string]int, len(aTokens))
	for _, t := range aTokens {
		set[t]++
	}
	hit := 0
	for _, t := range bTokens {
		if set[t] > 0 {
			hit++
		}
	}
	return float64(hit) / math.Sqrt(float64(len(aTokens)*len(bTokens)))
}

func rankSample(s Sample, useDecay bool) (rank int, ranked []Candidate) {
	cand := make([]Candidate, 0, len(s.Messages))
	for _, m := range s.Messages {
		score := sim(s.Query, m.Content)
		if useDecay {
			// 与项目中的 exp(-ageHours/24) 保持一致
			score *= math.Exp(-m.AgeHours / 24.0)
		}
		cand = append(cand, Candidate{ID: m.ID, Score: score})
	}
	sort.Slice(cand, func(i, j int) bool { return cand[i].Score > cand[j].Score })
	for i, c := range cand {
		if c.ID == s.GoldMessageID {
			return i + 1, cand
		}
	}
	return 0, cand
}

func loadSamples(path string) ([]Sample, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var samples []Sample
	return samples, json.Unmarshal(data, &samples)
}

func main() {
	samples, err := loadSamples("test/rag_eval_samples.json")
	if err != nil {
		panic(err)
	}
	var baseTop1, baseTop3, decayTop1, decayTop3 int
	var baseMRR, decayMRR float64
	for _, s := range samples {
		r1, baseRanked := rankSample(s, false)
		r2, decayRanked := rankSample(s, true)
		if r1 == 1 {
			baseTop1++
		}
		if r1 > 0 && r1 <= 3 {
			baseTop3++
		}
		if r1 > 0 {
			baseMRR += 1.0 / float64(r1)
		}
		if r2 == 1 {
			decayTop1++
		}
		if r2 > 0 && r2 <= 3 {
			decayTop3++
		}
		if r2 > 0 {
			decayMRR += 1.0 / float64(r2)
		}

		fmt.Printf("[%s] %s\n", s.ID, s.Scenario)
		fmt.Printf("  query: %s\n", s.Query)
		fmt.Printf("  baseline rank=%d top1=%s\n", r1, formatTop(baseRanked))
		fmt.Printf("  decay    rank=%d top1=%s\n", r2, formatTop(decayRanked))
	}
	n := float64(len(samples))
	if n == 0 {
		fmt.Println("no samples found")
		return
	}
	fmt.Printf("\nsummary\n")
	fmt.Printf("samples=%d\n", len(samples))
	fmt.Printf("baseline top1=%.2f%% top3=%.2f%% mrr=%.4f\n", 100*float64(baseTop1)/n, 100*float64(baseTop3)/n, baseMRR/n)
	fmt.Printf("decay    top1=%.2f%% top3=%.2f%% mrr=%.4f\n", 100*float64(decayTop1)/n, 100*float64(decayTop3)/n, decayMRR/n)
}

func formatTop(ranked []Candidate) string {
	limit := 3
	if len(ranked) < limit {
		limit = len(ranked)
	}
	parts := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		parts = append(parts, fmt.Sprintf("%s(%.3f)", ranked[i].ID, ranked[i].Score))
	}
	return strings.Join(parts, ", ")
}
