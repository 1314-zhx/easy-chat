package topic

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

const maxRoundSummaryRunes = 200

// BuildRoundSummary 将一轮用户问+AI答压成短摘要（不存全文）。
func BuildRoundSummary(userContent, aiContent string) string {
	user := trimSummary(userContent, 80)
	ai := trimSummary(aiContent, 120)
	if user == "" && ai == "" {
		return ""
	}
	if user == "" {
		return "AI: " + ai
	}
	if ai == "" {
		return "用户: " + user
	}
	return fmt.Sprintf("用户: %s | AI: %s", user, ai)
}

func trimSummary(s string, maxRunes int) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "\n", " ")
	if maxRunes <= 0 {
		return s
	}
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	runes := []rune(s)
	return string(runes[:maxRunes]) + "..."
}

// CandidateNameFromSummary 从轮次摘要生成候选主题名。
func CandidateNameFromSummary(summary string) string {
	s := strings.TrimSpace(summary)
	if s == "" {
		return "未命名主题"
	}
	if utf8.RuneCountInString(s) <= 24 {
		return s
	}
	return string([]rune(s)[:24]) + "..."
}

// MergeCandidateSummary 合并最近几轮摘要为候选摘要。
func MergeCandidateSummary(rounds []string) string {
	if len(rounds) == 0 {
		return ""
	}
	if len(rounds) == 1 {
		return rounds[0]
	}
	return strings.Join(rounds, "；")
}

func appendFIFO(buf []string, item string, max int) []string {
	if item == "" {
		return buf
	}
	buf = append(buf, item)
	if max <= 0 {
		max = 20
	}
	if len(buf) > max {
		buf = buf[len(buf)-max:]
	}
	return buf
}
