package topic

import (
	"encoding/json"
	"strings"

	"easy-chat/pkg/qwen"
)

// DisambiguateSameTopic LLM 灰区消歧：本轮是否仍属于当前正式主题。
func DisambiguateSameTopic(cfg qwen.Config, name, topicSummary, roundSummary string) (bool, error) {
	if strings.TrimSpace(roundSummary) == "" {
		return false, nil
	}
	system := "你是会话主题判定助手。只输出 JSON，不要其它文字。"
	user := strings.Join([]string{
		"当前主题名：" + name,
		"当前主题摘要：" + topicSummary,
		"本轮对话摘要：" + roundSummary,
		`请判断本轮是否仍属于当前主题。输出格式：{"same":true/false,"confidence":0.0-1.0}`,
	}, "\n")
	reply, err := qwen.Chat(cfg, system, user)
	if err != nil {
		return false, err
	}
	reply = strings.TrimSpace(reply)
	// 容错：截取 JSON 段
	if i := strings.Index(reply, "{"); i >= 0 {
		if j := strings.LastIndex(reply, "}"); j > i {
			reply = reply[i : j+1]
		}
	}
	var res struct {
		Same       bool    `json:"same"`
		Confidence float64 `json:"confidence"`
	}
	if err := json.Unmarshal([]byte(reply), &res); err != nil {
		return false, err
	}
	if !res.Same || res.Confidence < 0.6 {
		return false, nil
	}
	return true, nil
}

// RewriteTopicSummary 根据最近轮次摘要重写主题摘要。
func RewriteTopicSummary(cfg qwen.Config, oldSummary string, roundSummaries []string) (string, error) {
	if len(roundSummaries) == 0 {
		return oldSummary, nil
	}
	system := "你是会话主题摘要助手。根据对话摘要更新主题摘要，150字以内，保留关键实体，使用中文。"
	user := strings.Join([]string{
		"现有主题摘要：" + oldSummary,
		"最近对话摘要：",
		strings.Join(roundSummaries, "\n"),
		"请输出更新后的主题摘要，只输出摘要正文。",
	}, "\n")
	return qwen.Chat(cfg, system, user)
}

// GenerateInitialSummary 候选升格时生成首版正式摘要。
func GenerateInitialSummary(cfg qwen.Config, name string, roundSummaries []string) (string, error) {
	system := "你是会话主题摘要助手。根据对话摘要生成主题摘要，150字以内，使用中文。"
	user := strings.Join([]string{
		"主题名：" + name,
		"相关对话摘要：",
		strings.Join(roundSummaries, "\n"),
		"请输出主题摘要，只输出摘要正文。",
	}, "\n")
	summary, err := qwen.Chat(cfg, system, user)
	if err != nil || strings.TrimSpace(summary) == "" {
		if len(roundSummaries) > 0 {
			return roundSummaries[len(roundSummaries)-1], nil
		}
		return name, err
	}
	return strings.TrimSpace(summary), nil
}
