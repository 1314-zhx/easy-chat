package rag

import (
	"context"
	"easy-chat/apps/im/immodels"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"easy-chat/pkg/qwen"
)

var (
	reNameWo  = regexp.MustCompile(`(?i)我叫\s*([^\s，,。！？!?；;]{1,12})`)
	reNameShi = regexp.MustCompile(`(?i)我的名字是\s*([^\s，,。！？!?；;]{1,12})`)
	reCity    = regexp.MustCompile(`(?i)(?:我)?在\s*([^\s，,。！？!?]{2,8})(?:工作|生活|居住|上班)?`)
	rePrefer  = regexp.MustCompile(`(?i)我(?:喜欢|偏好|爱吃|爱喝)\s*([^\s，,。！？!?]{1,20})`)
)

// 误识别为姓名的疑问词（如「我叫什么」→ 什么）
var invalidNameTokens = map[string]struct{}{
	"什么": {}, "谁": {}, "哪个": {}, "哪": {}, "怎样": {}, "如何": {},
	"几": {}, "为什么": {}, "为何": {}, "多少": {},
}

// MemoryExtractor 从对话中提取并持久化用户记忆。
type MemoryExtractor struct {
	MemoryModel immodels.UserMemoryModel
	Qwen        qwen.Config
}

// NewMemoryExtractor 创建记忆提取器。
// MemoryModel 用于持久化用户记忆，Qwen 配置用于调用 LLM 进行复杂记忆提取。
func NewMemoryExtractor(m immodels.UserMemoryModel, cfg qwen.Config) *MemoryExtractor {
	return &MemoryExtractor{MemoryModel: m, Qwen: cfg}
}

// isValidNameToken 验证提取的姓名是否合法。
// 排除：空字符串、超长字符串（>12字符）、疑问词（什么/谁/哪个等）、疑问句中的人称。
func isValidNameToken(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" || len([]rune(name)) > 12 {
		return false
	}
	if _, ok := invalidNameTokens[name]; ok {
		return false
	}
	// 整句都是疑问句时不提取
	if strings.ContainsAny(name, "？?") {
		return false
	}
	return true
}

// IsPersonalInfoQuestion 用户在询问自己的姓名/偏好等。
func IsPersonalInfoQuestion(msg string) bool {
	msg = strings.TrimSpace(msg)
	patterns := []string{
		"我叫什么", "我是谁", "我的名字", "你还记得我吗", "我的名字是什么",
		"我爱吃什么", "我喜欢什么", "我的偏好", "你还记得我的名字",
		"你知道我叫什么", "你记得我叫什么",
	}
	for _, p := range patterns {
		if strings.Contains(msg, p) {
			return true
		}
	}
	return false
}

// ExtractFromRegex 使用正则表达式从文本中提取用户结构化信息。
// 支持提取：姓名（"我叫xxx" / "我的名字是xxx"）、城市（"我在xxx工作/生活"）、偏好（"我喜欢xxx" / "我爱吃xxx" / "我爱喝xxx"）。
// 自动过滤疑问句，避免将问句中的关键词误识别为用户信息。
func ExtractFromRegex(content string) (name, city, preference string) {
	// 疑问句本身不提取（避免「我叫什么」→ 姓名=什么）
	if IsPersonalInfoQuestion(content) {
		return "", "", ""
	}
	if m := reNameWo.FindStringSubmatch(content); len(m) > 1 && isValidNameToken(m[1]) {
		name = strings.TrimSpace(m[1])
	}
	if name == "" {
		if m := reNameShi.FindStringSubmatch(content); len(m) > 1 && isValidNameToken(m[1]) {
			name = strings.TrimSpace(m[1])
		}
	}
	if m := reCity.FindStringSubmatch(content); len(m) > 1 {
		city = m[1]
	}
	if m := rePrefer.FindStringSubmatch(content); len(m) > 1 {
		preference = m[1]
	}
	return
}

// MergeFromMessages 扫描用户消息并合并到 user_memory。
func (me *MemoryExtractor) MergeFromMessages(
	ctx context.Context,
	userId, conversationId string,
	logs []*immodels.ChatLog,
) (*immodels.UserMemory, error) {
	if userId == "" {
		return nil, nil
	}

	mem, _ := me.MemoryModel.FindOne(ctx, userId, conversationId)
	if mem == nil {
		mem = &immodels.UserMemory{
			UserId:         userId,
			ConversationId: conversationId,
		}
	}

	prefSet := make(map[string]struct{})
	for _, p := range mem.Preferences {
		prefSet[p] = struct{}{}
	}
	factSet := make(map[string]struct{})
	for _, f := range mem.Facts {
		factSet[f] = struct{}{}
	}

	changed := false
	for _, log := range logs {
		if log.SendId == "ai_assistant" {
			continue
		}
		name, city, pref := ExtractFromRegex(log.MsgContent)
		if name != "" && mem.Name != name {
			mem.Name = name
			changed = true
		}
		if city != "" && mem.City != city {
			mem.City = city
			changed = true
		}
		if pref != "" {
			if _, ok := prefSet[pref]; !ok {
				mem.Preferences = append(mem.Preferences, pref)
				prefSet[pref] = struct{}{}
				changed = true
			}
		}
	}

	if !changed && mem.Name == "" && mem.City == "" && len(mem.Preferences) == 0 {
		return mem, nil
	}
	if err := me.MemoryModel.Upsert(ctx, mem); err != nil {
		return mem, err
	}
	return mem, nil
}

// FormatUserMemory 格式化为 system prompt 片段。
func FormatUserMemory(mem *immodels.UserMemory) string {
	if mem == nil {
		return ""
	}
	var lines []string
	lines = append(lines, "【用户信息】")
	if mem.Name != "" {
		lines = append(lines, "• 姓名："+mem.Name)
	}
	if mem.City != "" {
		lines = append(lines, "• 城市："+mem.City)
	}
	for _, p := range mem.Preferences {
		lines = append(lines, "• 偏好："+p)
	}
	for _, f := range mem.Facts {
		lines = append(lines, "• "+f)
	}
	if mem.Summary != "" {
		lines = append(lines, "• 对话摘要："+mem.Summary)
	}
	if len(lines) <= 1 {
		return ""
	}
	return strings.Join(lines, "\n")
}

// ExtractWithLLM 使用 LLM 从近期对话提取结构化记忆（异步场景可调用）。
func (me *MemoryExtractor) ExtractWithLLM(
	ctx context.Context,
	userId, conversationId string,
	dialogue string,
) error {
	if strings.TrimSpace(dialogue) == "" {
		return nil
	}
	prompt := `从以下对话中提取用户稳定信息，仅输出 JSON，不要其它文字：
{"name":"","city":"","preferences":[],"facts":[]}
对话：
` + truncateRunes(dialogue, 2000)

	reply, err := qwen.Chat(me.Qwen, "你是信息提取助手，只输出合法 JSON。", prompt)
	if err != nil {
		return err
	}
	reply = strings.TrimSpace(reply)
	start := strings.Index(reply, "{")
	end := strings.LastIndex(reply, "}")
	if start < 0 || end <= start {
		return fmt.Errorf("invalid memory json")
	}
	reply = reply[start : end+1]

	var parsed struct {
		Name        string   `json:"name"`
		City        string   `json:"city"`
		Preferences []string `json:"preferences"`
		Facts       []string `json:"facts"`
	}
	if err := json.Unmarshal([]byte(reply), &parsed); err != nil {
		return err
	}

	mem, _ := me.MemoryModel.FindOne(ctx, userId, conversationId)
	if mem == nil {
		mem = &immodels.UserMemory{UserId: userId, ConversationId: conversationId}
	}
	if parsed.Name != "" {
		mem.Name = parsed.Name
	}
	if parsed.City != "" {
		mem.City = parsed.City
	}
	mem.Preferences = mergeUnique(mem.Preferences, parsed.Preferences)
	mem.Facts = mergeUnique(mem.Facts, parsed.Facts)
	return me.MemoryModel.Upsert(ctx, mem)
}

// mergeUnique 合并两个字符串切片并去重。
// base 中的元素保留顺序，add 中的新元素追加到末尾。
func mergeUnique(base, add []string) []string {
	set := make(map[string]struct{})
	for _, s := range base {
		set[s] = struct{}{}
	}
	for _, s := range add {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := set[s]; !ok {
			base = append(base, s)
			set[s] = struct{}{}
		}
	}
	return base
}

// truncateRunes 按 rune 截断字符串，支持中文字符。
// 用于限制 Prompt 长度，避免超出模型的 token 限制。
func truncateRunes(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max])
}

// RewriteQuery 将指代不明的用户问题改写为检索 query。
// recentSnippet: 历史对话摘要
// extraKeywords: 检索增强词（规则展开的结果）
func RewriteQuery(cfg qwen.Config, currentMessage, recentSnippet, extraKeywords string) (string, error) {
	if strings.TrimSpace(currentMessage) == "" {
		return currentMessage, nil
	}

	// 使用 System Prompt 定义任务，User Prompt 只包含实际输入
	// 示例使用与真实对话完全不同的水果场景避免混淆
	systemPrompt := "你是查询改写助手。根据上下文将用户问题中的指代词替换为具体事物。\n\n示例：\n上下文：用户:我喜欢香蕉\n问题：它多少钱\n答案：香蕉多少钱"

	userPrompt := fmt.Sprintf("上下文：\n%s\n\n问题：%s\n\n答案：", truncateRunes(recentSnippet, 400), currentMessage)

	result, err := qwen.Chat(cfg, systemPrompt, userPrompt)
	if err != nil {
		return currentMessage, err
	}
	// 清理返回结果
	result = strings.TrimSpace(result)
	result = strings.Trim(result, "\"'\"")
	// 如果返回为空或太短，使用原始查询
	if len(result) < 2 {
		return currentMessage, nil
	}
	return result, nil
}

// IsMostlyCJK 简单判断是否需要改写（过短或纯指代时改写）。
func NeedsQueryRewrite(msg string) bool {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return false
	}
	if len([]rune(msg)) <= 8 {
		return strings.Contains(msg, "那") || strings.Contains(msg, "这") ||
			strings.Contains(msg, "刚才") || strings.Contains(msg, "之前") || strings.Contains(msg, "它")
	}
	for _, r := range msg {
		if unicode.Is(unicode.Han, r) {
			return false
		}
	}
	return false
}
