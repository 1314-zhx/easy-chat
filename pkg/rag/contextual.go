package rag

import (
	"strings"

	"easy-chat/pkg/qwen"
)

// pronounReplacements 中文指代词 → 检索关键词映射。
// 用于数据增强，让llm更好理解上下文。
var pronounReplacements = map[string][]string{
	"那":    {"之前", "之前提到", "之前说的"},
	"这个":  {"当前", "现在", "这里"},
	"那个":  {"之前", "之前提到", "那个东西", "那件事"},
	"它":    {"它指代的对象", "这个", "那个"},
	"他们":  {"他们指代的对象", "那些人"},
	"这次":  {"这次", "本次"},
	"那次":  {"之前", "那次", "上次的"},
	"刚才":  {"最近", "刚刚", "刚才提到"},
	"之前":  {"之前", "之前说", "之前提到的"},
	"现在":  {"当前", "目前"},
	"这些":  {"这些", "这些内容"},
	"那些":  {"那些", "那些内容"},
	"这里":  {"这里", "这个地方"},
	"那里":  {"那里", "那个地方"},
	"此事":  {"这件事", "这个事情"},
	"那事":  {"那件事", "那个事情"},
	"上文":  {"上面", "前面", "前面提到"},
	"上述":  {"上面", "上面提到", "前面说"},
	"如此":  {"这样", "这样处理", "如此进行"},
	"同样":  {"同样", "相同方式", "一样处理"},
}

// NeedsResolution 判断消息是否含指代词，需要上下文消解。
func NeedsResolution(msg string) bool {
	if strings.TrimSpace(msg) == "" {
		return false
	}
	msg = strings.TrimSpace(msg)
	// 短消息（≤10字）含指代词概率高
	if len([]rune(msg)) <= 10 {
		return hasPronoun(msg)
	}
	return hasPronoun(msg)
}

// hasPronoun 检测消息中是否包含指代词。
// 遍历预定义的指代词列表（包括代词、指示词、时间指代词等）。
// 返回 true 表示消息可能需要上下文消解。
func hasPronoun(msg string) bool {
	pronouns := []string{
		"那", "这", "它", "他", "她", "他们", "她们",
		"这个", "那个", "这些", "那些",
		"刚才", "之前", "现在", "这里", "那里",
		"这次", "那次", "此事", "那事",
		"上文", "上述", "如此", "同样",
	}
	msg = strings.ToLower(msg)
	for _, p := range pronouns {
		if strings.Contains(msg, p) {
			return true
		}
	}
	return false
}

// ResolveQuery 根据指定策略消解含指代词的查询。
// 返回 (原始查询, 消解后查询)，strategy 可选 "expand"（规则展开）或 "llm"（LLM改写）。
// 若查询不含指代词，则消解后查询为空字符串。
func ResolveQuery(msg, historySnippet string, strategy string) (string, string) {
	// 如果没有指代词，就返回原文
	if !NeedsResolution(msg) {
		return msg, ""
	}
	switch strategy {
		// 如果使用规则策略，就使用规则展开
	case "expand":
		return msg, resolveByExpansion(msg)
		// 如果使用llm策略，就使用llm消解
	case "llm":
		return msg, resolveByLLM(msg, historySnippet)
	default:
		// 如果使用默认策略，就使用规则展开
		return msg, resolveByExpansion(msg)
	}
}

// resolveByExpansion 使用指代词替换表批量展开查询。
// 将消息中出现的指代词（如"那"、"这个"等）替换为对应的关键词列表（如"之前"、"之前提到"）。
// 适用于快速消解，对性能要求高的场景。
func resolveByExpansion(msg string) string {
	msg = strings.TrimSpace(msg)
	// 将整个表插入到原文后面，用空格分隔
	for pronoun, replacements := range pronounReplacements {
		if strings.Contains(msg, pronoun) {
			msg = msg + " " + strings.Join(replacements, " ")
		}
	}
	return msg
}

// resolveByLLM 调用 LLM 做深层消解（需要外部注入 Config）。
var globalQwenCfg qwen.Config

// SetQwenConfig 外部注入 qwen.Config，避免循环依赖。
func SetQwenConfig(cfg qwen.Config) {
	globalQwenCfg = cfg
}

// resolveByLLM 调用大语言模型进行深层指代消解。
// 将当前查询和历史对话片段一并发送给 LLM，让其理解上下文后改写为明确的检索查询。
// 适用于规则无法覆盖的复杂指代场景（如嵌套指代、隐含指代等）。
func resolveByLLM(msg, historySnippet string) string {
	// 空值检查，如果当前用户的提问（msg）
	// 或者截取到的历史对话片段（historySnippet）是空的，直接返回原话。
	if strings.TrimSpace(msg) == "" || strings.TrimSpace(historySnippet) == "" {
		return msg
	}
	if globalQwenCfg.ApiKey == "" {
		return msg
	}
	// 调用 RewriteQuery 函数进行指代消解
	resolved, err := RewriteQuery(globalQwenCfg, msg, historySnippet, "")
	if err != nil {
		return msg
	}
	return strings.TrimSpace(resolved)
}
