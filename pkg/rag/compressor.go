package rag

import "easy-chat/pkg/qwen"

// Compressor 上下文压缩器：将检索结果压缩为精炼片段，控制 token 消耗。
type Compressor struct {
	maxCharsPerMsg int
}

// NewCompressor 创建压缩器，默认每条消息最多保留 200 字符。
func NewCompressor(maxCharsPerMsg int) *Compressor {
	if maxCharsPerMsg <= 0 {
		maxCharsPerMsg = 200
	}
	return &Compressor{maxCharsPerMsg: maxCharsPerMsg}
}

// CompressMessages 将消息列表压缩，保留关键语义。
func (c *Compressor) CompressMessages(msgs []ScoredMessage) []ScoredMessage {
	if len(msgs) == 0 {
		return nil
	}

	compressed := make([]ScoredMessage, 0, len(msgs))
	for _, m := range msgs {
		content := m.Content
		if len([]rune(content)) > c.maxCharsPerMsg {
			content = truncateWithEllipsis(content, c.maxCharsPerMsg)
			m.Content = content
		}
		compressed = append(compressed, m)
	}
	return compressed
}

// CompressHistory 将 ChatMessage 列表按字符数截断，优先保最近。
// CompressHistory 对聊天历史进行压缩，确保总字符数不超过 maxChars。
// 策略：优先保留最近的消息，对超长消息进行截断。
func (c *Compressor) CompressHistory(msgs []qwen.ChatMessage, maxChars int) []qwen.ChatMessage {
	// 1. 边界检查：如果消息为空或预算为0，直接返回 nil
	if len(msgs) == 0 || maxChars <= 0 {
		return nil
	}

	// 2. 倒序排列：从最后一条消息开始处理（因为最近的对话通常最重要）
	reversed := make([]qwen.ChatMessage, 0, len(msgs))
	for i := len(msgs) - 1; i >= 0; i-- {
		reversed = append(reversed, msgs[i])
	}

	var result []qwen.ChatMessage
	used := 0 // 记录当前已使用的字符总数

	// 3. 遍历并筛选消息
	for _, m := range reversed {
		content := m.Content

		// 4. 按字符阶段，如果某条消息太长，强制截断到预设的单条上限
		if len([]rune(content)) > c.maxCharsPerMsg {
			content = truncateWithEllipsis(content, c.maxCharsPerMsg)
			m.Content = content
		}

		// 5. 累加字符数
		used += len([]rune(content))

		// 6. 预算检查：如果加上这条消息超出了总预算，则停止添加
		if used > maxChars {
			break
		}

		result = append(result, m)
	}

	// 7. 恢复顺序：因为之前是倒序处理的，现在需要把结果再反转回来，保持时间正序
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result
}

// truncateWithEllipsis 在句子边界处截断，中文友好（按 rune 截断）。
// 优先在句号、问号、感叹号等句末标点处截断，若找不到则在任意位置截断。
func truncateWithEllipsis(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}

	// 句末/段落停顿标点符号列表（优先级从高到低）
	endPunctuation := []rune{'。', '！', '？', '.', '!', '?', '，', ','}

	// 在 max 范围内寻找最后一个句末标点
	for i := max - 1; i >= 0; i-- {
		for _, p := range endPunctuation {
			if runes[i] == p {
				// 找到句末标点，在其后截断（保留标点）
				return string(runes[:i+1]) + "…"
			}
		}
	}

	// 找不到句末标点，退而求其次在空格处截断（保留英文单词完整性）
	for i := max - 1; i >= 0; i-- {
		if runes[i] == ' ' || runes[i] == '\n' {
			return string(runes[:i]) + "…"
		}
	}

	// 找不到任何合适的断点，直接在 max 处截断
	return string(runes[:max-2]) + "…"
}
