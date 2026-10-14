package logic

import (
	"context"
	"easy-chat/apps/ai/rpc/ai"
	"easy-chat/apps/ai/rpc/internal/svc"
	"easy-chat/pkg/qwen"
	"easy-chat/pkg/toolcall"
	"easy-chat/pkg/tools"
	"fmt"
	"strings"

	"github.com/zeromicro/go-zero/core/logx"
)

type ChatLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
	ragRetriever *RAGRetriever
}

func NewChatLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ChatLogic {
	return &ChatLogic{
		ctx:          ctx,
		svcCtx:       svcCtx,
		Logger:       logx.WithContext(ctx),
		ragRetriever: NewRAGRetriever(svcCtx),
	}
}

func (l *ChatLogic) Chat(in *ai.ChatRequest) (*ai.ChatResponse, error) {
	// 构造系统提示词，定义 AI 的身份、回答风格和行为规则
	systemPrompt := "你是 Easy-Chat 的 AI 助手小易。请使用中文回答，简洁友好。\n\n"
	systemPrompt += "【重要规则】\n"
	systemPrompt += "1. 如果提供了【用户信息】，请牢记并在回答时使用\n"
	systemPrompt += "2. 用户询问个人信息时，优先从【用户信息】中查找\n"
	systemPrompt += "3. 不要编造不存在的信息\n"
	systemPrompt += "4. 结合对话历史理解指代（如「刚才」「那个」）\n"
	systemPrompt += "5. 需要查天气、时间或 GitHub 信息时，请调用已注册的工具，不要编造数据\n"

	// history 用来保存从 RAG 检索出来的历史对话上下文
	var history []qwen.ChatMessage

	// 如果当前请求带有会话 ID，说明这是一个连续对话，不是全新的单轮问题
	if in.ConversationId != "" {
		// 走 RAG 检索：取回最近历史、语义相关内容、系统附加信息、用户记忆等
		retrieved, err := l.ragRetriever.Retrieve(
			l.ctx,
			in.ConversationId,
			in.Message,
			in.UserId,
			in.ExcludeSendTime,
		)
		if err != nil {
			// RAG 检索失败时记日志
			l.Logger.Errorf("RAG检索失败: %v", err)

			// 如果上游传了 HistoryContext，就把它拼进 systemPrompt 兜底
			if in.HistoryContext != "" {
				systemPrompt += "\n\n" + in.HistoryContext
			}
		} else if retrieved != nil {
			// 如果检索到了系统额外信息，把它拼进 systemPrompt
			if retrieved.SystemExtras != "" {
				systemPrompt += "\n\n" + retrieved.SystemExtras
			}

			// 把检索到的多轮历史赋给 history，后面一起喂给大模型
			history = retrieved.History

			// 如果开启了记忆抽取，并且有用户 ID，则异步抽取用户长期记忆
			if l.svcCtx.RuntimeConfig().RAG.MemoryExtractEnabled && in.UserId != "" {
				dialogue := formatDialogueForMemory(history, in.Message)
				l.ragRetriever.RefreshMemoryAsync(in.UserId, in.ConversationId, dialogue)
			}
		}
	} else if in.HistoryContext != "" {
		// 如果没有 conversationId，但上游传了历史上下文，也拼进 systemPrompt
		systemPrompt += "\n\n" + in.HistoryContext
	}

	// 获取 Qwen 的配置
	qwenCfg := l.svcCtx.QwenConfig()

	// 把当前注册的工具转换成 Qwen 可识别的工具定义
	qwenTools := tools.ToQwenToolDefinitions(l.svcCtx.ToolRegistry)

	// 调用大模型，传入：
	// 1. systemPrompt：系统提示词
	// 2. history：多轮历史
	// 3. in.Message：当前用户问题
	// 4. in.ToolResult：如果前一轮执行过工具，这里把工具结果带上
	// 5. qwenTools：可用工具列表
	chatRes, err := qwen.ChatWithMessages(qwenCfg, qwen.ChatRequest{
		SystemPrompt: systemPrompt,
		History:      history,
		UserMessage:  in.Message,
		ToolResult:   in.ToolResult,
		Tools:        qwenTools,
	})
	if err != nil {
		// 大模型调用失败，返回失败响应
		return &ai.ChatResponse{Success: false, ErrorMsg: err.Error()}, nil
	}

	// toolCall 用于承载模型返回的工具调用信息
	var toolCall ai.ToolCall

	// needAction 表示这次回复是不是需要业务层继续执行动作
	needAction := false

	// reply 是模型直接返回的自然语言回复
	reply := chatRes.Reply

	// 如果有回复，并且会话 ID 存在，则记录这一轮对话，推动话题追踪
	if reply != "" && in.ConversationId != "" {
		l.ragRetriever.TrackRound(in.ConversationId, in.Message, reply)
	}

	// 如果模型明确返回了 tool call 结构，说明它想先调用工具
	if chatRes.HasToolCall && chatRes.ToolCall != nil {
		// 把 Qwen 的工具调用格式转换成项目内部的 ToolCall
		parsed := toolcall.FromQwen(chatRes.ToolCall.Name, chatRes.ToolCall.Arguments)

		// 标记后续还需要执行动作
		needAction = true

		// 保存工具名和参数
		toolCall.Name = parsed.Name
		toolCall.Arguments = parsed.Arguments

		// 当前轮先不直接返回自然语言回复
		reply = ""
	} else if parsed := toolcall.Parse(reply); parsed.OK {
		// 兼容旧格式：
		// 如果模型把工具调用写成了 JSON 文本，也尝试从 reply 中解析
		needAction = true
		toolCall.Name = parsed.Name
		toolCall.Arguments = parsed.Arguments
		reply = ""
	}

	// 返回给上游 AI 回复结果
	// Reply：正常回复文本
	// ToolCall：如果需要工具调用，就返回工具调用信息
	// NeedAction：告诉上游是否还要继续执行工具逻辑
	return &ai.ChatResponse{
		Reply:      reply,
		Success:    true,
		ToolCall:   &toolCall,
		NeedAction: needAction,
	}, nil
}

// ToolsProto 供 task-mq 同步工具列表（可选，服务端以 ToolRegistry 为准）。
func (l *ChatLogic) ToolsProto() []*ai.Tool {
	defs := l.svcCtx.ToolRegistry.List()
	out := make([]*ai.Tool, 0, len(defs))
	for _, d := range defs {
		out = append(out, &ai.Tool{
			Name:        d.Name,
			Description: d.Description,
			Parameters:  d.Parameters,
		})
	}
	return out
}

func formatDialogueForMemory(history []qwen.ChatMessage, current string) string {
	var b strings.Builder
	for _, h := range history {
		role := "用户"
		if h.Role == "assistant" {
			role = "AI"
		}
		b.WriteString(role)
		b.WriteString(": ")
		b.WriteString(h.Content)
		b.WriteString("\n")
	}
	b.WriteString("用户: ")
	b.WriteString(current)
	return b.String()
}

// BuildToolResultLine 格式化工具结果注入第二轮对话。
func BuildToolResultLine(toolName, result string) string {
	return fmt.Sprintf("[%s] 结果: %s", toolName, result)
}
