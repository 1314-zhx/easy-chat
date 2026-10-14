// NEW_FILE_CODE
package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"easy-chat/pkg/qwen"
	"easy-chat/pkg/tools"
)

// MCPMessage 表示MCP消息结构
type MCPMessage struct {
	ID     string                 `json:"id"`
	Type   string                 `json:"type"` // "request", "response", "event"
	Method string                 `json:"method"`
	Params map[string]interface{} `json:"params,omitempty"`
	Result interface{}            `json:"result,omitempty"`
	Error  *MCPError              `json:"error,omitempty"`
}

// MCPError 表示MCP错误
type MCPError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// MCPServer MCP服务器接口
type MCPServer interface {
	HandleRequest(ctx context.Context, msg *MCPMessage) (*MCPMessage, error)
	RegisterMethod(method string, handler MCPHandler)
}

// MCPHandler MCP方法处理器
type MCPHandler func(ctx context.Context, params map[string]interface{}) (interface{}, error)

// DefaultMCPServer 默认MCP服务器实现
type DefaultMCPServer struct {
	handlers map[string]MCPHandler
	config   qwen.Config
	toolReg  *tools.Registry
	remote   *RemoteBridge
}

// NewDefaultMCPServer 创建默认MCP服务器
func NewDefaultMCPServer(qwenCfg qwen.Config, opts ServerOptions) *DefaultMCPServer {
	server := &DefaultMCPServer{
		handlers: make(map[string]MCPHandler),
		config:   qwenCfg,
		toolReg:  tools.NewRegistry(tools.Options{}),
		remote:   NewRemoteBridge(opts.Remote),
	}
	server.registerDefaultMethods()
	return server
}

// registerDefaultMethods 注册默认的MCP方法
func (s *DefaultMCPServer) registerDefaultMethods() {
	// 注册聊天方法
	s.RegisterMethod("chat/completion", s.handleChatCompletion)

	// 可以注册更多MCP标准方法
	s.RegisterMethod("tools/list", s.handleToolsList)
	s.RegisterMethod("tools/call", s.handleToolCall)
}

// RegisterMethod 注册MCP方法处理器
func (s *DefaultMCPServer) RegisterMethod(method string, handler MCPHandler) {
	s.handlers[method] = handler
}

// HandleRequest 处理MCP请求
func (s *DefaultMCPServer) HandleRequest(ctx context.Context, msg *MCPMessage) (*MCPMessage, error) {
	handler, exists := s.handlers[msg.Method]
	if !exists {
		return &MCPMessage{
			ID:   msg.ID,
			Type: "response",
			Error: &MCPError{
				Code:    -32601,
				Message: fmt.Sprintf("Method not found: %s", msg.Method),
			},
		}, nil
	}

	result, err := handler(ctx, msg.Params)
	if err != nil {
		return &MCPMessage{
			ID:   msg.ID,
			Type: "response",
			Error: &MCPError{
				Code:    -32000,
				Message: err.Error(),
			},
		}, nil
	}

	return &MCPMessage{
		ID:     msg.ID,
		Type:   "response",
		Result: result,
	}, nil
}

// handleChatCompletion 处理聊天完成（原生 Qwen 工具 + 本地/远程执行）
func (s *DefaultMCPServer) handleChatCompletion(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	model, _ := params["model"].(string)
	messages, _ := params["messages"].([]interface{})
	if len(messages) == 0 {
		return nil, fmt.Errorf("messages array is required")
	}

	var systemPrompt string
	var userMessage string
	var history []qwen.ChatMessage

	for _, msg := range messages {
		m, ok := msg.(map[string]interface{})
		if !ok {
			continue
		}
		role, _ := m["role"].(string)
		content, _ := m["content"].(string)
		switch role {
		case "system":
			systemPrompt = content
		case "user":
			if userMessage == "" {
				userMessage = content
			} else {
				history = append(history, qwen.ChatMessage{Role: "user", Content: content})
			}
		case "assistant":
			history = append(history, qwen.ChatMessage{Role: "assistant", Content: content})
		}
	}
	if userMessage == "" {
		return nil, fmt.Errorf("user message is required")
	}
	if systemPrompt == "" {
		systemPrompt = qwen.GetSystemPrompt()
	}

	qwenTools := tools.ToQwenToolDefinitions(s.toolReg)
	chatRes, err := qwen.ChatWithMessages(s.config, qwen.ChatRequest{
		SystemPrompt: systemPrompt,
		History:      history,
		UserMessage:  userMessage,
		Tools:        qwenTools,
	})
	if err != nil {
		return nil, err
	}

	reply := chatRes.Reply
	finishReason := "stop"

	if chatRes.HasToolCall && chatRes.ToolCall != nil {
		tc := chatRes.ToolCall
		var result string
		var execErr error
		if s.remote != nil && s.remote.IsRemoteTool(tc.Name) {
			args := make(map[string]interface{})
			for k, v := range tc.Arguments {
				args[k] = v
			}
			result, execErr = s.remote.CallTool(ctx, tc.Name, args)
		} else {
			result, execErr = s.toolReg.Execute(ctx, tc.Name, tc.Arguments, userMessage)
		}
		if execErr != nil {
			result = execErr.Error()
		}
		second, err := qwen.ChatWithMessages(s.config, qwen.ChatRequest{
			SystemPrompt: systemPrompt,
			History:      history,
			UserMessage:  userMessage,
			ToolResult:   fmt.Sprintf("[%s] 结果: %s", tc.Name, result),
			Tools:        qwenTools,
		})
		if err == nil {
			reply = second.Reply
		} else {
			reply = result
		}
	}

	return map[string]interface{}{
		"id":      fmt.Sprintf("chatcmpl-%d", time.Now().Unix()),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []map[string]interface{}{
			{
				"index": 0,
				"message": map[string]string{
					"role":    "assistant",
					"content": reply,
				},
				"finish_reason": finishReason,
			},
		},
		"usage": map[string]interface{}{
			"prompt_tokens":     len(strings.Split(systemPrompt+" "+userMessage, " ")),
			"completion_tokens": len(strings.Split(reply, " ")),
			"total_tokens":      len(strings.Split(systemPrompt+" "+userMessage+" "+reply, " ")),
		},
	}, nil
}

// handleToolsList 处理工具列表请求（本地工具 + 可选远程 GitHub MCP Server）
func (s *DefaultMCPServer) handleToolsList(ctx context.Context, _ map[string]interface{}) (interface{}, error) {
	list := s.toolReg.ListMCPSchema()
	if s.remote != nil {
		remoteTools, err := s.remote.ListTools(ctx)
		if err == nil {
			list = append(list, remoteTools...)
		}
	}
	return map[string]interface{}{"tools": list}, nil
}

// handleToolCall 处理工具调用请求
func (s *DefaultMCPServer) handleToolCall(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	toolName, _ := params["name"].(string)
	arguments, _ := params["arguments"].(map[string]interface{})
	userHint, _ := params["user_hint"].(string)

	if s.remote != nil && s.remote.IsRemoteTool(toolName) {
		text, err := s.remote.CallTool(ctx, toolName, arguments)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{"result": text}, nil
	}

	result, err := s.toolReg.Execute(ctx, toolName, tools.NormalizeArgs(arguments), userHint)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"result": result}, nil
}

// ParseMCPMessage 解析MCP消息
func ParseMCPMessage(data []byte) (*MCPMessage, error) {
	var msg MCPMessage
	err := json.Unmarshal(data, &msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

// SerializeMCPMessage 序列化MCP消息
func SerializeMCPMessage(msg *MCPMessage) ([]byte, error) {
	return json.Marshal(msg)
}
