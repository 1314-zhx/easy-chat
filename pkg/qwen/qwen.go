package qwen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Config struct {
	ApiKey          string
	Model           string
	Endpoint        string
	EmbeddingModel  string
}

func DefaultConfig() Config {
	return Config{
		ApiKey:         "sk-afa9ebfbb94440daa487bd13f33b8380",
		Model:          "qwen-turbo",
		Endpoint:       "https://dashscope.aliyuncs.com",
		EmbeddingModel: defaultEmbeddingModel,
	}
}

// ChatMessage 多轮对话消息。
type ChatMessage struct {
	Role    string
	Content string
}

// Chat 基础聊天函数（不带工具）
func Chat(config Config, systemPrompt, userMessage string) (string, error) {
	res, err := ChatWithMessages(config, ChatRequest{
		SystemPrompt: systemPrompt,
		UserMessage:  userMessage,
	})
	if err != nil {
		return "", err
	}
	return res.Reply, nil
}

// ChatWithMessages 支持多轮历史、原生工具调用与工具结果。
func ChatWithMessages(config Config, req ChatRequest) (ChatResult, error) {
	return ChatWithTools(config, req)
}

// ChatRequest 增强版聊天请求
type ChatRequest struct {
	SystemPrompt string
	UserMessage  string
	History      []ChatMessage
	Tools        []map[string]interface{}
	ToolResult   string
}

// ChatWithTools 支持工具调用的聊天函数，返回结构化结果（含 tool_calls）。
func ChatWithTools(config Config, req ChatRequest) (ChatResult, error) {
	url := fmt.Sprintf("%s/api/v1/services/aigc/text-generation/generation", config.Endpoint)

	messages := []map[string]string{
		{"role": "system", "content": req.SystemPrompt},
	}

	if req.ToolResult != "" {
		messages = append(messages, map[string]string{
			"role":    "system",
			"content": fmt.Sprintf("【工具结果】\n%s\n请根据工具结果回答用户的问题。", req.ToolResult),
		})
	}

	for _, h := range req.History {
		if h.Content == "" {
			continue
		}
		messages = append(messages, map[string]string{
			"role":    h.Role,
			"content": h.Content,
		})
	}

	messages = append(messages, map[string]string{
		"role":    "user",
		"content": req.UserMessage,
	})

	// 构建请求体
	requestBody := map[string]interface{}{
		"model": config.Model,
		"input": map[string]interface{}{
			"messages": messages,
		},
		"parameters": map[string]interface{}{
			"max_tokens":  1500,
			"temperature": 0.7,
		},
	}

	// 如果有工具定义，添加到参数中
	if len(req.Tools) > 0 {
		requestBody["parameters"].(map[string]interface{})["tools"] = req.Tools
		// 关键修复：添加工具调用的强制参数
		requestBody["parameters"].(map[string]interface{})["tool_choice"] = "auto"
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return ChatResult{}, fmt.Errorf("marshal error: %v", err)
	}

	httpReq, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return ChatResult{}, fmt.Errorf("create request error: %v", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+config.ApiKey)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return ChatResult{}, fmt.Errorf("send error: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ChatResult{}, fmt.Errorf("read error: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return ChatResult{}, fmt.Errorf("API error: %d, %s", resp.StatusCode, string(body))
	}

	fmt.Printf("Qwen Raw Response: %s\n", string(body))
	return parseChatResult(body)
}

func IsAIMessage(recvId string) bool {
	return recvId == "ai_assistant"
}

func GetSystemPrompt() string {
	return `你是 Easy-Chat 的 AI 智能助手小易。回答要简洁友好，使用中文。`
}
