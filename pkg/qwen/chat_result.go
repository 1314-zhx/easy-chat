package qwen

import (
	"encoding/json"
	"strings"
)

// ToolCallPayload 模型发起的工具调用。
type ToolCallPayload struct {
	Name      string
	Arguments map[string]string
}

// ChatResult 聊天 API 结构化结果。
type ChatResult struct {
	Reply       string
	ToolCall    *ToolCallPayload
	HasToolCall bool
}

func parseChatResult(body []byte) (ChatResult, error) {
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return ChatResult{}, err
	}

	output, _ := result["output"].(map[string]interface{})
	if output == nil {
		return ChatResult{}, errInvalidFormat(string(body))
	}

	if text, ok := output["text"].(string); ok && strings.TrimSpace(text) != "" {
		return ChatResult{Reply: strings.TrimSpace(text)}, nil
	}

	choices, _ := output["choices"].([]interface{})
	if len(choices) == 0 {
		return ChatResult{}, errInvalidFormat(string(body))
	}
	choice, _ := choices[0].(map[string]interface{})
	message, _ := choice["message"].(map[string]interface{})
	if message == nil {
		return ChatResult{}, errInvalidFormat(string(body))
	}

	if content, ok := message["content"].(string); ok && strings.TrimSpace(content) != "" {
		return ChatResult{Reply: strings.TrimSpace(content)}, nil
	}

	if tc := parseToolCallsFromMessage(message); tc != nil {
		return ChatResult{ToolCall: tc, HasToolCall: true}, nil
	}

	return ChatResult{}, errInvalidFormat(string(body))
}

func parseToolCallsFromMessage(message map[string]interface{}) *ToolCallPayload {
	raw, ok := message["tool_calls"]
	if !ok {
		return nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var calls []struct {
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
	}
	if err := json.Unmarshal(b, &calls); err != nil || len(calls) == 0 {
		return nil
	}
	fn := calls[0].Function
	args := map[string]string{}
	if fn.Arguments != "" {
		var obj map[string]interface{}
		if json.Unmarshal([]byte(fn.Arguments), &obj) == nil {
			for k, v := range obj {
				args[k] = stringifyArg(v)
			}
		}
	}
	return &ToolCallPayload{Name: fn.Name, Arguments: args}
}

func stringifyArg(v interface{}) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case nil:
		return ""
	default:
		b, _ := json.Marshal(t)
		return strings.TrimSpace(string(b))
	}
}

type formatError string

func (e formatError) Error() string { return string(e) }

func errInvalidFormat(body string) error {
	return formatError("invalid qwen response format: " + body)
}
