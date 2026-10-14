package toolcall

import (
	"encoding/json"
	"strings"
)

// Result 解析出的工具调用。
type Result struct {
	Name      string
	Arguments map[string]string
	OK        bool
}

// FromQwen 从通义原生 tool_calls 解析结果转换。
func FromQwen(name string, args map[string]string) Result {
	if name == "" {
		return Result{}
	}
	if args == nil {
		args = map[string]string{}
	}
	return Result{Name: name, Arguments: args, OK: true}
}

// Parse 从模型回复中解析工具调用，兼容多种 JSON 格式。
func Parse(reply string) Result {
	reply = strings.TrimSpace(reply)
	if reply == "" {
		return Result{}
	}

	// 通义原生 tool_calls 数组
	if strings.HasPrefix(reply, "[") {
		var calls []struct {
			Function struct {
				Name      string `json:"name"`
				Arguments string `json:"arguments"`
			} `json:"function"`
		}
		if err := json.Unmarshal([]byte(reply), &calls); err == nil && len(calls) > 0 {
			args := map[string]string{}
			if calls[0].Function.Arguments != "" {
				_ = json.Unmarshal([]byte(calls[0].Function.Arguments), &args)
			}
			return Result{Name: calls[0].Function.Name, Arguments: args, OK: true}
		}
	}

	start := strings.Index(reply, "{")
	end := strings.LastIndex(reply, "}")
	if start < 0 || end <= start {
		return Result{}
	}
	jsonStr := reply[start : end+1]

	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &raw); err != nil {
		return Result{}
	}

	name := firstString(raw, "action", "name", "function", "tool")
	args := extractArgsMap(raw)
	if name == "" {
		return Result{}
	}
	return Result{Name: name, Arguments: args, OK: true}
}

func firstString(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func extractArgsMap(raw map[string]interface{}) map[string]string {
	out := map[string]string{}
	for _, key := range []string{"args", "arguments", "parameters", "params"} {
		if v, ok := raw[key]; ok {
			switch t := v.(type) {
			case map[string]interface{}:
				for k, val := range t {
					out[k] = toString(val)
				}
				return out
			case map[string]string:
				return t
			case string:
				// arguments 可能是 JSON 字符串
				var nested map[string]string
				if json.Unmarshal([]byte(t), &nested) == nil {
					return nested
				}
			}
		}
	}
	return out
}

func toString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	default:
		b, _ := json.Marshal(t)
		return strings.TrimSpace(string(b))
	}
}

// ResolveCity 从工具参数或用户原文中解析城市名。
func ResolveCity(args map[string]string, userMessage string, supported []string) string {
	keys := []string{"city", "城市", "location", "地点", "参数", "name", "value"}
	for _, k := range keys {
		if v := strings.TrimSpace(args[k]); v != "" {
			if c := matchSupportedCity(v, supported); c != "" {
				return c
			}
		}
	}
	// 参数里只有一个非空值时（模型常用「参数」作 key）
	for _, v := range args {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if c := matchSupportedCity(v, supported); c != "" {
			return c
		}
	}
	return ExtractCityFromMessage(userMessage, supported)
}

func matchSupportedCity(name string, supported []string) string {
	name = strings.TrimSpace(name)
	name = strings.TrimSuffix(name, "市")
	name = strings.TrimSuffix(name, "省")
	for _, c := range supported {
		if name == c || strings.Contains(name, c) {
			return c
		}
	}
	return ""
}

// ExtractCityFromMessage 从用户消息中匹配支持的城市。
func ExtractCityFromMessage(msg string, supported []string) string {
	msg = strings.TrimSpace(msg)
	for _, c := range supported {
		if strings.Contains(msg, c) {
			return c
		}
	}
	return ""
}
