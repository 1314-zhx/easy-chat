package tools

// ToQwenToolDefinitions 将注册表工具转为通义千问 DashScope 原生 tools 格式。
func ToQwenToolDefinitions(reg *Registry) []map[string]interface{} {
	if reg == nil {
		return nil
	}
	raw := reg.ListMCPSchema()
	out := make([]map[string]interface{}, 0, len(raw))
	for _, t := range raw {
		out = append(out, map[string]interface{}{
			"type": "function",
			"function": map[string]interface{}{
				"name":        t["name"],
				"description": t["description"],
				"parameters":  t["parameters"],
			},
		})
	}
	return out
}

// GitHubTokenConfig 预留，未来 GitHub 工具扩展时注入 Token。
type GitHubTokenConfig struct {
	Token string
}
