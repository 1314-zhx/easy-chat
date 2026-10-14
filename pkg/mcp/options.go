package mcp

// ServerOptions MCP 服务配置。
type ServerOptions struct {
	Remote RemoteServerConfig
}

// RemoteServerConfig 外部 MCP Server（如官方 github-mcp-server）子进程配置。
type RemoteServerConfig struct {
	Enabled bool
	Command string   `json:",optional"`
	Args    []string `json:",optional"`
	Env     map[string]string
}
