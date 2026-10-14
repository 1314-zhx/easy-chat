package config

import (
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	Mongo MongoConfig
	Qwen  QwenConfig
	RAG   RAGConfig
	// Deleted:	Tools ToolsConfig
	MCP MCPConfig
}

// Deleted:// ToolsConfig 聊天侧工具（与 MCP.GitHub 可共用同一 Token）。
// Deleted:type ToolsConfig struct {
// Deleted:	GitHub ToolGitHubConfig `json:",optional"`
// Deleted:}
// Deleted:
// Deleted:type ToolGitHubConfig struct {
// Deleted:	Enabled bool   `json:",optional"`
// Deleted:	Token   string `json:",optional"`
// Deleted:}

type MongoConfig struct {
	Url string `json:",default=mongodb://root:easy-chat@127.0.0.1:27017"`
	Db  string `json:",default=easy-chat"`
}

type QwenConfig struct {
	ApiKey         string `json:",optional"`
	Model          string `json:",default=qwen-turbo"`
	Endpoint       string `json:",default=https://dashscope.aliyuncs.com"`
	EmbeddingModel string `json:",default=text-embedding-v3"`
}

type RAGConfig struct {
	Enabled              bool    `json:",optional,default=true"`
	TopK                 int     `json:",default=5"`
	RecentHistoryCount   int     `json:",default=5"`
	SimilarityThreshold  float64 `json:",optional,default=0.7"`
	SemanticSearchLimit  int     `json:",default=150"`
	MaxContextChars      int     `json:",default=6000"`
	QueryRewriteEnabled  bool    `json:",optional,default=true"`
	MemoryExtractEnabled bool    `json:",optional,default=true"`
}

type MCPConfig struct {
	Enabled          bool     `json:",default=true"`
	Version          string   `json:",default=1.0"`
	SupportedMethods []string `json:",optional"`
	// Deleted:	GitHub           MCPGitHubConfig
	Remote MCPRemoteConfig
}

// MCPRemoteConfig 子进程方式连接官方 MCP Server（如 github/github-mcp-server）。
type MCPRemoteConfig struct {
	Enabled bool              `json:",optional"`
	Command string            `json:",optional"`
	Args    []string          `json:",optional"`
	Env     map[string]string `json:",optional"`
}
