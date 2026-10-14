// NEW_FILE_CODE
package logic

import (
	"context"
	"easy-chat/apps/ai/rpc/ai"
	"easy-chat/apps/ai/rpc/internal/svc"
	"easy-chat/pkg/mcp"

	"github.com/zeromicro/go-zero/core/logx"
)

type MCPLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
	mcpServer *mcp.DefaultMCPServer
}

func NewMCPLogic(ctx context.Context, svcCtx *svc.ServiceContext) *MCPLogic {
	cfg := svcCtx.Config.MCP
	return &MCPLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
		mcpServer: mcp.NewDefaultMCPServer(svcCtx.QwenConfig(), mcp.ServerOptions{
			Remote: mcp.RemoteServerConfig{
				Enabled: cfg.Remote.Enabled,
				Command: cfg.Remote.Command,
				Args:    cfg.Remote.Args,
				Env:     cfg.Remote.Env,
			},
		}),
	}
}

// HandleMCPRequest 处理MCP请求
func (l *MCPLogic) HandleMCPRequest(request *ai.MCPRequest) (*ai.MCPResponse, error) {
	l.Infof("收到MCP请求: Method=%s", request.Method)

	// 解析MCP消息
	mcpMsg, err := mcp.ParseMCPMessage([]byte(request.Data))
	if err != nil {
		return &ai.MCPResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	// 处理MCP请求
	response, err := l.mcpServer.HandleRequest(l.ctx, mcpMsg)
	if err != nil {
		return &ai.MCPResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	// 序列化响应
	data, err := mcp.SerializeMCPMessage(response)
	if err != nil {
		return &ai.MCPResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &ai.MCPResponse{
		Success: true,
		Data:    string(data),
	}, nil
}
