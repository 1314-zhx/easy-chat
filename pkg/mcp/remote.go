package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

// RemoteBridge 通过 stdio 连接外部 MCP Server，代理 tools/list 与 tools/call。
type RemoteBridge struct {
	cfg    RemoteServerConfig
	mu     sync.Mutex
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	id     atomic.Int64
	tools  []map[string]interface{}
	prefix string // 远程工具名前缀，避免与本地冲突
}

func NewRemoteBridge(cfg RemoteServerConfig) *RemoteBridge {
	if !cfg.Enabled || cfg.Command == "" {
		return nil
	}
	return &RemoteBridge{cfg: cfg, prefix: "remote_"}
}

func (b *RemoteBridge) enabled() bool {
	return b != nil && b.cfg.Enabled && b.cfg.Command != ""
}

// ListTools 连接远程并拉取工具列表（带 remote_ 前缀）。
func (b *RemoteBridge) ListTools(ctx context.Context) ([]map[string]interface{}, error) {
	if !b.enabled() {
		return nil, nil
	}
	if err := b.ensureSession(ctx); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.tools) > 0 {
		return b.tools, nil
	}
	resp, err := b.call(ctx, "tools/list", map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	result, _ := resp["result"].(map[string]interface{})
	rawTools, _ := result["tools"].([]interface{})
	var out []map[string]interface{}
	for _, t := range rawTools {
		m, ok := t.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		if name != "" {
			m["name"] = b.prefix + name
			m["description"] = fmt.Sprintf("[GitHub MCP] %v", m["description"])
		}
		out = append(out, m)
	}
	b.tools = out
	return out, nil
}

// CallTool 调用远程工具（name 需带 remote_ 前缀）。
func (b *RemoteBridge) CallTool(ctx context.Context, name string, arguments map[string]interface{}) (string, error) {
	if !b.enabled() {
		return "", fmt.Errorf("remote MCP 未启用")
	}
	realName := name
	if len(name) > len(b.prefix) && name[:len(b.prefix)] == b.prefix {
		realName = name[len(b.prefix):]
	}
	if err := b.ensureSession(ctx); err != nil {
		return "", err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	resp, err := b.call(ctx, "tools/call", map[string]interface{}{
		"name":      realName,
		"arguments": arguments,
	})
	if err != nil {
		return "", err
	}
	resultMap, _ := resp["result"].(map[string]interface{})
	if resultMap == nil {
		bb, _ := json.Marshal(resp)
		return string(bb), nil
	}
	content, _ := resultMap["content"].([]interface{})
	var texts []string
	for _, c := range content {
		if block, ok := c.(map[string]interface{}); ok {
			if t, ok := block["text"].(string); ok {
				texts = append(texts, t)
			}
		}
	}
	if len(texts) == 0 {
		bb, _ := json.Marshal(resultMap)
		return string(bb), nil
	}
	return texts[0], nil
}

func (b *RemoteBridge) ensureSession(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stdin != nil {
		return nil
	}
	cmd := exec.CommandContext(ctx, b.cfg.Command, b.cfg.Args...)
	cmd.Env = append(cmd.Environ(), envMapToSlice(b.cfg.Env)...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("启动远程 MCP: %w", err)
	}
	b.cmd = cmd
	b.stdin = stdin
	b.stdout = bufio.NewReader(stdout)

	initParams := map[string]interface{}{
		"protocolVersion": "2024-11-05",
		"capabilities":  map[string]interface{}{},
		"clientInfo":      map[string]string{"name": "easy-chat", "version": "1.0"},
	}
	if _, err := b.callLocked(ctx, "initialize", initParams); err != nil {
		b.closeLocked()
		return err
	}
	_, _ = b.callLocked(ctx, "notifications/initialized", map[string]interface{}{})
	return nil
}

func (b *RemoteBridge) call(ctx context.Context, method string, params map[string]interface{}) (map[string]interface{}, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.callLocked(ctx, method, params)
}

func (b *RemoteBridge) callLocked(ctx context.Context, method string, params map[string]interface{}) (map[string]interface{}, error) {
	if b.stdin == nil {
		return nil, fmt.Errorf("MCP 会话未建立")
	}
	id := b.id.Add(1)
	req := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	}
	if method == "notifications/initialized" {
		req["id"] = nil
		delete(req, "id")
	}
	line, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	line = append(line, '\n')
	if _, err := b.stdin.Write(line); err != nil {
		return nil, err
	}
	if method == "notifications/initialized" {
		return map[string]interface{}{}, nil
	}

	type readResult struct {
		resp map[string]interface{}
		err  error
	}
	ch := make(chan readResult, 1)
	go func() {
		for {
			raw, err := b.stdout.ReadBytes('\n')
			if err != nil {
				ch <- readResult{err: err}
				return
			}
			var msg map[string]interface{}
			if json.Unmarshal(raw, &msg) != nil {
				continue
			}
			if msg["id"] == nil {
				continue // notification
			}
			switch v := msg["id"].(type) {
			case float64:
				if int64(v) != id {
					continue
				}
			case json.Number:
				n, _ := v.Int64()
				if n != id {
					continue
				}
			}
			ch <- readResult{resp: msg}
			return
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("远程 MCP 超时: %s", method)
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		if errObj, ok := r.resp["error"].(map[string]interface{}); ok {
			return nil, fmt.Errorf("remote mcp: %v", errObj["message"])
		}
		return r.resp, nil
	}
}

func (b *RemoteBridge) closeLocked() {
	if b.stdin != nil {
		_ = b.stdin.Close()
	}
	if b.cmd != nil && b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
	}
	b.stdin = nil
	b.stdout = nil
}

func envMapToSlice(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	return out
}

// IsRemoteTool 是否为远程代理工具名。
func (b *RemoteBridge) IsRemoteTool(name string) bool {
	return b.enabled() && len(name) > len(b.prefix) && name[:len(b.prefix)] == b.prefix
}
