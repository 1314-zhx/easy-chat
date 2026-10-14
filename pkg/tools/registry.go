package tools

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// Def 工具定义（Chat proto / MCP tools/list 共用）。
type Def struct {
	Name        string
	Description string
	Parameters  map[string]string // 参数名 -> 描述
	Aliases     []string          // 兼容旧名称，如 weather_query
}

// Handler 工具执行函数。
type Handler func(ctx context.Context, args map[string]string, userHint string) (string, error)

type entry struct {
	def     Def
	handler Handler
}

// Registry 工具注册表。
type Registry struct {
	mu      sync.RWMutex
	byName  map[string]*entry
	aliases map[string]string
}

// NewRegistry 创建并注册内置工具。
func NewRegistry(opts Options) *Registry {
	r := &Registry{
		byName:  make(map[string]*entry),
		aliases: make(map[string]string),
	}
	r.registerBuiltin(opts)
	return r
}

// Options 注册选项。
type Options struct {
	// Deleted:	GitHubToken string
}

func (r *Registry) register(name string, def Def, h Handler) {
	def.Name = name
	e := &entry{def: def, handler: h}
	r.byName[name] = e
	for _, alias := range def.Aliases {
		r.aliases[alias] = name
	}
}

// List 返回所有工具定义。
func (r *Registry) List() []Def {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Def, 0, len(r.byName))
	for _, e := range r.byName {
		out = append(out, e.def)
	}
	return out
}

// Execute 执行工具（自动解析别名）。
func (r *Registry) Execute(ctx context.Context, name string, args map[string]string, userHint string) (string, error) {
	r.mu.RLock()
	canonical := name
	if target, ok := r.aliases[name]; ok {
		canonical = target
	}
	e, ok := r.byName[canonical]
	r.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("未知工具: %s", name)
	}
	return e.handler(ctx, args, userHint)
}

// ListMCPSchema 转为 MCP tools/list 的 JSON 结构。
func (r *Registry) ListMCPSchema() []map[string]interface{} {
	defs := r.List()
	out := make([]map[string]interface{}, 0, len(defs))
	for _, d := range defs {
		props := map[string]interface{}{}
		required := []string{}
		for k, desc := range d.Parameters {
			props[k] = map[string]interface{}{
				"type":        "string",
				"description": desc,
			}
			required = append(required, k)
		}
		schema := map[string]interface{}{
			"type":       "object",
			"properties": props,
		}
		if len(required) > 0 {
			schema["required"] = required
		}
		out = append(out, map[string]interface{}{
			"name":        d.Name,
			"description": d.Description,
			"parameters":  schema,
		})
	}
	return out
}

// NormalizeArgs 将 MCP 的 map[string]interface{} 转为 map[string]string。
func NormalizeArgs(arguments map[string]interface{}) map[string]string {
	if arguments == nil {
		return nil
	}
	out := make(map[string]string, len(arguments))
	for k, v := range arguments {
		out[k] = fmt.Sprint(v)
	}
	return out
}

// Default 全局默认注册表（懒加载）。
var defaultRegistry *Registry

func Default(opts Options) *Registry {
	if defaultRegistry == nil {
		defaultRegistry = NewRegistry(opts)
	}
	return defaultRegistry
}

// ResetDefault 测试或热更新配置时重置。
func ResetDefault() {
	defaultRegistry = nil
}

func joinNames(names []string) string {
	return strings.Join(names, "、")
}
