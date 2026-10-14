package qwen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const defaultEmbeddingModel = "text-embedding-v3"

// Embed 调用通义千问文本向量接口，返回单条文本的 embedding。
func Embed(cfg Config, text string) ([]float32, error) {
	vecs, err := EmbedBatch(cfg, []string{text})
	if err != nil {
		return nil, err
	}
	if len(vecs) == 0 {
		return nil, fmt.Errorf("empty embedding response")
	}
	return vecs[0], nil
}

// EmbedBatch 批量向量化。
// 接收配置和一组文本，返回对应的向量切片，如果出错则返回 error。
func EmbedBatch(cfg Config, texts []string) ([][]float32, error) {
	// 如果传入的文本切片为空，直接返回 nil，不做后续处理
	if len(texts) == 0 {
		return nil, nil
	}
	// 获取配置中的模型名称，如果没有指定，则使用默认模型
	model := cfg.EmbeddingModel
	if model == "" {
		model = defaultEmbeddingModel
	}

	// 拼接向量生成 API 的请求地址
	url := fmt.Sprintf("%s/api/v1/services/embeddings/text-embedding/text-embedding", cfg.Endpoint)
	// 构造请求体，包含模型名称和待处理的文本数组
	body := map[string]interface{}{
		"model": model,
		"input": map[string]interface{}{
			"texts": texts,
		},
	}
	// 将请求体序列化为 JSON 格式
	jsonData, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	// 创建一个新的 POST 请求
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	// 设置请求头：指定内容类型为 JSON，并携带 Bearer Token 进行鉴权
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.ApiKey)

	// 初始化 HTTP 客户端，并设置 30 秒的请求超时时间
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	// 确保函数退出前关闭响应体，防止内存泄漏
	defer resp.Body.Close()

	// 读取服务器返回的响应内容
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	// 检查 HTTP 状态码，如果不是 200 OK，则返回包含状态码和错误信息的 error
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embedding API error: %d, %s", resp.StatusCode, string(respBody))
	}

	// 定义一个匿名结构体来接收并解析 API 返回的 JSON 数据
	var result struct {
		Output struct {
			Embeddings []struct {
				Embedding []float64 `json:"embedding"` // 假设 API 返回的是 float64 精度的向量
			} `json:"embeddings"`
		} `json:"output"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parse embedding response: %w", err)
	}

	// 初始化输出切片，预分配好容量以提升性能
	out := make([][]float32, 0, len(result.Output.Embeddings))
	// 遍历解析出的向量结果
	for _, e := range result.Output.Embeddings {
		// 将 API 返回的 float64 向量逐元素转换为 float32
		vec := make([]float32, len(e.Embedding))
		for i, v := range e.Embedding {
			vec[i] = float32(v)
		}
		out = append(out, vec)
	}
	return out, nil
}