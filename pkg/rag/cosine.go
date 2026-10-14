package rag

import "math"

// CosineSimilarity 计算两个向量的余弦相似度。
func CosineSimilarity(a, b []float32) float64 {
	if len(a) == 0 || len(b) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		af := float64(a[i])
		bf := float64(b[i])
		dot += af * bf
		normA += af * af
		normB += bf * bf
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

// Normalize 向量归一化（返回 L2 范数为 1 的向量）
// 归一化后余弦相似度等价于点积，可大幅减少计算量
func Normalize(v []float32) []float32 {
	if len(v) == 0 {
		return v
	}
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	norm := math.Sqrt(sum)
	if norm == 0 {
		return v
	}
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = float32(float64(x) / norm)
	}
	return out
}

// DotSimilarity 计算两个向量的点积（要求向量已归一化）
// 归一化向量的点积 = 余弦相似度，计算量减少 50%
func DotSimilarity(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	// 向量已归一化，余弦相似度 = 点积
	// 理论值为 [-1, 1]，但实际可能因浮点误差超出范围
	if sum > 1.0 {
		return 1.0
	}
	if sum < -1.0 {
		return -1.0
	}
	return sum
}
