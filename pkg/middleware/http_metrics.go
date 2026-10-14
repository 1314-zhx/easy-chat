/**
 * @Author: easy-chat
 * @Description: Prometheus HTTP 中间件，记录接口响应延迟
 */

package middleware

import (
	"net/http"
	"strconv"
	"time"

	"easy-chat/pkg/metrics"
)

// PrometheusInterceptor Prometheus 埋点中间件
// 用于记录 HTTP 接口的响应延迟和请求总量
func PrometheusInterceptor(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// 使用自定义 ResponseWriter 捕获 status code
		writer := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// 处理请求
		h(writer, r)

		// 记录延迟
		duration := float64(time.Since(start).Milliseconds())

		// 获取路径
		path := r.URL.Path
		method := r.Method
		status := strconv.Itoa(writer.statusCode)

		// 埋点
		metrics.HTTPRequestLatency.WithLabelValues(path, method, status).Observe(duration)
		metrics.HTTPRequestTotal.WithLabelValues(path, method, status).Inc()
	}
}

// responseWriter 包装原始 ResponseWriter，捕获 status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
