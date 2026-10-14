/**
 * @Author: easy-chat
 * @Description: HTTP 压测工具 - 模拟业务请求，生成 Prometheus 格式指标
 */

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// 压测配置
	baseURL   = flag.String("url", "http://localhost:12112", "基础 URL")
	requests  = flag.Int("n", 10000, "总请求数")
	concurrency = flag.Int("c", 100, "并发数")
	duration  = flag.Duration("d", 0, "压测持续时间，0表示按请求数")

	// 指标
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadtest_http_requests_total",
			Help: "Total HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "loadtest_http_request_duration_milliseconds",
			Help:    "HTTP request duration in milliseconds",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500},
		},
		[]string{"method", "path"},
	)

	// 统计
	success int64
	failed  int64
)

func main() {
	flag.Parse()

	// 注册指标
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpRequestDuration)

	// 启动指标服务
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":9091", nil)
	}()

	fmt.Printf("=== Easy-Chat HTTP 压测工具 ===\n")
	fmt.Printf("目标地址: %s\n", *baseURL)
	fmt.Printf("总请求数: %d\n", *requests)
	fmt.Printf("并发数: %d\n", *concurrency)
	fmt.Printf("指标端口: :9091/metrics\n")
	fmt.Printf("===================================\n\n")

	start := time.Now()

	// 根据 duration 或 requests 模式运行
	if *duration > 0 {
		runDuration(*duration)
	} else {
		runByCount()
	}

	duration := time.Since(start)

	// 输出结果
	fmt.Printf("\n=== 压测结果 ===\n")
	fmt.Printf("总耗时: %v\n", duration)
	fmt.Printf("总请求数: %d\n", *requests)
	fmt.Printf("成功: %d\n", atomic.LoadInt64(&success))
	fmt.Printf("失败: %d\n", atomic.LoadInt64(&failed))
	fmt.Printf("QPS: %.2f\n", float64(*requests)/duration.Seconds())
	fmt.Printf("================\n")

	// 输出 Prometheus 格式指标
	fmt.Printf("\n=== Prometheus 指标 ===\n")
	fmt.Printf("curl http://localhost:9091/metrics\n")
}

// 按请求数运行
func runByCount() {
	var wg sync.WaitGroup
	requestsPerWorker := *requests / *concurrency

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			start := workerId * requestsPerWorker
			end := start + requestsPerWorker
			for j := start; j < end; j++ {
				runRequest(j)
			}
		}(i)
	}
	wg.Wait()
}

// 按持续时间运行
func runDuration(d time.Duration) {
	var wg sync.WaitGroup
	endTime := time.Now().Add(d)
	var count int64

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(endTime) {
				runRequest(int(atomic.AddInt64(&count, 1)))
			}
		}()
	}
	wg.Wait()
	*requests = int(atomic.LoadInt64(&count))
}

// 执行单个请求
func runRequest(seq int) {
	// 随机选择接口
	apis := []struct {
		method string
		path   string
		body   interface{}
	}{
		{"GET", "/api/conversation/list", nil},
		{"POST", "/api/message/send", map[string]interface{}{
			"send_id":    fmt.Sprintf("user_%d", seq%100),
			"recv_id":    fmt.Sprintf("user_%d", (seq+1)%100),
			"content":    "test message",
			"chat_type":  1,
		}},
		{"GET", "/api/message/history", nil},
		{"GET", "/api/user/info", nil},
	}

	api := apis[seq%len(apis)]
	start := time.Now()

	var status string
	var statusCode int

	reqBody, _ := json.Marshal(api.body)
	req, err := http.NewRequest(api.method, *baseURL+api.path, bytes.NewBuffer(reqBody))
	if err != nil {
		status = "error"
		statusCode = 0
		atomic.AddInt64(&failed, 1)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("token", fmt.Sprintf("test_token_%d", seq%100))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		status = "error"
		statusCode = 0
		atomic.AddInt64(&failed, 1)
		return
	}
	statusCode = resp.StatusCode
	resp.Body.Close()

	duration := time.Since(start).Milliseconds()

	if statusCode >= 200 && statusCode < 300 {
		status = "success"
		atomic.AddInt64(&success, 1)
	} else {
		status = fmt.Sprintf("status_%d", statusCode)
		atomic.AddInt64(&failed, 1)
	}

	// 记录指标
	httpRequestsTotal.WithLabelValues(api.method, api.path, status).Inc()
	httpRequestDuration.WithLabelValues(api.method, api.path).Observe(float64(duration))
}
