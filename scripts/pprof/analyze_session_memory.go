/**
 * pprof 内存分析脚本 - 分析会话缓存内存占用
 * 
 * 使用方法:
 *   1. 确保 im.ws 服务已启用 pprof (已添加到 im.go)
 *   2. 运行压测增加连接: k6 run scripts/loadtest/ws_loadtest.js
 *   3. 运行本脚本: go run scripts/pprof/analyze_session_memory.go
 */

package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"time"
)

var (
	pprofAddr = flag.String("addr", "127.0.0.1:6060", "pprof server address")
	outputDir = flag.String("o", "./pprof_output", "output directory")
	interval  = flag.Duration("i", 5*time.Second, "sampling interval")
	samples   = flag.Int("n", 5, "number of samples to collect")
)

func main() {
	flag.Parse()

	// 创建输出目录
	os.MkdirAll(*outputDir, 0755)

	fmt.Println("=" + string(make([]byte, 60)) + "=")
	fmt.Println("Session Cache Memory Analysis with pprof")
	fmt.Println("=" + string(make([]byte, 60)) + "=")
	fmt.Printf("pprof server: %s\n", *pprofAddr)
	fmt.Printf("Output dir: %s\n", *outputDir)
	fmt.Printf("Samples: %d (interval: %s)\n\n", *samples, *interval)

	// 检查 pprof 服务是否可用
	if !checkPprofServer() {
		fmt.Println("ERROR: pprof server not reachable. Make sure im.ws is running with pprof enabled.")
		fmt.Println("Add to im.go:")
		fmt.Println("  import _ \"net/http/pprof\"")
		fmt.Println("  go http.ListenAndServe(\":6060\", nil)")
		os.Exit(1)
	}

	// 1. 收集堆内存快照
	fmt.Println("[1/4] Collecting heap snapshot...")
	collectHeap()

	// 2. 收集 goroutine profile
	fmt.Println("[2/4] Collecting goroutine profile...")
	collectGoroutine()

	// 3. 收集对象分配采样
	fmt.Println("[3/4] Collecting allocation profile...")
	collectAlloc()

	// 4. 生成火焰图
	fmt.Println("[4/4] Generating flame graph...")
	generateFlameGraph()

	// 5. 生成内存分析报告
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("Analysis Complete!")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("\nView reports:")
	fmt.Printf("  - Heap:   %s/heap.html\n", *outputDir)
	fmt.Printf("  - Flame:  %s/flamegraph.html\n", *outputDir)
	fmt.Printf("  - Text:   %s/heap.txt\n", *outputDir)

	// 打印关键指标
	fmt.Println("\n" + strings.Repeat("-", 60))
	printKeyMetrics()
}

// 1. 堆内存分析
func collectHeap() {
	// 获取 heap profile
	url := fmt.Sprintf("http://%s/debug/pprof/heap", *pprofAddr)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
		return
	}
	defer resp.Body.Close()

	// 保存原始 profile
	out, _ := os.Create(fmt.Sprintf("%s/heap.prof", *outputDir))
	io.Copy(out, resp.Body)
	out.Close()

	// 生成文本报告
	cmd := exec.Command("go", "tool", "pprof", "-text", "-inuse_space",
		fmt.Sprintf("http://%s/debug/pprof/heap", *pprofAddr))
	outFile, _ := os.Create(fmt.Sprintf("%s/heap.txt", *outputDir))
	cmd.Stdout = outFile
	cmd.Stderr = os.Stderr
	cmd.Run()
	outFile.Close()

	fmt.Println("  Saved: heap.prof, heap.txt")
}

// 2. Goroutine 分析
func collectGoroutine() {
	url := fmt.Sprintf("http://%s/debug/pprof/goroutine", *pprofAddr)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
		return
	}
	defer resp.Body.Close()

	out, _ := os.Create(fmt.Sprintf("%s/goroutine.prof", *outputDir))
	io.Copy(out, resp.Body)
	out.Close()

	// 文本分析
	cmd := exec.Command("go", "tool", "pprof", "-text",
		fmt.Sprintf("http://%s/debug/pprof/goroutine", *pprofAddr))
	outFile, _ := os.Create(fmt.Sprintf("%s/goroutine.txt", *outputDir))
	cmd.Stdout = outFile
	cmd.Stderr = os.Stderr
	cmd.Run()
	outFile.Close()

	fmt.Println("  Saved: goroutine.prof, goroutine.txt")
}

// 3. 分配分析
func collectAlloc() {
	url := fmt.Sprintf("http://%s/debug/pprof/alloc", *pprofAddr)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
		return
	}
	defer resp.Body.Close()

	out, _ := os.Create(fmt.Sprintf("%s/alloc.prof", *outputDir))
	io.Copy(out, resp.Body)
	out.Close()

	cmd := exec.Command("go", "tool", "pprof", "-text", "-inuse_space",
		fmt.Sprintf("http://%s/debug/pprof/alloc", *pprofAddr))
	outFile, _ := os.Create(fmt.Sprintf("%s/alloc.txt", *outputDir))
	cmd.Stdout = outFile
	cmd.Stderr = os.Stderr
	cmd.Run()
	outFile.Close()

	fmt.Println("  Saved: alloc.prof, alloc.txt")
}

// 4. 生成火焰图 (需要安装 go-torch 或火焰图脚本)
func generateFlameGraph() {
	// 下载并生成 SVG 火焰图
	cmd := exec.Command("go", "tool", "pprof", "-svg",
		"-output", fmt.Sprintf("%s/heap.svg", *outputDir),
		fmt.Sprintf("http://%s/debug/pprof/heap", *pprofAddr))
	cmd.Run()

	// 如果系统有 flamegraph 脚本
	if _, err := exec.LookPath("go-torch"); err == nil {
		cmd := exec.Command("go-torch", "-f", fmt.Sprintf("%s/flamegraph.html", *outputDir),
			fmt.Sprintf("http://%s/debug/pprof/heap", *pprofAddr))
		cmd.Run()
	} else {
		// 手动创建简单的 HTML 报告
		createHTMLReport()
	}
}

// 创建 HTML 报告
func createHTMLReport() {
	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head><title>Session Cache Memory Analysis</title></head>
<body>
<h1>Session Cache Memory Analysis</h1>
<h2>Heap SVG</h2>
<img src="heap.svg" alt="Heap Profile"/>
<h2>Key Metrics</h2>
<pre>%s</pre>
</body>
</html>`, getMetricsSummary())

	f, _ := os.Create(fmt.Sprintf("%s/heap.html", *outputDir))
	f.WriteString(html)
	f.Close()
}

// 检查 pprof 服务
func checkPprofServer() bool {
	resp, err := http.Get(fmt.Sprintf("http://%s/debug/pprof/", *pprofAddr))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

// 打印关键指标
func printKeyMetrics() {
	// 获取 runtime metrics
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Println("Go Runtime Memory Stats:")
	fmt.Printf("  Alloc:   %s\n", formatBytes(int64(m.Alloc)))
	fmt.Printf("  Total:   %s\n", formatBytes(int64(m.TotalAlloc)))
	fmt.Printf("  Sys:     %s\n", formatBytes(int64(m.Sys)))
	fmt.Printf("  Lookups: %d\n", m.Lookups)
	fmt.Printf("  Frees:   %d\n", m.Frees)
	fmt.Printf("  NumGC:   %d\n", m.NumGC)

	// 从 pprof 获取在线人数估算
	fmt.Println("\nNote: Check Grafana dashboard for live connection metrics")
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func getMetricsSummary() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return fmt.Sprintf(`
Memory Summary:
- Alloc: %s
- TotalAlloc: %s  
- Sys: %s
- NumGC: %d
- Goroutines: %d

Session Cache Analysis:
Use the .prof files with:
  go tool pprof -http=:8080 heap.prof
to interactive explore the memory profile.
`, formatBytes(int64(m.Alloc)), formatBytes(int64(m.TotalAlloc)),
		formatBytes(int64(m.Sys)), m.NumGC, runtime.NumGoroutine())
}
