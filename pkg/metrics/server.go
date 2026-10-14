/**
 * @Author: easy-chat
 * @Description: Prometheus 指标暴露接口
 */

package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zeromicro/go-zero/rest"
)

// RegisterMetricsServer 注册 /metrics 端点到 go-zero 服务
// 使用方式：在 main.go 中调用
//
//	serve := rest.MustNewServer(c.RestConf)
//	metrics.RegisterMetricsServer(serve)
//	defer serve.Start()
func RegisterMetricsServer(engine *rest.Server) {
	engine.AddRoute(rest.Route{
		Method:  http.MethodGet,
		Path:    "/metrics",
		Handler: func(w http.ResponseWriter, r *http.Request) {
			promhttp.Handler().ServeHTTP(w, r)
		},
	})
}

// RegisterMetricsHandler 直接注册到 http.Server
// 使用方式：
//
//	http.Handle("/metrics", promhttp.Handler())
//	http.ListenAndServe(":12112", nil)
func RegisterMetricsHandler() http.Handler {
	return promhttp.Handler()
}
