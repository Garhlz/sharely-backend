package metrics

import "github.com/prometheus/client_golang/prometheus"

var responseErrors = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gateway_response_errors_total", Help: "Errors emitted by gateway error handler, excluding Java passthrough responses."}, []string{"class"})

func init() { prometheus.MustRegister(responseErrors) }

// 只区分稳定类别，不将错误文本、用户 ID 或任意业务码作为标签。
func ObserveResponseError(code int) {
	class := "business"
	if code >= 50000 {
		class = "server"
	}
	responseErrors.WithLabelValues(class).Inc()
}
