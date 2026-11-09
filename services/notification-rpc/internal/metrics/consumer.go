package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var ConsumerUp = prometheus.NewGauge(prometheus.GaugeOpts{Name: "notification_consumer_up", Help: "Whether interaction consumer is subscribed."})
var ConsumerEnabled = prometheus.NewGauge(prometheus.GaugeOpts{Name: "notification_consumer_enabled", Help: "Whether interaction notification consumption is configured."})
var consumeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notification_consume_total", Help: "Handling and settlement outcomes; one message can increment multiple results. dead_letter is successful rejection, not DLQ depth."}, []string{"result"})
var consumeDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "notification_consume_duration_seconds", Help: "Duration of each valid message handling attempt.", Buckets: prometheus.DefBuckets}, []string{"result"})

func init() {
	prometheus.MustRegister(ConsumerUp, ConsumerEnabled, consumeTotal, consumeDuration)
	for _, result := range []string{"success", "retry", "acked", "dead_letter", "ack_error"} {
		consumeTotal.WithLabelValues(result).Add(0)
	}
}
func ObserveConsume(result string) { consumeTotal.WithLabelValues(result).Inc() }
func ObserveConsumeAttempt(result string, duration time.Duration) {
	ObserveConsume(result)
	consumeDuration.WithLabelValues(result).Observe(duration.Seconds())
}
