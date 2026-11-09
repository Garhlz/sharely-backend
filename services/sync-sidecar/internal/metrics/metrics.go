package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	amqp "github.com/rabbitmq/amqp091-go"
)

var ConsumerUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "sidecar_consumer_up", Help: "Whether each configured queue has an active subscription."}, []string{"queue"})
var settled = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "sidecar_consume_total", Help: "Settlement outcomes; acked is broker acknowledgement, not proof of every business side effect."}, []string{"queue", "result"})
var duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "sidecar_consume_duration_seconds", Help: "Time from delivery to settlement.", Buckets: prometheus.DefBuckets}, []string{"queue"})

func init() { prometheus.MustRegister(ConsumerUp, settled, duration) }

func InitQueue(queue string) {
	ConsumerUp.WithLabelValues(queue).Set(0)
	for _, result := range []string{"acked", "requeue", "dead_letter", "ack_error"} {
		settled.WithLabelValues(queue, result).Add(0)
	}
}

// 包装确认操作而不改变 Handler 的 Ack/Nack 决策；不记录消息 ID 等高基数标签。
func TrackDelivery(d amqp.Delivery, queue string) amqp.Delivery {
	if d.Acknowledger == nil {
		return d
	}
	d.Acknowledger = &acknowledger{Acknowledger: d.Acknowledger, queue: queue, start: time.Now()}
	return d
}

type acknowledger struct {
	amqp.Acknowledger
	queue string
	start time.Time
}

func (a *acknowledger) observe(result string, err error) error {
	if err != nil {
		result = "ack_error"
	}
	settled.WithLabelValues(a.queue, result).Inc()
	duration.WithLabelValues(a.queue).Observe(time.Since(a.start).Seconds())
	return err
}
func (a *acknowledger) Ack(tag uint64, multiple bool) error {
	return a.observe("acked", a.Acknowledger.Ack(tag, multiple))
}
func (a *acknowledger) Nack(tag uint64, multiple, requeue bool) error {
	result := "dead_letter"
	if requeue {
		result = "requeue"
	}
	return a.observe(result, a.Acknowledger.Nack(tag, multiple, requeue))
}
func (a *acknowledger) Reject(tag uint64, requeue bool) error {
	result := "dead_letter"
	if requeue {
		result = "requeue"
	}
	return a.observe(result, a.Acknowledger.Reject(tag, requeue))
}
