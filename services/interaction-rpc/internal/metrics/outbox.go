package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	OutboxPending      = prometheus.NewGauge(prometheus.GaugeOpts{Name: "interaction_outbox_pending", Help: "Pending outbox documents; valid only when collection succeeds."})
	OutboxOldest       = prometheus.NewGauge(prometheus.GaugeOpts{Name: "interaction_outbox_oldest_age_seconds", Help: "Age of oldest pending document in seconds."})
	OutboxCollectionUp = prometheus.NewGauge(prometheus.GaugeOpts{Name: "interaction_outbox_collection_up", Help: "Whether the last outbox snapshot succeeded."})
	OutboxSnapshotTime = prometheus.NewGauge(prometheus.GaugeOpts{Name: "interaction_outbox_snapshot_timestamp_seconds", Help: "Unix time of last successful outbox snapshot."})
	OutboxPublisherUp  = prometheus.NewGauge(prometheus.GaugeOpts{Name: "interaction_outbox_publisher_up", Help: "Whether publisher has declared queues and enabled confirms."})
	outboxPublish      = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "interaction_outbox_publish_total", Help: "Publish and delete outcomes; retries may count a document more than once."}, []string{"result"})
	outboxDuration     = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "interaction_outbox_publish_duration_seconds", Help: "Publish, confirm and delete duration.", Buckets: prometheus.DefBuckets}, []string{"result"})
)

func init() {
	prometheus.MustRegister(OutboxPending, OutboxOldest, OutboxCollectionUp, OutboxSnapshotTime, OutboxPublisherUp, outboxPublish, outboxDuration)
}

func ObserveOutboxPublish(result string, duration time.Duration) {
	outboxPublish.WithLabelValues(result).Inc()
	outboxDuration.WithLabelValues(result).Observe(duration.Seconds())
}
