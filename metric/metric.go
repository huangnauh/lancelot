package metric

import (
	"github.com/huangnauh/lancelot/version"
	"github.com/prometheus/client_golang/prometheus"
)

type metric struct {
	Leader          prometheus.Gauge
	InFlight        prometheus.Gauge
	RequestTotal    *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
	ErrorTotal      *prometheus.CounterVec
}

func newMetric() *metric {
	return &metric{
		Leader: prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: version.APP,
			Name:      "leader",
			Help:      "The leader of the cluster.",
		}),
		InFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: version.APP,
			Name:      "in_flight_requests",
			Help:      "A gauge of requests currently being served.",
		}),
		RequestTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Subsystem: version.APP,
				Name:      "requests_total",
				Help:      "A counter for requests.",
			},
			[]string{"user", "db", "command"},
		),
		RequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Subsystem: version.APP,
				Name:      "request_duration",
				Help:      "Bucketed histogram of request latencies.",
				Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20), // 0.1ms ~ 52s
			},
			[]string{"user", "db", "command"},
		),
		ErrorTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Subsystem: version.APP,
				Name:      "errors_total",
				Help:      "A counter for errors.",
			},
			[]string{"user", "db", "command"},
		),
	}
}

var Metric = newMetric()

func init() {
	prometheus.MustRegister(Metric.Leader, Metric.InFlight, Metric.RequestTotal, Metric.RequestDuration, Metric.ErrorTotal)
	Metric.Leader.Set(0)
}

// func MetricsHandle() {
// 	http.Handle("/metrics", promhttp.Handler())
// }
