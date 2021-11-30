package metric

import (
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.s.upyun.com/platform/lancelot/version"
)

type metric struct {
	InFlight        prometheus.Gauge
	RequestTotal    *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
}

func newMetric() *metric {
	return &metric{
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
			[]string{"command"},
		),
		RequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Subsystem: version.APP,
				Name:      "request_duration",
				Help:      "Bucketed histogram of request latencies.",
				Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20), // 0.1ms ~ 52s
			},
			[]string{"command"},
		),
	}
}

var Metric = newMetric()

func init() {
	prometheus.MustRegister(Metric.InFlight, Metric.RequestTotal, Metric.RequestDuration)
}

// func MetricsHandle() {
// 	http.Handle("/metrics", promhttp.Handler())
// }
