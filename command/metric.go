package command

import (
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.s.upyun.com/platform/lancelot/version"
)

type Metric struct {
	inFlight        prometheus.Gauge
	requestTotal    *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
}

func newMetric() *Metric {
	return &Metric{
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: version.APP,
			Name:      "in_flight_requests",
			Help:      "A gauge of requests currently being served.",
		}),
		requestTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Subsystem: version.APP,
				Name:      "requests_total",
				Help:      "A counter for requests.",
			},
			[]string{"command"},
		),
		requestDuration: prometheus.NewHistogramVec(
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

var metric = newMetric()

func (m *Metric) mustRegister() {
	prometheus.MustRegister(m.inFlight, m.requestTotal, m.requestDuration)
}

func init() {
}
