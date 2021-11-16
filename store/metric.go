package store

import (
	"github.com/tikv/client-go/v2/metrics"
)

func init() {
	metrics.RegisterMetrics()
}
