package store

import (
	"github.com/pingcap/tidb/metrics"
)

func init() {
	metrics.RegisterMetrics()
}
