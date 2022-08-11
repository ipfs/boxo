package metrics

import (
	"github.com/ipfs/go-metrics-interface"
)

var (
	// the 1<<18+15 is to observe old file chunks that are 1<<18 + 14 in size
	metricsBuckets = []float64{1 << 6, 1 << 10, 1 << 14, 1 << 18, 1<<18 + 15, 1 << 22}

	timeMetricsBuckets = []float64{1, 10, 30, 60, 90, 120, 600}
)

func DupHist() metrics.Histogram {
	return metrics.New("recv_dup_blocks_bytes", "Summary of duplicate data blocks recived").Histogram(metricsBuckets)
}

func AllHist() metrics.Histogram {
	return metrics.New("recv_all_blocks_bytes", "Summary of all data blocks recived").Histogram(metricsBuckets)
}

func SentHist() metrics.Histogram {
	return metrics.New("sent_all_blocks_bytes", "Histogram of blocks sent by this bitswap").Histogram(metricsBuckets)
}

func SendTimeHist() metrics.Histogram {
	return metrics.New("send_times", "Histogram of how long it takes to send messages in this bitswap").Histogram(timeMetricsBuckets)
}

func PendingEngineGauge() metrics.Gauge {
	return metrics.New("pending_tasks", "Total number of pending tasks").Gauge()
}

func ActiveEngineGauge() metrics.Gauge {
	return metrics.New("active_tasks", "Total number of active tasks").Gauge()
}

func PendingBlocksGauge() metrics.Gauge {
	return metrics.New("pending_block_tasks", "Total number of pending blockstore tasks").Gauge()
}

func ActiveBlocksGauge() metrics.Gauge {
	return metrics.New("active_block_tasks", "Total number of active blockstore tasks").Gauge()
}
