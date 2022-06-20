package metrics

import (
	"context"
	"sync"

	"github.com/ipfs/go-metrics-interface"
)

var (
	// the 1<<18+15 is to observe old file chunks that are 1<<18 + 14 in size
	metricsBuckets = []float64{1 << 6, 1 << 10, 1 << 14, 1 << 18, 1<<18 + 15, 1 << 22}

	timeMetricsBuckets = []float64{1, 10, 30, 60, 90, 120, 600}
)

type onceAble[T any] struct {
	o sync.Once
	v T
}

func (o *onceAble[T]) reuseOrInit(creator func() T) T {
	o.o.Do(func() {
		o.v = creator()
	})
	return o.v
}

// Metrics is a type which lazy initialize metrics objects.
// It MUST not be copied.
type Metrics struct {
	ctx context.Context

	dupHist      onceAble[metrics.Histogram]
	allHist      onceAble[metrics.Histogram]
	sentHist     onceAble[metrics.Histogram]
	sendTimeHist onceAble[metrics.Histogram]

	pendingEngineGauge onceAble[metrics.Gauge]
	activeEngineGauge  onceAble[metrics.Gauge]
	pendingBlocksGauge onceAble[metrics.Gauge]
	activeBlocksGauge  onceAble[metrics.Gauge]
}

func New(ctx context.Context) *Metrics {
	return &Metrics{ctx: metrics.CtxSubScope(ctx, "bitswap")}
}

// DupHist return recv_dup_blocks_bytes.
// Threadsafe
func (m *Metrics) DupHist() metrics.Histogram {
	return m.dupHist.reuseOrInit(func() metrics.Histogram {
		return metrics.NewCtx(m.ctx, "recv_dup_blocks_bytes", "Summary of duplicate data blocks recived").Histogram(metricsBuckets)
	})
}

// AllHist returns recv_all_blocks_bytes.
// Threadsafe
func (m *Metrics) AllHist() metrics.Histogram {
	return m.allHist.reuseOrInit(func() metrics.Histogram {
		return metrics.NewCtx(m.ctx, "recv_all_blocks_bytes", "Summary of all data blocks recived").Histogram(metricsBuckets)
	})
}

// SentHist returns sent_all_blocks_bytes.
// Threadsafe
func (m *Metrics) SentHist() metrics.Histogram {
	return m.sentHist.reuseOrInit(func() metrics.Histogram {
		return metrics.NewCtx(m.ctx, "sent_all_blocks_bytes", "Histogram of blocks sent by this bitswap").Histogram(metricsBuckets)
	})
}

// SendTimeHist returns send_times.
// Threadsafe
func (m *Metrics) SendTimeHist() metrics.Histogram {
	return m.sendTimeHist.reuseOrInit(func() metrics.Histogram {
		return metrics.NewCtx(m.ctx, "send_times", "Histogram of how long it takes to send messages in this bitswap").Histogram(timeMetricsBuckets)
	})
}

// PendingEngineGauge returns pending_tasks.
// Threadsafe
func (m *Metrics) PendingEngineGauge() metrics.Gauge {
	return m.pendingEngineGauge.reuseOrInit(func() metrics.Gauge {
		return metrics.NewCtx(m.ctx, "pending_tasks", "Total number of pending tasks").Gauge()
	})
}

// ActiveEngineGauge returns active_tasks.
// Threadsafe
func (m *Metrics) ActiveEngineGauge() metrics.Gauge {
	return m.activeEngineGauge.reuseOrInit(func() metrics.Gauge {
		return metrics.NewCtx(m.ctx, "active_tasks", "Total number of active tasks").Gauge()
	})
}

// PendingBlocksGauge returns pending_block_tasks.
// Threadsafe
func (m *Metrics) PendingBlocksGauge() metrics.Gauge {
	return m.pendingBlocksGauge.reuseOrInit(func() metrics.Gauge {
		return metrics.NewCtx(m.ctx, "pending_block_tasks", "Total number of pending blockstore tasks").Gauge()
	})
}

// ActiveBlocksGauge returns active_block_tasks.
// Threadsafe
func (m *Metrics) ActiveBlocksGauge() metrics.Gauge {
	return m.activeBlocksGauge.reuseOrInit(func() metrics.Gauge {
		return metrics.NewCtx(m.ctx, "active_block_tasks", "Total number of active blockstore tasks").Gauge()
	})
}
