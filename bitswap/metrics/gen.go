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

// Metrics is a type which lazy initialize metrics objects.
// It MUST not be copied.
type Metrics struct {
	ctx  context.Context
	lock sync.Mutex

	dupHist      metrics.Histogram
	allHist      metrics.Histogram
	sentHist     metrics.Histogram
	sendTimeHist metrics.Histogram

	pendingEngineGauge metrics.Gauge
	activeEngineGauge  metrics.Gauge
	pendingBlocksGauge metrics.Gauge
	activeBlocksGauge  metrics.Gauge
}

func New(ctx context.Context) *Metrics {
	return &Metrics{ctx: metrics.CtxSubScope(ctx, "bitswap")}
}

// DupHist return recv_dup_blocks_bytes.
// Threadsafe
func (m *Metrics) DupHist() metrics.Histogram {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.dupHist != nil {
		return m.dupHist
	}
	m.dupHist = metrics.NewCtx(m.ctx, "recv_dup_blocks_bytes", "Summary of duplicate data blocks recived").Histogram(metricsBuckets)
	return m.dupHist
}

// AllHist returns recv_all_blocks_bytes.
// Threadsafe
func (m *Metrics) AllHist() metrics.Histogram {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.allHist != nil {
		return m.allHist
	}
	m.allHist = metrics.NewCtx(m.ctx, "recv_all_blocks_bytes", "Summary of all data blocks recived").Histogram(metricsBuckets)
	return m.allHist
}

// SentHist returns sent_all_blocks_bytes.
// Threadsafe
func (m *Metrics) SentHist() metrics.Histogram {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.sentHist != nil {
		return m.sentHist
	}
	m.sentHist = metrics.NewCtx(m.ctx, "sent_all_blocks_bytes", "Histogram of blocks sent by this bitswap").Histogram(metricsBuckets)
	return m.sentHist
}

// SendTimeHist returns send_times.
// Threadsafe
func (m *Metrics) SendTimeHist() metrics.Histogram {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.sendTimeHist != nil {
		return m.sendTimeHist
	}
	m.sendTimeHist = metrics.NewCtx(m.ctx, "send_times", "Histogram of how long it takes to send messages in this bitswap").Histogram(timeMetricsBuckets)
	return m.sendTimeHist
}

// PendingEngineGauge returns pending_tasks.
// Threadsafe
func (m *Metrics) PendingEngineGauge() metrics.Gauge {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.pendingEngineGauge != nil {
		return m.pendingEngineGauge
	}
	m.pendingEngineGauge = metrics.NewCtx(m.ctx, "pending_tasks", "Total number of pending tasks").Gauge()
	return m.pendingEngineGauge
}

// ActiveEngineGauge returns active_tasks.
// Threadsafe
func (m *Metrics) ActiveEngineGauge() metrics.Gauge {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.activeEngineGauge != nil {
		return m.activeEngineGauge
	}
	m.activeEngineGauge = metrics.NewCtx(m.ctx, "active_tasks", "Total number of active tasks").Gauge()
	return m.activeEngineGauge
}

// PendingBlocksGauge returns pending_block_tasks.
// Threadsafe
func (m *Metrics) PendingBlocksGauge() metrics.Gauge {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.pendingBlocksGauge != nil {
		return m.pendingBlocksGauge
	}
	m.pendingBlocksGauge = metrics.NewCtx(m.ctx, "pending_block_tasks", "Total number of pending blockstore tasks").Gauge()
	return m.pendingBlocksGauge
}

// ActiveBlocksGauge returns active_block_tasks.
// Threadsafe
func (m *Metrics) ActiveBlocksGauge() metrics.Gauge {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.activeBlocksGauge != nil {
		return m.activeBlocksGauge
	}
	m.activeBlocksGauge = metrics.NewCtx(m.ctx, "active_block_tasks", "Total number of active blockstore tasks").Gauge()
	return m.activeBlocksGauge
}
