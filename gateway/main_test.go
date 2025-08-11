package gateway

import (
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMain(m *testing.M) {
	// Initialize metrics once for all tests using the default registry
	// This prevents race conditions when tests run in parallel
	// Each test that creates a handler should use its own registry in Config
	initializeMiddlewareMetrics(prometheus.DefaultRegisterer)

	// Run tests
	os.Exit(m.Run())
}
