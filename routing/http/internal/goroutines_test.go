package internal

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func sequence(n int) (items []int) {
	for i := 0; i < n; i++ {
		items = append(items, i+1)
	}
	return
}

func singleItemBatches(items []int) (batches [][]int) {
	for _, item := range items {
		batches = append(batches, []int{item})
	}
	return
}

func TestDoBatch(t *testing.T) {
	cases := []struct {
		name           string
		items          []int
		maxBatchSize   int
		maxConcurrency int
		shouldErrOnce  bool

		expBatches     [][]int
		expErrContains string
	}{
		{
			name: "no items",
		},
		{
			name:           "batch size = 1",
			items:          sequence(3),
			maxBatchSize:   1,
			maxConcurrency: 1,
			expBatches:     [][]int{{1}, {2}, {3}},
		},
		{
			name:           "batch size > 1",
			items:          sequence(6),
			maxBatchSize:   2,
			maxConcurrency: 2,
			expBatches:     [][]int{{1, 2}, {3, 4}, {5, 6}},
		},
		{
			name:           "a lot of items and concurrency",
			items:          sequence(1000),
			maxBatchSize:   1,
			maxConcurrency: 100,
			expBatches:     singleItemBatches(sequence(1000)),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			var mut sync.Mutex
			var batches [][]int

			var onceMut sync.Mutex
			var errored bool

			DoBatch(ctx, c.maxBatchSize, c.maxConcurrency, c.items, func(ctx context.Context, batch []int) error {
				if c.shouldErrOnce {
					onceMut.Lock()
					if !errored {
						errored = true
						defer onceMut.Unlock()
						return errors.New("boom")
					}
					onceMut.Unlock()
				}

				mut.Lock()
				batches = append(batches, batch)
				mut.Unlock()
				return nil
			})

			require.Equal(t, len(c.expBatches), len(batches), "expected equal len %v %v", c.expBatches, batches)
			for _, expBatch := range c.expBatches {
				requireContainsBatch(t, batches, expBatch)
			}
		})
	}
}

func requireContainsBatch(t *testing.T, batches [][]int, batch []int) {
	for _, b := range batches {
		if reflect.DeepEqual(batch, b) {
			return
		}
	}
	t.Fatalf("expected batch %v, but not found in batches %v", batch, batches)
}
