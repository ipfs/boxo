package decision

import (
	"context"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	process "github.com/jbenet/goprocess"
)

// blockstoreManager maintains a pool of workers that make requests to the blockstore.
type blockstoreManager struct {
	bs          bstore.Blockstore
	workerCount int
	jobs        chan func()
	px          process.Process
}

// newBlockstoreManager creates a new blockstoreManager with the given context
// and number of workers
func newBlockstoreManager(ctx context.Context, bs bstore.Blockstore, workerCount int) *blockstoreManager {
	return &blockstoreManager{
		bs:          bs,
		workerCount: workerCount,
		jobs:        make(chan func()),
	}
}

func (bsm *blockstoreManager) start(px process.Process) {
	bsm.px = px

	// Start up workers
	for i := 0; i < bsm.workerCount; i++ {
		px.Go(func(px process.Process) {
			bsm.worker()
		})
	}
}

func (bsm *blockstoreManager) worker() {
	for {
		select {
		case <-bsm.px.Closing():
			return
		case job := <-bsm.jobs:
			job()
		}
	}
}

func (bsm *blockstoreManager) addJob(ctx context.Context, job func()) {
	select {
	case <-ctx.Done():
	case <-bsm.px.Closing():
	case bsm.jobs <- job:
	}
}

func (bsm *blockstoreManager) getBlockSizes(ctx context.Context, ks []cid.Cid) map[cid.Cid]int {
	res := make(map[cid.Cid]int)
	if len(ks) == 0 {
		return res
	}

	var lk sync.Mutex
	bsm.jobPerKey(ctx, ks, func(c cid.Cid) {
		size, err := bsm.bs.GetSize(c)
		if err != nil {
			if err != bstore.ErrNotFound {
				log.Errorf("blockstore.GetSize(%s) error: %s", c, err)
			}
		} else {
			lk.Lock()
			res[c] = size
			lk.Unlock()
		}
	})

	return res
}

func (bsm *blockstoreManager) getBlocks(ctx context.Context, ks []cid.Cid) map[cid.Cid]blocks.Block {
	res := make(map[cid.Cid]blocks.Block)
	if len(ks) == 0 {
		return res
	}

	var lk sync.Mutex
	bsm.jobPerKey(ctx, ks, func(c cid.Cid) {
		blk, err := bsm.bs.Get(c)
		if err != nil {
			if err != bstore.ErrNotFound {
				log.Errorf("blockstore.Get(%s) error: %s", c, err)
			}
		} else {
			lk.Lock()
			res[c] = blk
			lk.Unlock()
		}
	})

	return res
}

func (bsm *blockstoreManager) jobPerKey(ctx context.Context, ks []cid.Cid, jobFn func(c cid.Cid)) {
	wg := sync.WaitGroup{}
	for _, k := range ks {
		c := k
		wg.Add(1)
		bsm.addJob(ctx, func() {
			jobFn(c)
			wg.Done()
		})
	}
	wg.Wait()
}
