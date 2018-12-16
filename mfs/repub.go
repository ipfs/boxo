package mfs

import (
	"context"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
)

// PubFunc is the function used by the `publish()` method.
type PubFunc func(context.Context, cid.Cid) error

// Republisher manages when to publish a given entry.
type Republisher struct {
	TimeoutLong  time.Duration
	TimeoutShort time.Duration
	Publish      chan struct{}
	pubfunc      PubFunc
	pubnowch     chan chan struct{}

	ctx    context.Context
	cancel func()

	lk      sync.Mutex
	val     cid.Cid
	lastpub cid.Cid
}

// NewRepublisher creates a new Republisher object to republish the given root
// using the given short and long time intervals.
func NewRepublisher(ctx context.Context, pf PubFunc, tshort, tlong time.Duration) *Republisher {
	ctx, cancel := context.WithCancel(ctx)
	return &Republisher{
		TimeoutShort: tshort,
		TimeoutLong:  tlong,
		Publish:      make(chan struct{}, 1),
		pubfunc:      pf,
		pubnowch:     make(chan chan struct{}),
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (p *Republisher) setVal(c cid.Cid) {
	p.lk.Lock()
	defer p.lk.Unlock()
	p.val = c
}

// WaitPub Returns immediately if `lastpub` value is consistent with the
// current value `val`, else will block until `val` has been published.
func (p *Republisher) WaitPub() {
	p.lk.Lock()
	consistent := p.lastpub == p.val
	p.lk.Unlock()
	if consistent {
		return
	}

	wait := make(chan struct{})
	p.pubnowch <- wait
	<-wait
}

func (p *Republisher) Close() error {
	err := p.publish(p.ctx)
	p.cancel()
	return err
}

// Touch signals that an update has occurred since the last publish.
// Multiple consecutive touches may extend the time period before
// the next Publish occurs in order to more efficiently batch updates.
func (np *Republisher) Update(c cid.Cid) {
	np.setVal(c)
	select {
	case np.Publish <- struct{}{}:
	default:
	}
}

// Run is the main republisher loop.
// TODO: Document according to:
// https://github.com/ipfs/go-ipfs/issues/5092#issuecomment-398524255.
func (np *Republisher) Run() {
	for {
		select {
		case <-np.Publish:
			quick := time.After(np.TimeoutShort)
			longer := time.After(np.TimeoutLong)

		wait:
			var pubnowresp chan struct{}

			select {
			case <-np.ctx.Done():
				return
			case <-np.Publish:
				quick = time.After(np.TimeoutShort)
				goto wait
			case <-quick:
			case <-longer:
			case pubnowresp = <-np.pubnowch:
			}

			err := np.publish(np.ctx)
			if pubnowresp != nil {
				pubnowresp <- struct{}{}
			}
			if err != nil {
				log.Errorf("republishRoot error: %s", err)
			}

		case <-np.ctx.Done():
			return
		}
	}
}

// publish calls the `PubFunc`.
func (np *Republisher) publish(ctx context.Context) error {
	np.lk.Lock()
	topub := np.val
	np.lk.Unlock()

	err := np.pubfunc(ctx, topub)
	if err != nil {
		return err
	}
	np.lk.Lock()
	np.lastpub = topub
	np.lk.Unlock()
	return nil
}
