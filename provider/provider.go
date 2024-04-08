package provider

import (
	"context"

	blocks "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/fetcher"
	fetcherhelpers "github.com/ipfs/boxo/fetcher/helpers"
	pin "github.com/ipfs/boxo/pinning/pinner"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

var logR = logging.Logger("reprovider.simple")

// Provider announces blocks to the network
type Provider interface {
	// Provide takes a cid and makes an attempt to announce it to the network
	Provide(cid.Cid) error
}

// Reprovider reannounces blocks to the network
type Reprovider interface {
	// Reprovide starts a new reprovide if one isn't running already.
	Reprovide(context.Context) error
}

// System defines the interface for interacting with the value
// provider system
type System interface {
	Close() error
	Stat() (ReproviderStats, error)
	Provider
	Reprovider
}

// KeyChanFunc is function streaming CIDs to pass to content routing
type KeyChanFunc func(context.Context) (<-chan cid.Cid, error)

// NewBlockstoreProvider returns key provider using bstore.AllKeysChan
func NewBlockstoreProvider(bstore blocks.Blockstore) KeyChanFunc {
	return func(ctx context.Context) (<-chan cid.Cid, error) {
		return bstore.AllKeysChan(ctx)
	}
}

// NewPinnedProvider returns provider supplying pinned keys
func NewPinnedProvider(onlyRoots bool, pinning pin.Pinner, fetchConfig fetcher.Factory) KeyChanFunc {
	return func(ctx context.Context) (<-chan cid.Cid, error) {
		set, err := pinSet(ctx, pinning, fetchConfig, onlyRoots)
		if err != nil {
			return nil, err
		}

		outCh := make(chan cid.Cid)
		go func() {
			defer close(outCh)
			for c := range set.New {
				select {
				case <-ctx.Done():
					return
				case outCh <- c:
				}
			}
		}()

		return outCh, nil
	}
}

func pinSet(ctx context.Context, pinning pin.Pinner, fetchConfig fetcher.Factory, onlyRoots bool) (*cidutil.StreamingSet, error) {
	// FIXME: Listing all pins code is duplicated thrice, twice in Kubo and here, maybe more.
	// If this were a method of the [pin.Pinner] life would be easier.
	set := cidutil.NewStreamingSet()

	go func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		defer close(set.New)

		for sc := range pinning.DirectKeys(ctx, false) {
			if sc.Err != nil {
				logR.Errorf("reprovide direct pins: %s", sc.Err)
				return
			}
			set.Visitor(ctx)(sc.Pin.Key)
		}

		session := fetchConfig.NewSession(ctx)
		for sc := range pinning.RecursiveKeys(ctx, false) {
			if sc.Err != nil {
				logR.Errorf("reprovide recursive pins: %s", sc.Err)
				return
			}
			set.Visitor(ctx)(sc.Pin.Key)
			if !onlyRoots {
				err := fetcherhelpers.BlockAll(ctx, session, cidlink.Link{Cid: sc.Pin.Key}, func(res fetcher.FetchResult) error {
					clink, ok := res.LastBlockLink.(cidlink.Link)
					if ok {
						set.Visitor(ctx)(clink.Cid)
					}
					return nil
				})
				if err != nil {
					logR.Errorf("reprovide indirect pins: %s", err)
					return
				}
			}
		}
	}()

	return set, nil
}

func NewPrioritizedProvider(priorityCids KeyChanFunc, otherCids KeyChanFunc) KeyChanFunc {
	return func(ctx context.Context) (<-chan cid.Cid, error) {
		outCh := make(chan cid.Cid)

		go func() {
			defer close(outCh)
			visited := map[string]struct{}{}

			handleStream := func(stream KeyChanFunc, markVisited bool) error {
				ch, err := stream(ctx)
				if err != nil {
					return err
				}

				for {
					select {
					case <-ctx.Done():
						return nil
					case c, ok := <-ch:
						if !ok {
							return nil
						}

						// Bytes of the multihash of the CID as key. Major routing systems
						// in use are multihash based. Advertising 2 CIDs with the same
						// multihash would not be very useful.
						str := string(c.Hash())
						if _, ok := visited[str]; ok {
							continue
						}

						select {
						case <-ctx.Done():
							return nil
						case outCh <- c:
							if markVisited {
								visited[str] = struct{}{}
							}
						}
					}
				}
			}

			err := handleStream(priorityCids, true)
			if err != nil {
				log.Warnf("error in prioritized strategy while handling priority CIDs: %w", err)
				return
			}

			err = handleStream(otherCids, false)
			if err != nil {
				log.Warnf("error in prioritized strategy while handling other CIDs: %w", err)
			}
		}()

		return outCh, nil
	}
}
