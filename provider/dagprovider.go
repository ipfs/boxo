package provider

import (
	"context"
	"fmt"

	"github.com/ipfs/boxo/fetcher"
	fetcherhelpers "github.com/ipfs/boxo/fetcher/helpers"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

func NewDAGProvider(root cid.Cid, fetchConfig fetcher.Factory) KeyChanFunc {
	return func(ctx context.Context) (<-chan cid.Cid, error) {
		if root == cid.Undef {
			return nil, fmt.Errorf("root CID cannot be empty")
		}

		set := cidutil.NewStreamingSet()

		go func() {
			defer close(set.New)
			session := fetchConfig.NewSession(ctx)
			err := fetcherhelpers.BlockAll(ctx, session, cidlink.Link{Cid: root}, func(res fetcher.FetchResult) error {
				clink, ok := res.LastBlockLink.(cidlink.Link)
				if ok {
					// if context is cancelled, nothing is written to new()
					_ = set.Visitor(ctx)(clink.Cid)
				}

				select {
				case <-ctx.Done():
					// halts traversal
					return ctx.Err()
				default:
				}
				return nil
			})
			if err != nil {
				log.Errorf("dagprovider dag traversal error: %s", err)
				return
			}
		}()

		return set.New, nil
	}
}
