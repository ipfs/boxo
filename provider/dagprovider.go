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

// NewDAGProvider returns a provider that traverses a DAG from a root CID
func NewDAGProvider(root cid.Cid, fetchConfig fetcher.Factory) KeyChanFunc {
	return func(ctx context.Context) (<-chan cid.Cid, error) {
		if root.String() == "" {
			return nil, fmt.Errorf("root CID cannot be empty")
		}
		if fetchConfig == nil {
			return nil, fmt.Errorf("fetcher config cannot be nil")
		}

		outCh := make(chan cid.Cid)

		go func() {
			defer close(outCh)
			set := cidutil.NewStreamingSet()
			session := fetchConfig.NewSession(ctx)

			err := fetcherhelpers.BlockAll(ctx, session, cidlink.Link{Cid: root}, func(res fetcher.FetchResult) error {
				clink, ok := res.LastBlockLink.(cidlink.Link)
				if !ok {
					log.Warnf("enexpected link type: %T", res.LastBlockLink)
					return nil
				}
				if !set.Visitor(ctx)(clink.Cid) {
					log.Warnf("error visiting CID %s", clink.Cid)
				}
				return nil
			})
			if err != nil {
				log.Errorf("dag traversal error: %s", err)
				return
			}

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
