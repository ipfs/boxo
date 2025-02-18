package provider

import (
	"context"
	"fmt"
	logger "log"

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

		outCh := make(chan cid.Cid, 64) // Add buffer to prevent blocking
		set := cidutil.NewStreamingSet()

		logger.Printf("Starting DAG traversal with root: %s", root)

		// Use buffered channel for coordination
		done := make(chan struct{}, 1)

		go func() {
			defer func() {
				close(set.New)
				done <- struct{}{} // Signal completion
			}()

			session := fetchConfig.NewSession(ctx)
			err := fetcherhelpers.BlockAll(ctx, session, cidlink.Link{Cid: root}, func(res fetcher.FetchResult) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					clink, ok := res.LastBlockLink.(cidlink.Link)
					if !ok {
						log.Warnf("unexpected link type: %T", res.LastBlockLink)
						return nil
					}
					set.Visitor(ctx)(clink.Cid)
					logger.Printf("Added CID to set: %s", clink.Cid)
					return nil
				}
			})
			if err != nil {
				log.Errorf("dag traversal error: %s", err)
				return
			}
			logger.Printf("Completed BlockAll traversal")
		}()

		go func() {
			defer close(outCh)

			for {
				select {
				case c, ok := <-set.New:
					if !ok {
						select {
						case <-done: // Wait for traversal to complete
							return
						case <-ctx.Done():
							return
						}
					}
					select {
					case <-ctx.Done():
						return
					case outCh <- c:
						logger.Printf("Sent CID to output: %s", c)
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		return outCh, nil
	}
}
