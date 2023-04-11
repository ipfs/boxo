package main

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/ipfs/boxo/exchange"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type proxyExchange struct {
	httpClient *http.Client
	gatewayURL string
}

func newProxyExchange(gatewayURL string, client *http.Client) exchange.Interface {
	if client == nil {
		client = &http.Client{
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		}
	}

	return &proxyExchange{
		gatewayURL: gatewayURL,
		httpClient: client,
	}
}

func (e *proxyExchange) fetch(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	urlStr := fmt.Sprintf("%s/ipfs/%s?format=raw", e.gatewayURL, c)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.ipld.raw")
	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status from remote gateway: %s", resp.Status)
	}

	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Validate incoming blocks
	// This is important since we are proxying block requests to an untrusted gateway.
	nc, err := c.Prefix().Sum(rb)
	if err != nil {
		return nil, blocks.ErrWrongHash
	}
	if !nc.Equals(c) {
		fmt.Printf("got %s vs %s\n", nc, c)
		return nil, blocks.ErrWrongHash
	}

	return blocks.NewBlockWithCid(rb, c)
}

func (e *proxyExchange) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	blk, err := e.fetch(ctx, c)
	if err != nil {
		return nil, err
	}
	return blk, nil
}

func (e *proxyExchange) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	ch := make(chan blocks.Block)

	// Note: this implementation of GetBlocks does not make use of worker pools or parallelism
	// However, production implementations generally will, and an advanced
	// version of this can be found in https://github.com/ipfs/bifrost-gateway/
	go func() {
		defer close(ch)
		for _, c := range cids {
			blk, err := e.fetch(ctx, c)
			if err != nil {
				return
			}
			select {
			case ch <- blk:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

func (e *proxyExchange) NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error {
	// Note: while not required this function could be used optimistically to prevent fetching
	// of data that the client has retrieved already even though a Get call is in progress.
	return nil
}

func (e *proxyExchange) Close() error {
	// Note: while nothing is strictly required to happen here it would be reasonable to close
	// existing connections and prevent new operations from starting.
	return nil
}
