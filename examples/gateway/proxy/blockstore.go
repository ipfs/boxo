package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/ipfs/boxo/blocks"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

var (
	errNotImplemented = errors.New("not implemented")
)

type proxyStore struct {
	httpClient *http.Client
	gatewayURL string
	validate   bool
}

func newProxyStore(gatewayURL string, client *http.Client) blockstore.Blockstore {
	if client == nil {
		client = http.DefaultClient
	}

	return &proxyStore{
		gatewayURL: gatewayURL,
		httpClient: client,
		// Enables block validation by default. Important since we are
		// proxying block requests to an untrusted gateway.
		validate: true,
	}
}

func (ps *proxyStore) fetch(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	u, err := url.Parse(fmt.Sprintf("%s/ipfs/%s?format=raw", ps.gatewayURL, c))
	if err != nil {
		return nil, err
	}
	resp, err := ps.httpClient.Do(&http.Request{
		Method: http.MethodGet,
		URL:    u,
		Header: http.Header{
			"Accept": []string{"application/vnd.ipld.raw"},
		},
	})
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

	if ps.validate {
		nc, err := c.Prefix().Sum(rb)
		if err != nil {
			return nil, blocks.ErrWrongHash
		}
		if !nc.Equals(c) {
			fmt.Printf("got %s vs %s\n", nc, c)
			return nil, blocks.ErrWrongHash
		}
	}
	return blocks.NewBlockWithCid(rb, c)
}

func (ps *proxyStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	blk, err := ps.fetch(ctx, c)
	if err != nil {
		return false, err
	}
	return blk != nil, nil
}

func (ps *proxyStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	blk, err := ps.fetch(ctx, c)
	if err != nil {
		return nil, err
	}
	return blk, nil
}

func (ps *proxyStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	blk, err := ps.fetch(ctx, c)
	if err != nil {
		return 0, err
	}
	return len(blk.RawData()), nil
}

func (ps *proxyStore) HashOnRead(enabled bool) {
	ps.validate = enabled
}

func (c *proxyStore) Put(context.Context, blocks.Block) error {
	return errNotImplemented
}

func (c *proxyStore) PutMany(context.Context, []blocks.Block) error {
	return errNotImplemented
}
func (c *proxyStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, errNotImplemented
}
func (c *proxyStore) DeleteBlock(context.Context, cid.Cid) error {
	return errNotImplemented
}
