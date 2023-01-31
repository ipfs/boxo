// gateway performs IPFS Gateway CAR requests in order to provide a [rapide.ServerDrivenDownloader] interface.
package gateway

import (
	"context"
	"fmt"
	"net/http"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/ipsl"
	"github.com/ipfs/go-libipfs/ipsl/unixfs"
	"github.com/ipfs/go-libipfs/rapide"
	"github.com/ipld/go-car/v2"
)

var _ rapide.ServerDrivenDownloader = Gateway{}

// Gateway allows to download car files from a gateway with the [rapide.ServerDriven] interface.
// It does not implement any traversal validation logic, and relies on the consumer (rapide) to care of this.
type Gateway struct {
	// PathName must be like: "https://example.org/ipfs/"
	PathName string

	// Client can be nil, then net/http.DefaultClient will be used.
	Client *http.Client
}

func (g Gateway) Download(ctx context.Context, root cid.Cid, traversal ipsl.Traversal) (blocks.BlockIterator, error) {
	_, ok := traversal.(unixfs.EverythingNode)
	if !ok {
		return nil, fmt.Errorf("http-car only supports unixfs.Everything traversal, got: %q", traversal)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.PathName+root.String(), http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/vnd.ipld.car")

	resp, err := g.getClient().Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("non 200 error code: %d", resp.StatusCode)
	}

	stream, err := car.NewBlockReader(resp.Body)
	if err != nil {
		return nil, err
	}

	return download{resp.Body.Close, stream}, nil
}

type download struct {
	close  func() error
	stream *car.BlockReader
}

func (d download) Next() (blocks.Block, error) {
	b, err := d.stream.Next()
	if err != nil {
		d.close()
	}
	return b, err
}

func (g Gateway) getClient() *http.Client {
	c := g.Client
	if c == nil {
		return http.DefaultClient
	}
	return c
}
