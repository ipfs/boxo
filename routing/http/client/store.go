package client

import (
	"context"

	"github.com/libp2p/go-libp2p-core/routing"
)

var _ routing.ValueStore = &Client{}

// PutValue adds value corresponding to given Key.
func (c *Client) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) error {
	return c.PutIPNS(ctx, []byte(key), val)
}

// GetValue searches for the value corresponding to given Key.
func (c *Client) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	return c.GetIPNS(ctx, []byte(key))
}

// SearchValue searches for better and better values from this value
// store corresponding to the given Key. By default implementations must
// stop the search after a good value is found. A 'good' value is a value
// that would be returned from GetValue.
//
// Useful when you want a result *now* but still want to hear about
// better/newer results.
//
// Implementations of this methods won't return ErrNotFound. When a value
// couldn't be found, the channel will get closed without passing any results
func (c *Client) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	resChan, err := c.GetIPNSAsync(ctx, []byte(key))
	if err != nil {
		return nil, err
	}

	outCh := make(chan []byte, 1)
	go func() {
		defer close(outCh)
		for {
			select {
			case <-ctx.Done():
				return
			case r, ok := <-resChan:
				if !ok {
					return
				}

				if r.Err != nil {
					continue
				}

				outCh <- r.Record
			}
		}
	}()

	return outCh, nil
}
