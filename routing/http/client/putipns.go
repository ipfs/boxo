package client

import (
	"context"

	"github.com/ipfs/go-delegated-routing/gen/proto"
)

func (fp *Client) PutIPNS(ctx context.Context, id []byte, record []byte) error {
	_, err := fp.client.PutIPNS(ctx, &proto.PutIPNSRequest{ID: id, Record: record})
	if err != nil {
		return err
	}
	return nil
}

type PutIPNSAsyncResult struct {
	Err error
}

func (fp *Client) PutIPNSAsync(ctx context.Context, id []byte, record []byte) (<-chan PutIPNSAsyncResult, error) {
	ch0, err := fp.client.PutIPNS_Async(ctx, &proto.PutIPNSRequest{ID: id, Record: record})
	if err != nil {
		return nil, err
	}
	ch1 := make(chan PutIPNSAsyncResult, 1)
	go func() {
		defer close(ch1)
		for {
			select {
			case <-ctx.Done():
				return
			case r0, ok := <-ch0:
				if !ok {
					return
				}

				ch1 <- PutIPNSAsyncResult{
					Err: r0.Err,
				}
			}
		}
	}()

	return ch1, nil
}
