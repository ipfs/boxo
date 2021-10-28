package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-delegated-routing/parser"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

var logger = logging.Logger("delegated-routing-client")

func (c *client) FindProviders(ctx context.Context, cid cid.Cid) ([]peer.AddrInfo, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := c.FindProvidersAsync(ctx, cid)
	if err != nil {
		return nil, err
	}
	var infos []peer.AddrInfo
	for {
		select {
		case r, ok := <-ch:
			if !ok {
				cancel()
				return infos, nil
			} else {
				if r.Err == nil {
					infos = append(infos, r.AddrInfo...)
				}
			}
		case <-ctx.Done():
			return infos, ctx.Err()
		}
	}
}

type FindProvidersAsyncResult struct {
	AddrInfo []peer.AddrInfo
	Err      error
}

func (c *client) FindProvidersAsync(ctx context.Context, cid cid.Cid) (<-chan FindProvidersAsyncResult, error) {
	req := parser.Envelope{
		Tag: parser.MethodGetP2PProvide,
		Payload: parser.GetP2PProvideRequest{
			Key: parser.ToDJSpecialBytes(cid.Hash()),
		},
	}
	b := &bytes.Buffer{}
	if err := json.NewEncoder(b).Encode(req); err != nil {
		return nil, err
	}

	// encode request in URL
	// url := fmt.Sprintf("%s?q=%s", c.endPoint, url.QueryEscape(b.String()))
	u := *c.endpoint
	q := url.Values{}
	q.Set("q", b.String())
	u.RawQuery = q.Encode()
	httpReq, err := http.NewRequestWithContext(ctx, "GET", u.String(), b)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, err
	}

	ch := make(chan FindProvidersAsyncResult, 1)
	go processFindProvidersAsyncResp(ctx, ch, resp.Body)
	return ch, nil
}

func processFindProvidersAsyncResp(ctx context.Context, ch chan<- FindProvidersAsyncResult, r io.Reader) {
	defer close(ch)
	for {
		if ctx.Err() != nil {
			return
		}

		dec := json.NewDecoder(r)
		env := parser.Envelope{Payload: &parser.GetP2PProvideResponse{}}
		err := dec.Decode(&env)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			ch <- FindProvidersAsyncResult{Err: err}
			return
		}

		if env.Tag != parser.MethodGetP2PProvide {
			continue
		}

		provResp, ok := env.Payload.(*parser.GetP2PProvideResponse)
		if !ok {
			logger.Infof("possibly talking to a newer server, unknown response %v", env.Payload)
			continue
		}

		infos := []peer.AddrInfo{}
		for _, maBytes := range provResp.Peers {
			addrBytes, err := parser.FromDJSpecialBytes(maBytes)
			if err != nil {
				logger.Infof("cannot decode address bytes (%v)", err)
				continue
			}
			ma, err := multiaddr.NewMultiaddrBytes(addrBytes)
			if err != nil {
				logger.Infof("cannot parse multiaddress (%v)", err)
				continue
			}
			ai, err := peer.AddrInfoFromP2pAddr(ma)
			if err != nil {
				logger.Infof("cannot parse peer from multiaddress (%v)", err)
				continue
			}
			infos = append(infos, *ai)
		}
		ch <- FindProvidersAsyncResult{AddrInfo: infos}
	}
}
