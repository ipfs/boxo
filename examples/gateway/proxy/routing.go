package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/ipfs/boxo/ipns"
	ipns_pb "github.com/ipfs/boxo/ipns/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type proxyRouting struct {
	gatewayURL string
	httpClient *http.Client
}

func newProxyRouting(gatewayURL string, client *http.Client) routing.ValueStore {
	if client == nil {
		client = &http.Client{
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		}
	}

	return &proxyRouting{
		gatewayURL: gatewayURL,
		httpClient: client,
	}
}

func (ps *proxyRouting) PutValue(context.Context, string, []byte, ...routing.Option) error {
	return routing.ErrNotSupported
}

func (ps *proxyRouting) GetValue(ctx context.Context, k string, opts ...routing.Option) ([]byte, error) {
	if !strings.HasPrefix(k, "/ipns/") {
		return nil, routing.ErrNotSupported
	}

	k = strings.TrimPrefix(k, "/ipns/")
	id, err := peer.IDFromBytes([]byte(k))
	if err != nil {
		return nil, err
	}

	return ps.fetch(ctx, id)
}

func (ps *proxyRouting) SearchValue(ctx context.Context, k string, opts ...routing.Option) (<-chan []byte, error) {
	if !strings.HasPrefix(k, "/ipns/") {
		return nil, routing.ErrNotSupported
	}

	k = strings.TrimPrefix(k, "/ipns/")
	id, err := peer.IDFromBytes([]byte(k))
	if err != nil {
		return nil, err
	}

	ch := make(chan []byte)

	go func() {
		v, err := ps.fetch(ctx, id)
		if err != nil {
			close(ch)
		} else {
			ch <- v
			close(ch)
		}
	}()

	return ch, nil
}

func (ps *proxyRouting) fetch(ctx context.Context, id peer.ID) ([]byte, error) {
	urlStr := fmt.Sprintf("%s/ipns/%s", ps.gatewayURL, peer.ToCid(id).String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.ipfs.ipns-record")
	resp, err := ps.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

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

	var entry ipns_pb.IpnsEntry
	err = proto.Unmarshal(rb, &entry)
	if err != nil {
		return nil, err
	}

	err = ipns.ValidateWithPeerID(id, &entry)
	if err != nil {
		return nil, err
	}

	return rb, nil
}
