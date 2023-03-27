package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/ipfs/boxo/ipns"
	ipns_pb "github.com/ipfs/boxo/ipns/pb"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

type proxyRouting struct {
	gatewayURL string
	httpClient *http.Client
}

func newProxyRouting(gatewayURL string, client *http.Client) routing.ValueStore {
	if client == nil {
		client = http.DefaultClient
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
	u, err := url.Parse(fmt.Sprintf("%s/ipns/%s", ps.gatewayURL, peer.ToCid(id).String()))
	if err != nil {
		return nil, err
	}
	resp, err := ps.httpClient.Do(&http.Request{
		Method: http.MethodGet,
		URL:    u,
		Header: http.Header{
			"Accept": []string{"application/vnd.ipfs.ipns-record"},
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

	var entry ipns_pb.IpnsEntry
	err = proto.Unmarshal(rb, &entry)
	if err != nil {
		return nil, err
	}

	pub, err := id.ExtractPublicKey()
	if err != nil {
		// Make sure it works with all those RSA that cannot be embedded into the
		// Peer ID.
		if len(entry.PubKey) > 0 {
			pub, err = ic.UnmarshalPublicKey(entry.PubKey)
		}
	}
	if err != nil {
		return nil, err
	}

	err = ipns.Validate(pub, &entry)
	if err != nil {
		return nil, err
	}

	return rb, nil
}
