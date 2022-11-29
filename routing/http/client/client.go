package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ipfs/go-cid"
	delegatedrouting "github.com/ipfs/go-delegated-routing"
	ipns "github.com/ipfs/go-ipns"
	logging "github.com/ipfs/go-log/v2"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

var logger = logging.Logger("service/delegatedrouting")

type client struct {
	baseURL    string
	httpClient httpClient
	validator  record.Validator
	clock      clock.Clock

	peerID   peer.ID
	addrs    []delegatedrouting.Multiaddr
	identity crypto.PrivKey

	// called immeidately after signing a provide req
	// used for testing, e.g. testing the server with a mangled signature
	afterSignCallback func(req *delegatedrouting.BitswapWriteProviderRequest)
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type option func(*client)

func WithIdentity(identity crypto.PrivKey) option {
	return func(c *client) {
		c.identity = identity
	}
}

func WithHTTPClient(h httpClient) option {
	return func(c *client) {
		c.httpClient = h
	}
}

func WithProviderInfo(peerID peer.ID, addrs []multiaddr.Multiaddr) option {
	return func(c *client) {
		c.peerID = peerID
		for _, a := range addrs {
			c.addrs = append(c.addrs, delegatedrouting.Multiaddr{Multiaddr: a})
		}
	}
}

// New creates a content routing API client.
// The Provider and identity parameters are option. If they are nil, the `Provide` method will not function.
func New(baseURL string, opts ...option) (*client, error) {
	client := &client{
		baseURL:    baseURL,
		httpClient: http.DefaultClient,
		validator:  ipns.Validator{},
		clock:      clock.New(),
	}

	for _, opt := range opts {
		opt(client)
	}

	if client.identity != nil && client.peerID.Size() != 0 && !client.peerID.MatchesPublicKey(client.identity.GetPublic()) {
		return nil, errors.New("identity does not match provider")
	}

	return client, nil
}

func (c *client) FindProviders(ctx context.Context, key cid.Cid) ([]delegatedrouting.Provider, error) {
	url := c.baseURL + "/v1/providers/" + key.String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, httpError(resp.StatusCode, resp.Body)
	}

	parsedResp := &delegatedrouting.FindProvidersResponse{}
	err = json.NewDecoder(resp.Body).Decode(parsedResp)
	return parsedResp.Providers, err
}

func (c *client) ProvideBitswap(ctx context.Context, keys []cid.Cid, ttl time.Duration) (time.Duration, error) {
	if c.identity == nil {
		return 0, errors.New("cannot provide Bitswap records without an identity")
	}
	if c.peerID.Size() == 0 {
		return 0, errors.New("cannot provide Bitswap records without a peer ID")
	}

	ks := make([]delegatedrouting.CID, len(keys))
	for i, c := range keys {
		ks[i] = delegatedrouting.CID{Cid: c}
	}

	now := c.clock.Now()

	req := delegatedrouting.BitswapWriteProviderRequest{
		Protocol: "bitswap",
		Payload: delegatedrouting.BitswapWriteProviderRequestPayload{
			Keys:        ks,
			AdvisoryTTL: &delegatedrouting.Duration{Duration: ttl},
			Timestamp:   &delegatedrouting.Time{Time: now},
			ID:          &c.peerID,
			Addrs:       c.addrs,
		},
	}
	err := req.Sign(c.peerID, c.identity)
	if err != nil {
		return 0, err
	}

	if c.afterSignCallback != nil {
		c.afterSignCallback(&req)
	}

	advisoryTTL, err := c.provideSignedBitswapRecord(ctx, &req)
	if err != nil {
		return 0, err
	}

	return advisoryTTL, err
}

// ProvideAsync makes a provide request to a delegated router
func (c *client) provideSignedBitswapRecord(ctx context.Context, bswp *delegatedrouting.BitswapWriteProviderRequest) (time.Duration, error) {
	req := delegatedrouting.WriteProvidersRequest{Providers: []delegatedrouting.Provider{bswp}}

	url := c.baseURL + "/v1/providers"

	b, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(b))
	if err != nil {
		return 0, err
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return 0, fmt.Errorf("making HTTP req to provide a signed record: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, httpError(resp.StatusCode, resp.Body)
	}
	provideResult := delegatedrouting.WriteProvidersResponse{Protocols: []string{"bitswap"}}
	err = json.NewDecoder(resp.Body).Decode(&provideResult)
	if err != nil {
		return 0, err
	}
	if len(provideResult.ProvideResults) != 1 {
		return 0, fmt.Errorf("expected 1 result but got %d", len(provideResult.ProvideResults))
	}

	return provideResult.ProvideResults[0].(*delegatedrouting.BitswapWriteProviderResponse).AdvisoryTTL, nil
}
