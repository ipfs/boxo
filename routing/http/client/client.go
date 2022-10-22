package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ipfs/go-cid"
	delegatedrouting "github.com/ipfs/go-delegated-routing"
	"github.com/ipfs/go-delegated-routing/internal"
	ipns "github.com/ipfs/go-ipns"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/multiformats/go-multicodec"
)

var logger = logging.Logger("service/delegatedrouting")

type client struct {
	baseURL    string
	httpClient httpClient
	validator  record.Validator
	clock      clock.Clock

	provider delegatedrouting.Provider
	identity crypto.PrivKey
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

func WithProvider(p delegatedrouting.Provider) option {
	return func(c *client) {
		c.provider = p
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

	if client.identity != nil && client.provider.PeerID.Size() != 0 && !client.provider.PeerID.MatchesPublicKey(client.identity.GetPublic()) {
		return nil, errors.New("identity does not match provider")
	}

	return client, nil
}

func (c *client) FindProviders(ctx context.Context, key cid.Cid) ([]peer.AddrInfo, error) {
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

	parsedResp := &delegatedrouting.FindProvidersResult{}
	err = json.NewDecoder(resp.Body).Decode(parsedResp)
	if err != nil {
		return nil, err
	}

	infos := []peer.AddrInfo{}
	for _, prov := range parsedResp.Providers {
		supportsBitswap := false
		for _, proto := range prov.Protocols {
			if proto.Codec == multicodec.TransportBitswap {
				supportsBitswap = true
				break
			}
		}
		if !supportsBitswap {
			continue
		}
		infos = append(infos, peer.AddrInfo{
			ID:    prov.PeerID,
			Addrs: prov.Addrs,
		})
	}
	return infos, nil
}

func (c *client) Provide(ctx context.Context, keys []cid.Cid, ttl time.Duration) (time.Duration, error) {
	if c.identity == nil {
		return 0, errors.New("cannot Provide without an identity")
	}
	if c.provider.PeerID.Size() == 0 {
		return 0, errors.New("cannot Provide without a provider")
	}

	keysStrs := make([]string, len(keys))
	for i, c := range keys {
		keysStrs[i] = c.String()
	}
	reqPayload := delegatedrouting.ProvideRequestPayload{
		Keys:        keysStrs,
		AdvisoryTTL: ttl,
		Timestamp:   c.clock.Now().UnixMilli(),
		Provider:    c.provider,
	}

	req := delegatedrouting.ProvideRequest{}
	err := req.SetPayload(reqPayload)
	if err != nil {
		return 0, fmt.Errorf("setting payload: %w", err)
	}

	err = req.Sign(c.provider.PeerID, c.identity)
	if err != nil {
		return 0, err
	}

	advisoryTTL, err := c.provideSignedRecord(ctx, req)
	if err != nil {
		return 0, err
	}

	return advisoryTTL, err
}

type provideRequest struct {
	Keys      []cid.Cid
	Protocols map[string]interface{}
}

// ProvideAsync makes a provide request to a delegated router
func (c *client) provideSignedRecord(ctx context.Context, req delegatedrouting.ProvideRequest) (time.Duration, error) {
	if !req.IsSigned() {
		return 0, errors.New("request is not signed")
	}

	url := c.baseURL + "/v1/providers"

	reqBodyBuf, err := internal.MarshalJSON(req)
	if err != nil {
		return 0, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, reqBodyBuf)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return 0, fmt.Errorf("making HTTP req to provide a signed record: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, httpError(resp.StatusCode, resp.Body)
	}

	provideResult := delegatedrouting.ProvideResult{}
	err = json.NewDecoder(resp.Body).Decode(&provideResult)
	return provideResult.AdvisoryTTL, err
}

func (c *client) Ready(ctx context.Context) (bool, error) {
	url := c.baseURL + "/v1/ping"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusServiceUnavailable {
		return false, nil
	}
	return false, fmt.Errorf("unexpected HTTP status code '%d'", resp.StatusCode)
}
