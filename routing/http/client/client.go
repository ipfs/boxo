package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ipfs/go-cid"
	delegatedrouting "github.com/ipfs/go-delegated-routing"
	ipns "github.com/ipfs/go-ipns"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multicodec"
)

var logger = logging.Logger("service/delegatedrouting")

type Client struct {
	baseURL    string
	httpClient httpClient
	validator  record.Validator

	provider delegatedrouting.Provider
	identity crypto.PrivKey

	// the maximum number of concurrent requests sent for a single Provide request
	maxProvideConcurrency int
	maxProvideBatchSize   int
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type option func(*Client)

func WithIdentity(identity crypto.PrivKey) option {
	return func(c *Client) {
		c.identity = identity
	}
}

func WithMaxProvideConcurrency(max int) option {
	return func(c *Client) {
		c.maxProvideConcurrency = max
	}
}
func WithMaxProvideBatchSize(max int) option {
	return func(c *Client) {
		c.maxProvideBatchSize = max
	}
}

// NewClient creates a client.
// The Provider and identity parameters are option. If they are nil, the `Provide` method will not function.
func New(baseURL string, c httpClient, provider delegatedrouting.Provider, opts ...option) (*Client, error) {
	client := &Client{
		baseURL:               baseURL,
		httpClient:            c,
		validator:             ipns.Validator{},
		provider:              provider,
		maxProvideConcurrency: 5,
		maxProvideBatchSize:   100,
	}

	for _, opt := range opts {
		opt(client)
	}

	if client.identity != nil && !provider.PeerID.MatchesPublicKey(client.identity.GetPublic()) {
		return nil, errors.New("identity does not match provider")
	}

	return client, nil
}

func (fp *Client) FindProviders(ctx context.Context, key cid.Cid) ([]peer.AddrInfo, error) {
	url := fp.baseURL + "/v1/providers/" + key.String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := fp.httpClient.Do(req)
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

func (fp *Client) Provide(ctx context.Context, keys []cid.Cid, ttl time.Duration) (time.Duration, error) {
	keysStrs := make([]string, len(keys))
	for i, c := range keys {
		keysStrs[i] = c.String()
	}
	reqPayload := delegatedrouting.ProvideRequestPayload{
		Keys:        keysStrs,
		AdvisoryTTL: ttl,
		Timestamp:   time.Now().Unix(),
		Provider:    fp.provider,
	}

	reqPayloadBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return 0, err
	}
	reqPayloadEncoded, err := multibase.Encode(multibase.Base64, reqPayloadBytes)
	if err != nil {
		return 0, err
	}

	req := delegatedrouting.ProvideRequest{Payload: reqPayloadEncoded}

	if fp.identity != nil {
		if err := req.Sign(fp.provider.PeerID, fp.identity); err != nil {
			return 0, err
		}
	}

	advisoryTTL, err := fp.provideSignedRecord(ctx, req)
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
func (fp *Client) provideSignedRecord(ctx context.Context, req delegatedrouting.ProvideRequest) (time.Duration, error) {
	if !req.IsSigned() {
		return 0, errors.New("request is not signed")
	}

	url := fp.baseURL + "/v1/providers"

	reqBody, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))

	resp, err := fp.httpClient.Do(httpReq)
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

func (fp *Client) Ready(ctx context.Context) (bool, error) {
	url := fp.baseURL + "/v1/ping"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}

	resp, err := fp.httpClient.Do(req)
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
