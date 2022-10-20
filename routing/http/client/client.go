package client

import (
	"errors"
	"net/http"

	delegatedrouting "github.com/ipfs/go-delegated-routing"
	ipns "github.com/ipfs/go-ipns"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/crypto"
	record "github.com/libp2p/go-libp2p-record"
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
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// TODO: batch Provide and PutIPNS into batches of at most 100

// NewClient creates a client.
// The Provider and identity parameters are option. If they are nil, the `Provide` method will not function.
func NewClient(baseURL string, c httpClient, p delegatedrouting.Provider, identity crypto.PrivKey) (*Client, error) {
	if !p.Peer.ID.MatchesPublicKey(identity.GetPublic()) {
		return nil, errors.New("identity does not match provider")
	}

	return &Client{
		baseURL:               baseURL,
		httpClient:            c,
		validator:             ipns.Validator{},
		provider:              p,
		identity:              identity,
		maxProvideConcurrency: 5,
	}, nil
}
