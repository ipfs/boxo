package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/ipfs/go-cid"
	delegatedrouting "github.com/ipfs/go-delegated-routing"
	"github.com/multiformats/go-multibase"
)

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
		if err := req.Sign(fp.provider.Peer.ID, fp.identity); err != nil {
			return 0, err
		}
	}

	advisoryTTL, err := fp.ProvideSignedRecord(ctx, req)
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
func (fp *Client) ProvideSignedRecord(ctx context.Context, req delegatedrouting.ProvideRequest) (time.Duration, error) {
	if !req.IsSigned() {
		return 0, errors.New("request is not signed")
	}

	url := path.Join(fp.baseURL, "providers")

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
