package client

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"path"

	"github.com/libp2p/go-libp2p-core/peer"
)

func (fp *Client) PutIPNSRecord(ctx context.Context, id []byte, record []byte) error {
	_, err := peer.IDFromBytes(id)
	if err != nil {
		return fmt.Errorf("invalid peer ID: %w", err)
	}
	url := path.Join(fp.baseURL, "ipns")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(record))
	if err != nil {
		return err
	}
	resp, err := fp.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return httpError(resp.StatusCode, resp.Body)
	}

	return nil
}
