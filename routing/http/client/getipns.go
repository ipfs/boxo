package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"

	ipns "github.com/ipfs/go-ipns"
	"github.com/libp2p/go-libp2p-core/peer"
)

func (fp *Client) GetIPNSRecord(ctx context.Context, id []byte) ([]byte, error) {
	peerID, err := peer.IDFromBytes(id)
	if err != nil {
		return nil, fmt.Errorf("invalid peer ID: %w", err)
	}
	url := path.Join(fp.baseURL, "ipns", peerID.String())
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

	buf := &bytes.Buffer{}
	_, err = io.Copy(buf, resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading IPNS record: %w", err)
	}

	// validate the record
	recordBytes := buf.Bytes()
	err = fp.validator.Validate(ipns.RecordKey(peerID), recordBytes)
	if err != nil {
		return nil, err
	}

	return recordBytes, nil
}
