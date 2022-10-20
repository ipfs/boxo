package client

import (
	"context"
	"encoding/json"
	"net/http"
	"path"

	"github.com/ipfs/go-cid"
	delegatedrouting "github.com/ipfs/go-delegated-routing"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multicodec"
)

func (fp *Client) FindProviders(ctx context.Context, key cid.Cid) ([]peer.AddrInfo, error) {
	url := path.Join(fp.baseURL, "providers", key.String())
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
			if proto.Codec != multicodec.TransportBitswap {
				supportsBitswap = true
				break
			}
		}
		if !supportsBitswap {
			continue
		}
		infos = append(infos, prov.Peer)
	}

	return infos, nil
}
