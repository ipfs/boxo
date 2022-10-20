package client

import (
	"context"
	"encoding/json"
	"net/http"
	"path"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
)

func (p *Provider) UnmarshalJSON(b []byte) error {
	type prov struct {
		Peer      peer.AddrInfo
		Protocols []TransferProtocol
	}
	tempProv := prov{}
	err := json.Unmarshal(b, &tempProv)
	if err != nil {
		return err
	}

	p.Peer = tempProv.Peer
	p.Protocols = tempProv.Protocols

	p.Peer.Addrs = nil
	for _, ma := range tempProv.Peer.Addrs {
		_, last := multiaddr.SplitLast(ma)
		if last != nil && last.Protocol().Code == multiaddr.P_P2P {
			logger.Infof("dropping provider multiaddress %v ending in /p2p/peerid", ma)
			continue
		}
		p.Peer.Addrs = append(p.Peer.Addrs, ma)
	}

	return nil
}

type findProvidersResponse struct {
	Providers []Provider
}

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

	parsedResp := &findProvidersResponse{}
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
