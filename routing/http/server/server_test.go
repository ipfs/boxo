package server

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

func TestGetP2PProvideResponseIsValidDAGJSON(t *testing.T) {
	id, err := peer.Decode("QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N")
	if err != nil {
		t.Fatal(err)
	}
	info := peer.AddrInfo{
		ID:    id,
		Addrs: []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/7.7.7.7/tcp/4242")},
	}
	resp := GenerateGetP2PProvideResponse([]peer.AddrInfo{info})
	buf, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	println(string(buf))
	nb := basicnode.Prototype.Any.NewBuilder()
	if err = dagjson.Decode(nb, bytes.NewBuffer(buf)); err != nil {
		t.Errorf("decoding dagjson (%v)", err)
	}
}
