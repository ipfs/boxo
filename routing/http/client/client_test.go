package client

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/multiformats/go-multiaddr"
)

func TestParseFindProvsResp(t *testing.T) {

	id := "QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N"
	ma1 := base64.RawStdEncoding.EncodeToString(multiaddr.StringCast("/ip4/7.7.7.7/tcp/4242/p2p/" + id).Bytes())
	ma2 := base64.RawStdEncoding.EncodeToString(multiaddr.StringCast("/ip4/8.8.8.8/tcp/4242/p2p/" + id).Bytes())

	// These multiaddrs are incorrect, they're base64padded text multiaddrs instead of base64-unpadded byte multiaddrs
	respStr := fmt.Sprintf(`{"tag" : "get-p2p-provide",
"payload" : {
	"peers" : [
		{"/" : {"bytes" : %q}},
		{"/" : {"bytes" : %q}}
	]
}
}
`, ma1, ma2)
	r := strings.NewReader(respStr)
	ch := make(chan FindProvidersAsyncResult, 2)
	processFindProvidersAsyncResp(context.Background(), ch, r)
	p1, ok := <-ch
	if !ok {
		t.Fatalf("expecting 1st provider")
	}
	if p1.Err != nil {
		t.Fatal(p1.Err)
	}
	if len(p1.AddrInfo) != 2 {
		t.Fatalf("unexpected length")
	}
	if p1.AddrInfo[0].ID.String() != id {
		t.Errorf("got %v, expecting %v", p1.AddrInfo[0].ID.String(), id)
	}
	if len(p1.AddrInfo[0].Addrs) != 1 || p1.AddrInfo[0].Addrs[0].String() != "/ip4/7.7.7.7/tcp/4242" {
		t.Errorf("unexpecting address")
	}
	if len(p1.AddrInfo[1].Addrs) != 1 || p1.AddrInfo[1].Addrs[0].String() != "/ip4/8.8.8.8/tcp/4242" {
		t.Errorf("unexpecting address")
	}
}
