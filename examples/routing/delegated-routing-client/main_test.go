package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindProviders(t *testing.T) {
	cidStr := "bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/routing/v1/providers/"+cidStr {
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.Write([]byte(`{"Schema":"peer","ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn","Addrs":["/ip4/111.222.222.111/tcp/5734"],"Protocols":["transport-bitswap"]}` + "\n"))
			w.Write([]byte(`{"Schema":"peer","ID":"12D3KooWS6BmwfQEZcRqCHCBbDL2DF5a6F7dZnbPFkwmZCuLEK5f","Addrs":["/ip4/127.0.0.1/tcp/6434"],"Protocols":["horse"]}` + "\n")) // this one will be skipped by DefaultProtocolFilter
			w.Write([]byte(`{"Schema":"peer","ID":"12D3KooWB6RAWgcmHAP7TGEGK7utV2ZuqSzX1DNjRa97TtJ7139n","Addrs":["/ip4/127.0.0.1/tcp/5734"],"Protocols":[]}` + "\n"))
		}
	}))
	t.Cleanup(ts.Close)

	out := &bytes.Buffer{}
	err := run(out, ts.URL, cidStr, "", "", time.Second)
	assert.Contains(t, out.String(), "12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn\n\tProtocols: [transport-bitswap]\n\tAddresses: [/ip4/111.222.222.111/tcp/5734]\n")
	assert.Contains(t, out.String(), "12D3KooWB6RAWgcmHAP7TGEGK7utV2ZuqSzX1DNjRa97TtJ7139n\n\tProtocols: []\n\tAddresses: [/ip4/127.0.0.1/tcp/5734]\n")
	assert.NoError(t, err)
}

func TestFindPeers(t *testing.T) {
	pidStr := "bafzaajaiaejcbkboq2tin6dkdc2vinbbn2dgowzn3u5izpjwxejheogw23scafkz"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/routing/v1/peers/"+pidStr {
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.Write([]byte(`{"Schema":"peer","ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn","Addrs":["/ip4/111.222.222.111/tcp/5734"],"Protocols":["transport-bitswap"]}` + "\n"))
		}
	}))
	t.Cleanup(ts.Close)

	out := &bytes.Buffer{}
	err := run(out, ts.URL, "", pidStr, "", time.Second)
	assert.Contains(t, out.String(), "12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn\n\tProtocols: [transport-bitswap]\n\tAddresses: [/ip4/111.222.222.111/tcp/5734]\n")
	assert.NoError(t, err)
}

func TestGetIPNS(t *testing.T) {
	name, rec := makeNameAndRecord(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/routing/v1/ipns/"+name.String() {
			w.Header().Set("Content-Type", "application/vnd.ipfs.ipns-record")
			w.Write(rec)
		}
	}))
	t.Cleanup(ts.Close)

	out := &bytes.Buffer{}
	err := run(out, ts.URL, "", "", name.String(), time.Second)
	assert.Contains(t, out.String(), fmt.Sprintf("/ipns/%s\n\tSignature: VALID\n\tValue: /ipfs/bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4\n", name.String()))
	assert.NoError(t, err)
}

func makeNameAndRecord(t *testing.T) (ipns.Name, []byte) {
	sk, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	pid, err := peer.IDFromPrivateKey(sk)
	require.NoError(t, err)

	cid, err := cid.Decode("bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")
	require.NoError(t, err)

	path := path.FromCid(cid)
	eol := time.Now().Add(time.Hour * 48)
	ttl := time.Second * 20

	record, err := ipns.NewRecord(sk, path, 1, eol, ttl)
	require.NoError(t, err)

	rawRecord, err := ipns.MarshalRecord(record)
	require.NoError(t, err)

	return ipns.NameFromPeer(pid), rawRecord
}
