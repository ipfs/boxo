package feather_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/gateway"
	"github.com/ipfs/boxo/unixfs/feather"
	"github.com/ipfs/go-cid"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
	"github.com/stretchr/testify/assert"
)

func newGateway(t *testing.T, fixture string) (*httptest.Server, cid.Cid) {
	t.Helper()

	r, err := os.Open(filepath.Join("./testdata", fixture))
	assert.NoError(t, err)

	blockStore, err := carblockstore.NewReadOnly(r, nil)
	assert.NoError(t, err)

	t.Cleanup(func() {
		blockStore.Close()
		r.Close()
	})

	cids, err := blockStore.Roots()
	assert.NoError(t, err)
	assert.Len(t, cids, 1)

	blockService := blockservice.New(blockStore, offline.Exchange(blockStore))

	backend, err := gateway.NewBlocksBackend(blockService)
	assert.NoError(t, err)

	handler := gateway.NewHandler(gateway.Config{}, backend)

	ts := httptest.NewServer(handler)
	t.Cleanup(func() { ts.Close() })

	return ts, cids[0]
}

func newFeather(t *testing.T, fixture string) (*feather.Client, cid.Cid) {
	t.Helper()

	gw, cid := newGateway(t, fixture)
	f, err := feather.NewClient(feather.WithHTTPClient(gw.Client()), feather.WithStaticGateway(gw.URL))
	assert.NoError(t, err)
	return f, cid
}

func mustParseHex(s string) []byte {
	v, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return v
}

func TestFileWithManyRawLeaves(t *testing.T) {
	f, root := newFeather(t, "file-with-many-raw-leaves.car")
	file, err := f.DownloadFile(root)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, file.Close()) }()
	h := sha256.New()
	_, err = io.Copy(h, file)
	assert.NoError(t, err)

	if !bytes.Equal(h.Sum(nil), mustParseHex("5e38d403b548e38fe350410347f6310b757203b19be6cd5323ec3ca56404b387")) {
		t.Error("decoded content does not match expected")
	}
}
