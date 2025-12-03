package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ipfs/boxo/examples/gateway/common"
	"github.com/ipfs/boxo/gateway"
	"github.com/stretchr/testify/assert"
)

const (
	BaseCID = "bafybeidhua2wpy27vo3t7ms22ybc7m7iqkm2opiebpjmo24lvixcnvznnu"
)

func newTestServer() (*httptest.Server, io.Closer, error) {
	blockService, _, f, err := newBlockServiceFromCAR("./test.car")
	if err != nil {
		return nil, nil, err
	}

	backend, err := gateway.NewBlocksBackend(blockService)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}

	handler := common.NewHandler(backend)
	ts := httptest.NewServer(handler)
	return ts, f, nil
}

func TestDirectoryTraverse(t *testing.T) {
	ts, f, err := newTestServer()
	assert.NoError(t, err)
	defer f.Close()

	res, err := http.Get(ts.URL + "/ipfs/" + BaseCID + "/hello.txt")
	assert.NoError(t, err)

	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	assert.NoError(t, err)
	assert.EqualValues(t, string(body), "hello world\n")
}

func TestFile(t *testing.T) {
	ts, f, err := newTestServer()
	assert.NoError(t, err)
	defer f.Close()

	res, err := http.Get(ts.URL + "/ipfs/bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")
	assert.NoError(t, err)

	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	assert.NoError(t, err)
	assert.EqualValues(t, string(body), "hello world\n")
}

func TestDirectoryAsRawBlock(t *testing.T) {
	ts, f, err := newTestServer()
	assert.NoError(t, err)
	defer f.Close()

	res, err := http.Get(ts.URL + "/ipfs/" + BaseCID + "?format=raw")
	assert.NoError(t, err)
	defer res.Body.Close()

	assert.Equal(t, http.StatusOK, res.StatusCode)

	contentType := res.Header.Get("Content-Type")
	assert.Equal(t, "application/vnd.ipld.raw", contentType)

	body, err := io.ReadAll(res.Body)
	assert.NoError(t, err)

	// Raw bytes of the dag-pb directory block
	expected := []byte{
		0x12, 0x33, 0x0a, 0x24, 0x01, 0x70, 0x12, 0x20, 0xcc, 0x59, 0x55, 0x20,
		0xef, 0xfc, 0x28, 0xd5, 0x74, 0x1f, 0x94, 0x8b, 0x71, 0xac, 0x23, 0x00,
		0xce, 0x22, 0x0d, 0x6f, 0xfd, 0xc6, 0x89, 0xc0, 0x7f, 0x41, 0xda, 0x45,
		0x16, 0xcc, 0xed, 0x05, 0x12, 0x07, 0x65, 0x79, 0x65, 0x2e, 0x70, 0x6e,
		0x67, 0x18, 0xd0, 0xc8, 0x10, 0x12, 0x33, 0x0a, 0x24, 0x01, 0x55, 0x12,
		0x20, 0xa9, 0x48, 0x90, 0x4f, 0x2f, 0x0f, 0x47, 0x9b, 0x8f, 0x81, 0x97,
		0x69, 0x4b, 0x30, 0x18, 0x4b, 0x0d, 0x2e, 0xd1, 0xc1, 0xcd, 0x2a, 0x1e,
		0xc0, 0xfb, 0x85, 0xd2, 0x99, 0xa1, 0x92, 0xa4, 0x47, 0x12, 0x09, 0x68,
		0x65, 0x6c, 0x6c, 0x6f, 0x2e, 0x74, 0x78, 0x74, 0x18, 0x0c, 0x0a, 0x02,
		0x08, 0x01,
	}
	assert.True(t, bytes.Equal(body, expected), "raw block bytes should match")
}
