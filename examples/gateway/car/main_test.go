package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ipfs/boxo/examples/gateway/common"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/basicnode"
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

	gateway, err := common.NewBlocksGateway(blockService, nil)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}

	handler := common.NewBlocksHandler(gateway, 0)
	ts := httptest.NewServer(handler)
	return ts, f, nil
}

func TestDirectoryTraverse(t *testing.T) {
	ts, f, err := newTestServer()
	assert.Nil(t, err)
	defer f.Close()

	res, err := http.Get(ts.URL + "/ipfs/" + BaseCID + "/hello.txt")
	assert.Nil(t, err)

	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	assert.Nil(t, err)
	assert.EqualValues(t, string(body), "hello world\n")
}

func TestFile(t *testing.T) {
	ts, f, err := newTestServer()
	assert.Nil(t, err)
	defer f.Close()

	res, err := http.Get(ts.URL + "/ipfs/bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")
	assert.Nil(t, err)

	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	assert.Nil(t, err)
	assert.EqualValues(t, string(body), "hello world\n")
}

func TestDirectoryAsDAG(t *testing.T) {
	ts, f, err := newTestServer()
	assert.Nil(t, err)
	defer f.Close()

	res, err := http.Get(ts.URL + "/ipfs/" + BaseCID + "?format=dag-json")
	assert.Nil(t, err)
	defer res.Body.Close()

	contentType := res.Header.Get("Content-Type")
	assert.EqualValues(t, contentType, "application/vnd.ipld.dag-json")

	// Parses the DAG-JSON response.
	dag := basicnode.Prototype.Any.NewBuilder()
	err = dagjson.Decode(dag, res.Body)
	assert.Nil(t, err)

	// Checks for the links inside the logical model.
	links, err := dag.Build().LookupByString("Links")
	assert.Nil(t, err)

	// Checks if there are 2 links.
	assert.EqualValues(t, links.Length(), 2)

	// Check if the first item is correct.
	n, err := links.LookupByIndex(0)
	assert.Nil(t, err)
	assert.NotNil(t, n)

	nameNode, err := n.LookupByString("Name")
	assert.Nil(t, err)
	assert.NotNil(t, nameNode)

	name, err := nameNode.AsString()
	assert.Nil(t, err)
	assert.EqualValues(t, name, "eye.png")

	hashNode, err := n.LookupByString("Hash")
	assert.Nil(t, err)
	assert.NotNil(t, hashNode)

	hash, err := hashNode.AsLink()
	assert.Nil(t, err)
	assert.EqualValues(t, hash.String(), "bafybeigmlfksb374fdkxih4urny2yiyazyra2375y2e4a72b3jcrnthnau")
}
