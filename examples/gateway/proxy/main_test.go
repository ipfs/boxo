package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ipfs/boxo/blocks"
	"github.com/ipfs/boxo/examples/gateway/common"
	"github.com/ipfs/go-blockservice"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/stretchr/testify/assert"
)

const (
	HelloWorldCID = "bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e"
)

func newProxyGateway(t *testing.T, rs *httptest.Server) *httptest.Server {
	blockStore := newProxyStore(rs.URL, nil)
	blockService := blockservice.New(blockStore, offline.Exchange(blockStore))
	routing := newProxyRouting(rs.URL, nil)

	gateway, err := common.NewBlocksGateway(blockService, routing)
	if err != nil {
		t.Error(err)
	}

	handler := common.NewBlocksHandler(gateway, 0)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	return ts
}

func TestErrorOnInvalidContent(t *testing.T) {
	rs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("wrong data"))
	}))
	t.Cleanup(rs.Close)
	ts := newProxyGateway(t, rs)

	res, err := http.Get(ts.URL + "/ipfs/" + HelloWorldCID)
	assert.Nil(t, err)

	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	assert.Nil(t, err)
	assert.EqualValues(t, res.StatusCode, http.StatusInternalServerError)
	assert.Contains(t, string(body), blocks.ErrWrongHash.Error())
}

func TestPassOnOnCorrectContent(t *testing.T) {
	rs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello world"))
	}))
	t.Cleanup(rs.Close)
	ts := newProxyGateway(t, rs)

	res, err := http.Get(ts.URL + "/ipfs/" + HelloWorldCID)
	assert.Nil(t, err)

	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	assert.Nil(t, err)
	assert.EqualValues(t, res.StatusCode, http.StatusOK)
	assert.EqualValues(t, string(body), "hello world")
}
