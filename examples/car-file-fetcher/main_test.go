package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	HelloWorldCID = "bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e"
)

func TestErrorOnInvalidContent(t *testing.T) {
	rs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("wrong data"))
	}))
	t.Cleanup(rs.Close)

	err := fetch(rs.URL, "/ipfs/"+HelloWorldCID, "hello.txt", "", 0)
	require.Error(t, err)
}

func TestSuccessOnValidContent(t *testing.T) {
	data, err := os.ReadFile("./hello.car")
	require.NoError(t, err)

	rs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(data)
	}))
	t.Cleanup(rs.Close)

	err = fetch(rs.URL, "/ipfs/"+HelloWorldCID, filepath.Join(t.TempDir(), "hello.txt"), "", 0)
	require.NoError(t, err)
}
