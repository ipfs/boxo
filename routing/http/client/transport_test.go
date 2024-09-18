package client

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testServer struct {
	bytesToWrite int
}

func (s *testServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bytes := make([]byte, s.bytesToWrite)
	for i := 0; i < s.bytesToWrite; i++ {
		bytes[i] = 'a'
	}
	_, err := w.Write(bytes)
	if err != nil {
		panic(err)
	}
}

func TestResponseBodyLimitedTransport(t *testing.T) {
	for _, c := range []struct {
		name       string
		limit      int64
		serverSend int

		expErr string
	}{
		{
			name:       "under the limit should succeed",
			limit:      1 << 20,
			serverSend: 1 << 19,
		},
		{
			name:       "over the limit should fail",
			limit:      1 << 20,
			serverSend: 1 << 21,
			expErr:     "reached read limit of 1048576 bytes after reading",
		},
		{
			name:       "exactly on the limit should succeed",
			limit:      1 << 20,
			serverSend: 1 << 20,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(&testServer{bytesToWrite: c.serverSend})
			t.Cleanup(server.Close)

			client := server.Client()
			client.Transport = &ResponseBodyLimitedTransport{
				LimitBytes:   c.limit,
				RoundTripper: client.Transport,
			}

			resp, err := client.Get(server.URL)
			require.NoError(t, err)
			defer resp.Body.Close()

			_, err = io.ReadAll(resp.Body)

			if c.expErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Contains(t, err.Error(), c.expErr)
			}
		})
	}
}

func TestUserAgentVersionString(t *testing.T) {
	// forks will have to update below lines to pass test
	assert.Equal(t, importPath(), "github.com/ipfs/boxo")
	// @unknown because we run in tests
	assert.Equal(t, moduleVersion(), "github.com/ipfs/boxo@unknown")
}
