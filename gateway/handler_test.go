package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEtagMatch(t *testing.T) {
	for _, test := range []struct {
		header   string // value in If-None-Match HTTP header
		cidEtag  string
		dirEtag  string
		expected bool // expected result of etagMatch(header, cidEtag, dirEtag)
	}{
		{"", `"etag"`, "", false},                        // no If-None-Match
		{"", "", `"etag"`, false},                        // no If-None-Match
		{`"etag"`, `"etag"`, "", true},                   // file etag match
		{`W/"etag"`, `"etag"`, "", true},                 // file etag match
		{`"foo", W/"bar", W/"etag"`, `"etag"`, "", true}, // file etag match (array)
		{`"foo",W/"bar",W/"etag"`, `"etag"`, "", true},   // file etag match (compact array)
		{`"etag"`, "", `W/"etag"`, true},                 // dir etag match
		{`"etag"`, "", `W/"etag"`, true},                 // dir etag match
		{`W/"etag"`, "", `W/"etag"`, true},               // dir etag match
		{`*`, `"etag"`, "", true},                        // wildcard etag match
	} {
		result := etagMatch(test.header, test.cidEtag, test.dirEtag)
		assert.Equalf(t, test.expected, result, "etagMatch(%q, %q, %q)", test.header, test.cidEtag, test.dirEtag)
	}
}



func TestWithResponseWriteTimeout(t *testing.T) {
	tests := []struct {
		name           string
		handler        http.HandlerFunc // Test scenario handler implementation
		timeout        time.Duration    // Timeout configuration for middleware
		expectStatus   int              // Anticipated HTTP response code
		expectedChunks int              // Expected number of completed writes
	}{
		{
			name: "Normal completion - write within timeout",
			handler: func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-time.After(100 * time.Millisecond):
					w.Write([]byte("chunk\n"))
				case <-r.Context().Done():
				}
			},
			timeout:        300 * time.Millisecond,
			expectStatus:   http.StatusOK,
			expectedChunks: 1,
		},
		{
			name: "Timeout triggered - write after deadline",
			handler: func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-time.After(400 * time.Millisecond): // Exceeds 300ms timeout
					w.Write([]byte("chunk\n")) // Should be post-timeout
				case <-r.Context().Done():
				}
			},
			timeout:        300 * time.Millisecond,
			expectStatus:   http.StatusGatewayTimeout,
			expectedChunks: 0,
		},
		{
			name: "Timer reset with staggered writes",
			handler: func(w http.ResponseWriter, r *http.Request) {
				for i := 0; i < 3; i++ {
					select {
					case <-time.After(200 * time.Millisecond): // Each write within timeout window
						w.Write([]byte("chunk\n")) // Resets timer on each write
					case <-r.Context().Done():
						return
					}
				}
			},
			timeout:        300 * time.Millisecond,
			expectStatus:   http.StatusOK,
			expectedChunks: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize test components
			req := httptest.NewRequest("GET", "http://example.com", nil)
			rec := httptest.NewRecorder()

			// Configure middleware chain
			wrappedHandler := WithResponseWriteTimeout(tt.handler, tt.timeout)
			wrappedHandler.ServeHTTP(rec, req)

			// Validate HTTP status code
			if rec.Code != tt.expectStatus {
				t.Errorf("Status code validation failed: expected %d, received %d",
					tt.expectStatus, rec.Code)
			}

			// Verify response body integrity
			actualChunks := strings.Count(rec.Body.String(), "chunk")
			if actualChunks != tt.expectedChunks {
				t.Errorf("Data chunk mismatch: anticipated %d, obtained %d",
					tt.expectedChunks, actualChunks)
			}

			// Ensure timeout responses don't set headers
			if tt.expectStatus == http.StatusGatewayTimeout {
				if contentType := rec.Header().Get("Content-Type"); contentType != "" {
					t.Errorf("Timeout response contains unexpected Content-Type: %s", contentType)
				}
			}
		})
	}
}
