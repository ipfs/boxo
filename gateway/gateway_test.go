package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/namesys"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/path/resolver"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGatewayGet(t *testing.T) {
	ts, backend, root := newTestServerAndNode(t, "fixtures.car")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p, err := path.Join(path.FromCid(root), "subdir", "fnord")
	require.NoError(t, err)

	k, err := backend.resolvePathNoRootsReturned(ctx, p)
	require.NoError(t, err)

	mustMakeDNSLinkPath := func(domain string) path.Path {
		p, err := path.NewPath("/ipns/" + domain)
		require.NoError(t, err)
		return p
	}

	backend.namesys["/ipns/example.com"] = newMockNamesysItem(path.FromCid(k.RootCid()), 0)
	backend.namesys["/ipns/working.example.com"] = newMockNamesysItem(k, 0)
	backend.namesys["/ipns/double.example.com"] = newMockNamesysItem(mustMakeDNSLinkPath("working.example.com"), 0)
	backend.namesys["/ipns/triple.example.com"] = newMockNamesysItem(mustMakeDNSLinkPath("double.example.com"), 0)
	backend.namesys["/ipns/broken.example.com"] = newMockNamesysItem(mustMakeDNSLinkPath(k.RootCid().String()), 0)
	// We picked .man because:
	// 1. It's a valid TLD.
	// 2. Go treats it as the file extension for "man" files (even though
	//    nobody actually *uses* this extension, AFAIK).
	//
	// Unfortunately, this may not work on all platforms as file type
	// detection is platform dependent.
	backend.namesys["/ipns/example.man"] = newMockNamesysItem(k, 0)

	for _, test := range []struct {
		host   string
		path   string
		status int
		text   string
	}{
		{"127.0.0.1:8080", "/", http.StatusNotFound, "404 page not found\n"},
		{"127.0.0.1:8080", "/ipfs", http.StatusBadRequest, "invalid path \"/ipfs/\": path does not have enough components\n"},
		{"127.0.0.1:8080", "/ipns", http.StatusBadRequest, "invalid path \"/ipns/\": path does not have enough components\n"},
		{"127.0.0.1:8080", "/" + k.RootCid().String(), http.StatusNotFound, "404 page not found\n"},
		{"127.0.0.1:8080", "/ipfs/this-is-not-a-cid", http.StatusBadRequest, "invalid path \"/ipfs/this-is-not-a-cid\": invalid cid: illegal base32 data at input byte 3\n"},
		{"127.0.0.1:8080", k.String(), http.StatusOK, "fnord"},
		{"127.0.0.1:8080", "/ipns/nxdomain.example.com", http.StatusInternalServerError, "failed to resolve /ipns/nxdomain.example.com: " + namesys.ErrResolveFailed.Error() + "\n"},
		{"127.0.0.1:8080", "/ipns/%0D%0A%0D%0Ahello", http.StatusInternalServerError, "failed to resolve /ipns/\\r\\n\\r\\nhello: " + namesys.ErrResolveFailed.Error() + "\n"},
		{"127.0.0.1:8080", "/ipns/k51qzi5uqu5djucgtwlxrbfiyfez1nb0ct58q5s4owg6se02evza05dfgi6tw5", http.StatusInternalServerError, "failed to resolve /ipns/k51qzi5uqu5djucgtwlxrbfiyfez1nb0ct58q5s4owg6se02evza05dfgi6tw5: " + namesys.ErrResolveFailed.Error() + "\n"},
		{"127.0.0.1:8080", "/ipns/example.com", http.StatusOK, "fnord"},
		{"example.com", "/", http.StatusOK, "fnord"},

		{"working.example.com", "/", http.StatusOK, "fnord"},
		{"double.example.com", "/", http.StatusOK, "fnord"},
		{"triple.example.com", "/", http.StatusOK, "fnord"},
		{"working.example.com", k.String(), http.StatusNotFound, "failed to resolve /ipns/working.example.com" + k.String() + ": no link named \"ipfs\" under " + k.RootCid().String() + "\n"},
		{"broken.example.com", "/", http.StatusInternalServerError, "failed to resolve /ipns/broken.example.com/: " + namesys.ErrResolveFailed.Error() + "\n"},
		{"broken.example.com", k.String(), http.StatusInternalServerError, "failed to resolve /ipns/broken.example.com" + k.String() + ": " + namesys.ErrResolveFailed.Error() + "\n"},
		// This test case ensures we don't treat the TLD as a file extension.
		{"example.man", "/", http.StatusOK, "fnord"},
	} {
		testName := "http://" + test.host + test.path
		t.Run(testName, func(t *testing.T) {
			req := mustNewRequest(t, http.MethodGet, ts.URL+test.path, nil)
			req.Host = test.host
			resp := mustDo(t, req)
			defer resp.Body.Close()
			require.Equal(t, "text/plain; charset=utf-8", resp.Header.Get("Content-Type"))
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, test.status, resp.StatusCode, "body", body)
			require.Equal(t, test.text, string(body))
		})
	}
}

func TestHeaders(t *testing.T) {
	t.Parallel()

	ts, backend, root := newTestServerAndNode(t, "headers-test.car")

	var (
		rootCID = "bafybeidbcy4u6y55gsemlubd64zk53xoxs73ifd6rieejxcr7xy46mjvky"

		dirCID   = "bafybeihta5xfgxcmyxyq6druvidc7es6ogffdd6zel22l3y4wddju5xxsu"
		dirPath  = "/ipfs/" + rootCID + "/subdir/"
		dirRoots = rootCID + "," + dirCID

		hamtFileCID   = "bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"
		hamtFilePath  = "/ipfs/" + rootCID + "/hamt/685.txt"
		hamtFileRoots = rootCID + ",bafybeidbclfqleg2uojchspzd4bob56dqetqjsj27gy2cq3klkkgxtpn4i," + hamtFileCID

		fileCID   = "bafkreiba3vpkcqpc6xtp3hsatzcod6iwneouzjoq7ymy4m2js6gc3czt6i"
		filePath  = "/ipfs/" + rootCID + "/subdir/fnord"
		fileRoots = dirRoots + "," + fileCID

		dagCborCID   = "bafyreiaocls5bt2ha5vszv5pwz34zzcdf3axk3uqa56bgsgvlkbezw67hq"
		dagCborPath  = "/ipfs/" + rootCID + "/subdir/dag-cbor-document"
		dagCborRoots = dirRoots + "," + dagCborCID
	)

	t.Run("Cache-Control uses TTL for /ipns/ when it is known", func(t *testing.T) {
		t.Parallel()

		ts, backend, root := newTestServerAndNode(t, "ipns-hostname-redirects.car")
		backend.namesys["/ipns/example.net"] = newMockNamesysItem(path.FromCid(root), time.Second*30)
		backend.namesys["/ipns/example.com"] = newMockNamesysItem(path.FromCid(root), time.Second*55)
		backend.namesys["/ipns/unknown.com"] = newMockNamesysItem(path.FromCid(root), 0)

		testCases := []struct {
			path         string
			cacheControl string
		}{
			{"/ipns/example.net/", "public, max-age=30, stale-while-revalidate=2678400"}, // As generated directory listing
			{"/ipns/example.com/", "public, max-age=55, stale-while-revalidate=2678400"}, // As generated directory listing (different)
			{"/ipns/unknown.com/", ""},                                   // As generated directory listing (unknown TTL)
			{"/ipns/example.net/foo/", "public, max-age=30"},             // As index.html directory listing
			{"/ipns/example.net/foo/index.html", "public, max-age=30"},   // As deserialized UnixFS file
			{"/ipns/example.net/?format=raw", "public, max-age=30"},      // As Raw block
			{"/ipns/example.net/?format=dag-json", "public, max-age=30"}, // As DAG-JSON block
			{"/ipns/example.net/?format=dag-cbor", "public, max-age=30"}, // As DAG-CBOR block
			{"/ipns/example.net/?format=car", "public, max-age=30"},      // As CAR block
		}

		for _, testCase := range testCases {
			req := mustNewRequest(t, http.MethodGet, ts.URL+testCase.path, nil)
			res := mustDoWithoutRedirect(t, req)
			if testCase.cacheControl == "" {
				assert.Empty(t, res.Header["Cache-Control"])
			} else {
				assert.Equal(t, testCase.cacheControl, res.Header.Get("Cache-Control"))
			}
		}
	})

	t.Run("Cache-Control is not immutable on generated /ipfs/ HTML dir listings", func(t *testing.T) {
		req := mustNewRequest(t, http.MethodGet, ts.URL+"/ipfs/"+rootCID+"/", nil)
		res := mustDoWithoutRedirect(t, req)

		// check the immutable tag isn't set
		hdrs, ok := res.Header["Cache-Control"]
		if ok {
			for _, hdr := range hdrs {
				assert.NotContains(t, hdr, "immutable", "unexpected Cache-Control: immutable on directory listing")
			}
		}
	})

	t.Run("ETag is based on CID and response format", func(t *testing.T) {
		test := func(responseFormat string, path string, format string, args ...any) {
			t.Run(responseFormat, func(t *testing.T) {
				url := ts.URL + path
				req := mustNewRequest(t, http.MethodGet, url, nil)
				req.Header.Add("Accept", responseFormat)
				res := mustDoWithoutRedirect(t, req)
				_, err := io.Copy(io.Discard, res.Body)
				require.NoError(t, err)
				defer res.Body.Close()
				require.Equal(t, http.StatusOK, res.StatusCode)
				require.Regexp(t, `^`+fmt.Sprintf(format, args...)+`$`, res.Header.Get("Etag"))
			})
		}
		test("", dirPath, `"DirIndex-(.*)_CID-%s"`, dirCID)
		test("text/html", dirPath, `"DirIndex-(.*)_CID-%s"`, dirCID)
		test(carResponseFormat, dirPath, `W/"%s.car.7of9u8ojv38vd"`, rootCID) // ETags of CARs on a Path have the root CID in the Etag and hashed information to derive the correct Etag of the full request.
		test(rawResponseFormat, dirPath, `"%s.raw"`, dirCID)
		test(tarResponseFormat, dirPath, `W/"%s.x-tar"`, dirCID)

		test("", hamtFilePath, `"%s"`, hamtFileCID)
		test("text/html", hamtFilePath, `"%s"`, hamtFileCID)
		test(carResponseFormat, hamtFilePath, `W/"%s.car.2uq26jdcsk50p"`, rootCID) // ETags of CARs on a Path have the root CID in the Etag and hashed information to derive the correct Etag of the full request.
		test(rawResponseFormat, hamtFilePath, `"%s.raw"`, hamtFileCID)
		test(tarResponseFormat, hamtFilePath, `W/"%s.x-tar"`, hamtFileCID)

		test("", filePath, `"%s"`, fileCID)
		test("text/html", filePath, `"%s"`, fileCID)
		test(carResponseFormat, filePath, `W/"%s.car.fgq8i0qnhsq01"`, rootCID)
		test(rawResponseFormat, filePath, `"%s.raw"`, fileCID)
		test(tarResponseFormat, filePath, `W/"%s.x-tar"`, fileCID)

		test("", dagCborPath, `"%s.dag-cbor"`, dagCborCID)
		test("text/html", dagCborPath+"/", `"DagIndex-(.*)_CID-%s"`, dagCborCID)
		test(carResponseFormat, dagCborPath, `W/"%s.car.5mg3mekeviba5"`, rootCID)
		test(rawResponseFormat, dagCborPath, `"%s.raw"`, dagCborCID)
		test(dagJsonResponseFormat, dagCborPath, `"%s.dag-json"`, dagCborCID)
		test(dagCborResponseFormat, dagCborPath, `"%s.dag-cbor"`, dagCborCID)
	})

	t.Run("If-None-Match with previous Etag returns Not Modified", func(t *testing.T) {
		test := func(responseFormat string, path string) {
			t.Run(responseFormat, func(t *testing.T) {
				url := ts.URL + path
				req := mustNewRequest(t, http.MethodGet, url, nil)
				req.Header.Add("Accept", responseFormat)
				res := mustDoWithoutRedirect(t, req)
				_, err := io.Copy(io.Discard, res.Body)
				require.NoError(t, err)
				defer res.Body.Close()
				require.Equal(t, http.StatusOK, res.StatusCode)
				etag := res.Header.Get("Etag")
				require.NotEmpty(t, etag)

				req = mustNewRequest(t, http.MethodGet, url, nil)
				req.Header.Add("Accept", responseFormat)
				req.Header.Add("If-None-Match", etag)
				res = mustDoWithoutRedirect(t, req)
				_, err = io.Copy(io.Discard, res.Body)
				require.NoError(t, err)
				defer res.Body.Close()
				require.Equal(t, http.StatusNotModified, res.StatusCode)
			})
		}

		test("", dirPath)
		test("text/html", dirPath)
		test(carResponseFormat, dirPath)
		test(rawResponseFormat, dirPath)
		test(tarResponseFormat, dirPath)

		test("", hamtFilePath)
		test("text/html", hamtFilePath)
		test(carResponseFormat, hamtFilePath)
		test(rawResponseFormat, hamtFilePath)
		test(tarResponseFormat, hamtFilePath)

		test("", filePath)
		test("text/html", filePath)
		test(carResponseFormat, filePath)
		test(rawResponseFormat, filePath)
		test(tarResponseFormat, filePath)

		test("", dagCborPath)
		test("text/html", dagCborPath+"/")
		test(carResponseFormat, dagCborPath)
		test(rawResponseFormat, dagCborPath)
		test(dagJsonResponseFormat, dagCborPath)
		test(dagCborResponseFormat, dagCborPath)
	})

	// We have UnixFS1.5 tests in TestHeadersUnixFSModeModTime, here we test default behavior (DAG without modtime)
	t.Run("If-Modified-Since is noop against DAG without optional UnixFS 1.5 mtime", func(t *testing.T) {
		test := func(responseFormat string, path string) {
			t.Run(responseFormat, func(t *testing.T) {
				// Make regular request and read Last-Modified
				url := ts.URL + path
				req := mustNewRequest(t, http.MethodGet, url, nil)
				req.Header.Add("Accept", responseFormat)
				res := mustDoWithoutRedirect(t, req)
				_, err := io.Copy(io.Discard, res.Body)
				require.NoError(t, err)
				defer res.Body.Close()
				require.Equal(t, http.StatusOK, res.StatusCode)
				lastModified := res.Header.Get("Last-Modified")
				require.Empty(t, lastModified)

				// Make second request with If-Modified-Since far in past and expect normal response
				req = mustNewRequest(t, http.MethodGet, url, nil)
				req.Header.Add("Accept", responseFormat)
				req.Header.Add("If-Modified-Since", "Mon, 13 Jun 2000 22:18:32 GMT")
				res = mustDoWithoutRedirect(t, req)
				_, err = io.Copy(io.Discard, res.Body)
				require.NoError(t, err)
				defer res.Body.Close()
				require.Equal(t, http.StatusOK, res.StatusCode)
			})
		}

		test("", dirPath)
		test("text/html", dirPath)
		test(carResponseFormat, dirPath)
		test(rawResponseFormat, dirPath)
		test(tarResponseFormat, dirPath)

		test("", hamtFilePath)
		test("text/html", hamtFilePath)
		test(carResponseFormat, hamtFilePath)
		test(rawResponseFormat, hamtFilePath)
		test(tarResponseFormat, hamtFilePath)

		test("", filePath)
		test("text/html", filePath)
		test(carResponseFormat, filePath)
		test(rawResponseFormat, filePath)
		test(tarResponseFormat, filePath)

		test("", dagCborPath)
		test("text/html", dagCborPath+"/")
		test(carResponseFormat, dagCborPath)
		test(rawResponseFormat, dagCborPath)
		test(dagJsonResponseFormat, dagCborPath)
		test(dagCborResponseFormat, dagCborPath)
	})

	t.Run("X-Ipfs-Roots contains expected values", func(t *testing.T) {
		test := func(responseFormat string, path string, roots string) {
			t.Run(responseFormat, func(t *testing.T) {
				url := ts.URL + path
				req := mustNewRequest(t, http.MethodGet, url, nil)
				req.Header.Add("Accept", responseFormat)
				res := mustDoWithoutRedirect(t, req)
				_, err := io.Copy(io.Discard, res.Body)
				require.NoError(t, err)
				defer res.Body.Close()
				require.Equal(t, http.StatusOK, res.StatusCode)
				require.Equal(t, roots, res.Header.Get("X-Ipfs-Roots"))
			})
		}

		test("", dirPath, dirRoots)
		test("text/html", dirPath, dirRoots)
		test(carResponseFormat, dirPath, dirRoots)
		test(rawResponseFormat, dirPath, dirRoots)
		test(tarResponseFormat, dirPath, dirRoots)

		test("", hamtFilePath, hamtFileRoots)
		test("text/html", hamtFilePath, hamtFileRoots)
		test(carResponseFormat, hamtFilePath, hamtFileRoots)
		test(rawResponseFormat, hamtFilePath, hamtFileRoots)
		test(tarResponseFormat, hamtFilePath, hamtFileRoots)

		test("", filePath, fileRoots)
		test("text/html", filePath, fileRoots)
		test(carResponseFormat, filePath, fileRoots)
		test(rawResponseFormat, filePath, fileRoots)
		test(tarResponseFormat, filePath, fileRoots)

		test("", dagCborPath, dagCborRoots)
		test("text/html", dagCborPath+"/", dagCborRoots)
		test(carResponseFormat, dagCborPath, dagCborRoots)
		test(rawResponseFormat, dagCborPath, dagCborRoots)
		test(dagJsonResponseFormat, dagCborPath, dagCborRoots)
		test(dagCborResponseFormat, dagCborPath, dagCborRoots)
	})

	t.Run("If-None-Match with wrong value forces path resolution, but X-Ipfs-Roots is correct (regression)", func(t *testing.T) {
		test := func(responseFormat string, path string, roots string) {
			t.Run(responseFormat, func(t *testing.T) {
				url := ts.URL + path
				req := mustNewRequest(t, http.MethodGet, url, nil)
				req.Header.Add("Accept", responseFormat)
				req.Header.Add("If-None-Match", "just-some-gibberish")
				res := mustDoWithoutRedirect(t, req)
				_, err := io.Copy(io.Discard, res.Body)
				require.NoError(t, err)
				defer res.Body.Close()
				require.Equal(t, http.StatusOK, res.StatusCode)
				require.Equal(t, roots, res.Header.Get("X-Ipfs-Roots"))
			})
		}

		test("", dirPath, dirRoots)
		test("text/html", dirPath, dirRoots)
		test(carResponseFormat, dirPath, dirRoots)
		test(rawResponseFormat, dirPath, dirRoots)
		test(tarResponseFormat, dirPath, dirRoots)

		test("", hamtFilePath, hamtFileRoots)
		test("text/html", hamtFilePath, hamtFileRoots)
		test(carResponseFormat, hamtFilePath, hamtFileRoots)
		test(rawResponseFormat, hamtFilePath, hamtFileRoots)
		test(tarResponseFormat, hamtFilePath, hamtFileRoots)

		test("", filePath, fileRoots)
		test("text/html", filePath, fileRoots)
		test(carResponseFormat, filePath, fileRoots)
		test(rawResponseFormat, filePath, fileRoots)
		test(tarResponseFormat, filePath, fileRoots)

		test("", dagCborPath, dagCborRoots)
		test("text/html", dagCborPath+"/", dagCborRoots)
		test(carResponseFormat, dagCborPath, dagCborRoots)
		test(rawResponseFormat, dagCborPath, dagCborRoots)
		test(dagJsonResponseFormat, dagCborPath, dagCborRoots)
		test(dagCborResponseFormat, dagCborPath, dagCborRoots)
	})

	// Ensures CORS headers are present in HTTP OPTIONS responses
	// https://developer.mozilla.org/en-US/docs/Glossary/Preflight_request
	t.Run("CORS Preflight Headers", func(t *testing.T) {
		// Expect boxo/gateway library's default CORS allowlist for Method
		headerACAM := "Access-Control-Allow-Methods"
		expectedACAM := []string{http.MethodGet, http.MethodHead, http.MethodOptions}

		// Set custom CORS policy to ensure we test user config end-to-end
		headerACAO := "Access-Control-Allow-Origin"
		expectedACAO := "https://other.example.net"
		headers := map[string][]string{}
		headers[headerACAO] = []string{expectedACAO}

		ts := newTestServerWithConfigAndHeaders(t, backend, Config{
			PublicGateways: map[string]*PublicGateway{
				"subgw.example.com": {
					Paths:                 []string{"/ipfs", "/ipns"},
					UseSubdomains:         true,
					DeserializedResponses: true,
				},
			},
			DeserializedResponses: true,
		}, headers)
		t.Logf("test server url: %s", ts.URL)

		testCORSPreflightRequest := func(t *testing.T, path, hostHeader string, requestOriginHeader string, code int) {
			req, err := http.NewRequest(http.MethodOptions, ts.URL+path, nil)
			assert.Nil(t, err)

			if hostHeader != "" {
				req.Host = hostHeader
			}

			if requestOriginHeader != "" {
				req.Header.Add("Origin", requestOriginHeader)
			}

			t.Logf("test req: %+v", req)

			// Expect no redirect for OPTIONS request -- https://github.com/ipfs/kubo/issues/9983#issuecomment-1599673976
			res := mustDoWithoutRedirect(t, req)
			defer res.Body.Close()

			t.Logf("test res: %+v", res)

			// Expect success
			assert.Equal(t, code, res.StatusCode)

			// Expect OPTIONS response to have custom CORS header set by user
			assert.Equal(t, expectedACAO, res.Header.Get(headerACAO))

			// Expect OPTIONS response to have implicit default Allow-Methods
			// set by boxo/gateway library
			assert.Equal(t, expectedACAM, res.Header[headerACAM])
		}

		cid := root.String()

		t.Run("HTTP OPTIONS response is OK and has defined headers", func(t *testing.T) {
			t.Parallel()
			testCORSPreflightRequest(t, "/ipfs/"+cid, "", "", http.StatusOK)
		})

		t.Run("HTTP OPTIONS response for cross-origin /ipfs/cid is OK and has CORS headers", func(t *testing.T) {
			t.Parallel()
			testCORSPreflightRequest(t, "/ipfs/"+cid, "", "https://other.example.net", http.StatusOK)
		})

		t.Run("HTTP OPTIONS response for cross-origin /ipfs/cid is HTTP 301 and includes CORS headers (path gw redirect on subdomain gw)", func(t *testing.T) {
			t.Parallel()
			testCORSPreflightRequest(t, "/ipfs/"+cid, "subgw.example.com", "https://other.example.net", http.StatusMovedPermanently)
		})

		t.Run("HTTP OPTIONS response for cross-origin is HTTP 200 and has CORS headers (host header on subdomain gw)", func(t *testing.T) {
			t.Parallel()
			testCORSPreflightRequest(t, "/", cid+".ipfs.subgw.example.com", "https://other.example.net", http.StatusOK)
		})
	})

	t.Run("Content-Location is set when possible", func(t *testing.T) {
		backend, root := newMockBackend(t, "fixtures.car")
		backend.namesys["/ipns/dnslink-gateway.com"] = newMockNamesysItem(path.FromCid(root), 0)

		ts := newTestServerWithConfig(t, backend, Config{
			NoDNSLink: false,
			PublicGateways: map[string]*PublicGateway{
				"dnslink-gateway.com": {
					Paths:                 []string{},
					NoDNSLink:             false,
					DeserializedResponses: true,
				},
				"subdomain-gateway.com": {
					Paths:                 []string{"/ipfs", "/ipns"},
					UseSubdomains:         true,
					NoDNSLink:             true,
					DeserializedResponses: true,
				},
			},
			DeserializedResponses: true,
		})

		runTest := func(name, path, accept, host, expectedContentLocationHdr string) {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				req := mustNewRequest(t, http.MethodGet, ts.URL+path, nil)

				if accept != "" {
					req.Header.Set("Accept", accept)
				}

				if host != "" {
					req.Host = host
				}

				resp := mustDoWithoutRedirect(t, req)
				defer resp.Body.Close()

				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)

				require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
				require.Equal(t, expectedContentLocationHdr, resp.Header.Get("Content-Location"))
			})
		}

		contentPath := path.FromCid(root).String() + "/empty-dir/"
		subdomainGatewayHost := root.String() + ".ipfs.subdomain-gateway.com"
		dnslinkGatewayHost := "dnslink-gateway.com"

		runTest("Regular gateway with default format", contentPath, "", "", "")
		runTest("Regular gateway with Accept: application/vnd.ipld.car;version=1;order=dfs;dups=n sets correct Content-Location", contentPath, "application/vnd.ipld.car;version=1;order=dfs;dups=n", "", contentPath+"?car-dups=n&car-order=dfs&car-version=1&format=car")
		runTest("Regular gateway with ?dag-scope=entity&format=car", contentPath+"?dag-scope=entity&format=car", "", "", "")
		runTest("Regular gateway preserves query parameters", contentPath+"?a=b&c=d", dagCborResponseFormat, "", contentPath+"?a=b&c=d&format=dag-cbor")
		runTest("Subdomain gateway with default format", "/empty-dir/", "", subdomainGatewayHost, "")
		runTest("DNSLink gateway with default format", "/empty-dir/", "", dnslinkGatewayHost, "")

		for responseFormat, formatParam := range responseFormatToFormatParam {
			if responseFormat == ipnsRecordResponseFormat {
				continue
			}

			runTest("Regular gateway with Accept: "+responseFormat, contentPath, responseFormat, "", contentPath+"?format="+formatParam)
			runTest("Regular gateway with ?format="+formatParam, contentPath+"?format="+formatParam, "", "", "")

			runTest("Subdomain gateway with Accept: "+responseFormat, "/empty-dir/", responseFormat, subdomainGatewayHost, "/empty-dir/?format="+formatParam)
			runTest("Subdomain gateway with ?format="+formatParam, "/empty-dir/?format="+formatParam, "", subdomainGatewayHost, "")

			runTest("DNSLink gateway with Accept: "+responseFormat, "/empty-dir/", responseFormat, dnslinkGatewayHost, "/empty-dir/?format="+formatParam)
			runTest("DNSLink gateway with ?format="+formatParam, "/empty-dir/?format="+formatParam, "", dnslinkGatewayHost, "")
		}

		runTest("Accept: application/vnd.ipld.car overrides ?format=raw in Content-Location", contentPath+"?format=raw", "application/vnd.ipld.car", "", contentPath+"?format=car")
	})
}

// Testing a DAG with (optional) UnixFS1.5 modification time
func TestHeadersUnixFSModeModTime(t *testing.T) {
	t.Parallel()

	ts, _, root := newTestServerAndNode(t, "unixfs-dir-with-mode-mtime.car")
	var (
		rootCID  = root.String() // "bafybeidbcy4u6y55gsemlubd64zk53xoxs73ifd6rieejxcr7xy46mjvky"
		filePath = "/ipfs/" + rootCID + "/file1"
		dirPath  = "/ipfs/" + rootCID + "/dir1/"
	)

	t.Run("If-Modified-Since matching UnixFS 1.5 modtime returns Not Modified", func(t *testing.T) {
		test := func(responseFormat string, path string, entityType string, supported bool) {
			t.Run(fmt.Sprintf("%s/%s support=%t", responseFormat, entityType, supported), func(t *testing.T) {
				// Make regular request and read Last-Modified
				url := ts.URL + path
				req := mustNewRequest(t, http.MethodGet, url, nil)
				req.Header.Add("Accept", responseFormat)
				res := mustDoWithoutRedirect(t, req)
				_, err := io.Copy(io.Discard, res.Body)
				require.NoError(t, err)
				defer res.Body.Close()
				require.Equal(t, http.StatusOK, res.StatusCode)
				lastModified := res.Header.Get("Last-Modified")
				if supported {
					assert.NotEmpty(t, lastModified)
				} else {
					assert.Empty(t, lastModified)
					lastModified = "Mon, 13 Jun 2022 22:18:32 GMT" // manually set value for use in next steps
				}

				ifModifiedSinceTime, err := time.Parse(time.RFC1123, lastModified)
				require.NoError(t, err)
				oneHourBefore := ifModifiedSinceTime.Add(-1 * time.Hour).Truncate(time.Second)
				oneHourAfter := ifModifiedSinceTime.Add(1 * time.Hour).Truncate(time.Second)
				oneHourBeforeStr := oneHourBefore.Format(time.RFC1123)
				oneHourAfterStr := oneHourAfter.Format(time.RFC1123)
				lastModifiedStr := ifModifiedSinceTime.Format(time.RFC1123)

				// Make second request with If-Modified-Since and value read from response to first request
				req = mustNewRequest(t, http.MethodGet, url, nil)
				req.Header.Add("Accept", responseFormat)
				req.Header.Add("If-Modified-Since", lastModifiedStr)
				res = mustDoWithoutRedirect(t, req)
				_, err = io.Copy(io.Discard, res.Body)
				require.NoError(t, err)
				defer res.Body.Close()
				if supported {
					// 304 on exact match, can skip body
					assert.Equal(t, http.StatusNotModified, res.StatusCode)
				} else {
					assert.Equal(t, http.StatusOK, res.StatusCode)
				}

				// Make third request with If-Modified-Since 1h before value read from response to first request
				// and expect HTTP 200
				req = mustNewRequest(t, http.MethodGet, url, nil)
				req.Header.Add("Accept", responseFormat)
				req.Header.Add("If-Modified-Since", oneHourBeforeStr)
				res = mustDoWithoutRedirect(t, req)
				_, err = io.Copy(io.Discard, res.Body)
				require.NoError(t, err)
				defer res.Body.Close()
				// always return 200 with body because mtime from unixfs is after value from If-Modified-Since
				// so it counts as an update
				assert.Equal(t, http.StatusOK, res.StatusCode)

				// Make third request with If-Modified-Since 1h after value read from response to first request
				// and expect HTTP 200
				req = mustNewRequest(t, http.MethodGet, url, nil)
				req.Header.Add("Accept", responseFormat)
				req.Header.Add("If-Modified-Since", oneHourAfterStr)
				res = mustDoWithoutRedirect(t, req)
				_, err = io.Copy(io.Discard, res.Body)
				require.NoError(t, err)
				defer res.Body.Close()
				if supported {
					// 304 because mtime from unixfs is before value from If-Modified-Since
					// so no update, can skip body
					assert.Equal(t, http.StatusNotModified, res.StatusCode)
				} else {
					assert.Equal(t, http.StatusOK, res.StatusCode)
				}
			})
		}

		file, dir := "file", "directory"
		// supported on file-based web responses
		test("", filePath, file, true)
		test("text/html", filePath, file, true)

		// not supported on other formats
		// we may implement support for If-Modified-Since for below request types
		// if users raise the need, but If-None-Match is way better
		test(carResponseFormat, filePath, file, false)
		test(rawResponseFormat, filePath, file, false)
		test(tarResponseFormat, filePath, file, false)

		test("", dirPath, dir, false)
		test("text/html", dirPath, dir, false)
		test(carResponseFormat, dirPath, dir, false)
		test(rawResponseFormat, dirPath, dir, false)
		test(tarResponseFormat, dirPath, dir, false)
	})
}

func TestGoGetSupport(t *testing.T) {
	ts, _, root := newTestServerAndNode(t, "fixtures.car")

	// mimic go-get
	req := mustNewRequest(t, http.MethodGet, ts.URL+"/ipfs/"+root.String()+"?go-get=1", nil)
	res := mustDoWithoutRedirect(t, req)
	require.Equal(t, http.StatusOK, res.StatusCode)
}

func TestRedirects(t *testing.T) {
	t.Parallel()

	t.Run("IPNS Base58 Multihash Redirect", func(t *testing.T) {
		ts, _, _ := newTestServerAndNode(t, "fixtures.car")

		t.Run("ED25519 Base58-encoded key", func(t *testing.T) {
			t.Parallel()

			req := mustNewRequest(t, http.MethodGet, ts.URL+"/ipns/12D3KooWRBy97UB99e3J6hiPesre1MZeuNQvfan4gBziswrRJsNK?keep=query", nil)
			res := mustDoWithoutRedirect(t, req)
			require.Equal(t, "/ipns/k51qzi5uqu5dlvj2baxnqndepeb86cbk3ng7n3i46uzyxzyqj2xjonzllnv0v8?keep=query", res.Header.Get("Location"))
		})

		t.Run("RSA Base58-encoded key", func(t *testing.T) {
			t.Parallel()

			req := mustNewRequest(t, http.MethodGet, ts.URL+"/ipns/QmcJM7PRfkSbcM5cf1QugM5R37TLRKyJGgBEhXjLTB8uA2?keep=query", nil)
			res := mustDoWithoutRedirect(t, req)
			require.Equal(t, "/ipns/k2k4r8ol4m8kkcqz509c1rcjwunebj02gcnm5excpx842u736nja8ger?keep=query", res.Header.Get("Location"))
		})
	})

	t.Run("URI Query Redirects", func(t *testing.T) {
		t.Parallel()
		ts, _, _ := newTestServerAndNode(t, "fixtures.car")

		cid := "QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR"
		for _, test := range []struct {
			path     string
			status   int
			location string
		}{
			// - Browsers will send original URI in URL-escaped form
			// - We expect query parameters to be persisted
			// - We drop fragments, as those should not be sent by a browser
			{"/ipfs/?uri=ipfs%3A%2F%2FQmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco%2Fwiki%2FFoo_%C4%85%C4%99.html%3Ffilename%3Dtest-%C4%99.html%23header-%C4%85", http.StatusMovedPermanently, "/ipfs/QmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco/wiki/Foo_%C4%85%C4%99.html?filename=test-%C4%99.html"},
			{"/ipfs/?uri=ipns%3A%2F%2Fexample.com%2Fwiki%2FFoo_%C4%85%C4%99.html%3Ffilename%3Dtest-%C4%99.html", http.StatusMovedPermanently, "/ipns/example.com/wiki/Foo_%C4%85%C4%99.html?filename=test-%C4%99.html"},
			{"/ipfs/?uri=ipfs://" + cid, http.StatusMovedPermanently, "/ipfs/" + cid},
			{"/ipfs?uri=ipfs://" + cid, http.StatusMovedPermanently, "/ipfs/" + cid},
			{"/ipfs/?uri=ipns://" + cid, http.StatusMovedPermanently, "/ipns/" + cid},
			{"/ipns/?uri=ipfs%3A%2F%2FQmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco%2Fwiki%2FFoo_%C4%85%C4%99.html%3Ffilename%3Dtest-%C4%99.html%23header-%C4%85", http.StatusMovedPermanently, "/ipfs/QmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco/wiki/Foo_%C4%85%C4%99.html?filename=test-%C4%99.html"},
			{"/ipns/?uri=ipns%3A%2F%2Fexample.com%2Fwiki%2FFoo_%C4%85%C4%99.html%3Ffilename%3Dtest-%C4%99.html", http.StatusMovedPermanently, "/ipns/example.com/wiki/Foo_%C4%85%C4%99.html?filename=test-%C4%99.html"},
			{"/ipns?uri=ipns://" + cid, http.StatusMovedPermanently, "/ipns/" + cid},
			{"/ipns/?uri=ipns://" + cid, http.StatusMovedPermanently, "/ipns/" + cid},
			{"/ipns/?uri=ipfs://" + cid, http.StatusMovedPermanently, "/ipfs/" + cid},
			{"/ipfs/?uri=unsupported://" + cid, http.StatusBadRequest, ""},
			{"/ipfs/?uri=invaliduri", http.StatusBadRequest, ""},
			{"/ipfs/?uri=" + cid, http.StatusBadRequest, ""},
		} {
			testName := ts.URL + test.path
			t.Run(testName, func(t *testing.T) {
				req := mustNewRequest(t, http.MethodGet, ts.URL+test.path, nil)
				resp := mustDoWithoutRedirect(t, req)
				defer resp.Body.Close()
				require.Equal(t, test.status, resp.StatusCode)
				require.Equal(t, test.location, resp.Header.Get("Location"))
			})
		}
	})

	t.Run("IPNS Hostname Redirects", func(t *testing.T) {
		t.Parallel()

		ts, backend, root := newTestServerAndNode(t, "ipns-hostname-redirects.car")
		backend.namesys["/ipns/example.net"] = newMockNamesysItem(path.FromCid(root), 0)

		// make request to directory containing index.html
		req := mustNewRequest(t, http.MethodGet, ts.URL+"/foo", nil)
		req.Host = "example.net"
		res := mustDoWithoutRedirect(t, req)

		// expect 301 redirect to same path, but with trailing slash
		require.Equal(t, http.StatusMovedPermanently, res.StatusCode)
		hdr := res.Header["Location"]
		require.Positive(t, len(hdr), "location header not present")
		require.Equal(t, hdr[0], "/foo/")

		// make request with prefix to directory containing index.html
		req = mustNewRequest(t, http.MethodGet, ts.URL+"/foo", nil)
		req.Host = "example.net"
		res = mustDoWithoutRedirect(t, req)
		// expect 301 redirect to same path, but with prefix and trailing slash
		require.Equal(t, http.StatusMovedPermanently, res.StatusCode)

		hdr = res.Header["Location"]
		require.Positive(t, len(hdr), "location header not present")
		require.Equal(t, hdr[0], "/foo/")

		// make sure /version isn't exposed
		req = mustNewRequest(t, http.MethodGet, ts.URL+"/version", nil)
		req.Host = "example.net"
		res = mustDoWithoutRedirect(t, req)
		require.Equal(t, http.StatusNotFound, res.StatusCode)
	})

	t.Run("_redirects file with If-None-Match header", func(t *testing.T) {
		t.Parallel()

		backend, root := newMockBackend(t, "redirects-spa.car")
		backend.namesys["/ipns/example.com"] = newMockNamesysItem(path.FromCid(root), 0)

		ts := newTestServerWithConfig(t, backend, Config{
			NoDNSLink: false,
			PublicGateways: map[string]*PublicGateway{
				"example.com": {
					UseSubdomains:         true,
					DeserializedResponses: true,
				},
			},
			DeserializedResponses: true,
		})

		missingPageURL := ts.URL + "/missing-page"

		do := func(method string) {
			// Make initial request to non-existing page that should return the contents
			// of index.html as per the _redirects file.
			req := mustNewRequest(t, method, missingPageURL, nil)
			req.Header.Add("Accept", "text/html")
			req.Host = "example.com"

			res := mustDoWithoutRedirect(t, req)
			defer res.Body.Close()

			// Check statuses and body.
			require.Equal(t, http.StatusOK, res.StatusCode)
			if method != http.MethodHead {
				body, err := io.ReadAll(res.Body)
				require.NoError(t, err)
				require.Equal(t, "hello world\n", string(body))
			}

			// Check Etag.
			etag := res.Header.Get("Etag")
			require.NotEmpty(t, etag)

			// Repeat request with Etag as If-None-Match value. Expect 304 Not Modified.
			req = mustNewRequest(t, method, missingPageURL, nil)
			req.Header.Add("Accept", "text/html")
			req.Host = "example.com"
			req.Header.Add("If-None-Match", etag)

			res = mustDoWithoutRedirect(t, req)
			defer res.Body.Close()
			require.Equal(t, http.StatusNotModified, res.StatusCode)
		}

		do(http.MethodGet)
		do(http.MethodHead)
	})

	t.Run("_redirects file redirect preserves both static and dynamic query parameters", func(t *testing.T) {
		t.Parallel()

		backend, root := newMockBackend(t, "redirects-query-params.car")
		backend.namesys["/ipns/example.org"] = newMockNamesysItem(path.FromCid(root), 0)

		ts := newTestServerWithConfig(t, backend, Config{
			NoDNSLink: false,
			PublicGateways: map[string]*PublicGateway{
				"example.org": {
					UseSubdomains:         true,
					DeserializedResponses: true,
				},
			},
			DeserializedResponses: true,
		})

		// Request for missing page with some dynamic parameters
		missingPageURL := ts.URL + "/source1/source-file?dynamic-query1=dynamic-value1&dynamic-query2=dynamic-value2"

		// Expected response is redirect URL that preserves both static and dynamic parameters
		expectedTargetURL := "/target-file?dynamic-query1=dynamic-value1&dynamic-query2=dynamic-value2&static-query1=static-val1&static-query2=static-val2"

		do := func(method string) {
			// Make initial request to non-existing page that should return redirect that contains both query params from original request AND target URL in _redirects file.
			req := mustNewRequest(t, method, missingPageURL, nil)
			req.Header.Add("Accept", "text/html")
			req.Host = "example.org"

			res := mustDoWithoutRedirect(t, req)
			defer res.Body.Close()

			// Status code should match 301 from _redirects catch-all rule
			require.Equal(t, http.StatusMovedPermanently, res.StatusCode)

			// Check Redirect target contains all query parameters
			redirectURL := res.Header.Get("Location")
			require.Equal(t, expectedTargetURL, redirectURL)

		}

		do(http.MethodGet)
		do(http.MethodHead)
	})

	t.Run("_redirects file redirect supports named placeholders as query parameters", func(t *testing.T) {
		t.Parallel()

		backend, root := newMockBackend(t, "redirects-query-params.car")
		backend.namesys["/ipns/example.org"] = newMockNamesysItem(path.FromCid(root), 0)

		ts := newTestServerWithConfig(t, backend, Config{
			NoDNSLink: false,
			PublicGateways: map[string]*PublicGateway{
				"example.org": {
					UseSubdomains:         true,
					DeserializedResponses: true,
				},
			},
			DeserializedResponses: true,
		})

		// Request for missing page under path that has named placeholders and some dynamic parameters
		missingPageURL := ts.URL + "/source2/codeA/nameA?dynamic-query1=dynamic-value1&dynamic-query2=dynamic-value2"

		// Expected response is redirect URL that preserves both named and dynamic parameters
		expectedTargetURL := "/target-file?code=codeA&dynamic-query1=dynamic-value1&dynamic-query2=dynamic-value2&name=nameA"

		do := func(method string) {
			// Make initial request to non-existing page that should return redirect that contains both query params from original request AND target URL in _redirects file.
			req := mustNewRequest(t, method, missingPageURL, nil)
			req.Header.Add("Accept", "text/html")
			req.Host = "example.org"

			res := mustDoWithoutRedirect(t, req)
			defer res.Body.Close()

			// Status code should match 301 from _redirects catch-all rule
			require.Equal(t, http.StatusMovedPermanently, res.StatusCode)

			// Check Redirect target contains all query parameters
			redirectURL := res.Header.Get("Location")
			require.Equal(t, expectedTargetURL, redirectURL)

		}

		do(http.MethodGet)
		do(http.MethodHead)
	})

	t.Run("_redirects file redirect supports catch-all splat with dynamic query parameters", func(t *testing.T) {
		t.Parallel()

		backend, root := newMockBackend(t, "redirects-query-params.car")
		backend.namesys["/ipns/example.org"] = newMockNamesysItem(path.FromCid(root), 0)

		ts := newTestServerWithConfig(t, backend, Config{
			NoDNSLink: false,
			PublicGateways: map[string]*PublicGateway{
				"example.org": {
					UseSubdomains:         true,
					DeserializedResponses: true,
				},
			},
			DeserializedResponses: true,
		})

		// Request for missing page under path that is a catch-all splat (incl. some dynamic parameters)
		missingPageURL := ts.URL + "/source3/this/is/catch-all/splat-with?dynamic-query1=dynamic-value1&dynamic-query2=dynamic-value2"

		// Expected response is redirect URL to a different server that preserves entire splat, including dynamic query parameters
		expectedTargetURL := "https://example.net/target3/this/is/catch-all/splat-with?dynamic-query1=dynamic-value1&dynamic-query2=dynamic-value2"

		do := func(method string) {
			// Make initial request to non-existing page that should return redirect that contains both query params from original request AND target URL in _redirects file.
			req := mustNewRequest(t, method, missingPageURL, nil)
			req.Header.Add("Accept", "text/html")
			req.Host = "example.org"

			res := mustDoWithoutRedirect(t, req)
			defer res.Body.Close()

			// Status code should match 301 from _redirects catch-all rule
			require.Equal(t, http.StatusMovedPermanently, res.StatusCode)

			// Check Redirect target contains all query parameters
			redirectURL := res.Header.Get("Location")
			require.Equal(t, expectedTargetURL, redirectURL)

		}

		do(http.MethodGet)
		do(http.MethodHead)
	})

	t.Run("Superfluous namespace", func(t *testing.T) {
		t.Parallel()

		backend, root := newMockBackend(t, "fixtures.car")
		backend.namesys["/ipns/dnslink-gateway.com"] = newMockNamesysItem(path.FromCid(root), 0)
		backend.namesys["/ipns/dnslink-website.com"] = newMockNamesysItem(path.FromCid(root), 0)

		ts := newTestServerWithConfig(t, backend, Config{
			NoDNSLink: false,
			PublicGateways: map[string]*PublicGateway{
				"dnslink-gateway.com": {
					Paths:                 []string{"/ipfs", "/ipns"},
					NoDNSLink:             false,
					DeserializedResponses: true,
				},
				"dnslink-website.com": {
					Paths:                 []string{},
					NoDNSLink:             false,
					DeserializedResponses: true,
				},
				"gateway.com": {
					Paths:                 []string{"/ipfs"},
					UseSubdomains:         false,
					NoDNSLink:             true,
					DeserializedResponses: true,
				},
				"subdomain-gateway.com": {
					Paths:                 []string{"/ipfs", "/ipns"},
					UseSubdomains:         true,
					NoDNSLink:             true,
					DeserializedResponses: true,
				},
			},
			DeserializedResponses: true,
		})

		for _, test := range []struct {
			host     string
			path     string
			status   int
			location string
		}{
			// Barebones gateway
			{"", "/ipfs/ipfs/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR", http.StatusMovedPermanently, "/ipfs/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR"},
			{"", "/ipfs/ipns/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR", http.StatusMovedPermanently, "/ipns/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR"},
			{"", "/ipfs/ipns/dnslink.com", http.StatusMovedPermanently, "/ipns/dnslink.com"},

			// DNSLink Gateway with /ipfs and /ipns enabled
			{"dnslink-gateway.com", "/ipfs/ipfs/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR", http.StatusMovedPermanently, "/ipfs/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR"},
			{"dnslink-gateway.com", "/ipfs/ipns/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR", http.StatusMovedPermanently, "/ipns/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR"},
			{"dnslink-gateway.com", "/ipfs/ipns/dnslink.com", http.StatusMovedPermanently, "/ipns/dnslink.com"},

			// DNSLink Gateway without /ipfs and /ipns
			{"dnslink-website.com", "/ipfs/ipfs/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR", http.StatusNotFound, ""},
			{"dnslink-website.com", "/ipfs/ipns/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR", http.StatusNotFound, ""},
			{"dnslink-website.com", "/ipfs/ipns/dnslink.com", http.StatusNotFound, ""},

			// Public gateway
			{"gateway.com", "/ipfs/ipfs/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR", http.StatusMovedPermanently, "/ipfs/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR"},
			{"gateway.com", "/ipfs/ipns/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR", http.StatusMovedPermanently, "/ipns/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR"},
			{"gateway.com", "/ipfs/ipns/dnslink.com", http.StatusMovedPermanently, "/ipns/dnslink.com"},

			// Subdomain gateway
			{"subdomain-gateway.com", "/ipfs/ipfs/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR", http.StatusMovedPermanently, "/ipfs/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR"},
			{"subdomain-gateway.com", "/ipfs/ipns/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR", http.StatusMovedPermanently, "/ipns/QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR"},
			{"subdomain-gateway.com", "/ipfs/ipns/dnslink.com", http.StatusMovedPermanently, "/ipns/dnslink.com"},
		} {
			testName := ts.URL + test.path
			if test.host != "" {
				testName += " " + test.host
			}

			t.Run(testName, func(t *testing.T) {
				req := mustNewRequest(t, http.MethodGet, ts.URL+test.path, nil)
				req.Header.Set("Accept", "text/html")
				if test.host != "" {
					req.Host = test.host
				}
				resp := mustDoWithoutRedirect(t, req)
				defer resp.Body.Close()
				require.Equal(t, test.status, resp.StatusCode)
				require.Equal(t, test.location, resp.Header.Get("Location"))
			})
		}
	})
}

func TestDeserializedResponses(t *testing.T) {
	t.Parallel()

	t.Run("IPFS", func(t *testing.T) {
		t.Parallel()

		backend, root := newMockBackend(t, "fixtures.car")

		ts := newTestServerWithConfig(t, backend, Config{
			NoDNSLink: false,
			PublicGateways: map[string]*PublicGateway{
				"trustless.com": {
					Paths: []string{"/ipfs", "/ipns"},
				},
				"trusted.com": {
					Paths:                 []string{"/ipfs", "/ipns"},
					DeserializedResponses: true,
				},
			},
		})

		trustedFormats := []string{"", "dag-json", "dag-cbor", "tar", "json", "cbor"}
		trustlessFormats := []string{"raw", "car"}

		doRequest := func(t *testing.T, path, host string, expectedStatus int) {
			req := mustNewRequest(t, http.MethodGet, ts.URL+path, nil)
			if host != "" {
				req.Host = host
			}
			res := mustDoWithoutRedirect(t, req)
			defer res.Body.Close()
			assert.Equal(t, expectedStatus, res.StatusCode)
		}

		doIpfsCidRequests := func(t *testing.T, formats []string, host string, expectedStatus int) {
			for _, format := range formats {
				doRequest(t, "/ipfs/"+root.String()+"/?format="+format, host, expectedStatus)
			}
		}

		doIpfsCidPathRequests := func(t *testing.T, formats []string, host string, expectedStatus int) {
			for _, format := range formats {
				doRequest(t, "/ipfs/"+root.String()+"/empty-dir/?format="+format, host, expectedStatus)
			}
		}

		trustedTests := func(t *testing.T, host string) {
			doIpfsCidRequests(t, trustlessFormats, host, http.StatusOK)
			doIpfsCidRequests(t, trustedFormats, host, http.StatusOK)
			doIpfsCidPathRequests(t, trustlessFormats, host, http.StatusOK)
			doIpfsCidPathRequests(t, trustedFormats, host, http.StatusOK)
		}

		trustlessTests := func(t *testing.T, host string) {
			doIpfsCidRequests(t, trustlessFormats, host, http.StatusOK)
			doIpfsCidRequests(t, trustedFormats, host, http.StatusNotAcceptable)
			doIpfsCidPathRequests(t, trustedFormats, host, http.StatusNotAcceptable)
			doIpfsCidPathRequests(t, []string{"raw"}, host, http.StatusNotAcceptable)
			doIpfsCidPathRequests(t, []string{"car"}, host, http.StatusOK)
		}

		t.Run("Explicit Trustless Gateway", func(t *testing.T) {
			t.Parallel()
			trustlessTests(t, "trustless.com")
		})

		t.Run("Explicit Trusted Gateway", func(t *testing.T) {
			t.Parallel()
			trustedTests(t, "trusted.com")
		})

		t.Run("Implicit Default Trustless Gateway", func(t *testing.T) {
			t.Parallel()
			trustlessTests(t, "not.configured.com")
			trustlessTests(t, "localhost")
			trustlessTests(t, "127.0.0.1")
			trustlessTests(t, "::1")
		})
	})

	t.Run("IPNS", func(t *testing.T) {
		t.Parallel()

		backend, root := newMockBackend(t, "fixtures.car")
		backend.namesys["/ipns/trustless.com"] = newMockNamesysItem(path.FromCid(root), 0)
		backend.namesys["/ipns/trusted.com"] = newMockNamesysItem(path.FromCid(root), 0)

		ts := newTestServerWithConfig(t, backend, Config{
			NoDNSLink: false,
			PublicGateways: map[string]*PublicGateway{
				"trustless.com": {
					Paths: []string{"/ipfs", "/ipns"},
				},
				"trusted.com": {
					Paths:                 []string{"/ipfs", "/ipns"},
					DeserializedResponses: true,
				},
			},
		})

		doRequest := func(t *testing.T, path, host string, expectedStatus int) {
			req := mustNewRequest(t, http.MethodGet, ts.URL+path, nil)
			if host != "" {
				req.Host = host
			}
			res := mustDoWithoutRedirect(t, req)
			defer res.Body.Close()
			assert.Equal(t, expectedStatus, res.StatusCode)
		}

		// DNSLink only. Not supported for trustless. Supported for trusted, except
		// format=ipns-record which is unavailable for DNSLink.
		doRequest(t, "/", "trustless.com", http.StatusNotAcceptable)
		doRequest(t, "/empty-dir/", "trustless.com", http.StatusNotAcceptable)
		doRequest(t, "/?format=ipns-record", "trustless.com", http.StatusNotAcceptable)

		doRequest(t, "/", "trusted.com", http.StatusOK)
		doRequest(t, "/empty-dir/", "trusted.com", http.StatusOK)
		doRequest(t, "/?format=ipns-record", "trusted.com", http.StatusBadRequest)
	})
}

type errorMockBackend struct {
	err error
}

func (mb *errorMockBackend) Get(ctx context.Context, path path.ImmutablePath, getRange ...ByteRange) (ContentPathMetadata, *GetResponse, error) {
	return ContentPathMetadata{}, nil, mb.err
}

func (mb *errorMockBackend) GetAll(ctx context.Context, path path.ImmutablePath) (ContentPathMetadata, files.Node, error) {
	return ContentPathMetadata{}, nil, mb.err
}

func (mb *errorMockBackend) GetBlock(ctx context.Context, path path.ImmutablePath) (ContentPathMetadata, files.File, error) {
	return ContentPathMetadata{}, nil, mb.err
}

func (mb *errorMockBackend) Head(ctx context.Context, path path.ImmutablePath) (ContentPathMetadata, *HeadResponse, error) {
	return ContentPathMetadata{}, nil, mb.err
}

func (mb *errorMockBackend) GetCAR(ctx context.Context, path path.ImmutablePath, params CarParams) (ContentPathMetadata, io.ReadCloser, error) {
	return ContentPathMetadata{}, nil, mb.err
}

func (mb *errorMockBackend) ResolveMutable(ctx context.Context, p path.Path) (path.ImmutablePath, time.Duration, time.Time, error) {
	return path.ImmutablePath{}, 0, time.Time{}, mb.err
}

func (mb *errorMockBackend) GetIPNSRecord(ctx context.Context, c cid.Cid) ([]byte, error) {
	return nil, mb.err
}

func (mb *errorMockBackend) GetDNSLinkRecord(ctx context.Context, hostname string) (path.Path, error) {
	return nil, mb.err
}

func (mb *errorMockBackend) IsCached(ctx context.Context, p path.Path) bool {
	return false
}

func (mb *errorMockBackend) ResolvePath(ctx context.Context, path path.ImmutablePath) (ContentPathMetadata, error) {
	return ContentPathMetadata{}, mb.err
}

func TestErrorBubblingFromBackend(t *testing.T) {
	t.Parallel()

	testError := func(name string, err error, status int) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			backend := &errorMockBackend{err: fmt.Errorf("wrapped for testing purposes: %w", err)}
			ts := newTestServer(t, backend)
			req := mustNewRequest(t, http.MethodGet, ts.URL+"/ipns/en.wikipedia-on-ipfs.org", nil)
			res := mustDo(t, req)
			require.Equal(t, status, res.StatusCode)
		})
	}

	testError("404 Not Found from IPLD", &ipld.ErrNotFound{}, http.StatusNotFound)
	testError("404 Not Found from path resolver", &resolver.ErrNoLink{}, http.StatusNotFound)
	testError("502 Bad Gateway", ErrBadGateway, http.StatusBadGateway)
	testError("504 Gateway Timeout", ErrGatewayTimeout, http.StatusGatewayTimeout)

	testErrorRetryAfter := func(name string, err error, status int, headerValue string, headerLength int) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			backend := &errorMockBackend{err: fmt.Errorf("wrapped for testing purposes: %w", err)}
			ts := newTestServer(t, backend)

			req := mustNewRequest(t, http.MethodGet, ts.URL+"/ipns/en.wikipedia-on-ipfs.org", nil)
			res := mustDo(t, req)
			require.Equal(t, status, res.StatusCode)
			require.Equal(t, headerValue, res.Header.Get("Retry-After"))
			require.Equal(t, headerLength, len(res.Header.Values("Retry-After")))
		})
	}

	testErrorRetryAfter("429 Too Many Requests without Retry-After header", ErrTooManyRequests, http.StatusTooManyRequests, "", 0)
	testErrorRetryAfter("429 Too Many Requests without Retry-After header", NewErrorRetryAfter(ErrTooManyRequests, 0*time.Second), http.StatusTooManyRequests, "", 0)
	testErrorRetryAfter("429 Too Many Requests with Retry-After header", NewErrorRetryAfter(ErrTooManyRequests, 3600*time.Second), http.StatusTooManyRequests, "3600", 1)
}

type panicMockBackend struct {
	panicOnHostnameHandler bool
}

func (mb *panicMockBackend) Get(ctx context.Context, immutablePath path.ImmutablePath, ranges ...ByteRange) (ContentPathMetadata, *GetResponse, error) {
	panic("i am panicking")
}

func (mb *panicMockBackend) GetAll(ctx context.Context, immutablePath path.ImmutablePath) (ContentPathMetadata, files.Node, error) {
	panic("i am panicking")
}

func (mb *panicMockBackend) GetBlock(ctx context.Context, immutablePath path.ImmutablePath) (ContentPathMetadata, files.File, error) {
	panic("i am panicking")
}

func (mb *panicMockBackend) Head(ctx context.Context, immutablePath path.ImmutablePath) (ContentPathMetadata, *HeadResponse, error) {
	panic("i am panicking")
}

func (mb *panicMockBackend) GetCAR(ctx context.Context, immutablePath path.ImmutablePath, params CarParams) (ContentPathMetadata, io.ReadCloser, error) {
	panic("i am panicking")
}

func (mb *panicMockBackend) ResolveMutable(ctx context.Context, p path.Path) (path.ImmutablePath, time.Duration, time.Time, error) {
	panic("i am panicking")
}

func (mb *panicMockBackend) GetIPNSRecord(ctx context.Context, c cid.Cid) ([]byte, error) {
	panic("i am panicking")
}

func (mb *panicMockBackend) GetDNSLinkRecord(ctx context.Context, hostname string) (path.Path, error) {
	// GetDNSLinkRecord is also called on the WithHostname handler. We have this option
	// to disable panicking here so we can test if both the regular gateway handler
	// and the hostname handler can handle panics.
	if mb.panicOnHostnameHandler {
		panic("i am panicking")
	}

	return nil, errors.New("not implemented")
}

func (mb *panicMockBackend) IsCached(ctx context.Context, p path.Path) bool {
	panic("i am panicking")
}

func (mb *panicMockBackend) ResolvePath(ctx context.Context, immutablePath path.ImmutablePath) (ContentPathMetadata, error) {
	panic("i am panicking")
}

func TestPanicStatusCode(t *testing.T) {
	t.Parallel()

	t.Run("Panic on Handler", func(t *testing.T) {
		t.Parallel()

		backend := &panicMockBackend{}
		ts := newTestServer(t, backend)
		req := mustNewRequest(t, http.MethodGet, ts.URL+"/ipfs/bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e", nil)
		res := mustDo(t, req)
		require.Equal(t, http.StatusInternalServerError, res.StatusCode)
	})

	t.Run("Panic on Hostname Handler", func(t *testing.T) {
		t.Parallel()

		backend := &panicMockBackend{panicOnHostnameHandler: true}
		ts := newTestServer(t, backend)
		req := mustNewRequest(t, http.MethodGet, ts.URL+"/ipfs/bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e", nil)
		res := mustDo(t, req)
		require.Equal(t, http.StatusInternalServerError, res.StatusCode)
	})
}

func TestBrowserErrorHTML(t *testing.T) {
	t.Parallel()
	ts, _, root := newTestServerAndNode(t, "fixtures.car")

	t.Run("plain error if request does not have Accept: text/html", func(t *testing.T) {
		t.Parallel()

		req := mustNewRequest(t, http.MethodGet, ts.URL+"/ipfs/"+root.String()+"/nonexisting-link", nil)
		res := mustDoWithoutRedirect(t, req)
		require.Equal(t, http.StatusNotFound, res.StatusCode)
		require.NotContains(t, res.Header.Get("Content-Type"), "text/html")

		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		require.NotContains(t, string(body), "<!DOCTYPE html>")
	})

	t.Run("html error if request has Accept: text/html", func(t *testing.T) {
		t.Parallel()

		req := mustNewRequest(t, http.MethodGet, ts.URL+"/ipfs/"+root.String()+"/nonexisting-link", nil)
		req.Header.Set("Accept", "text/html")

		res := mustDoWithoutRedirect(t, req)
		require.Equal(t, http.StatusNotFound, res.StatusCode)
		require.Contains(t, res.Header.Get("Content-Type"), "text/html")

		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), "<!DOCTYPE html>")
	})
}
