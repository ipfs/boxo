package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/boxo/path"
)

// contentPathToCarUrl returns an URL that allows retrieval of specified resource
// from a trustless gateway that implements IPIP-402
func contentPathToCarUrl(path path.ImmutablePath, params CarParams) *url.URL {
	return &url.URL{
		Path:     path.String(),
		RawQuery: carParamsToString(params),
	}
}

// carParamsToString converts CarParams to URL parameters compatible with IPIP-402
func carParamsToString(params CarParams) string {
	paramsBuilder := strings.Builder{}
	paramsBuilder.WriteString("format=car") // always send explicit format in URL, this  makes debugging easier, even when Accept header was set
	if params.Scope != "" {
		paramsBuilder.WriteString("&dag-scope=")
		paramsBuilder.WriteString(string(params.Scope))
	}
	if params.Range != nil {
		paramsBuilder.WriteString("&entity-bytes=")
		paramsBuilder.WriteString(strconv.FormatInt(params.Range.From, 10))
		paramsBuilder.WriteString(":")
		if params.Range.To != nil {
			paramsBuilder.WriteString(strconv.FormatInt(*params.Range.To, 10))
		} else {
			paramsBuilder.WriteString("*")
		}
	}
	return paramsBuilder.String()
}

// GatewayError translates underlying blockstore error into one that gateway code will return as HTTP 502 or 504
// it also makes sure Retry-After hint from remote blockstore will be passed to HTTP client, if present.
func GatewayError(err error) error {
	if errors.Is(err, &ErrorStatusCode{}) ||
		errors.Is(err, &ErrorRetryAfter{}) {
		// already correct error
		return err
	}

	// All timeouts should produce 504 Gateway Timeout
	if errors.Is(err, context.DeadlineExceeded) ||
		// errors.Is(err, caboose.ErrTimeout) ||
		// Unfortunately this is not an exported type so we have to check for the content.
		strings.Contains(err.Error(), "Client.Timeout exceeded") {
		return fmt.Errorf("%w: %s", ErrGatewayTimeout, err.Error())
	}

	// (Saturn) errors that support the RetryAfter interface need to be converted
	// to the correct gateway error, such that the HTTP header is set.
	for v := err; v != nil; v = errors.Unwrap(v) {
		if r, ok := v.(interface{ RetryAfter() time.Duration }); ok {
			return NewErrorRetryAfter(err, r.RetryAfter())
		}
	}

	// everything else returns 502 Bad Gateway
	return fmt.Errorf("%w: %s", ErrBadGateway, err.Error())
}

type remoteCarFetcher struct {
	httpClient *http.Client
	gatewayURL []string
	validate   bool
	rand       *rand.Rand
}

func newRemoteCarFetcher(gatewayURL []string) (CarFetcher, error) {
	if len(gatewayURL) == 0 {
		return nil, errors.New("missing gateway URLs to which to proxy")
	}

	return &remoteCarFetcher{
		gatewayURL: gatewayURL,
		httpClient: newRemoteHTTPClient(),
		// Enables block validation by default. Important since we are
		// proxying block requests to an untrusted gateway.
		validate: true,
		rand:     rand.New(rand.NewSource(time.Now().Unix())),
	}, nil
}

func (ps *remoteCarFetcher) Fetch(ctx context.Context, path string, cb DataCallback) error {
	urlStr := fmt.Sprintf("%s%s", ps.getRandomGatewayURL(), path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return err
	}
	log.Debugw("car fetch", "url", req.URL)
	req.Header.Set("Accept", "application/vnd.ipld.car;order=dfs;dups=y")
	resp, err := ps.httpClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		errData, err := io.ReadAll(resp.Body)
		if err != nil {
			err = fmt.Errorf("could not read error message: %w", err)
		} else {
			err = fmt.Errorf("%q", string(errData))
		}
		return fmt.Errorf("http error from car gateway: %s: %w", resp.Status, err)
	}

	err = cb(path, resp.Body)
	if err != nil {
		resp.Body.Close()
		return err
	}
	return resp.Body.Close()
}

func (ps *remoteCarFetcher) getRandomGatewayURL() string {
	return ps.gatewayURL[ps.rand.Intn(len(ps.gatewayURL))]
}
