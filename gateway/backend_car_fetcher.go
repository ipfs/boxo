package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"
)

type DataCallback func(resource string, reader io.Reader) error

// CarFetcher powers a [CarBackend].
type CarFetcher interface {
	Fetch(ctx context.Context, path string, cb DataCallback) error
}

type remoteCarFetcher struct {
	httpClient *http.Client
	gatewayURL []string
	rand       *rand.Rand
}

// NewRemoteCarFetcher returns a [CarFetcher] that is backed by one or more gateways
// that support partial CAR requests, as described in [IPIP-402]. You can optionally
// pass your own [http.Client].
//
// [IPIP-402]: https://specs.ipfs.tech/ipips/ipip-0402
func NewRemoteCarFetcher(gatewayURL []string, httpClient *http.Client) (CarFetcher, error) {
	if len(gatewayURL) == 0 {
		return nil, errors.New("missing gateway URLs to which to proxy")
	}

	if httpClient == nil {
		httpClient = newRemoteHTTPClient()
	}

	return &remoteCarFetcher{
		gatewayURL: gatewayURL,
		httpClient: httpClient,
		rand:       rand.New(rand.NewSource(time.Now().Unix())),
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

type retryCarFetcher struct {
	inner   CarFetcher
	retries int
}

// NewRetryCarFetcher returns a [CarFetcher] that retries to fetch up to the given
// [allowedRetries] using the [inner] [CarFetcher]. If the inner fetcher returns
// an [ErrPartialResponse] error, then the number of retries is reset to the initial
// maximum allowed retries.
func NewRetryCarFetcher(inner CarFetcher, allowedRetries int) (CarFetcher, error) {
	if allowedRetries <= 0 {
		return nil, errors.New("number of retries must be a number larger than 0")
	}

	return &retryCarFetcher{
		inner:   inner,
		retries: allowedRetries,
	}, nil
}

func (r *retryCarFetcher) Fetch(ctx context.Context, path string, cb DataCallback) error {
	return r.fetch(ctx, path, cb, r.retries)
}

func (r *retryCarFetcher) fetch(ctx context.Context, path string, cb DataCallback, retriesLeft int) error {
	err := r.inner.Fetch(ctx, path, cb)
	if err == nil {
		return nil
	}

	if retriesLeft > 0 {
		retriesLeft--
	} else {
		return fmt.Errorf("retry fetcher out of retries: %w", err)
	}

	switch t := err.(type) {
	case ErrPartialResponse:
		if len(t.StillNeed) > 1 {
			return errors.New("only a single request at a time is supported")
		}

		// Resets the number of retries for partials, mimicking Caboose logic.
		retriesLeft = r.retries

		return r.fetch(ctx, t.StillNeed[0], cb, retriesLeft)
	default:
		return r.fetch(ctx, path, cb, retriesLeft)
	}
}
