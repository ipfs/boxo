package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-path/resolver"
)

var (
	ErrInternalServerError = errors.New(http.StatusText(http.StatusInternalServerError))
	ErrGatewayTimeout      = errors.New(http.StatusText(http.StatusGatewayTimeout))
	ErrBadGateway          = errors.New(http.StatusText(http.StatusBadGateway))
	ErrServiceUnavailable  = errors.New(http.StatusText(http.StatusServiceUnavailable))
	ErrTooManyRequests     = errors.New(http.StatusText(http.StatusTooManyRequests))
)

type errRetryAfter struct {
	RetryAfter time.Duration
	Err        error
}

// NewErrorWithRetryAfter wraps any error in RetryAfter hint that
// gets passed to HTTP clients in Retry-After HTTP header
func NewErrorWithRetryAfter(err error, retryAfter time.Duration) *errRetryAfter {
	if err == nil {
		err = ErrServiceUnavailable
	}
	if retryAfter < 0 {
		retryAfter = 0
	}
	return &errRetryAfter{
		RetryAfter: retryAfter,
		Err:        err,
	}
}

func (e *errRetryAfter) Error() string {
	text := e.Err.Error()
	if e.RetryAfter != 0 {
		text += fmt.Sprintf(", retry after %s", e.Humanized())
	}
	return text
}

func (e *errRetryAfter) Unwrap() error {
	return e.Err
}

func (e *errRetryAfter) Is(err error) bool {
	switch err.(type) {
	case *errRetryAfter:
		return true
	default:
		return false
	}
}

func (e *errRetryAfter) RoundSeconds() time.Duration {
	return e.RetryAfter.Round(time.Second)
}

func (e *errRetryAfter) Humanized() string {
	return e.RoundSeconds().String()
}

// HTTPHeaderValue returns the Retry-After header value as a string, representing the number
// of seconds to wait before making a new request, rounded to the nearest second.
// This function follows the Retry-After header definition as specified in RFC 9110.
func (e *errRetryAfter) HTTPHeaderValue() string {
	return strconv.Itoa(int(e.RoundSeconds().Seconds()))
}

func webError(w http.ResponseWriter, err error, defaultCode int) {
	code := defaultCode

	// Handle Retry-After header
	var era *errRetryAfter
	if errors.As(err, &era) {
		if era.RetryAfter > 0 {
			w.Header().Set("Retry-After", era.HTTPHeaderValue())
			// Adjust defaultCode if needed
			if code != http.StatusTooManyRequests && code != http.StatusServiceUnavailable {
				code = http.StatusTooManyRequests
			}
		}
		err = era.Unwrap()
	}

	// Handle Status Code
	switch {
	case isErrNotFound(err):
		code = http.StatusNotFound
	case errors.Is(err, ErrGatewayTimeout),
		errors.Is(err, context.DeadlineExceeded):
		code = http.StatusGatewayTimeout
	case errors.Is(err, ErrBadGateway):
		code = http.StatusBadGateway
	case errors.Is(err, ErrTooManyRequests):
		code = http.StatusTooManyRequests
	case errors.Is(err, ErrServiceUnavailable):
		code = http.StatusServiceUnavailable
	}

	http.Error(w, err.Error(), code)
}

func isErrNotFound(err error) bool {
	if ipld.IsNotFound(err) {
		return true
	}

	// Checks if err is a resolver.ErrNoLink. resolver.ErrNoLink does not implement
	// the .Is interface and cannot be directly compared to. Therefore, errors.Is
	// always returns false with it.
	for {
		_, ok := err.(resolver.ErrNoLink)
		if ok {
			return true
		}

		err = errors.Unwrap(err)
		if err == nil {
			return false
		}
	}
}

func webRequestError(w http.ResponseWriter, err *requestError) {
	webError(w, err.Err, err.StatusCode)
}

// Custom type for collecting error details to be handled by `webRequestError`
type requestError struct {
	StatusCode int
	Err        error
}

func newRequestError(err error, statusCode int) *requestError {
	return &requestError{
		Err:        err,
		StatusCode: statusCode,
	}
}
