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
	ErrGatewayTimeout = errors.New(http.StatusText(http.StatusGatewayTimeout))
	ErrBadGateway     = errors.New(http.StatusText(http.StatusBadGateway))
)

type ErrTooManyRequests struct {
	RetryAfter time.Duration
}

func (e *ErrTooManyRequests) Error() string {
	text := http.StatusText(http.StatusTooManyRequests)
	if e.RetryAfter != 0 {
		text += fmt.Sprintf(", retry after %s", e.RetryAfterHuman())
	}
	return text
}

func (e *ErrTooManyRequests) Is(err error) bool {
	switch err.(type) {
	case *ErrTooManyRequests:
		return true
	default:
		return false
	}
}

func (e *ErrTooManyRequests) RetryAfterRoundSeconds() time.Duration {
	return e.RetryAfter.Round(time.Second)
}

func (e *ErrTooManyRequests) RetryAfterHuman() string {
	return e.RetryAfterRoundSeconds().String()
}

func (e *ErrTooManyRequests) RetryAfterHeader() string {
	return strconv.Itoa(int(e.RetryAfterRoundSeconds().Seconds()))
}

func webError(w http.ResponseWriter, err error, defaultCode int) {
	code := defaultCode

	switch {
	case isErrNotFound(err):
		code = http.StatusNotFound
	case errors.Is(err, ErrGatewayTimeout),
		errors.Is(err, context.DeadlineExceeded):
		code = http.StatusGatewayTimeout
	case errors.Is(err, ErrBadGateway):
		code = http.StatusBadGateway
	case errors.Is(err, &ErrTooManyRequests{}):
		var tooManyRequests *ErrTooManyRequests
		_ = errors.As(err, &tooManyRequests)
		if tooManyRequests.RetryAfter > 0 {
			w.Header().Set("Retry-After", tooManyRequests.RetryAfterHeader())
		}

		code = http.StatusTooManyRequests
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
