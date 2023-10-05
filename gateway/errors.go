package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/boxo/gateway/assets"
	"github.com/ipfs/boxo/path/resolver"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
)

var (
	ErrInternalServerError = NewErrorStatusCodeFromStatus(http.StatusInternalServerError)
	ErrGatewayTimeout      = NewErrorStatusCodeFromStatus(http.StatusGatewayTimeout)
	ErrBadGateway          = NewErrorStatusCodeFromStatus(http.StatusBadGateway)
	ErrServiceUnavailable  = NewErrorStatusCodeFromStatus(http.StatusServiceUnavailable)
	ErrTooManyRequests     = NewErrorStatusCodeFromStatus(http.StatusTooManyRequests)
)

// ErrorRetryAfter wraps any error with "retry after" hint. When an error of this type
// returned to the gateway handler by an [IPFSBackend], the retry after value will be
// passed to the HTTP client in a [Retry-After] HTTP header.
//
// [Retry-After]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
type ErrorRetryAfter struct {
	Err        error
	RetryAfter time.Duration
}

func NewErrorRetryAfter(err error, retryAfter time.Duration) *ErrorRetryAfter {
	if err == nil {
		err = ErrServiceUnavailable
	}
	if retryAfter < 0 {
		retryAfter = 0
	}
	return &ErrorRetryAfter{
		RetryAfter: retryAfter,
		Err:        err,
	}
}

func (e *ErrorRetryAfter) Error() string {
	var text string
	if e.Err != nil {
		text = e.Err.Error()
	}
	if e.RetryAfter != 0 {
		text += fmt.Sprintf(", retry after %s", e.humanizedRoundSeconds())
	}
	return text
}

func (e *ErrorRetryAfter) Unwrap() error {
	return e.Err
}

func (e *ErrorRetryAfter) Is(err error) bool {
	switch err.(type) {
	case *ErrorRetryAfter:
		return true
	default:
		return false
	}
}

// RetryAfterHeader returns the [Retry-After] header value as a string, representing the number
// of seconds to wait before making a new request, rounded to the nearest second.
// This function follows the [Retry-After] header definition as specified in RFC 9110.
//
// [Retry-After]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
func (e *ErrorRetryAfter) RetryAfterHeader() string {
	return strconv.Itoa(int(e.roundSeconds().Seconds()))
}

func (e *ErrorRetryAfter) roundSeconds() time.Duration {
	return e.RetryAfter.Round(time.Second)
}

func (e *ErrorRetryAfter) humanizedRoundSeconds() string {
	return e.roundSeconds().String()
}

// ErrorStatusCode wraps any error with a specific HTTP status code. When an error
// of this type is returned to the gateway handler by an [IPFSBackend], the status
// code will be used for the response status.
type ErrorStatusCode struct {
	StatusCode int
	Err        error
}

func NewErrorStatusCodeFromStatus(statusCode int) *ErrorStatusCode {
	return NewErrorStatusCode(errors.New(http.StatusText(statusCode)), statusCode)
}

func NewErrorStatusCode(err error, statusCode int) *ErrorStatusCode {
	return &ErrorStatusCode{
		Err:        err,
		StatusCode: statusCode,
	}
}

func (e *ErrorStatusCode) Is(err error) bool {
	switch err.(type) {
	case *ErrorStatusCode:
		return true
	default:
		return false
	}
}

func (e *ErrorStatusCode) Error() string {
	var text string
	if e.Err != nil {
		text = e.Err.Error()
	}
	return text
}

func (e *ErrorStatusCode) Unwrap() error {
	return e.Err
}

func webError(w http.ResponseWriter, r *http.Request, c *Config, err error, defaultCode int) {
	code := defaultCode

	// Pass Retry-After hint to the client
	var era *ErrorRetryAfter
	if errors.As(err, &era) {
		if era.RetryAfter > 0 {
			w.Header().Set("Retry-After", era.RetryAfterHeader())
			// Adjust defaultCode if needed
			if code != http.StatusTooManyRequests && code != http.StatusServiceUnavailable {
				code = http.StatusTooManyRequests
			}
		}
		err = era.Unwrap()
	}

	// Handle status code
	switch {
	case errors.Is(err, &cid.ErrInvalidCid{}):
		code = http.StatusBadRequest
	case isErrNotFound(err):
		code = http.StatusNotFound
	case errors.Is(err, context.DeadlineExceeded):
		code = http.StatusGatewayTimeout
	}

	// Handle explicit code in ErrorResponse
	var gwErr *ErrorStatusCode
	if errors.As(err, &gwErr) {
		code = gwErr.StatusCode
	}

	acceptsHTML := !c.DisableHTMLErrors && strings.Contains(r.Header.Get("Accept"), "text/html")
	if acceptsHTML {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(code)
		_ = assets.ErrorTemplate.Execute(w, assets.ErrorTemplateData{
			GlobalData: assets.GlobalData{
				Menu: c.Menu,
			},
			StatusCode: code,
			StatusText: http.StatusText(code),
			Error:      err.Error(),
		})
	} else {
		http.Error(w, err.Error(), code)
	}
}

// isErrNotFound returns true for IPLD errors that should return 4xx errors (e.g. the path doesn't exist, the data is
// the wrong type, etc.), rather than issues with just finding and retrieving the data.
func isErrNotFound(err error) bool {
	// Checks if err is of a type that does not implement the .Is interface and
	// cannot be directly compared to. Therefore, errors.Is cannot be used.
	for {
		_, ok := err.(resolver.ErrNoLink)
		if ok {
			return true
		}

		_, ok = err.(datamodel.ErrWrongKind)
		if ok {
			return true
		}

		_, ok = err.(datamodel.ErrNotExists)
		if ok {
			return true
		}

		err = errors.Unwrap(err)
		if err == nil {
			return false
		}
	}
}
