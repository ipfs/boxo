package gateway

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestErrRetryAfterIs(t *testing.T) {
	t.Parallel()
	var err error

	err = NewErrorRetryAfter(errors.New("test"), 10*time.Second)
	require.True(t, errors.Is(err, &ErrorRetryAfter{}), "pointer to error must be error")

	err = fmt.Errorf("wrapped: %w", err)
	require.True(t, errors.Is(err, &ErrorRetryAfter{}), "wrapped pointer to error must be error")
}

func TestErrRetryAfterAs(t *testing.T) {
	t.Parallel()

	var (
		err   error
		errRA *ErrorRetryAfter
	)

	err = NewErrorRetryAfter(errors.New("test"), 25*time.Second)
	require.True(t, errors.As(err, &errRA), "pointer to error must be error")
	require.EqualValues(t, errRA.RetryAfter, 25*time.Second)

	err = fmt.Errorf("wrapped: %w", err)
	require.True(t, errors.As(err, &errRA), "wrapped pointer to error must be error")
	require.EqualValues(t, errRA.RetryAfter, 25*time.Second)
}

func TestWebError(t *testing.T) {
	t.Parallel()

	// Create a handler to be able to test `webError`.
	config := &Config{Headers: map[string][]string{}}

	t.Run("429 Too Many Requests", func(t *testing.T) {
		t.Parallel()

		err := fmt.Errorf("wrapped for testing: %w", NewErrorRetryAfter(ErrTooManyRequests, 0))
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/blah", nil)
		webError(w, r, config, err, http.StatusInternalServerError)
		require.Equal(t, http.StatusTooManyRequests, w.Result().StatusCode)
		require.Zero(t, len(w.Result().Header.Values("Retry-After")))
	})

	t.Run("429 Too Many Requests with Retry-After header", func(t *testing.T) {
		t.Parallel()

		err := NewErrorRetryAfter(ErrTooManyRequests, 25*time.Second)
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/blah", nil)
		webError(w, r, config, err, http.StatusInternalServerError)
		require.Equal(t, http.StatusTooManyRequests, w.Result().StatusCode)
		require.Equal(t, "25", w.Result().Header.Get("Retry-After"))
	})

	t.Run("503 Service Unavailable with Retry-After header", func(t *testing.T) {
		t.Parallel()

		err := NewErrorRetryAfter(ErrServiceUnavailable, 50*time.Second)
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/blah", nil)
		webError(w, r, config, err, http.StatusInternalServerError)
		require.Equal(t, http.StatusServiceUnavailable, w.Result().StatusCode)
		require.Equal(t, "50", w.Result().Header.Get("Retry-After"))
	})

	t.Run("ErrorStatusCode propagates HTTP Status Code", func(t *testing.T) {
		t.Parallel()

		err := NewErrorStatusCodeFromStatus(http.StatusTeapot)
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/blah", nil)
		webError(w, r, config, err, http.StatusInternalServerError)
		require.Equal(t, http.StatusTeapot, w.Result().StatusCode)
	})
}
