package gateway

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestErrRetryAfterIs(t *testing.T) {
	var err error

	err = NewErrorWithRetryAfter(errors.New("test"), 10*time.Second)
	assert.True(t, errors.Is(err, &errRetryAfter{}), "pointer to error must be error")

	err = fmt.Errorf("wrapped: %w", err)
	assert.True(t, errors.Is(err, &errRetryAfter{}), "wrapped pointer to error must be error")
}

func TestErrRetryAfterAs(t *testing.T) {
	var (
		err   error
		errRA *errRetryAfter
	)

	err = NewErrorWithRetryAfter(errors.New("test"), 25*time.Second)
	assert.True(t, errors.As(err, &errRA), "pointer to error must be error")
	assert.EqualValues(t, errRA.RetryAfter, 25*time.Second)

	err = fmt.Errorf("wrapped: %w", err)
	assert.True(t, errors.As(err, &errRA), "wrapped pointer to error must be error")
	assert.EqualValues(t, errRA.RetryAfter, 25*time.Second)
}

func TestWebError(t *testing.T) {
	t.Parallel()

	t.Run("429 Too Many Requests", func(t *testing.T) {
		err := fmt.Errorf("wrapped for testing: %w", NewErrorWithRetryAfter(ErrTooManyRequests, 0))
		w := httptest.NewRecorder()
		webError(w, err, http.StatusInternalServerError)
		assert.Equal(t, http.StatusTooManyRequests, w.Result().StatusCode)
		assert.Zero(t, len(w.Result().Header.Values("Retry-After")))
	})

	t.Run("429 Too Many Requests with Retry-After header", func(t *testing.T) {
		err := NewErrorWithRetryAfter(ErrTooManyRequests, 25*time.Second)
		w := httptest.NewRecorder()
		webError(w, err, http.StatusInternalServerError)
		assert.Equal(t, http.StatusTooManyRequests, w.Result().StatusCode)
		assert.Equal(t, "25", w.Result().Header.Get("Retry-After"))
	})
}
