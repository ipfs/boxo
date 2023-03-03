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

func TestErrTooManyRequestsIs(t *testing.T) {
	var err error

	err = &ErrTooManyRequests{RetryAfter: 10 * time.Second}
	assert.True(t, errors.Is(err, &ErrTooManyRequests{}), "pointer to error must be error")

	err = fmt.Errorf("wrapped: %w", err)
	assert.True(t, errors.Is(err, &ErrTooManyRequests{}), "wrapped pointer to error must be error")
}

func TestErrTooManyRequestsAs(t *testing.T) {
	var (
		err    error
		errTMR *ErrTooManyRequests
	)

	err = &ErrTooManyRequests{RetryAfter: 25 * time.Second}
	assert.True(t, errors.As(err, &errTMR), "pointer to error must be error")
	assert.EqualValues(t, errTMR.RetryAfter, 25*time.Second)

	err = fmt.Errorf("wrapped: %w", err)
	assert.True(t, errors.As(err, &errTMR), "wrapped pointer to error must be error")
	assert.EqualValues(t, errTMR.RetryAfter, 25*time.Second)
}

func TestWebError(t *testing.T) {
	t.Parallel()

	t.Run("429 Too Many Requests", func(t *testing.T) {
		err := fmt.Errorf("wrapped for testing: %w", &ErrTooManyRequests{})
		w := httptest.NewRecorder()
		webError(w, err, http.StatusInternalServerError)
		assert.Equal(t, http.StatusTooManyRequests, w.Result().StatusCode)
		assert.Zero(t, len(w.Result().Header.Values("Retry-After")))
	})

	t.Run("429 Too Many Requests with Retry-After header", func(t *testing.T) {
		err := &ErrTooManyRequests{RetryAfter: 25 * time.Second}
		w := httptest.NewRecorder()
		webError(w, err, http.StatusInternalServerError)
		assert.Equal(t, http.StatusTooManyRequests, w.Result().StatusCode)
		assert.Equal(t, "25", w.Result().Header.Get("Retry-After"))
	})
}
