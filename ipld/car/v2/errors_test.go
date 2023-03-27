package car

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewErrCidTooLarge_ErrorContainsSizes(t *testing.T) {
	subject := &ErrCidTooLarge{MaxSize: 1413, CurrentSize: 1414}
	require.EqualError(t, subject, "cid size is larger than max allowed (1414 > 1413)")
}
