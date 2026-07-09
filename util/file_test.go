package util_test

import (
	"testing"

	"github.com/ipfs/boxo/util"
)

func TestFileDoesNotExist(t *testing.T) {
	t.Parallel()
	if util.FileExists("i would be surprised to discover that this file exists") {
		t.Fail()
	}
}
