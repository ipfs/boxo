package util_test

import (
	"testing"
	"time"

	"github.com/ipfs/boxo/util"
)

func TestTimeFormatParseInversion(t *testing.T) {
	v, err := util.ParseRFC3339(util.FormatRFC3339(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	if v.Location() != time.UTC {
		t.Fatal("Time should be UTC")
	}
}
