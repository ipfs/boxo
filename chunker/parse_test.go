package chunk

import (
	"bytes"
	"testing"
)

func TestParse(t *testing.T) {
	max := 1000
	r := bytes.NewReader(randBuf(t, max))
	chk1 := "rabin-18-25-32"
	chk2 := "rabin-15-23-31"
	_, err := parseRabinString(r, chk1)
	if err != nil {
		t.Errorf(err.Error())
	}
	_, err = parseRabinString(r, chk2)
	if err == ErrRabinMin {
		t.Log("it should be ErrRabinMin here.")
	}
}
