package chunk

import (
	"bytes"
	"fmt"
	"testing"
)

const (
	testTwoThirdsOfBlockPayloadLimit = 2 * (float32(BlockPayloadLimit) / float32(3))
)

func TestParseRabin(t *testing.T) {
	r := bytes.NewReader(randBuf(t, 1000))

	_, err := FromString(r, "rabin-18-25-32")
	if err != nil {
		t.Errorf(err.Error())
	}

	_, err = FromString(r, "rabin-15-23-31")
	if err != ErrRabinMin {
		t.Fatalf("Expected an 'ErrRabinMin' error, got: %#v", err)
	}

	_, err = FromString(r, "rabin-20-20-21")
	if err == nil || err.Error() != "incorrect format: rabin-min must be smaller than rabin-avg" {
		t.Fatalf("Expected an arg-out-of-order error, got: %#v", err)
	}

	_, err = FromString(r, "rabin-19-21-21")
	if err == nil || err.Error() != "incorrect format: rabin-avg must be smaller than rabin-max" {
		t.Fatalf("Expected an arg-out-of-order error, got: %#v", err)
	}

	_, err = FromString(r, fmt.Sprintf("rabin-19-21-%d", BlockPayloadLimit))
	if err != nil {
		t.Fatalf("Expected success, got: %#v", err)
	}

	_, err = FromString(r, fmt.Sprintf("rabin-19-21-%d", 1+BlockPayloadLimit))
	if err != ErrSizeMax {
		t.Fatalf("Expected 'ErrSizeMax', got: %#v", err)
	}

	_, err = FromString(r, fmt.Sprintf("rabin-%.0f", testTwoThirdsOfBlockPayloadLimit))
	if err != nil {
		t.Fatalf("Expected success, got: %#v", err)
	}

	_, err = FromString(r, fmt.Sprintf("rabin-%.0f", 1+testTwoThirdsOfBlockPayloadLimit))
	if err != ErrSizeMax {
		t.Fatalf("Expected 'ErrSizeMax', got: %#v", err)
	}

}

func TestParseSize(t *testing.T) {
	r := bytes.NewReader(randBuf(t, 1000))

	_, err := FromString(r, "size-0")
	if err != ErrSize {
		t.Fatalf("Expected an 'ErrSize' error, got: %#v", err)
	}

	_, err = FromString(r, "size-32")
	if err != nil {
		t.Fatalf("Expected success, got: %#v", err)
	}

	_, err = FromString(r, fmt.Sprintf("size-%d", BlockPayloadLimit))
	if err != nil {
		t.Fatalf("Expected success, got: %#v", err)
	}

	_, err = FromString(r, fmt.Sprintf("size-%d", 1+BlockPayloadLimit))
	if err != ErrSizeMax {
		t.Fatalf("Expected 'ErrSizeMax', got: %#v", err)
	}
}
