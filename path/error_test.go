package path

import (
	"errors"
	"testing"
)

func TestErrorIs(t *testing.T) {
	if !errors.Is(ErrInvalidPath{path: "foo", error: errors.New("bar")}, ErrInvalidPath{}) {
		t.Fatal("error must be error")
	}

	if !errors.Is(&ErrInvalidPath{path: "foo", error: errors.New("bar")}, ErrInvalidPath{}) {
		t.Fatal("pointer to error must be error")
	}
}
