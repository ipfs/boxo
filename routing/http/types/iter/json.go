package iter

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// FromReaderJSON returns an iterator over the given reader that reads whitespace-delimited JSON values.
func FromReaderJSON[T any](r io.Reader) *JSONIter[T] {
	return &JSONIter[T]{Decoder: json.NewDecoder(r), Reader: r}
}

// JSONIter iterates over whitespace-delimited JSON values of a byte stream.
// This closes the reader if it is a closer, to faciliate easy reading of HTTP responses.
type JSONIter[T any] struct {
	Decoder *json.Decoder
	Reader  io.Reader

	done bool
}

func (j *JSONIter[T]) Next() (T, bool, error) {
	var val T

	if j.done {
		return val, false, nil
	}

	err := j.Decoder.Decode(&val)
	if errors.Is(err, io.EOF) {
		return val, false, j.Close()
	}
	if err != nil {
		j.Close()
		return val, false, fmt.Errorf("json iterator: %w", err)
	}

	return val, true, nil
}

func (j *JSONIter[T]) Close() error {
	j.done = true
	if closer, ok := j.Reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
