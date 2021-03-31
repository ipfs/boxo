package data

import (
	"fmt"
)

type ErrWrongNodeType struct {
	Expected int64
	Actual   int64
}

func (e ErrWrongNodeType) Error() string {
	expectedName, ok := DataTypeNames[e.Expected]
	if !ok {
		expectedName = "Unknown Type"
	}
	actualName, ok := DataTypeNames[e.Actual]
	if !ok {
		actualName = "Unknown Type"
	}
	return fmt.Sprintf("Incorrect Node Type: (UnixFSData) expected type: %s, actual type: %s", expectedName, actualName)
}
