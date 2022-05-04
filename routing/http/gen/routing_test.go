package main

import (
	"testing"

	"github.com/ipld/edelweiss/compile"
)

func TestCompile(t *testing.T) {
	x := &compile.GoPkgCodegen{
		GoPkgDirPath: "",
		GoPkgPath:    "test",
		Defs:         proto,
	}
	goFile, err := x.Compile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = goFile.Generate(); err != nil {
		t.Fatal(err)
	}
}
