package ipsl_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	. "github.com/ipfs/go-libipfs/ipsl"
)

func reflect(scope string, nodes ...SomeNode) (SomeNode, error) {
	if scope != "" {
		return SomeNode{}, fmt.Errorf("got non empty scope while compiling reflect: %q", scope)
	}

	if len(nodes) != 1 {
		return SomeNode{}, fmt.Errorf("got arguments list that is not one while compiling reflect: %#v", nodes)
	}

	return nodes[0], nil
}

func TestBasicCompileWithBuiltin(t *testing.T) {
	var c Compiler
	c.SetBuiltin("reflect", reflect)

	const code = `(reflect $bafkqaaa)`
	node, n, err := c.Compile(strings.NewReader(code))
	if err != nil {
		t.Fatalf("failed to compile: %s", err.Error())
	}
	if n != len(code) {
		t.Errorf("bytes red does not match code size")
	}

	if node.Type != TypeCid {
		t.Fatalf("type does not match, expected Cid; got: %s", node.Type)
	}
	expected := cid.MustParse("bafkqaaa")
	if c := node.Node.(CidLiteral).Cid; !c.Equals(expected) {
		t.Errorf("cid does not match, expected: %s; got %s", expected, c)
	}
}

func TestBasic2CompileWithBuiltin(t *testing.T) {
	var c Compiler
	c.SetBuiltin("reflect", reflect)

	const code = `(reflect (reflect $bafkqaaa))`
	node, n, err := c.Compile(strings.NewReader(code))
	if err != nil {
		t.Fatalf("failed to compile: %s", err.Error())
	}
	if n != len(code) {
		t.Errorf("bytes red does not match code size")
	}

	if node.Type != TypeCid {
		t.Fatalf("type does not match, expected Cid; got: %s", node.Type)
	}
	expected := cid.MustParse("bafkqaaa")
	if c := node.Node.(CidLiteral).Cid; !c.Equals(expected) {
		t.Errorf("cid does not match, expected: %s; got %s", expected, c)
	}
}

func FuzzCompile(f *testing.F) {
	var c Compiler
	c.SetBuiltin("reflect", reflect)

	f.Add(`(reflect (reflect $bafkqaaa))`)
	f.Fuzz(func(_ *testing.T, code string) {
		c.Compile(strings.NewReader(code))
	})
}
