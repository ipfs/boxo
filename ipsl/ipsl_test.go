package ipsl_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	. "github.com/ipfs/go-libipfs/ipsl"
)

func reflect(expectedScope string) func(string, ...SomeNode) (SomeNode, error) {
	return func(scope string, nodes ...SomeNode) (SomeNode, error) {
		if scope != expectedScope {
			return SomeNode{}, fmt.Errorf("got unexpected scope while compiling reflect: %q instead of: %q", scope, expectedScope)
		}

		if len(nodes) != 1 {
			return SomeNode{}, fmt.Errorf("got arguments list that is not one while compiling reflect: %#v", nodes)
		}

		return nodes[0], nil
	}
}

func TestBasicCompileWithBuiltin(t *testing.T) {
	var c Compiler
	c.SetBuiltin("reflect", reflect(""))

	const code = `(reflect $bafkqaaa)`
	node, n, err := c.Compile(strings.NewReader(code))
	if err != nil {
		t.Fatalf("failed to compile: %s", err.Error())
	}
	if n != len(code) {
		t.Errorf("bytes red does not match code size")
	}

	cidlit, ok := node.Node.(CidLiteral)
	if !ok {
		t.Fatalf("type does not match, expected Cid; got: %s", PrettyNodeType(node.Node))
	}
	expected := cid.MustParse("bafkqaaa")
	if !cidlit.Cid.Equals(expected) {
		t.Errorf("cid does not match, expected: %s; got %s", expected, cidlit.Cid)
	}
}

func TestBasic2CompileWithBuiltin(t *testing.T) {
	var c Compiler
	c.SetBuiltin("reflect", reflect(""))

	const code = `(reflect{some comment to test ([)] comments}(reflect $bafkqaaa))`
	node, n, err := c.Compile(strings.NewReader(code))
	if err != nil {
		t.Fatalf("failed to compile: %s", err.Error())
	}
	if n != len(code) {
		t.Errorf("bytes red does not match code size")
	}

	cidlit, ok := node.Node.(CidLiteral)
	if !ok {
		t.Fatalf("type does not match, expected Cid; got: %s", PrettyNodeType(node.Node))
	}
	expected := cid.MustParse("bafkqaaa")
	if !cidlit.Cid.Equals(expected) {
		t.Errorf("cid does not match, expected: %s; got %s", expected, cidlit.Cid)
	}
}

type mockScopeNode struct {
	scope map[string]NodeCompiler
}

func (n mockScopeNode) Serialize() (AstNode, error) {
	return AstNode{
		Type: SyntaxTypeValueNode,
		Args: []AstNode{{
			Type:    SyntaxTypeToken,
			Literal: "load-test-scope",
		}},
	}, nil
}

func (n mockScopeNode) SerializeForNetwork() (AstNode, error) { return n.Serialize() }

func (n mockScopeNode) GetScope() (map[string]NodeCompiler, error) {
	return n.scope, nil
}

func TestScopeCompileWithBuiltin(t *testing.T) {
	var c Compiler
	c.SetBuiltin("load-test-scope", func(scope string, nodes ...SomeNode) (SomeNode, error) {
		if scope != "" {
			return SomeNode{}, fmt.Errorf("got non empty scope while compiling reflect: %q", scope)
		}

		if len(nodes) != 0 {
			return SomeNode{}, fmt.Errorf("got arguments list that is not empty: %#v", nodes)
		}

		return SomeNode{
			Node: mockScopeNode{map[string]NodeCompiler{
				"reflect":             reflect("test-scope"),
				"reflect.cursed.name": reflect("test-scope"),
			}},
		}, nil
	})

	const code = `[test-scope (load-test-scope) (test-scope.reflect (test-scope.reflect.cursed.name $bafkqaaa))]`
	node, n, err := c.Compile(strings.NewReader(code))
	if err != nil {
		t.Fatalf("failed to compile: %s", err.Error())
	}
	if n != len(code) {
		t.Errorf("bytes red does not match code size")
	}

	cidlit, ok := node.Node.(CidLiteral)
	if !ok {
		t.Fatalf("type does not match, expected Cid; got: %s", PrettyNodeType(node.Node))
	}
	expected := cid.MustParse("bafkqaaa")
	if !cidlit.Cid.Equals(expected) {
		t.Errorf("cid does not match, expected: %s; got %s", expected, cidlit.Cid)
	}
}

func TestEmpty(t *testing.T) {
	const code = `(empty)`
	trav, n, err := CompileToTraversal(strings.NewReader(code))
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}
	if n != len(code) {
		t.Errorf("unexpected code length returned: expected %d; got %d", len(code), n)
	}

	ast, err := trav.Serialize()
	if err != nil {
		t.Errorf("unexpected error: %s", err.Error())
	} else {
		rebuiltCode := ast.String()
		if rebuiltCode != code {
			t.Errorf("serialized code does not match: expected %q, got %q", code, rebuiltCode)
		}
	}
	ast, err = trav.SerializeForNetwork()
	if err != nil {
		t.Errorf("unexpected error: %s", err.Error())
	} else {
		rebuiltCode := ast.String()
		if rebuiltCode != code {
			t.Errorf("serialized code does not match: expected %q, got %q", code, rebuiltCode)
		}
	}

	cids, err := trav.Traverse(blocks.NewBlock([]byte("some bytes")))
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}
	if len(cids) != 0 {
		t.Errorf("unexpected traversal results matching empty")
	}
}

func TestUnexpectedEOFInBrokenNode(t *testing.T) {
	_, _, err := (&Compiler{}).Compile(strings.NewReader(`(empty {this is some unterminated node}`))
	if err == nil {
		t.Fatal("unexpected incorrect code does not return an error")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("unexpected error: expected %q; got %q", io.ErrUnexpectedEOF, err)
	}
}

func FuzzCompile(f *testing.F) {
	var c Compiler
	c.SetBuiltin("reflect", reflect(""))

	f.Add(`(reflect (reflect $bafkqaaa))`)
	f.Fuzz(func(_ *testing.T, code string) {
		c.Compile(strings.NewReader(code))
	})
}
