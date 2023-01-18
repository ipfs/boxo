package ipsl

import (
	"fmt"

	"github.com/ipfs/go-cid"
)

type CidTraversalPair struct {
	Cid       cid.Cid
	Traversal Traversal
}

type Traversal interface {
	Node

	// Traverse must never be called with bytes not matching the cid.
	// The bytes must never be modified by the implementations.
	Traverse(cid.Cid, []byte) ([]CidTraversalPair, error)
}

type Node interface {
	Reflect() (SomeNode, error)

	// Serialize returns the real representation of what is being executed.
	Serialize() (AstNode, error)

	// SerializeForNetwork returns a representation that is very likely to make an other node produce the same traversal as the current one.
	// The main point of this is for builtin polyfills, for example a builtin node can choose to make itself a pick node that select between the builtin or some wasm module.
	SerializeForNetwork() (AstNode, error)
}

type Scope interface {
	Node

	GetScope() (map[string]NodeCompiler, error)
}

// An AllNode traverse all the traversals with the same cid it is given to.
type AllNode struct {
	Traversals []Traversal
}

func All(traversals ...Traversal) Traversal {
	return AllNode{traversals}
}

func CompileAll(scopeName string, arguments ...SomeNode) (SomeNode, error) {
	if scopeName != "" {
		panic("builtin all called not in the builtin scope")
	}

	traversals := make([]Traversal, len(arguments))
	for i, c := range arguments {
		if c.Type != TypeTraversal {
			return SomeNode{}, ErrTypeError{fmt.Sprintf("trying to build an all node but received a %s argument", c.Type.String())}
		}

		traversals[i] = c.Node.(Traversal)
	}

	return SomeNode{All(traversals...), TypeTraversal}, nil
}

func (n AllNode) Traverse(c cid.Cid, _ []byte) ([]CidTraversalPair, error) {
	r := make([]CidTraversalPair, len(n.Traversals))
	for i, t := range n.Traversals {
		r[i] = CidTraversalPair{c, t}
	}
	return r, nil
}

func (n AllNode) Reflect() (SomeNode, error) {
	return SomeNode{
		Type: TypeTraversal,
		Node: n,
	}, nil
}

func (n AllNode) Serialize() (AstNode, error) {
	args := make([]AstNode, len(n.Traversals)+1)
	args[0] = AstNode{
		Type:    SyntaxTypeToken,
		Literal: "all",
	}

	argsTraversals := args[1:]
	for i, child := range n.Traversals {
		var err error
		argsTraversals[i], err = child.Serialize()
		if err != nil {
			return AstNode{}, err
		}
	}

	return AstNode{
		Type: SyntaxTypeValueNode,
		Args: args,
	}, nil
}

func (n AllNode) SerializeForNetwork() (AstNode, error) {
	args := make([]AstNode, len(n.Traversals)+1)
	args[0] = AstNode{
		Type:    SyntaxTypeToken,
		Literal: "all",
	}

	argsTraversals := args[1:]
	for i, child := range n.Traversals {
		var err error
		argsTraversals[i], err = child.SerializeForNetwork()
		if err != nil {
			return AstNode{}, err
		}
	}

	return AstNode{
		Type: SyntaxTypeValueNode,
		Args: args,
	}, nil
}

var _ Node = CidLiteral{}

type CidLiteral struct {
	Cid cid.Cid
}

func (c CidLiteral) Reflect() (SomeNode, error) {
	return SomeNode{
		Type: TypeCid,
		Node: c,
	}, nil
}

func (c CidLiteral) Serialize() (AstNode, error) {
	return AstNode{
		Type:    SyntaxTypeCidLiteral,
		Literal: c.Cid,
	}, nil
}

func (c CidLiteral) SerializeForNetwork() (AstNode, error) {
	return c.Serialize()
}
