package ipsl

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
)

type CidTraversalPair struct {
	Cid       cid.Cid
	Traversal Traversal
}

type Traversal interface {
	Node

	// Traverse must never be called with bytes not matching the cid.
	// The bytes must never be modified by the implementations.
	Traverse(blocks.Block) ([]CidTraversalPair, error)
}

type Node interface {
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
		trav, ok := c.Node.(Traversal)
		if !ok {
			return SomeNode{}, ErrTypeError{fmt.Sprintf("trying to build an all node but received wrong type as %T argument", PrettyNodeType(c.Node))}
		}

		traversals[i] = trav
	}

	return SomeNode{All(traversals...)}, nil
}

func (n AllNode) Traverse(b blocks.Block) ([]CidTraversalPair, error) {
	c := b.Cid()
	r := make([]CidTraversalPair, len(n.Traversals))
	for i, t := range n.Traversals {
		r[i] = CidTraversalPair{c, t}
	}
	return r, nil
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

func (c CidLiteral) Serialize() (AstNode, error) {
	return AstNode{
		Type:    SyntaxTypeCidLiteral,
		Literal: c.Cid,
	}, nil
}

func (c CidLiteral) SerializeForNetwork() (AstNode, error) {
	return c.Serialize()
}

// EmptyTraversal is a traversal that always returns nothing
type EmptyTraversal struct{}

func Empty() Traversal {
	return EmptyTraversal{}
}

func (c EmptyTraversal) Traverse(_ blocks.Block) ([]CidTraversalPair, error) {
	return []CidTraversalPair{}, nil
}

func (c EmptyTraversal) Serialize() (AstNode, error) {
	return AstNode{
		Type: SyntaxTypeValueNode,
		Args: []AstNode{{
			Type:    SyntaxTypeToken,
			Literal: "empty",
		}},
	}, nil
}

func (c EmptyTraversal) SerializeForNetwork() (AstNode, error) {
	return c.Serialize()
}

func CompileEmpty(scopeName string, arguments ...SomeNode) (SomeNode, error) {
	if scopeName != "" {
		panic("builtin empty called not in the builtin scope")
	}

	if len(arguments) != 0 {
		return SomeNode{}, ErrTypeError{fmt.Sprintf("empty node called with %d arguments, empty does not take arguments", len(arguments))}
	}

	return SomeNode{Empty()}, nil
}

func PrettyNodeType(n Node) string {
	switch n.(type) {
	case Traversal:
		return "Traversal"
	case Scope:
		return "Scope"
	case CidLiteral:
		return "Cid"
	default:
		return fmt.Sprintf("unknown node type %T", n)
	}
}
