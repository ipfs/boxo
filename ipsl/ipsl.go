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
	// The []Scope must return the self Scope and the Scopes of all children nodes.
	Serialize() (AstNode, []BoundScope, error)

	// SerializeForNetwork returns a representation that is very likely to make an other node produce the same traversal as the current one.
	// The main point of this is for builtin polyfills, for example a builtin node can choose to make itself a pick node that select between the builtin or some wasm module.
	// The Scope must return the self Scope and the Scopes of all children nodes.
	SerializeForNetwork() (AstNode, []BoundScope, error)
}

type Scope interface {
	Node

	// GetScope returns a scope mapping, consumers are not allowed to modify the ScopeMapping.
	Scope(bound string) (ScopeMapping, error)

	// Eq must return true if the two scopes are the same.
	// Eq must not take name binding in consideration.
	Eq(Scope) bool
}

// BoundScope represent a Scope that have been named.
type BoundScope interface {
	Scope

	Bound() string
}

// An AllNode traverse all the traversals with the same cid it is given to.
type AllNode struct {
	Traversals []Traversal
}

func All(traversals ...Traversal) Traversal {
	return AllNode{traversals}
}

func compileAll(arguments ...SomeNode) (SomeNode, error) {
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

func (n AllNode) serialize(serialize func(n Node) (AstNode, []BoundScope, error)) (AstNode, []BoundScope, error) {
	args := make([]AstNode, len(n.Traversals)+1)
	args[0] = AstNode{
		Type:    SyntaxTypeToken,
		Literal: "all",
	}

	var scopes []BoundScope
	argsTraversals := args[1:]
	for i, child := range n.Traversals {
		var err error
		var lscopes []BoundScope
		argsTraversals[i], lscopes, err = serialize(child)
		scopes = UniqScopes(append(scopes, lscopes...))
		if err != nil {
			return AstNode{}, nil, err
		}
	}

	return AstNode{
		Type: SyntaxTypeValueNode,
		Args: args,
	}, scopes, nil
}

func (n AllNode) Serialize() (AstNode, []BoundScope, error) {
	return n.serialize(func(n Node) (AstNode, []BoundScope, error) {
		return n.Serialize()
	})
}

func (n AllNode) SerializeForNetwork() (AstNode, []BoundScope, error) {
	return n.serialize(func(n Node) (AstNode, []BoundScope, error) {
		return n.SerializeForNetwork()
	})
}

var _ Node = CidLiteral{}

type CidLiteral struct {
	Cid cid.Cid
}

func (c CidLiteral) Serialize() (AstNode, []BoundScope, error) {
	return AstNode{
		Type:    SyntaxTypeCidLiteral,
		Literal: c.Cid,
	}, []BoundScope{}, nil
}

func (c CidLiteral) SerializeForNetwork() (AstNode, []BoundScope, error) {
	return c.Serialize()
}

var _ Node = StringLiteral{}

type StringLiteral struct {
	Str string
}

func (c StringLiteral) Serialize() (AstNode, []BoundScope, error) {
	return AstNode{
		Type:    SyntaxTypeStringLiteral,
		Literal: c.Str,
	}, []BoundScope{}, nil
}

func (c StringLiteral) SerializeForNetwork() (AstNode, []BoundScope, error) {
	return c.Serialize()
}

var _ Node = None{}

type None struct{}

func (c None) Serialize() (AstNode, []BoundScope, error) {
	return AstNode{
		Type: SyntaxTypeNone,
	}, []BoundScope{}, nil
}

func (c None) SerializeForNetwork() (AstNode, []BoundScope, error) {
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

func (c EmptyTraversal) Serialize() (AstNode, []BoundScope, error) {
	return AstNode{
		Type: SyntaxTypeValueNode,
		Args: []AstNode{{
			Type:    SyntaxTypeToken,
			Literal: "empty",
		}},
	}, []BoundScope{}, nil
}

func (c EmptyTraversal) SerializeForNetwork() (AstNode, []BoundScope, error) {
	return c.Serialize()
}

func compileEmpty(arguments ...SomeNode) (SomeNode, error) {
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
	case StringLiteral:
		return "String"
	case None:
		return "None"
	default:
		return fmt.Sprintf("unknown node type %T", n)
	}
}
