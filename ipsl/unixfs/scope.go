package unixfs

import (
	"github.com/ipfs/go-libipfs/ipsl"
)

// Name is the builtin scope name for unixfs.
const Name = "/unixfs/v0.0.1"

var _ ipsl.Scope = scope{}

// scope provides the unixfs scope
type scope struct{}

type boundScope struct {
	scope

	boundName string
}

func (s boundScope) Bound() string {
	return s.boundName
}

func Scope() ipsl.Scope {
	return scope{}
}

func (s scope) Eq(other ipsl.Scope) bool {
	return s == other
}

func (s scope) Scope(bound string) (ipsl.ScopeMapping, error) {
	b := boundScope{s, bound}
	return ipsl.ScopeMapping{
		"everything": compileEverything(b),
	}, nil
}

func (scope) Serialize() (ipsl.AstNode, []ipsl.BoundScope, error) {
	return ipsl.AstNode{
		Type: ipsl.SyntaxTypeValueNode,
		Args: []ipsl.AstNode{
			{Type: ipsl.SyntaxTypeToken, Literal: "load-builtin-scope"},
			{Type: ipsl.SyntaxTypeStringLiteral, Literal: string(Name)},
		},
	}, []ipsl.BoundScope{}, nil
}

func (s scope) SerializeForNetwork() (ipsl.AstNode, []ipsl.BoundScope, error) {
	// TODO: return a (pick (load-builtin-scope ...) (load-builtin-wasm ...)) when we have wasm support.
	return s.Serialize()
}
