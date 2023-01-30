package unixfs

import (
	"github.com/ipfs/go-libipfs/ipsl"
)

// Name is the ScopeName for unixfs.
const Name ipsl.ScopeName = "/unixfs/v0.0.1"

var _ ipsl.Scope = scope{}

// scope provides the unixfs scope
type scope struct{}

func Scope() ipsl.Scope {
	return scope{}
}

func (scope) GetScope() (ipsl.ScopeMapping, error) {
	return ipsl.ScopeMapping{
		"everything": compileEverything,
	}, nil
}

func (scope) Serialize() (ipsl.AstNode, error) {
	return ipsl.AstNode{
		Type: ipsl.SyntaxTypeValueNode,
		Args: []ipsl.AstNode{
			{Type: ipsl.SyntaxTypeToken, Literal: "load-builtin-scope"},
			{Type: ipsl.SyntaxTypeStringLiteral, Literal: string(Name)},
		},
	}, nil
}

func (s scope) SerializeForNetwork() (ipsl.AstNode, error) {
	// TODO: return a (pick (load-builtin-scope ...) (load-builtin-wasm ...)) when we have wasm support.
	return s.Serialize()
}
