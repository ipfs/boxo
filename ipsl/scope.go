package ipsl

import "strings"

type frame struct {
	scope  ScopeMapping
	prefix string

	next *frame
}

func (f *frame) get(s string) (NodeCompiler, bool) {
	names := strings.SplitN(s, ".", 2)
	first := names[0]
	var second string
	if len(names) > 1 {
		second = names[1]
	} else {
		second = first
		first = ""
	}
	if f.prefix != first {
		goto Unmatch
	}
	{
		r, ok := f.scope[second]
		if ok {
			return r, true
		}
	}

Unmatch:
	if f.next == nil {
		return nil, false
	}

	return f.next.get(s)
}

// NodeCompiler is responsible for compiling arguments to a node.
// scope will be nil if this is called for the builtin scope.
type NodeCompiler func(arguments ...SomeNode) (SomeNode, error)

type ScopeMapping map[string]NodeCompiler
