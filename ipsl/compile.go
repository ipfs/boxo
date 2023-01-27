package ipsl

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
)

type UnreadableRuneReader interface {
	io.RuneReader

	UnreadRune() error
}

// ErrTypeError indicagtes a syntax error, this error will always be errors.Is able.
type ErrTypeError struct {
	msg string
}

func (e ErrTypeError) Error() string {
	return e.msg
}

// ErrSyntaxError indicates a syntax error, this error will always be errors.Is able.
type ErrSyntaxError struct {
	msg string
}

func (e ErrSyntaxError) Error() string {
	return e.msg
}

// CompileToTraversal returns a Traversal that has been compiled, the number of bytes red and an error.
// It compiles with the default builtin scope.
func CompileToTraversal(rr UnreadableRuneReader) (Traversal, int, error) {
	return (&Compiler{}).CompileToTraversal(rr)
}

// Compile returns some node that has been compiled, the number of bytes red and an error.
// It compiles with the default builtin scope.
func Compile(rr UnreadableRuneReader) (SomeNode, int, error) {
	return (&Compiler{}).Compile(rr)
}

// Compiler allows to do multiple compilations and customise which builtins are available by default.
// The main point of this is for the testsuite because it allows to add mocked builtins nodes.
// You should most likely just use the CompileToTraversal and Compile functions.
// The zero value is valid and include the default builtin scope.
type Compiler struct {
	builtinFrame frame
	initFrame    sync.Once
}

func (c *Compiler) initDefaultFrame() {
	c.initFrame.Do(func() {
		c.builtinFrame.scope = make(map[string]NodeCompiler)
		c.builtinFrame.next = &defaultBuiltinFrame
	})
}

func (c *Compiler) SetBuiltin(name string, nodeCompiler NodeCompiler) {
	c.initDefaultFrame()
	c.builtinFrame.scope[name] = nodeCompiler
}

// CompileToTraversal returns a Traversal that has been compiled, the number of bytes red and an error.
// It is thread safe to do multiple compilations at once on the same Compiler. But not with AddBuiltin.
func (c *Compiler) CompileToTraversal(rr UnreadableRuneReader) (Traversal, int, error) {
	node, n, err := c.Compile(rr)
	if err != nil {
		return nil, n, err
	}
	traversal, ok := node.Node.(Traversal)
	if !ok {
		return nil, n, ErrTypeError{fmt.Sprintf("expected type Traversal; got %q", PrettyNodeType(node.Node))}
	}
	return traversal, n, nil
}

// Compile returns some node that has been compiled, the number of bytes red and an error.
// It is thread safe to do multiple compilations at once on the same Compiler. But not with AddBuiltin.
func (c *Compiler) Compile(rr UnreadableRuneReader) (SomeNode, int, error) {
	c.initDefaultFrame()
	return compiler{rr}.compileNextNodeWithoutClosure(&c.builtinFrame)
}

// compiler represent a single pass compilation attempt
type compiler struct {
	rr UnreadableRuneReader
}

// compileNextNode compile the next node, it will return a non zero rune if a closing brace has been reached.
// It will never return a non zero SomeNode as well as a non zero rune.
func (c compiler) compileNextNode(f *frame) (SomeNode, rune, int, error) {
	var sum int
	for {
		r, n, err := c.rr.ReadRune()
		sum += n
		switch err {
		case nil:
		case io.EOF:
			err = io.ErrUnexpectedEOF
			fallthrough
		default:
			return SomeNode{}, 0, sum, fmt.Errorf("compileNextNode ReadRune: %w", err)
		}

		switch r {
		case '(':
			node, n, err := c.compileValueNode(f)
			sum += n
			if err != nil {
				return SomeNode{}, 0, sum, fmt.Errorf("compileNextNode compileValueNode: %w", err)
			}
			return node, 0, sum, nil
		case '[':
			node, n, err := c.compileScopeNode(f)
			sum += n
			if err != nil {
				return SomeNode{}, 0, sum, fmt.Errorf("compileNextNode compileScopeNode: %w", err)
			}
			return node, 0, sum, nil
		case ')', ']':
			// We cannot see a } in correct code because they are swalloed by skipComment.
			return SomeNode{}, r, sum, nil
		case '$':
			node, n, err := c.compileCid()
			sum += n
			if err != nil {
				return SomeNode{}, 0, sum, fmt.Errorf("compileNextNode compileCid: %w", err)
			}
			return node, 0, sum, nil
		// TODO: implement other literals
		case '{':
			n, err := c.skipComment()
			sum += n
			if err != nil {
				return SomeNode{}, 0, sum, fmt.Errorf("compileNextNode skipComment: %w", err)
			}
		case ' ', '\n', '\t':
			continue
		default:
			return SomeNode{}, 0, sum, fmt.Errorf("while parsing next node encountered unkown rune %q", string(r))
		}
	}
}

func (c compiler) compileNextNodeWithoutClosure(f *frame) (SomeNode, int, error) {
	node, end, n, err := c.compileNextNode(f)
	if err != nil {
		return SomeNode{}, n, err
	}
	if end != 0 {
		return SomeNode{}, n, ErrSyntaxError{fmt.Sprintf("unexpected node termination %q", string(end))}
	}

	return node, n, err
}

func (c compiler) compileCid() (SomeNode, int, error) {
	token, n, err := c.readToken()
	if err != nil {
		return SomeNode{}, n, err
	}

	cid, err := cid.Decode(string(token))
	if err != nil {
		return SomeNode{}, n, err
	}

	return SomeNode{
		Node: CidLiteral{cid},
	}, n, nil
}

func (c compiler) compileValueNode(f *frame) (SomeNode, int, error) {
	var sum int
	tokenRunes, n, err := c.readToken()
	sum += n
	if err != nil {
		return SomeNode{}, sum, err
	}

	var arguments []SomeNode
argumentLoop:
	for {
		node, end, n, err := c.compileNextNode(f)
		sum += n
		if err != nil {
			return SomeNode{}, sum, err
		}

		switch end {
		case ')':
			break argumentLoop
		case 0:
		default:
			return SomeNode{}, sum, ErrSyntaxError{fmt.Sprintf("incorrect termination for a value node: %q", string(end))}
		}

		arguments = append(arguments, node)
	}

	token := string(tokenRunes)
	scopeName, compiler, ok := f.get(token)
	if !ok {
		return SomeNode{}, sum, ErrTypeError{fmt.Sprintf("did not found token %s in current scope", token)}
	}

	result, err := compiler(scopeName, arguments...)
	return result, sum, err
}

func (c compiler) compileScopeNode(f *frame) (SomeNode, int, error) {
	var sum int
	token, n, err := c.readToken()
	sum += n
	if err != nil {
		return SomeNode{}, sum, err
	}

	scope, n, err := c.compileNextNodeWithoutClosure(f)
	sum += n
	if err != nil {
		return SomeNode{}, sum, err
	}
	scopeObject, ok := scope.Node.(Scope)
	if !ok {
		return SomeNode{}, sum, ErrTypeError{fmt.Sprintf("expected Scope type node; got %s", PrettyNodeType(scope.Node))}
	}

	scopeMap, err := scopeObject.GetScope()
	if err != nil {
		return SomeNode{}, sum, err
	}

	boundToken := string(token)
	nextFrame := &frame{
		prefix: boundToken,
		scope:  scopeMap,

		next: f,
	}

	result, n, err := c.compileNextNodeWithoutClosure(nextFrame)
	sum += n
	if err != nil {
		return SomeNode{}, sum, err
	}

	// checks that nothings follows after the scope
	afterNode, end, n, err := c.compileNextNode(nextFrame)
	sum += n
	if err != nil {
		return SomeNode{}, sum, err
	}

	switch end {
	case ']':
		break
	case 0:
		ast, err := afterNode.Node.Serialize()
		if err != nil {
			// can't Serialize the node, don't include node in error message
			return SomeNode{}, sum, ErrSyntaxError{fmt.Sprintf("unexpected node following value node inside scope node %q", ast.String())}
		}
		return SomeNode{}, sum, ErrSyntaxError{"unexpected node following value node inside scope node"}
	default:
		return SomeNode{}, sum, ErrSyntaxError{fmt.Sprintf("incorrect terminator for a scope node %q", string(end))}
	}

	sn := scopeNode{boundToken, scope, result.Node}
	switch result.Node.(type) {
	case CidLiteral:
		return result, sum, nil
	case Traversal:
		return SomeNode{traversalScopeNode{sn}}, sum, nil
	case Scope:
		return SomeNode{scopeScopeNode{sn}}, sum, nil
	// TODO: implement more types when they are added.
	default:
		return SomeNode{}, sum, fmt.Errorf("unimplemented type forwarding in scope for %q", PrettyNodeType(result.Node))
	}
}

type scopeNode struct {
	boundToken string
	scope      SomeNode
	result     Node
}

func (n scopeNode) Serialize() (AstNode, error) {
	scopeAst, err := n.scope.Node.Serialize()
	if err != nil {
		return AstNode{}, err
	}
	resultAst, err := n.scope.Node.Serialize()
	if err != nil {
		return AstNode{}, err
	}

	return AstNode{
		Type: SyntaxTypeScopeNode,
		Args: []AstNode{
			{
				Type:    SyntaxTypeToken,
				Literal: n.boundToken,
			},
			scopeAst,
			resultAst,
		},
	}, nil
}

func (n scopeNode) SerializeForNetwork() (AstNode, error) {
	scopeAst, err := n.scope.Node.SerializeForNetwork()
	if err != nil {
		return AstNode{}, err
	}
	resultAst, err := n.scope.Node.SerializeForNetwork()
	if err != nil {
		return AstNode{}, err
	}

	return AstNode{
		Type: SyntaxTypeScopeNode,
		Args: []AstNode{
			{
				Type:    SyntaxTypeToken,
				Literal: n.boundToken,
			},
			scopeAst,
			resultAst,
		},
	}, nil
}

type traversalScopeNode struct {
	scopeNode
}

func (n traversalScopeNode) Traverse(b blocks.Block) ([]CidTraversalPair, error) {
	r, err := n.scopeNode.result.(Traversal).Traverse(b)
	if err != nil {
		return nil, err
	}

	// Wrap objects in scope nodes so Serialize output the correct scope node first.
	for i, v := range r {
		r[i].Traversal = traversalScopeNode{scopeNode{n.scopeNode.boundToken, n.scopeNode.scope, v.Traversal}}
	}

	return r, nil
}

type scopeScopeNode struct {
	scopeNode
}

func (n scopeScopeNode) GetScope() (map[string]NodeCompiler, error) {
	return n.scopeNode.result.(Scope).GetScope()
}

func (c compiler) skipComment() (int, error) {
	var sum int
	for {
		r, n, err := c.rr.ReadRune()
		sum += n
		if err != nil {
			return sum, err
		}

		if r != '}' {
			continue
		}

		return sum, nil
	}
}

func (c compiler) readToken() ([]rune, int, error) {
	var sum int
	var token []rune
	for {
		r, n, err := c.rr.ReadRune()
		sum += n
		if err != nil {
			return nil, sum, err
		}

		switch r {
		case '{':
			n, err := c.skipComment()
			sum += n
			if err != nil {
				return nil, sum, err
			}
			if len(token) == 0 {
				continue
			}
			return token, sum, nil
		case 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '_', '-', '.':
			token = append(token, r)
			continue
		default:
			// token break
			err := c.rr.UnreadRune()
			if err != nil {
				return nil, sum, err
			}
			sum -= n

			return token, sum, nil
		}
	}
}

// SomeNode is any ipsl expression, this is used for reflection within the compiler.
type SomeNode struct {
	Node Node
}

type AstNode struct {
	Type SyntaxType
	Args []AstNode
	// Literals is set for literals types and tokens (to string)
	// Literals must always be either a cid, a string, or a number (uint64)
	Literal any
}

func (a AstNode) String() string {
	switch a.Type {
	case SyntaxTypeToken:
		return a.Literal.(string)
	case SyntaxTypeValueNode:
		r := make([]string, len(a.Args))
		for i, v := range a.Args {
			r[i] = v.String()
		}
		return "(" + strings.Join(r, " ") + ")"
	case SyntaxTypeScopeNode:
		r := make([]string, len(a.Args))
		for i, v := range a.Args {
			r[i] = v.String()
		}
		return "[" + strings.Join(r, " ") + "]"
	case SyntaxTypeStringLiteral:
		return strconv.Quote(a.Literal.(string))
	case SyntaxTypeCidLiteral:
		return "$" + a.Literal.(cid.Cid).String()
	case SyntaxTypeNumberLiteral:
		return strconv.FormatUint(a.Literal.(uint64), 10)
	default:
		return a.Type.String()
	}
}

type SyntaxType uint8

const (
	SyntaxTypeUnkown SyntaxType = iota
	SyntaxTypeToken
	SyntaxTypeValueNode
	SyntaxTypeScopeNode
	SyntaxTypeStringLiteral
	SyntaxTypeCidLiteral
	SyntaxTypeNumberLiteral
)

func (st SyntaxType) String() string {
	switch st {
	case SyntaxTypeToken:
		return "Token"
	case SyntaxTypeValueNode:
		return "Value Node"
	case SyntaxTypeScopeNode:
		return "Scope Node"
	case SyntaxTypeStringLiteral:
		return "String Literal"
	case SyntaxTypeCidLiteral:
		return "Cid Literal"
	case SyntaxTypeNumberLiteral:
		return "Number Literal"
	default:
		return "Unknown syntax type of " + strconv.FormatUint(uint64(st), 10)
	}
}
