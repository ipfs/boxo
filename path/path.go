// Package path contains utilities to work with ipfs paths.
package path

import (
	"fmt"
	gopath "path"
	"strconv"
	"strings"

	"github.com/ipfs/go-cid"
)

type Namespace uint

func (namespace Namespace) String() string {
	switch namespace {
	case IPFSNamespace:
		return "ipfs"
	case IPNSNamespace:
		return "ipns"
	case IPLDNamespace:
		return "ipld"
	default:
		return "unknown path namespace: " + strconv.FormatUint(uint64(namespace), 10)
	}
}

// Mutable returns false if the data under this namespace is guaranteed to not change.
func (namespace Namespace) Mutable() bool {
	return namespace == IPNSNamespace
}

const (
	IPFSNamespace Namespace = iota
	IPNSNamespace
	IPLDNamespace
)

// Path is a generic, valid, and well-formed path. A valid path is shaped as follows:
//
//	/{namespace}/{root}[/remaining/path]
//
// Where:
//
//  1. Namespace is "ipfs", "ipld", or "ipns".
//  2. If namespace is "ipfs" or "ipld", "root" must be a valid [cid.Cid].
//  3. If namespace is "ipns", "root" may be a [ipns.Name] or a DNSLink FQDN.
//
// [DNSLink]: https://dnslink.dev/
type Path interface {
	// String returns the path as a string.
	String() string

	// Namespace returns the first component of the path. For example, the namespace
	// of "/ipfs/bafy" is "ipfs".
	Namespace() Namespace

	// Segments returns the different elements of a path delimited by a forward
	// slash ("/"). The returned array must not contain any empty segments, and
	// must have a length of at least two: the first element must be the namespace,
	// and the second must be root.
	//
	// Examples:
	// 		- "/ipld/bafkqaaa" returns ["ipld", "bafkqaaa"]
	// 		- "/ipfs/bafkqaaa/a/b/" returns ["ipfs", "bafkqaaa", "a", "b"]
	// 		- "/ipns/dnslink.net" returns ["ipns", "dnslink.net"]
	Segments() []string
}

var _ Path = path{}

type path struct {
	str       string
	namespace Namespace
}

func (p path) String() string {
	return p.str
}

func (p path) Namespace() Namespace {
	return p.namespace
}

func (p path) Segments() []string {
	// Trim slashes from beginning and end, such that we do not return empty segments.
	str := strings.TrimSuffix(p.str, "/")
	str = strings.TrimPrefix(str, "/")

	return strings.Split(str, "/")
}

// ImmutablePath is a [Path] which is guaranteed to have an immutable [Namespace].
type ImmutablePath interface {
	Path

	// Cid returns the [cid.Cid] of the root object of the path.
	Cid() cid.Cid

	// Remainder returns the unresolved parts of the path.
	Remainder() string
}

var _ Path = immutablePath{}
var _ ImmutablePath = immutablePath{}

type immutablePath struct {
	path Path
	cid  cid.Cid
}

func NewImmutablePath(p Path) (ImmutablePath, error) {
	if p.Namespace().Mutable() {
		return nil, fmt.Errorf("path was expected to be immutable: %s", p.String())
	}

	segments := p.Segments()
	cid, err := cid.Decode(segments[1])
	if err != nil {
		return nil, &ErrInvalidPath{error: fmt.Errorf("invalid CID: %w", err), path: p.String()}
	}

	return immutablePath{path: p, cid: cid}, nil
}

func (ip immutablePath) String() string {
	return ip.path.String()
}

func (ip immutablePath) Namespace() Namespace {
	return ip.path.Namespace()
}

func (ip immutablePath) Segments() []string {
	return ip.path.Segments()
}

func (ip immutablePath) Cid() cid.Cid {
	return ip.cid
}

func (ip immutablePath) Remainder() string {
	remainder := strings.Join(ip.Segments()[2:], "/")
	if remainder != "" {
		remainder = "/" + remainder
	}
	return remainder
}

// NewIPFSPath returns a new "/ipfs" path with the provided CID.
func NewIPFSPath(cid cid.Cid) ImmutablePath {
	return &immutablePath{
		path: path{
			str:       fmt.Sprintf("/%s/%s", IPFSNamespace, cid.String()),
			namespace: IPFSNamespace,
		},
		cid: cid,
	}
}

// NewIPLDPath returns a new "/ipld" path with the provided CID.
func NewIPLDPath(cid cid.Cid) ImmutablePath {
	return &immutablePath{
		path: path{
			str:       fmt.Sprintf("/%s/%s", IPLDNamespace, cid.String()),
			namespace: IPLDNamespace,
		},
		cid: cid,
	}
}

// NewPath takes the given string and returns a well-forme and sanitized [Path].
// The given string is cleaned through [gopath.Clean], but preserving the final
// trailing slash. This function returns an error when the given string is not
// a valid path.
func NewPath(str string) (Path, error) {
	cleaned := gopath.Clean(str)
	components := strings.Split(cleaned, "/")

	if strings.HasSuffix(str, "/") {
		// Do not forget to store the trailing slash!
		cleaned += "/"
	}

	// Shortest valid path is "/{namespace}/{element}". That yields at least three
	// components: [" " "{namespace}" "{element}"]. The first component must therefore
	// be empty.
	if len(components) < 3 || components[0] != "" {
		return nil, &ErrInvalidPath{error: fmt.Errorf("not enough path components"), path: str}
	}

	switch components[1] {
	case "ipfs", "ipld":
		if components[2] == "" {
			return nil, &ErrInvalidPath{error: fmt.Errorf("not enough path components"), path: str}
		}

		cid, err := cid.Decode(components[2])
		if err != nil {
			return nil, &ErrInvalidPath{error: fmt.Errorf("invalid CID: %w", err), path: str}
		}

		ns := IPFSNamespace
		if components[1] == "ipld" {
			ns = IPLDNamespace
		}

		return immutablePath{
			path: path{
				str:       cleaned,
				namespace: ns,
			},
			cid: cid,
		}, nil
	case "ipns":
		if components[2] == "" {
			return nil, &ErrInvalidPath{error: fmt.Errorf("not enough path components"), path: str}
		}

		return path{
			str:       cleaned,
			namespace: IPNSNamespace,
		}, nil
	default:
		return nil, &ErrInvalidPath{error: fmt.Errorf("unknown namespace %q", components[1]), path: str}
	}
}

// NewPathFromSegments creates a new [Path] from the provided segments. This
// function simply calls [NewPath] internally with the segments concatenated
// using a forward slash "/" as separator. Please see [Path.Segments] for more
// information about how segments must be structured.
func NewPathFromSegments(segments ...string) (Path, error) {
	return NewPath("/" + strings.Join(segments, "/"))
}

// Join joins a [Path] with certain segments and returns a new [Path].
func Join(p Path, segments ...string) (Path, error) {
	s := p.Segments()
	s = append(s, segments...)
	return NewPathFromSegments(s...)
}
