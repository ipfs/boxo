// Package path contains utilities to work with ipfs paths.
package path

import (
	"fmt"
	gopath "path"
	"strings"

	"github.com/ipfs/go-cid"
)

type Namespace string

func (namespace Namespace) String() string {
	return string(namespace)
}

// Mutable returns false if the data under this namespace is guaranteed to not change.
func (namespace Namespace) Mutable() bool {
	return namespace != IPFSNamespace && namespace != IPLDNamespace
}

const (
	IPFSNamespace Namespace = "ipfs"
	IPNSNamespace Namespace = "ipns"
	IPLDNamespace Namespace = "ipld"
)

// Path is a generic, valid, and well-formed path. A valid path is shaped as follows:
//
//	/{namespace}/{root}[/remaining/path]
//
// Where:
//
//  1. Namespace is "ipfs", "ipld", or "ipns".
//  2. If namespace is "ipfs" or "ipld", "root" must be a valid [cid.Cid].
//  3. If namespace is "ipns", "root" may be a [ipns.Name] or a [DNSLink] FQDN.
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
	return StringToSegments(p.str)
}

// ImmutablePath is a [Path] which is guaranteed to have an immutable [Namespace].
type ImmutablePath interface {
	Path

	// Cid returns the [cid.Cid] of the root object of the path.
	Cid() cid.Cid
}

var _ Path = immutablePath{}
var _ ImmutablePath = immutablePath{}

type immutablePath struct {
	path Path
	cid  cid.Cid
}

func NewImmutablePath(p Path) (ImmutablePath, error) {
	if p.Namespace().Mutable() {
		return nil, &ErrInvalidPath{err: ErrExpectedImmutable, path: p.String()}
	}

	segments := p.Segments()
	cid, err := cid.Decode(segments[1])
	if err != nil {
		return nil, &ErrInvalidPath{err: err, path: p.String()}
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

// FromCid returns a new "/ipfs" path with the provided CID.
func FromCid(cid cid.Cid) ImmutablePath {
	return immutablePath{
		path: path{
			str:       fmt.Sprintf("/%s/%s", IPFSNamespace, cid.String()),
			namespace: IPFSNamespace,
		},
		cid: cid,
	}
}

// NewPath takes the given string and returns a well-formed and sanitized [Path].
// The given string is cleaned through [gopath.Clean], but preserving the final
// trailing slash. This function returns an error when the given string is not
// a valid content path.
func NewPath(str string) (Path, error) {
	segments := StringToSegments(str)

	// Shortest valid path is "/{namespace}/{root}". That yields at least two
	// segments: ["{namespace}" "{root}"]. Therefore, here we check if the original
	// string begins with "/" (any path must), if we have at least two segments, and if
	// the root is non-empty. The namespace is checked further below.
	if !strings.HasPrefix(str, "/") || len(segments) < 2 || segments[1] == "" {
		return nil, &ErrInvalidPath{err: ErrInsufficientComponents, path: str}
	}

	cleaned := SegmentsToString(segments...)
	if strings.HasSuffix(str, "/") {
		// Do not forget to preserve the trailing slash!
		cleaned += "/"
	}

	switch segments[0] {
	case "ipfs", "ipld":
		cid, err := cid.Decode(segments[1])
		if err != nil {
			return nil, &ErrInvalidPath{err: err, path: str}
		}

		ns := IPFSNamespace
		if segments[0] == "ipld" {
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
		return path{
			str:       cleaned,
			namespace: IPNSNamespace,
		}, nil
	default:
		return nil, &ErrInvalidPath{err: fmt.Errorf("%w: %q", ErrUnknownNamespace, segments[0]), path: str}
	}
}

// NewPathFromSegments creates a new [Path] from the provided segments. This
// function simply calls [NewPath] internally with the segments concatenated
// using a forward slash "/" as separator. Please see [Path.Segments] for more
// information about how segments must be structured.
func NewPathFromSegments(segments ...string) (Path, error) {
	return NewPath(SegmentsToString(segments...))
}

// Join joins a [Path] with certain segments and returns a new [Path].
func Join(p Path, segments ...string) (Path, error) {
	s := p.Segments()
	s = append(s, segments...)
	return NewPathFromSegments(s...)
}

// SegmentsToString converts an array of segments into a string. The returned string
// will always be prefixed with a "/" if there are any segments. For example, if the
// given segments array is ["foo", "bar"], the returned value will be "/foo/bar".
// Given an empty array, an empty string is returned.
func SegmentsToString(segments ...string) string {
	str := strings.Join(segments, "/")
	if str != "" {
		str = "/" + str
	}
	return str
}

// StringToSegments converts a string into an array of segments. This function follows
// the rules of [Path.Segments]: the path is first cleaned through [gopath.Clean] and
// no empty segments are returned.
func StringToSegments(str string) []string {
	str = gopath.Clean(str)
	if str == "." {
		return nil
	}
	// Trim slashes from beginning and end, such that we do not return empty segments.
	str = strings.TrimSuffix(str, "/")
	str = strings.TrimPrefix(str, "/")
	if str == "" {
		return nil
	}
	return strings.Split(str, "/")
}
