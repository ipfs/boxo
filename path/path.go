// Package path contains utilities to work with ipfs paths.
package path

import (
	"fmt"
	gopath "path"
	"strconv"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
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

	// Root returns the [cid.Cid] of the root object of the path. Root can return
	// [cid.Undef] for Mutable IPNS paths that use [DNSLink].
	//
	// [DNSLink]: https://dnslink.dev/
	Root() cid.Cid

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

// ResolvedPath is a [Path] which was resolved to the last resolvable node.
type ResolvedPath interface {
	Path

	// Cid returns the [cid.Cid] of the node referenced by the path.
	Cid() cid.Cid

	// Remainder returns the unresolved parts of the path.
	Remainder() string
}

var _ Path = ImmutablePath{}

// ImmutablePath is a [Path] which is guaranteed to return "false" to [Path.Mutable].
type ImmutablePath struct {
	path Path
}

func NewImmutablePath(p Path) (ImmutablePath, error) {
	if p.Namespace().Mutable() {
		return ImmutablePath{}, fmt.Errorf("path was expected to be immutable: %s", p.String())
	}

	return ImmutablePath{path: p}, nil
}

func (ip ImmutablePath) String() string {
	return ip.path.String()
}

func (ip ImmutablePath) Namespace() Namespace {
	return ip.path.Namespace()
}

func (ip ImmutablePath) Root() cid.Cid {
	return ip.path.Root()
}

func (ip ImmutablePath) Segments() []string {
	return ip.path.Segments()
}

type path struct {
	str       string
	root      cid.Cid
	namespace Namespace
}

func (p path) String() string {
	return p.str
}

func (p path) Namespace() Namespace {
	return p.namespace
}

func (p path) Root() cid.Cid {
	return p.root
}

func (p path) Segments() []string {
	// Trim slashes from beginning and end, such that we do not return empty segments.
	str := strings.TrimSuffix(p.str, "/")
	str = strings.TrimPrefix(str, "/")

	return strings.Split(str, "/")
}

type resolvedPath struct {
	path
	cid       cid.Cid
	remainder string
}

func (p resolvedPath) Cid() cid.Cid {
	return p.cid
}

func (p resolvedPath) Remainder() string {
	return p.remainder
}

// NewIPFSPath returns a new "/ipfs" path with the provided CID.
func NewIPFSPath(cid cid.Cid) ResolvedPath {
	return &resolvedPath{
		path: path{
			str:       fmt.Sprintf("/%s/%s", IPFSNamespace, cid.String()),
			root:      cid,
			namespace: IPFSNamespace,
		},
		cid:       cid,
		remainder: "",
	}
}

// NewIPLDPath returns a new "/ipld" path with the provided CID.
func NewIPLDPath(cid cid.Cid) ResolvedPath {
	return &resolvedPath{
		path: path{
			str:       fmt.Sprintf("/%s/%s", IPLDNamespace, cid.String()),
			root:      cid,
			namespace: IPLDNamespace,
		},
		cid:       cid,
		remainder: "",
	}
}

// NewIPNSPath returns a new "/ipns" path with the provided CID.
func NewIPNSPath(cid cid.Cid) Path {
	return &path{
		str:       fmt.Sprintf("/%s/%s", IPNSNamespace, cid.String()),
		root:      cid,
		namespace: IPNSNamespace,
	}
}

// NewDNSLinkPath returns a new "/ipns" path with the provided domain.
func NewDNSLinkPath(domain string) Path {
	return &path{
		str:       fmt.Sprintf("/%s/%s", IPNSNamespace, domain),
		root:      cid.Undef,
		namespace: IPNSNamespace,
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

		root, err := cid.Decode(components[2])
		if err != nil {
			return nil, &ErrInvalidPath{error: fmt.Errorf("invalid CID: %w", err), path: str}
		}

		ns := IPFSNamespace
		if components[1] == "ipld" {
			ns = IPLDNamespace
		}

		return NewImmutablePath(&path{
			str:       cleaned,
			root:      root,
			namespace: ns,
		})
	case "ipns":
		if components[2] == "" {
			return nil, &ErrInvalidPath{error: fmt.Errorf("not enough path components"), path: str}
		}

		var root cid.Cid
		pid, err := peer.Decode(components[2])
		if err != nil {
			// DNSLink.
			root = cid.Undef
		} else {
			root = peer.ToCid(pid)
		}

		return &path{
			str:       cleaned,
			root:      root,
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

// NewResolvedPath creates a new [ResolvedPath] from an existing path, with a
// resolved CID and remainder path. This function is intended to be used only
// by resolver implementations.
func NewResolvedPath(p Path, cid cid.Cid, remainder string) ResolvedPath {
	return &resolvedPath{
		path: path{
			str:       p.String(),
			root:      p.Root(),
			namespace: p.Namespace(),
		},
		cid:       cid,
		remainder: remainder,
	}
}

// Join joins a [Path] with certain segments and returns a new [Path].
func Join(p Path, segments ...string) (Path, error) {
	s := p.Segments()
	s = append(s, segments...)
	return NewPathFromSegments(s...)
}
