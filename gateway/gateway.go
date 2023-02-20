package gateway

import (
	"context"
	"io"
	"net/http"
	"sort"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

// Config is the configuration used when creating a new gateway handler.
type Config struct {
	Headers map[string][]string
}

// ImmutablePath TODO: This isn't great, as its just a signal and mutable paths fulfill the same interface
type ImmutablePath interface {
	// String returns the path as a string.
	String() string

	// Namespace returns the first component of the path.
	//
	// For example path "/ipfs/QmHash", calling Namespace() will return "ipfs"
	//
	// Calling this method on invalid paths (IsValid() != nil) will result in
	// empty string
	Namespace() string

	// IsValid checks if this path is a valid ipfs Path, returning nil iff it is
	// valid
	IsValid() error
}

type GatewayMetadata struct {
	PathSegmentRoots []cid.Cid
	LastSegment      path.Resolved
	RedirectInfo     RedirectInfo
}

// RedirectInfo is used to help the gateway figure out what to happen if _redirects were leveraged
type RedirectInfo struct {
	RedirectUsed       bool
	StatusCode         int
	Redirect4xxPage    files.File
	Redirect4xxTo      path.Path
	Redirect4xxPageCid cid.Cid
	Redirect3xxTo      string
}

type GetOpt func(*getOpts) error
type getOpts struct {
	rangeFrom int
	rangeTo   int

	fullDepth bool
	rawBlock  bool
}

type dummyGetOpts struct{}

var GetOptions dummyGetOpts

// GetRange is a range request for some file data
// Note: currently a full file object should be returned but reading from values outside the range can error
func (o dummyGetOpts) GetRange(from, to int) GetOpt {
	return func(options *getOpts) error {
		options.rangeFrom = from
		options.rangeTo = to
		return nil
	}
}

// GetFullDepth fetches all data linked from the last logical node in the path. In particular, recursively fetch an
// entire UnixFS directory.
func (o dummyGetOpts) GetFullDepth() GetOpt {
	return func(options *getOpts) error {
		options.fullDepth = true
		return nil
	}
}

// GetRawBlock fetches the data at the last path element as if it were a raw block rather than something more complex
// such as a UnixFS file or directory.
func (o dummyGetOpts) GetRawBlock() GetOpt {
	return func(options *getOpts) error {
		options.rawBlock = true
		return nil
	}
}

type API interface {
	// Get returns a file or directory depending on what the path is that has been requested.
	// This Get follows the ipfs:// web semantics which means handling of index.html and _redirects files.
	// There are multiple options passable to this function, read them for more information.
	Get(context.Context, ImmutablePath, ...GetOpt) (GatewayMetadata, files.Node, error)

	// Head returns a file or directory depending on what the path is that has been requested.
	// This Head follows the ipfs:// web semantics which means handling of index.html and _redirects files.
	Head(context.Context, ImmutablePath) (GatewayMetadata, files.Node, error)

	// GetCAR returns a CAR file for the given immutable path
	GetCAR(context.Context, ImmutablePath) (GatewayMetadata, io.ReadCloser, error)

	// IsCached returns whether or not the path exists locally.
	IsCached(context.Context, path.Path) bool

	// GetIPNSRecord retrieves the best IPNS record for a given CID (libp2p-key)
	// from the routing system.
	GetIPNSRecord(context.Context, cid.Cid) ([]byte, error)

	// ResolveMutable takes a mutable path and resolves it into an immutable one. This means recursively resolving any
	// DNSLink or IPNS records.
	ResolveMutable(context.Context, path.Path) (ImmutablePath, error)

	// GetDNSLinkRecord returns the DNSLink TXT record for the provided FQDN.
	// Unlike ResolvePath, it does not perform recursive resolution. It only
	// checks for the existence of a DNSLink TXT record with path starting with
	// /ipfs/ or /ipns/ and returns the path as-is.
	GetDNSLinkRecord(context.Context, string) (path.Path, error)
}

// A helper function to clean up a set of headers:
// 1. Canonicalizes.
// 2. Deduplicates.
// 3. Sorts.
func cleanHeaderSet(headers []string) []string {
	// Deduplicate and canonicalize.
	m := make(map[string]struct{}, len(headers))
	for _, h := range headers {
		m[http.CanonicalHeaderKey(h)] = struct{}{}
	}
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}

	// Sort
	sort.Strings(result)
	return result
}

// AddAccessControlHeaders adds default headers used for controlling
// cross-origin requests. This function adds several values to the
// Access-Control-Allow-Headers and Access-Control-Expose-Headers entries.
// If the Access-Control-Allow-Origin entry is missing a value of '*' is
// added, indicating that browsers should allow requesting code from any
// origin to access the resource.
// If the Access-Control-Allow-Methods entry is missing a value of 'GET' is
// added, indicating that browsers may use the GET method when issuing cross
// origin requests.
func AddAccessControlHeaders(headers map[string][]string) {
	// Hard-coded headers.
	const ACAHeadersName = "Access-Control-Allow-Headers"
	const ACEHeadersName = "Access-Control-Expose-Headers"
	const ACAOriginName = "Access-Control-Allow-Origin"
	const ACAMethodsName = "Access-Control-Allow-Methods"

	if _, ok := headers[ACAOriginName]; !ok {
		// Default to *all*
		headers[ACAOriginName] = []string{"*"}
	}
	if _, ok := headers[ACAMethodsName]; !ok {
		// Default to GET
		headers[ACAMethodsName] = []string{http.MethodGet}
	}

	headers[ACAHeadersName] = cleanHeaderSet(
		append([]string{
			"Content-Type",
			"User-Agent",
			"Range",
			"X-Requested-With",
		}, headers[ACAHeadersName]...))

	headers[ACEHeadersName] = cleanHeaderSet(
		append([]string{
			"Content-Length",
			"Content-Range",
			"X-Chunked-Output",
			"X-Stream-Output",
			"X-Ipfs-Path",
			"X-Ipfs-Roots",
		}, headers[ACEHeadersName]...))
}

type RequestContextKey string

const (
	DNSLinkHostnameKey RequestContextKey = "dnslink-hostname"
	GatewayHostnameKey RequestContextKey = "gw-hostname"
)
