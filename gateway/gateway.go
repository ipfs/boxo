package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

var (
	ErrGatewayTimeout = errors.New(http.StatusText(http.StatusGatewayTimeout))
	ErrBadGateway     = errors.New(http.StatusText(http.StatusBadGateway))
)

// Config is the configuration used when creating a new gateway handler.
type Config struct {
	Headers map[string][]string
}

// TODO: Is this what we want for ImmutablePath?
type ImmutablePath struct {
	p path.Path
}

func NewImmutablePath(p path.Path) (ImmutablePath, error) {
	if p.Mutable() {
		return ImmutablePath{}, fmt.Errorf("path cannot be mutable")
	}
	return ImmutablePath{p: p}, nil
}

func (i ImmutablePath) String() string {
	return i.p.String()
}

func (i ImmutablePath) Namespace() string {
	return i.p.Namespace()
}

func (i ImmutablePath) Mutable() bool {
	return false
}

func (i ImmutablePath) IsValid() error {
	return i.p.IsValid()
}

var _ path.Path = (*ImmutablePath)(nil)

type ContentPathMetadata struct {
	PathSegmentRoots []cid.Cid
	LastSegment      path.Resolved
	ContentType      string // Only used for UnixFS requests
}

// TODO: These functional options seems a little unwieldly here and require a bunch of text, would just having more functions be better?
type GetOpt func(*GetOptions) error

// GetOptions should exclusively only Range, FullDepth, or RawBlock not a combination
// If RangeFrom and RangeTo are both 0 it implies wanting the full file
type GetOptions struct {
	RangeFrom int
	RangeTo   int

	FullDepth bool
	RawBlock  bool
}

type dummyGetOpts struct{}

var CommonGetOptions dummyGetOpts

// GetRange is a range request for some file data
// Note: currently a full file object should be returned but reading from values outside the range can error
func (o dummyGetOpts) GetRange(from, to int) GetOpt {
	return func(options *GetOptions) error {
		options.RangeFrom = from
		options.RangeTo = to
		return nil
	}
}

// GetFullDepth fetches all data linked from the last logical node in the path. In particular, recursively fetch an
// entire UnixFS directory.
func (o dummyGetOpts) GetFullDepth() GetOpt {
	return func(options *GetOptions) error {
		options.FullDepth = true
		return nil
	}
}

// GetRawBlock fetches the data at the last path element as if it were a raw block rather than something more complex
// such as a UnixFS file or directory.
func (o dummyGetOpts) GetRawBlock() GetOpt {
	return func(options *GetOptions) error {
		options.RawBlock = true
		return nil
	}
}

// API TODO: We might need to define some sentinel errors here to help the Gateway figure out what HTTP Status Code to return
// For example, distinguishing between timeout/failures to fetch vs receiving invalid data, vs impossible paths vs wrong data types, etc.
type API interface {
	// Get returns a file or directory depending on what the path is that has been requested.
	// There are multiple options passable to this function, read them for more information.
	Get(context.Context, ImmutablePath, ...GetOpt) (ContentPathMetadata, files.Node, error)

	// Head returns a file or directory depending on what the path is that has been requested.
	// For UnixFS files should return a file where at least the first 1024 bytes can be read and has the correct file size
	// For all other data types returning just size information is sufficient
	// TODO: give function more explicit return types
	Head(context.Context, ImmutablePath) (ContentPathMetadata, files.Node, error)

	// GetCAR returns a CAR file for the given immutable path
	// Returns an initial error if there was an issue before the CAR streaming begins as well as a channel with a single
	// that may contain a single error for if any errors occur during the streaming. If there was an initial error the
	// error channel is nil
	// TODO: Make this function signature better
	GetCAR(context.Context, ImmutablePath) (ContentPathMetadata, io.ReadCloser, error, <-chan error)

	// IsCached returns whether or not the path exists locally.
	IsCached(context.Context, path.Path) bool

	// GetIPNSRecord retrieves the best IPNS record for a given CID (libp2p-key)
	// from the routing system.
	GetIPNSRecord(context.Context, cid.Cid) ([]byte, error)

	// ResolveMutable takes a mutable path and resolves it into an immutable one. This means recursively resolving any
	// DNSLink or IPNS records.
	//
	// For example, given a mapping from `/ipns/dnslink.tld -> /ipns/ipns-id/mydirectory` and `/ipns/ipns-id` to
	// `/ipfs/some-cid`, the result of passing `/ipns/dnslink.tld/myfile` would be `/ipfs/some-cid/mydirectory/myfile`.
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
	ContentPathKey     RequestContextKey = "content-path"
)
