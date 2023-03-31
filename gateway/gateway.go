package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"

	"github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/go-cid"
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

// ByteRange describes a range request within a UnixFS file. From and To mostly follow [HTTP Byte Range] Request semantics.
// From >= 0 and To = nil: Get the file (From, Length)
// From >= 0 and To >= 0: Get the range (From, To)
// From >= 0 and To <0: Get the range (From, Length - To)
//
// [HTTP Byte Range]: https://httpwg.org/specs/rfc9110.html#rfc.section.14.1.2
type ByteRange struct {
	From uint64
	To   *int64
}

type GetResponse struct {
	bytes             files.File
	directoryMetadata *directoryMetadata
}

type directoryMetadata struct {
	dagSize uint64
	entries <-chan unixfs.LinkResult
}

func NewGetResponseFromFile(file files.File) *GetResponse {
	return &GetResponse{bytes: file}
}

func NewGetResponseFromDirectoryListing(dagSize uint64, entries <-chan unixfs.LinkResult) *GetResponse {
	return &GetResponse{directoryMetadata: &directoryMetadata{dagSize, entries}}
}

// IPFSBackend is the required set of functionality used to implement the IPFS HTTP Gateway specification.
// To signal error types to the gateway code (so that not everything is a HTTP 500) return an error wrapped with NewErrorResponse.
// There are also some existing error types that the gateway code knows how to handle (e.g. context.DeadlineExceeded
// and various IPLD pathing related errors).
type IPFSBackend interface {

	// Get returns a GetResponse with UnixFS file, directory or a block in IPLD
	// format e.g., (DAG-)CBOR/JSON.
	//
	// Returned Directories are preferably a minimum info required for enumeration: Name, Size, and Cid.
	//
	// Optional ranges follow [HTTP Byte Ranges] notation and can be used for
	// pre-fetching specific sections of a file or a block.
	//
	// Range notes:
	//   - Generating response to a range request may require additional data
	//     beyond the passed ranges (e.g. a single byte range from the middle of a
	//     file will still need magic bytes from the very beginning for content
	//     type sniffing).
	//   - A range request for a directory currently holds no semantic meaning.
	//
	// [HTTP Byte Ranges]: https://httpwg.org/specs/rfc9110.html#rfc.section.14.1.2
	Get(context.Context, ImmutablePath, ...ByteRange) (ContentPathMetadata, *GetResponse, error)

	// GetAll returns a UnixFS file or directory depending on what the path is that has been requested. Directories should
	// include all content recursively.
	GetAll(context.Context, ImmutablePath) (ContentPathMetadata, files.Node, error)

	// GetBlock returns a single block of data
	GetBlock(context.Context, ImmutablePath) (ContentPathMetadata, files.File, error)

	// Head returns a file or directory depending on what the path is that has been requested.
	// For UnixFS files should return a file which has the correct file size and either returns the ContentType in ContentPathMetadata or
	// enough data (e.g. 3kiB) such that the content type can be determined by sniffing.
	// For all other data types returning just size information is sufficient
	// TODO: give function more explicit return types
	Head(context.Context, ImmutablePath) (ContentPathMetadata, files.Node, error)

	// ResolvePath resolves the path using UnixFS resolver. If the path does not
	// exist due to a missing link, it should return an error of type:
	// NewErrorResponse(fmt.Errorf("no link named %q under %s", name, cid), http.StatusNotFound)
	ResolvePath(context.Context, ImmutablePath) (ContentPathMetadata, error)

	// GetCAR returns a CAR file for the given immutable path
	// Returns an initial error if there was an issue before the CAR streaming begins as well as a channel with a single
	// that may contain a single error for if any errors occur during the streaming. If there was an initial error the
	// error channel is nil
	// TODO: Make this function signature better
	GetCAR(context.Context, ImmutablePath) (ContentPathMetadata, io.ReadCloser, <-chan error, error)

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
