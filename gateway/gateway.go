package gateway

import (
	"context"
	"net/http"
	"sort"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/files"
	iface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

// Config is the configuration used when creating a new gateway handler.
type Config struct {
	Headers map[string][]string
}

// API defines the minimal set of API services required for a gateway handler.
type API interface {
	// GetUnixFsNode returns a read-only handle to a file tree referenced by a path.
	GetUnixFsNode(context.Context, path.Resolved) (files.Node, error)

	// LsUnixFsDir returns the list of links in a directory.
	LsUnixFsDir(context.Context, path.Resolved) (<-chan iface.DirEntry, error)

	// GetBlock return a block from a certain CID.
	GetBlock(context.Context, cid.Cid) (blocks.Block, error)

	// GetIPNSRecord retrieves the best IPNS record for a given CID (libp2p-key)
	// from the routing system.
	GetIPNSRecord(context.Context, cid.Cid) ([]byte, error)

	// GetDNSLinkRecord returns the DNSLink TXT record for the provided FQDN.
	// Unlike ResolvePath, it does not perform recursive resolution. It only
	// checks for the existence of a DNSLink TXT record with path starting with
	// /ipfs/ or /ipns/ and returns the path as-is.
	GetDNSLinkRecord(context.Context, string) (path.Path, error)

	// IsCached returns whether or not the path exists locally.
	IsCached(context.Context, path.Path) bool

	// ResolvePath resolves the path using UnixFS resolver. If the path does not
	// exist due to a missing link, it should return an error of type:
	// https://pkg.go.dev/github.com/ipfs/go-path@v0.3.0/resolver#ErrNoLink
	ResolvePath(context.Context, path.Path) (path.Resolved, error)
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
