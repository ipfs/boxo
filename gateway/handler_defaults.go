package gateway

import (
	"context"
	"fmt"
	"github.com/ipfs/go-libipfs/files"
	mc "github.com/multiformats/go-multicodec"
	"net/http"
	"time"

	ipath "github.com/ipfs/interface-go-ipfs-core/path"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func (i *handler) serveDefaults(ctx context.Context, w http.ResponseWriter, r *http.Request, imPath ImmutablePath, contentPath ipath.Path, begin time.Time, requestedContentType string, logger *zap.SugaredLogger) bool {
	ctx, span := spanTrace(ctx, "ServeDefaults", trace.WithAttributes(attribute.String("path", imPath.String())))
	defer span.End()

	var gwMetadata GatewayMetadata
	var data files.Node
	var err error

	switch r.Method {
	case http.MethodHead:
		// TODO: Should these follow _redirects
		gwMetadata, data, err = i.api.Head(ctx, imPath)
		if !i.handleNonUnixFSRequestErrors(w, imPath, err) { // TODO: even though this might be UnixFS there shouldn't be anything special for HEAD requests
			return false
		}
		defer data.Close()
	case http.MethodGet:
		gwMetadata, data, err = i.api.Get(ctx, imPath)
		if isUnixfsResponseFormat(requestedContentType) {
			if !i.handleUnixFSRequestErrors(w, r, imPath, err, logger) {
				return false
			}

			if gwMetadata.RedirectInfo.RedirectUsed {
				// If we have origin isolation (subdomain gw, DNSLink website),
				// and response type is UnixFS (default for website hosting)
				// we can leverage the presence of an _redirects file and apply rules defined there.
				// See: https://github.com/ipfs/specs/pull/290
				if hasOriginIsolation(r) {
					logger.Debugw("applied a rule from _redirects file")
					// TODO: apply 404s amd 300s
				} else {
					// This shouldn't be possible to reach
					webError(w, "invalid use of _redirects", fmt.Errorf("cannot use _redirects without origin isolation"), http.StatusInternalServerError)
					return false
				}

				ri := gwMetadata.RedirectInfo

				// 4xx
				if ri.StatusCode == 404 || ri.StatusCode == 410 || ri.StatusCode == 451 {
					// TODO: confirm no need to set roots headers here
					if err := i.serve4xx(w, r, ri.Redirect4xxTo, ri.Redirect4xxPageCid, ri.Redirect4xxPage, ri.StatusCode); err != nil {
						webError(w, "unable to serve redirect 404", err, http.StatusInternalServerError)
						return false
					}
				}

				// redirect
				if ri.StatusCode >= 301 && ri.StatusCode <= 308 {
					http.Redirect(w, r, ri.Redirect3xxTo, ri.StatusCode)
					return true // TODO: confirm this is reasonable
				}
			}
		} else {
			if !i.handleNonUnixFSRequestErrors(w, imPath, err) {
				return false
			}
		}
		defer data.Close()
	default:
		// This shouldn't be possible to reach which is why it is a 500 rather than 4XX error
		webError(w, "invalid method", fmt.Errorf("cannot use this HTTP method with the given request"), http.StatusInternalServerError)
		return false
	}

	// TODO: should this include _redirects 200s?
	if err := i.setIpfsRootsHeader(w, gwMetadata); err != nil {
		webRequestError(w, err)
		return false
	}

	resolvedPath := gwMetadata.LastSegment
	switch mc.Code(resolvedPath.Cid().Prefix().Codec) {
	case mc.Json, mc.DagJson, mc.Cbor, mc.DagCbor:
		blockData, ok := data.(files.File)
		if !ok { // This should never happen
			webError(w, "decoding error", fmt.Errorf("data not a usable as a file"), http.StatusInternalServerError)
			return false
		}
		logger.Debugw("serving codec", "path", contentPath)
		return i.renderCodec(r.Context(), w, r, resolvedPath, blockData, contentPath, begin, requestedContentType)
	default:
		logger.Debugw("serving unixfs", "path", contentPath)
		return i.serveUnixFS(r.Context(), w, r, resolvedPath, data, contentPath, begin, logger)
	}
}
