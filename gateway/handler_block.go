package gateway

import (
	"context"
	"net/http"
	"time"

	ipath "github.com/ipfs/boxo/coreiface/path"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// serveRawBlock returns bytes behind a raw block
func (i *handler) serveRawBlock(ctx context.Context, w http.ResponseWriter, r *http.Request, imPath ImmutablePath, contentPath ipath.Path, begin time.Time) bool {
	ctx, span := spanTrace(ctx, "Handler.ServeRawBlock", trace.WithAttributes(attribute.String("path", imPath.String())))
	defer span.End()

	pathMetadata, data, err := i.api.GetBlock(ctx, imPath)
	if !i.handleRequestErrors(w, r, contentPath, err) {
		return false
	}
	defer data.Close()

	if err := i.setIpfsRootsHeader(w, pathMetadata); err != nil {
		i.webRequestError(w, r, err)
		return false
	}

	blockCid := pathMetadata.LastSegment.Cid()

	// Set Content-Disposition
	var name string
	if urlFilename := r.URL.Query().Get("filename"); urlFilename != "" {
		name = urlFilename
	} else {
		name = blockCid.String() + ".bin"
	}
	setContentDispositionHeader(w, name, "attachment")

	// Set remaining headers
	modtime := addCacheControlHeaders(w, r, contentPath, blockCid, "application/vnd.ipld.raw")
	w.Header().Set("Content-Type", "application/vnd.ipld.raw")
	w.Header().Set("X-Content-Type-Options", "nosniff") // no funny business in the browsers :^)

	// ServeContent will take care of
	// If-None-Match+Etag, Content-Length and range requests
	_, dataSent, _ := ServeContent(w, r, name, modtime, data)

	if dataSent {
		// Update metrics
		i.rawBlockGetMetric.WithLabelValues(contentPath.Namespace()).Observe(time.Since(begin).Seconds())
	}

	return dataSent
}
