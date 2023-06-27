package gateway

import (
	"context"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// serveRawBlock returns bytes behind a raw block
func (i *handler) serveRawBlock(ctx context.Context, w http.ResponseWriter, r *http.Request, rq *requestData) bool {
	ctx, span := spanTrace(ctx, "Handler.ServeRawBlock", trace.WithAttributes(attribute.String("path", rq.immutablePath.String())))
	defer span.End()

	pathMetadata, data, err := i.backend.GetBlock(ctx, rq.mostlyResolvedPath())
	if !i.handleRequestErrors(w, r, rq.contentPath, err) {
		return false
	}
	defer data.Close()

	setIpfsRootsHeader(w, rq, &pathMetadata)

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
	modtime := addCacheControlHeaders(w, r, rq.contentPath, blockCid, rawResponseFormat)
	w.Header().Set("Content-Type", rawResponseFormat)
	w.Header().Set("X-Content-Type-Options", "nosniff") // no funny business in the browsers :^)

	// ServeContent will take care of
	// If-None-Match+Etag, Content-Length and range requests
	_, dataSent, _ := serveContent(w, r, name, modtime, data)

	if dataSent {
		// Update metrics
		i.rawBlockGetMetric.WithLabelValues(rq.contentPath.Namespace()).Observe(time.Since(rq.begin).Seconds())
	}

	return dataSent
}
