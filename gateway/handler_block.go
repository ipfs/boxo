package gateway

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	ipath "github.com/ipfs/boxo/coreiface/path"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// serveRawBlock returns bytes behind a raw block
func (i *handler) serveRawBlock(ctx context.Context, w http.ResponseWriter, r *http.Request, resolvedPath ipath.Resolved, contentPath ipath.Path, begin time.Time) bool {
	ctx, span := spanTrace(ctx, "ServeRawBlock", trace.WithAttributes(attribute.String("path", resolvedPath.String())))
	defer span.End()

	blockCid := resolvedPath.Cid()
	block, err := i.api.GetBlock(ctx, blockCid)
	if err != nil {
		err = fmt.Errorf("error getting block %s: %w", blockCid.String(), err)
		webError(w, err, http.StatusInternalServerError)
		return false
	}
	content := bytes.NewReader(block.RawData())

	// Set Content-Disposition
	var name string
	if urlFilename := r.URL.Query().Get("filename"); urlFilename != "" {
		name = urlFilename
	} else {
		name = blockCid.String() + ".bin"
	}
	setContentDispositionHeader(w, name, "attachment")

	// Set remaining headers
	modtime := addCacheControlHeaders(w, r, contentPath, blockCid)
	w.Header().Set("Content-Type", "application/vnd.ipld.raw")
	w.Header().Set("X-Content-Type-Options", "nosniff") // no funny business in the browsers :^)

	// ServeContent will take care of
	// If-None-Match+Etag, Content-Length and range requests
	_, dataSent, _ := ServeContent(w, r, name, modtime, content)

	if dataSent {
		// Update metrics
		i.rawBlockGetMetric.WithLabelValues(contentPath.Namespace()).Observe(time.Since(begin).Seconds())
	}

	return dataSent
}
