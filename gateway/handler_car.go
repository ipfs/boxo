package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	ipath "github.com/ipfs/interface-go-ipfs-core/path"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// serveCAR returns a CAR stream for specific DAG+selector
func (i *handler) serveCAR(ctx context.Context, w http.ResponseWriter, r *http.Request, imPath ImmutablePath, contentPath ipath.Path, carVersion string, begin time.Time) bool {
	ctx, span := spanTrace(ctx, "ServeCAR", trace.WithAttributes(attribute.String("path", imPath.String())))
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	switch carVersion {
	case "": // noop, client does not care about version
	case "1": // noop, we support this
	default:
		err := fmt.Errorf("only version=1 is supported")
		webError(w, "unsupported CAR version", err, http.StatusBadRequest)
		return false
	}

	gwMetadata, carFile, err := i.api.GetCAR(ctx, imPath)
	if !i.handleNonUnixFSRequestErrors(w, imPath, err) {
		return false
	}
	defer carFile.Close()

	rootCid := gwMetadata.LastSegment.Cid()

	// Set Content-Disposition
	var name string
	if urlFilename := r.URL.Query().Get("filename"); urlFilename != "" {
		name = urlFilename
	} else {
		name = rootCid.String() + ".car"
	}
	setContentDispositionHeader(w, name, "attachment")

	// Set Cache-Control (same logic as for a regular files)
	addCacheControlHeaders(w, r, contentPath, rootCid)

	// Weak Etag W/ because we can't guarantee byte-for-byte identical
	// responses, but still want to benefit from HTTP Caching. Two CAR
	// responses for the same CID and selector will be logically equivalent,
	// but when CAR is streamed, then in theory, blocks may arrive from
	// datastore in non-deterministic order.
	etag := `W/` + getEtag(r, rootCid)
	w.Header().Set("Etag", etag)

	// Finish early if Etag match
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return false
	}

	// Make it clear we don't support range-requests over a car stream
	// Partial downloads and resumes should be handled using requests for
	// sub-DAGs and IPLD selectors: https://github.com/ipfs/go-ipfs/issues/8769
	w.Header().Set("Accept-Ranges", "none")

	w.Header().Set("Content-Type", "application/vnd.ipld.car; version=1")
	w.Header().Set("X-Content-Type-Options", "nosniff") // no funny business in the browsers :^)

	if _, err := io.Copy(w, carFile); err != nil {
		// We return error as a trailer, however it is not something browsers can access
		// (https://github.com/mdn/browser-compat-data/issues/14703)
		// Due to this, we suggest client always verify that
		// the received CAR stream response is matching requested DAG selector
		w.Header().Set("X-Stream-Error", err.Error())
		return false
	}

	// Update metrics
	i.carStreamGetMetric.WithLabelValues(contentPath.Namespace()).Observe(time.Since(begin).Seconds())
	return true
}
