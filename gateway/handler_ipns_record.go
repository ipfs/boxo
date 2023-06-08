package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	ipath "github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func (i *handler) serveIpnsRecord(ctx context.Context, w http.ResponseWriter, r *http.Request, contentPath ipath.Path, begin time.Time, logger *zap.SugaredLogger) bool {
	ctx, span := spanTrace(ctx, "Handler.ServeIPNSRecord", trace.WithAttributes(attribute.String("path", contentPath.String())))
	defer span.End()

	if contentPath.Namespace() != "ipns" {
		err := fmt.Errorf("%s is not an IPNS link", contentPath.String())
		i.webError(w, r, err, http.StatusBadRequest)
		return false
	}

	key := contentPath.String()
	key = strings.TrimSuffix(key, "/")
	key = strings.TrimPrefix(key, "/ipns/")
	if strings.Count(key, "/") != 0 {
		err := errors.New("cannot find ipns key for subpath")
		i.webError(w, r, err, http.StatusBadRequest)
		return false
	}

	c, err := cid.Decode(key)
	if err != nil {
		i.webError(w, r, err, http.StatusBadRequest)
		return false
	}

	rawRecord, err := i.backend.GetIPNSRecord(ctx, c)
	if err != nil {
		i.webError(w, r, err, http.StatusInternalServerError)
		return false
	}

	record, err := ipns.UnmarshalIpnsEntry(rawRecord)
	if err != nil {
		i.webError(w, r, err, http.StatusInternalServerError)
		return false
	}

	// Set cache control headers based on the TTL set in the IPNS record. If the
	// TTL is not present, we use the Last-Modified tag. We are tracking IPNS
	// caching on: https://github.com/ipfs/kubo/issues/1818.
	// TODO: use addCacheControlHeaders once #1818 is fixed.
	recordEtag := strconv.FormatUint(xxhash.Sum64(rawRecord), 32)
	w.Header().Set("Etag", recordEtag)

	// Terminate early if Etag matches. We cannot rely on handleIfNoneMatch since
	// we use the raw record to generate the etag value.
	if etagMatch(r.Header.Get("If-None-Match"), recordEtag) {
		w.WriteHeader(http.StatusNotModified)
		return false
	}

	if record.Ttl != nil {
		seconds := int(time.Duration(*record.Ttl).Seconds())
		w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", seconds))
	} else {
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	}

	// Set Content-Disposition
	var name string
	if urlFilename := r.URL.Query().Get("filename"); urlFilename != "" {
		name = urlFilename
	} else {
		name = key + ".ipns-record"
	}
	setContentDispositionHeader(w, name, "attachment")

	w.Header().Set("Content-Type", ipnsRecordResponseFormat)
	w.Header().Set("X-Content-Type-Options", "nosniff")

	_, err = w.Write(rawRecord)
	if err == nil {
		// Update metrics
		i.ipnsRecordGetMetric.WithLabelValues(contentPath.Namespace()).Observe(time.Since(begin).Seconds())
		return true
	}

	return false
}
