package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/ipfs/go-cid"
	ipns_pb "github.com/ipfs/go-ipns/pb"
	ipath "github.com/ipfs/interface-go-ipfs-core/path"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func (i *handler) serveIpnsRecord(ctx context.Context, w http.ResponseWriter, r *http.Request, resolvedPath ipath.Resolved, contentPath ipath.Path, begin time.Time, logger *zap.SugaredLogger) bool {
	ctx, span := spanTrace(ctx, "ServeIPNSRecord", trace.WithAttributes(attribute.String("path", resolvedPath.String())))
	defer span.End()

	if contentPath.Namespace() != "ipns" {
		err := fmt.Errorf("%s is not an IPNS link", contentPath.String())
		webError(w, err.Error(), err, http.StatusBadRequest)
		return false
	}

	key := contentPath.String()
	key = strings.TrimSuffix(key, "/")
	key = strings.TrimPrefix(key, "/ipns/")
	if strings.Count(key, "/") != 0 {
		err := errors.New("cannot find ipns key for subpath")
		webError(w, err.Error(), err, http.StatusBadRequest)
		return false
	}

	c, err := cid.Decode(key)
	if err != nil {
		webError(w, err.Error(), err, http.StatusBadRequest)
		return false
	}

	rawRecord, err := i.api.GetIPNSRecord(ctx, c)
	if err != nil {
		webError(w, err.Error(), err, http.StatusInternalServerError)
		return false
	}

	var record ipns_pb.IpnsEntry
	err = proto.Unmarshal(rawRecord, &record)
	if err != nil {
		webError(w, err.Error(), err, http.StatusInternalServerError)
		return false
	}

	// Set cache control headers based on the TTL set in the IPNS record. If the
	// TTL is not present, we use the Last-Modified tag. We are tracking IPNS
	// caching on: https://github.com/ipfs/kubo/issues/1818.
	// TODO: use addCacheControlHeaders once #1818 is fixed.
	w.Header().Set("Etag", getEtag(r, resolvedPath.Cid()))
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

	w.Header().Set("Content-Type", "application/vnd.ipfs.ipns-record")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	_, err = w.Write(rawRecord)
	if err == nil {
		// Update metrics
		i.ipnsRecordGetMetric.WithLabelValues(contentPath.Namespace()).Observe(time.Since(begin).Seconds())
		return true
	}

	return false
}
