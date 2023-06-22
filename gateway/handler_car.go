package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/ipfs/go-cid"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/multierr"
)

const (
	carRangeBytesKey          = "entity-bytes"
	carTerminalElementTypeKey = "dag-scope"
)

// serveCAR returns a CAR stream for specific DAG+selector
func (i *handler) serveCAR(ctx context.Context, w http.ResponseWriter, r *http.Request, rq *requestData) bool {
	ctx, span := spanTrace(ctx, "Handler.ServeCAR", trace.WithAttributes(attribute.String("path", rq.immutablePath.String())))
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	switch rq.responseParams["version"] {
	case "": // noop, client does not care about version
	case "1": // noop, we support this
	default:
		err := fmt.Errorf("unsupported CAR version: only version=1 is supported")
		i.webError(w, r, err, http.StatusBadRequest)
		return false
	}

	params, err := getCarParams(r, rq.responseParams)
	if err != nil {
		i.webError(w, r, err, http.StatusBadRequest)
		return false
	}

	rootCid, lastSegment, err := getCarRootCidAndLastSegment(rq.immutablePath)
	if err != nil {
		i.webError(w, r, err, http.StatusInternalServerError)
		return false
	}

	// Set Content-Disposition
	var name string
	if urlFilename := r.URL.Query().Get("filename"); urlFilename != "" {
		name = urlFilename
	} else {
		name = rootCid.String()
		if lastSegment != "" {
			name += "_" + lastSegment
		}
		name += ".car"
	}
	setContentDispositionHeader(w, name, "attachment")

	// Set Cache-Control (same logic as for a regular files)
	addCacheControlHeaders(w, r, rq.contentPath, rootCid, carResponseFormat)

	// Generate the CAR Etag.
	etag := getCarEtag(rq.immutablePath, params, rootCid)
	w.Header().Set("Etag", etag)

	// Terminate early if Etag matches. We cannot rely on handleIfNoneMatch since
	// since it does not contain the parameters information we retrieve here.
	if etagMatch(r.Header.Get("If-None-Match"), etag) {
		w.WriteHeader(http.StatusNotModified)
		return false
	}

	md, carFile, err := i.backend.GetCAR(ctx, rq.immutablePath, params)
	if !i.handleRequestErrors(w, r, rq.contentPath, err) {
		return false
	}
	defer carFile.Close()
	setIpfsRootsHeader(w, rq, &md)

	// Make it clear we don't support range-requests over a car stream
	// Partial downloads and resumes should be handled using requests for
	// sub-DAGs and IPLD selectors: https://github.com/ipfs/go-ipfs/issues/8769
	w.Header().Set("Accept-Ranges", "none")

	w.Header().Set("Content-Type", getContentTypeFromCarParams(params))
	w.Header().Set("X-Content-Type-Options", "nosniff") // no funny business in the browsers :^)

	_, copyErr := io.Copy(w, carFile)
	carErr := carFile.Close()
	streamErr := multierr.Combine(carErr, copyErr)
	if streamErr != nil {
		// Update fail metric
		i.carStreamFailMetric.WithLabelValues(rq.contentPath.Namespace()).Observe(time.Since(rq.begin).Seconds())

		// We return error as a trailer, however it is not something browsers can access
		// (https://github.com/mdn/browser-compat-data/issues/14703)
		// Due to this, we suggest client always verify that
		// the received CAR stream response is matching requested DAG selector
		w.Header().Set("X-Stream-Error", streamErr.Error())
		return false
	}

	// Update metrics
	i.carStreamGetMetric.WithLabelValues(rq.contentPath.Namespace()).Observe(time.Since(rq.begin).Seconds())
	return true
}

func getCarParams(r *http.Request, formatParams map[string]string) (*CarParams, error) {
	queryParams := r.URL.Query()
	rangeStr, hasRange := queryParams.Get(carRangeBytesKey), queryParams.Has(carRangeBytesKey)
	scopeStr, hasScope := queryParams.Get(carTerminalElementTypeKey), queryParams.Has(carTerminalElementTypeKey)

	params := CarParams{}
	if hasRange {
		rng, err := NewDagByteRange(rangeStr)
		if err != nil {
			err = fmt.Errorf("invalid entity-bytes: %w", err)
			return nil, err
		}
		params.Range = &rng
	}

	if hasScope {
		switch s := DagScope(scopeStr); s {
		case DagScopeEntity, DagScopeAll, DagScopeBlock:
			params.Scope = s
		default:
			err := fmt.Errorf("unsupported dag-scope %s", scopeStr)
			return nil, err
		}
	} else {
		params.Scope = DagScopeAll
	}

	switch order := DagOrder(formatParams["order"]); order {
	case DagOrderUnknown, DagOrderDFS:
		params.Order = order
	case "":
		params.Order = DagOrderUnknown
	default:
		return nil, fmt.Errorf("unsupported order %s", order)
	}

	switch dups := formatParams["dups"]; dups {
	case "y":
		v := true
		params.Duplicates = &v
	case "n":
		v := false
		params.Duplicates = &v
	case "":
		// Acceptable, we do not set anything.
	default:
		return nil, fmt.Errorf("unsupported dups %s", dups)
	}

	return &params, nil
}

func getContentTypeFromCarParams(params *CarParams) string {
	h := strings.Builder{}
	h.WriteString(carResponseFormat)
	h.WriteString("; version=1; order=")

	if params.Order != "" {
		h.WriteString(string(params.Order))
	} else {
		h.WriteString(string(DagOrderUnknown))
	}

	if params.Duplicates != nil {
		h.WriteString("; dups=")
		if *params.Duplicates {
			h.WriteString("y")
		} else {
			h.WriteString("n")
		}
	}

	return h.String()
}

func getCarRootCidAndLastSegment(imPath ImmutablePath) (cid.Cid, string, error) {
	imPathStr := imPath.String()
	if !strings.HasPrefix(imPathStr, "/ipfs/") {
		return cid.Undef, "", fmt.Errorf("path does not have /ipfs/ prefix")
	}

	firstSegment, remainingSegments, _ := strings.Cut(imPathStr[6:], "/")
	rootCid, err := cid.Decode(firstSegment)
	if err != nil {
		return cid.Undef, "", err
	}

	// Almost like path.Base(remainingSegments), but without special case for empty strings.
	lastSegment := strings.TrimRight(remainingSegments, "/")
	if i := strings.LastIndex(lastSegment, "/"); i >= 0 {
		lastSegment = lastSegment[i+1:]
	}

	return rootCid, lastSegment, err
}

func getCarEtag(imPath ImmutablePath, params *CarParams, rootCid cid.Cid) string {
	data := imPath.String()
	if params.Scope != DagScopeAll {
		data += "." + string(params.Scope)
	}

	if params.Range != nil {
		if params.Range.From != 0 || params.Range.To != nil {
			data += "." + strconv.FormatInt(params.Range.From, 10)
			if params.Range.To != nil {
				data += "." + strconv.FormatInt(*params.Range.To, 10)
			}
		}
	}

	suffix := strconv.FormatUint(xxhash.Sum64([]byte(data)), 32)
	return `W/"` + rootCid.String() + ".car." + suffix + `"`
}
