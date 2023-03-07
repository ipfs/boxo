package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/go-libipfs/files"
	mc "github.com/multiformats/go-multicodec"

	ipath "github.com/ipfs/interface-go-ipfs-core/path"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func (i *handler) serveDefaults(ctx context.Context, w http.ResponseWriter, r *http.Request, maybeResolvedImPath ImmutablePath, immutableContentPath ImmutablePath, contentPath ipath.Path, begin time.Time, requestedContentType string, logger *zap.SugaredLogger) bool {
	ctx, span := spanTrace(ctx, "ServeDefaults", trace.WithAttributes(attribute.String("path", contentPath.String())))
	defer span.End()

	var gwMetadata ContentPathMetadata
	var data files.Node
	var err error

	switch r.Method {
	case http.MethodHead:
		// TODO: Should these follow _redirects
		gwMetadata, data, err = i.api.Head(ctx, maybeResolvedImPath)
		if !i.handleNonUnixFSRequestErrors(w, contentPath, err) { // TODO: even though this might be UnixFS there shouldn't be anything special for HEAD requests
			return false
		}
		defer data.Close()
	case http.MethodGet:
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			gwMetadata, data, err = i.api.Get(ctx, maybeResolvedImPath)
		} else {
			// TODO: Add tests for range parsing
			var ranges []GetRange
			ranges, err = parseRange(rangeHeader)
			if err != nil {
				// This shouldn't be possible to reach which is why it is a 500 rather than 4XX error
				webError(w, fmt.Errorf("invalid range request: %w", err), http.StatusBadRequest)
				return false
			}
			gwMetadata, data, err = i.api.GetRange(ctx, maybeResolvedImPath, ranges...)
		}

		if err != nil {
			if isUnixfsResponseFormat(requestedContentType) {
				forwardedPath, continueProcessing := i.handleUnixFSRequestErrors(w, r, maybeResolvedImPath, immutableContentPath, contentPath, err, logger)
				if !continueProcessing {
					return false
				}
				gwMetadata, data, err = i.api.Get(ctx, forwardedPath)
				if err != nil {
					err = fmt.Errorf("failed to resolve %s: %w", debugStr(contentPath.String()), err)
					webError(w, err, http.StatusInternalServerError)
				}
			} else {
				if !i.handleNonUnixFSRequestErrors(w, contentPath, err) {
					return false
				}
			}
		}
		defer data.Close()
	default:
		// This shouldn't be possible to reach which is why it is a 500 rather than 4XX error
		webError(w, fmt.Errorf("invalid method: cannot use this HTTP method with the given request"), http.StatusInternalServerError)
		return false
	}

	if err := i.setIpfsRootsHeader(w, gwMetadata); err != nil {
		webRequestError(w, err)
		return false
	}

	resolvedPath := gwMetadata.LastSegment
	switch mc.Code(resolvedPath.Cid().Prefix().Codec) {
	case mc.Json, mc.DagJson, mc.Cbor, mc.DagCbor:
		blockData, ok := data.(files.File)
		if !ok { // This should never happen
			webError(w, fmt.Errorf("decoding error: data not a usable as a file"), http.StatusInternalServerError)
			return false
		}
		logger.Debugw("serving codec", "path", contentPath)
		return i.renderCodec(r.Context(), w, r, resolvedPath, blockData, contentPath, begin, requestedContentType)
	default:
		logger.Debugw("serving unixfs", "path", contentPath)
		return i.serveUnixFS(r.Context(), w, r, resolvedPath, data, gwMetadata.ContentType, contentPath, begin, logger)
	}
}

// parseRange parses a Range header string as per RFC 7233.
func parseRange(s string) ([]GetRange, error) {
	if s == "" {
		return nil, nil // header not present
	}
	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		return nil, errors.New("invalid range")
	}
	var ranges []GetRange
	for _, ra := range strings.Split(s[len(b):], ",") {
		ra = textproto.TrimString(ra)
		if ra == "" {
			continue
		}
		start, end, ok := strings.Cut(ra, "-")
		if !ok {
			return nil, errors.New("invalid range")
		}
		start, end = textproto.TrimString(start), textproto.TrimString(end)
		var r GetRange
		if start == "" {
			r.From = 0
			// If no start is specified, end specifies the
			// range start relative to the end of the file,
			// and we are dealing with <suffix-length>
			// which has to be a non-negative integer as per
			// RFC 7233 Section 2.1 "Byte-Ranges".
			if end == "" || end[0] == '-' {
				return nil, errors.New("invalid range")
			}
			i, err := strconv.ParseInt(end, 10, 64)
			if i < 0 || err != nil {
				return nil, errors.New("invalid range")
			}
			r.To = &i
		} else {
			i, err := strconv.ParseUint(start, 10, 64)
			if err != nil {
				return nil, errors.New("invalid range")
			}
			r.From = i
			if end == "" {
				// If no end is specified, range extends to end of the file.
				r.To = nil
			} else {
				i, err := strconv.ParseInt(end, 10, 64)
				if err != nil || i < 0 || r.From > uint64(i) {
					return nil, errors.New("invalid range")
				}
				r.To = &i
			}
		}
		ranges = append(ranges, r)
	}
	return ranges, nil
}
