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

	"github.com/ipfs/boxo/files"
	mc "github.com/multiformats/go-multicodec"

	ipath "github.com/ipfs/boxo/coreiface/path"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func (i *handler) serveDefaults(ctx context.Context, w http.ResponseWriter, r *http.Request, maybeResolvedImPath ImmutablePath, immutableContentPath ImmutablePath, contentPath ipath.Path, begin time.Time, requestedContentType string, logger *zap.SugaredLogger) bool {
	ctx, span := spanTrace(ctx, "Handler.ServeDefaults", trace.WithAttributes(attribute.String("path", contentPath.String())))
	defer span.End()

	var (
		pathMetadata           ContentPathMetadata
		bytesResponse          files.File
		isDirectoryHeadRequest bool
		directoryMetadata      *directoryMetadata
		err                    error
		ranges                 []ByteRange
	)

	switch r.Method {
	case http.MethodHead:
		var data files.Node
		pathMetadata, data, err = i.api.Head(ctx, maybeResolvedImPath)
		if !i.handleRequestErrors(w, contentPath, err) {
			return false
		}
		defer data.Close()
		if _, ok := data.(files.Directory); ok {
			isDirectoryHeadRequest = true
		} else if f, ok := data.(files.File); ok {
			bytesResponse = f
		} else {
			webError(w, fmt.Errorf("unsupported response type"), http.StatusInternalServerError)
			return false
		}
	case http.MethodGet:
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			// TODO: Add tests for range parsing
			ranges, err = parseRange(rangeHeader)
			if err != nil {
				webError(w, fmt.Errorf("invalid range request: %w", err), http.StatusBadRequest)
				return false
			}
		}

		var getResp *GetResponse
		// TODO: passing only resolved path here, instead of contentPath is
		// harming content routing. Knowing original immutableContentPath will
		// allow backend to find  providers for parents, even when internal
		// CIDs are not announced, and will provide better key for caching
		// related DAGs.
		pathMetadata, getResp, err = i.api.Get(ctx, maybeResolvedImPath, ranges...)
		if err != nil {
			if isWebRequest(requestedContentType) {
				forwardedPath, continueProcessing := i.handleWebRequestErrors(w, r, maybeResolvedImPath, immutableContentPath, contentPath, err, logger)
				if !continueProcessing {
					return false
				}
				pathMetadata, getResp, err = i.api.Get(ctx, forwardedPath, ranges...)
				if err != nil {
					err = fmt.Errorf("failed to resolve %s: %w", debugStr(contentPath.String()), err)
					webError(w, err, http.StatusInternalServerError)
					return false
				}
			} else {
				if !i.handleRequestErrors(w, contentPath, err) {
					return false
				}
			}
		}
		if getResp.bytes != nil {
			bytesResponse = getResp.bytes
			defer bytesResponse.Close()
		} else {
			directoryMetadata = getResp.directoryMetadata
		}

	default:
		// This shouldn't be possible to reach which is why it is a 500 rather than 4XX error
		webError(w, fmt.Errorf("invalid method: cannot use this HTTP method with the given request"), http.StatusInternalServerError)
		return false
	}

	// TODO: check if we have a bug when maybeResolvedImPath is resolved and i.setIpfsRootsHeader works with pathMetadata returned by Get(maybeResolvedImPath)
	if err := i.setIpfsRootsHeader(w, pathMetadata); err != nil {
		webRequestError(w, err)
		return false
	}

	resolvedPath := pathMetadata.LastSegment
	switch mc.Code(resolvedPath.Cid().Prefix().Codec) {
	case mc.Json, mc.DagJson, mc.Cbor, mc.DagCbor:
		if bytesResponse == nil { // This should never happen
			webError(w, fmt.Errorf("decoding error: data not usable as a file"), http.StatusInternalServerError)
			return false
		}
		logger.Debugw("serving codec", "path", contentPath)
		return i.renderCodec(r.Context(), w, r, resolvedPath, bytesResponse, contentPath, begin, requestedContentType)
	default:
		logger.Debugw("serving unixfs", "path", contentPath)
		ctx, span := spanTrace(ctx, "Handler.ServeUnixFS", trace.WithAttributes(attribute.String("path", resolvedPath.String())))
		defer span.End()

		// Handling Unixfs file
		if bytesResponse != nil {
			logger.Debugw("serving unixfs file", "path", contentPath)
			return i.serveFile(ctx, w, r, resolvedPath, contentPath, bytesResponse, pathMetadata.ContentType, begin)
		}

		// Handling Unixfs directory
		if directoryMetadata != nil || isDirectoryHeadRequest {
			logger.Debugw("serving unixfs directory", "path", contentPath)
			return i.serveDirectory(ctx, w, r, resolvedPath, contentPath, isDirectoryHeadRequest, directoryMetadata, ranges, begin, logger)
		}

		webError(w, fmt.Errorf("unsupported UnixFS type"), http.StatusInternalServerError)
		return false
	}
}

// parseRange parses a Range header string as per RFC 7233.
func parseRange(s string) ([]ByteRange, error) {
	if s == "" {
		return nil, nil // header not present
	}
	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		return nil, errors.New("invalid range")
	}
	var ranges []ByteRange
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
		var r ByteRange
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
