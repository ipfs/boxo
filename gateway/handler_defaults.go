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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func (i *handler) serveDefaults(ctx context.Context, w http.ResponseWriter, r *http.Request, contentPath contentPathRequest, begin time.Time, requestedContentType string, logger *zap.SugaredLogger) bool {
	ctx, span := spanTrace(ctx, "ServeDefaults", trace.WithAttributes(attribute.String("path", contentPath.originalRequestedPath.String())))
	defer span.End()

	var (
		pathMetadata           ContentPathMetadata
		bytesResponse          files.File
		isDirectoryHeadRequest bool
		dirListing             *directoryResponse
		err                    error
	)

	switch r.Method {
	case http.MethodHead:
		var data files.Node
		pathMetadata, data, err = i.api.Head(ctx, contentPath.immutablePartiallyResolvedPath)
		if !i.handleNonUnixFSRequestErrors(w, contentPath.originalRequestedPath, err) { // TODO: even though this might be UnixFS there shouldn't be anything special for HEAD requests
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
		if rangeHeader == "" {
			var getResp *GetResponse
			pathMetadata, getResp, err = i.api.Get(ctx, contentPath.immutablePartiallyResolvedPath)
			if err != nil {
				if isUnixfsResponseFormat(requestedContentType) {
					forwardedPath, continueProcessing := i.handleUnixFSRequestErrors(w, r, contentPath, err, logger)
					if !continueProcessing {
						return false
					}
					pathMetadata, getResp, err = i.api.Get(ctx, forwardedPath)
					if err != nil {
						err = fmt.Errorf("failed to resolve %s: %w", debugStr(contentPath.originalRequestedPath.String()), err)
						webError(w, err, http.StatusInternalServerError)
					}
				} else {
					if !i.handleNonUnixFSRequestErrors(w, contentPath.originalRequestedPath, err) {
						return false
					}
				}
			}
			if getResp.bytes != nil {
				bytesResponse = getResp.bytes
				defer bytesResponse.Close()
			} else {
				dirListing = getResp.directory
			}
		} else {
			// TODO: Add tests for range parsing
			var ranges []GetRange
			ranges, err = parseRange(rangeHeader)
			if err != nil {
				webError(w, fmt.Errorf("invalid range request: %w", err), http.StatusBadRequest)
				return false
			}
			pathMetadata, bytesResponse, err = i.api.GetRange(ctx, contentPath.immutablePartiallyResolvedPath, ranges...)
			if err != nil {
				if isUnixfsResponseFormat(requestedContentType) {
					forwardedPath, continueProcessing := i.handleUnixFSRequestErrors(w, r, contentPath, err, logger)
					if !continueProcessing {
						return false
					}
					pathMetadata, bytesResponse, err = i.api.GetRange(ctx, forwardedPath, ranges...)
					if err != nil {
						err = fmt.Errorf("failed to resolve %s: %w", debugStr(contentPath.originalRequestedPath.String()), err)
						webError(w, err, http.StatusInternalServerError)
					}
				} else {
					if !i.handleNonUnixFSRequestErrors(w, contentPath.originalRequestedPath, err) {
						return false
					}
				}
			}
			defer bytesResponse.Close()
		}
	default:
		// This shouldn't be possible to reach which is why it is a 500 rather than 4XX error
		webError(w, fmt.Errorf("invalid method: cannot use this HTTP method with the given request"), http.StatusInternalServerError)
		return false
	}

	if err := i.setIpfsRootsHeader(w, pathMetadata); err != nil {
		webRequestError(w, err)
		return false
	}

	contentPath.finalResolvedPath = pathMetadata.LastSegment
	switch mc.Code(contentPath.finalResolvedPath.Cid().Prefix().Codec) {
	case mc.Json, mc.DagJson, mc.Cbor, mc.DagCbor:
		if bytesResponse == nil { // This should never happen
			webError(w, fmt.Errorf("decoding error: data not a usable as a file"), http.StatusInternalServerError)
			return false
		}
		logger.Debugw("serving codec", "path", contentPath.originalRequestedPath)
		return i.renderCodec(r.Context(), w, r, contentPath, bytesResponse, begin, requestedContentType)
	default:
		logger.Debugw("serving unixfs", "path", contentPath)
		ctx, span := spanTrace(ctx, "ServeUnixFS", trace.WithAttributes(attribute.String("path", contentPath.finalResolvedPath.String())))
		defer span.End()

		// Handling Unixfs file
		if bytesResponse != nil {
			logger.Debugw("serving unixfs file", "path", contentPath.originalRequestedPath)
			return i.serveFile(ctx, w, r, contentPath, bytesResponse, pathMetadata.ContentType, begin)
		}

		// Handling Unixfs directory
		if !isDirectoryHeadRequest && dirListing == nil {
			webError(w, fmt.Errorf("unsupported UnixFS type"), http.StatusInternalServerError)
			return false
		}

		logger.Debugw("serving unixfs directory", "path", contentPath.originalRequestedPath)
		return i.serveDirectory(ctx, w, r, contentPath, isDirectoryHeadRequest, dirListing, begin, logger)
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
