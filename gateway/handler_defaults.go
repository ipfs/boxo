package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"

	"github.com/ipfs/boxo/files"
	mc "github.com/multiformats/go-multicodec"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (i *handler) serveDefaults(ctx context.Context, w http.ResponseWriter, r *http.Request, rq *requestData) bool {
	ctx, span := spanTrace(ctx, "Handler.ServeDefaults", trace.WithAttributes(attribute.String("path", rq.contentPath.String())))
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
		pathMetadata, data, err = i.backend.Head(ctx, rq.mostlyResolvedPath())
		if err != nil {
			if isWebRequest(rq.responseFormat) {
				forwardedPath, continueProcessing := i.handleWebRequestErrors(w, r, rq.mostlyResolvedPath(), rq.immutablePath, rq.contentPath, err, rq.logger)
				if !continueProcessing {
					return false
				}
				pathMetadata, data, err = i.backend.Head(ctx, forwardedPath)
				if err != nil {
					err = fmt.Errorf("failed to resolve %s: %w", debugStr(rq.contentPath.String()), err)
					i.webError(w, r, err, http.StatusInternalServerError)
					return false
				}
			} else {
				if !i.handleRequestErrors(w, r, rq.contentPath, err) {
					return false
				}
			}
		}
		defer data.Close()
		if _, ok := data.(files.Directory); ok {
			isDirectoryHeadRequest = true
		} else if f, ok := data.(files.File); ok {
			bytesResponse = f
		} else {
			i.webError(w, r, fmt.Errorf("unsupported response type"), http.StatusInternalServerError)
			return false
		}
	case http.MethodGet:
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			// TODO: Add tests for range parsing
			ranges, err = parseRange(rangeHeader)
			if err != nil {
				i.webError(w, r, fmt.Errorf("invalid range request: %w", err), http.StatusBadRequest)
				return false
			}
		}

		var getResp *GetResponse
		// TODO: passing only resolved path here, instead of contentPath is
		// harming content routing. Knowing original immutableContentPath will
		// allow backend to find  providers for parents, even when internal
		// CIDs are not announced, and will provide better key for caching
		// related DAGs.
		pathMetadata, getResp, err = i.backend.Get(ctx, rq.mostlyResolvedPath(), ranges...)
		if err != nil {
			if isWebRequest(rq.responseFormat) {
				forwardedPath, continueProcessing := i.handleWebRequestErrors(w, r, rq.mostlyResolvedPath(), rq.immutablePath, rq.contentPath, err, rq.logger)
				if !continueProcessing {
					return false
				}
				pathMetadata, getResp, err = i.backend.Get(ctx, forwardedPath, ranges...)
				if err != nil {
					err = fmt.Errorf("failed to resolve %s: %w", debugStr(rq.contentPath.String()), err)
					i.webError(w, r, err, http.StatusInternalServerError)
					return false
				}
			} else {
				if !i.handleRequestErrors(w, r, rq.contentPath, err) {
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
		i.webError(w, r, fmt.Errorf("invalid method: cannot use this HTTP method with the given request"), http.StatusInternalServerError)
		return false
	}

	setIpfsRootsHeader(w, rq, &pathMetadata)

	resolvedPath := pathMetadata.LastSegment
	switch mc.Code(resolvedPath.Cid().Prefix().Codec) {
	case mc.Json, mc.DagJson, mc.Cbor, mc.DagCbor:
		if bytesResponse == nil { // This should never happen
			i.webError(w, r, fmt.Errorf("decoding error: data not usable as a file"), http.StatusInternalServerError)
			return false
		}
		rq.logger.Debugw("serving codec", "path", rq.contentPath)
		return i.renderCodec(r.Context(), w, r, rq, bytesResponse)
	default:
		rq.logger.Debugw("serving unixfs", "path", rq.contentPath)
		ctx, span := spanTrace(ctx, "Handler.ServeUnixFS", trace.WithAttributes(attribute.String("path", resolvedPath.String())))
		defer span.End()

		// Handling Unixfs file
		if bytesResponse != nil {
			rq.logger.Debugw("serving unixfs file", "path", rq.contentPath)
			return i.serveFile(ctx, w, r, resolvedPath, rq.contentPath, bytesResponse, pathMetadata.ContentType, rq.begin)
		}

		// Handling Unixfs directory
		if directoryMetadata != nil || isDirectoryHeadRequest {
			rq.logger.Debugw("serving unixfs directory", "path", rq.contentPath)
			return i.serveDirectory(ctx, w, r, resolvedPath, rq.contentPath, isDirectoryHeadRequest, directoryMetadata, ranges, rq.begin, rq.logger)
		}

		i.webError(w, r, fmt.Errorf("unsupported UnixFS type"), http.StatusInternalServerError)
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
