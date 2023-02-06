package gateway

import (
	"bytes"
	"context"
	"fmt"
	"html"
	"net/http"
	"strings"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/gateway/assets"
	ipath "github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	mc "github.com/multiformats/go-multicodec"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// codecToContentType maps the supported IPLD codecs to the HTTP Content
// Type they should have.
var codecToContentType = map[mc.Code]string{
	mc.Json:    "application/json",
	mc.Cbor:    "application/cbor",
	mc.DagJson: "application/vnd.ipld.dag-json",
	mc.DagCbor: "application/vnd.ipld.dag-cbor",
}

// contentTypeToRaw maps the HTTP Content Type to the respective codec that
// allows raw response without any conversion.
var contentTypeToRaw = map[string][]mc.Code{
	"application/json": {mc.Json, mc.DagJson},
	"application/cbor": {mc.Cbor, mc.DagCbor},
}

// contentTypeToCodec maps the HTTP Content Type to the respective codec. We
// only add here the codecs that we want to convert-to-from.
var contentTypeToCodec = map[string]mc.Code{
	"application/vnd.ipld.dag-json": mc.DagJson,
	"application/vnd.ipld.dag-cbor": mc.DagCbor,
}

// contentTypeToExtension maps the HTTP Content Type to the respective file
// extension, used in Content-Disposition header when downloading the file.
var contentTypeToExtension = map[string]string{
	"application/json":              ".json",
	"application/vnd.ipld.dag-json": ".json",
	"application/cbor":              ".cbor",
	"application/vnd.ipld.dag-cbor": ".cbor",
}

func (i *handler) serveCodec(ctx context.Context, w http.ResponseWriter, r *http.Request, resolvedPath ipath.Resolved, contentPath ipath.Path, begin time.Time, requestedContentType string) bool {
	ctx, span := spanTrace(ctx, "ServeCodec", trace.WithAttributes(attribute.String("path", resolvedPath.String()), attribute.String("requestedContentType", requestedContentType)))
	defer span.End()

	cidCodec := mc.Code(resolvedPath.Cid().Prefix().Codec)
	responseContentType := requestedContentType

	// If the resolved path still has some remainder, return error for now.
	// TODO: handle this when we have IPLD Patch (https://ipld.io/specs/patch/) via HTTP PUT
	// TODO: (depends on https://github.com/ipfs/kubo/issues/4801 and https://github.com/ipfs/kubo/issues/4782)
	if resolvedPath.Remainder() != "" {
		path := strings.TrimSuffix(resolvedPath.String(), resolvedPath.Remainder())
		err := fmt.Errorf("%q of %q could not be returned: reading IPLD Kinds other than Links (CBOR Tag 42) is not implemented: try reading %q instead", resolvedPath.Remainder(), resolvedPath.String(), path)
		webError(w, "unsupported pathing", err, http.StatusNotImplemented)
		return false
	}

	// If no explicit content type was requested, the response will have one based on the codec from the CID
	if requestedContentType == "" {
		cidContentType, ok := codecToContentType[cidCodec]
		if !ok {
			// Should not happen unless function is called with wrong parameters.
			err := fmt.Errorf("content type not found for codec: %v", cidCodec)
			webError(w, "internal error", err, http.StatusInternalServerError)
			return false
		}
		responseContentType = cidContentType
	}

	// Set HTTP headers (for caching etc)
	modtime := addCacheControlHeaders(w, r, contentPath, resolvedPath.Cid())
	name := setCodecContentDisposition(w, r, resolvedPath, responseContentType)
	w.Header().Set("Content-Type", responseContentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")

	// No content type is specified by the user (via Accept, or format=). However,
	// we support this format. Let's handle it.
	if requestedContentType == "" {
		isDAG := cidCodec == mc.DagJson || cidCodec == mc.DagCbor
		acceptsHTML := strings.Contains(r.Header.Get("Accept"), "text/html")
		download := r.URL.Query().Get("download") == "true"

		if isDAG && acceptsHTML && !download {
			return i.serveCodecHTML(ctx, w, r, resolvedPath, contentPath)
		} else {
			// This covers CIDs with codec 'json' and 'cbor' as those do not have
			// an explicit requested content type.
			return i.serveCodecRaw(ctx, w, r, resolvedPath, contentPath, name, modtime, begin)
		}
	}

	// If DAG-JSON or DAG-CBOR was requested using corresponding plain content type
	// return raw block as-is, without conversion
	skipCodecs, ok := contentTypeToRaw[requestedContentType]
	if ok {
		for _, skipCodec := range skipCodecs {
			if skipCodec == cidCodec {
				return i.serveCodecRaw(ctx, w, r, resolvedPath, contentPath, name, modtime, begin)
			}
		}
	}

	// Otherwise, the user has requested a specific content type (a DAG-* variant).
	// Let's first get the codecs that can be used with this content type.
	toCodec, ok := contentTypeToCodec[requestedContentType]
	if !ok {
		// This is never supposed to happen unless function is called with wrong parameters.
		err := fmt.Errorf("unsupported content type: %s", requestedContentType)
		webError(w, err.Error(), err, http.StatusInternalServerError)
		return false
	}

	// This handles DAG-* conversions and validations.
	return i.serveCodecConverted(ctx, w, r, resolvedPath, contentPath, toCodec, modtime, begin)
}

func (i *handler) serveCodecHTML(ctx context.Context, w http.ResponseWriter, r *http.Request, resolvedPath ipath.Resolved, contentPath ipath.Path) bool {
	// A HTML directory index will be presented, be sure to set the correct
	// type instead of relying on autodetection (which may fail).
	w.Header().Set("Content-Type", "text/html")

	// Clear Content-Disposition -- we want HTML to be rendered inline
	w.Header().Del("Content-Disposition")

	// Generated index requires custom Etag (output may change between Kubo versions)
	dagEtag := getDagIndexEtag(resolvedPath.Cid())
	w.Header().Set("Etag", dagEtag)

	// Remove Cache-Control for now to match UnixFS dir-index-html responses
	// (we don't want browser to cache HTML forever)
	// TODO: if we ever change behavior for UnixFS dir listings, same changes should be applied here
	w.Header().Del("Cache-Control")

	cidCodec := mc.Code(resolvedPath.Cid().Prefix().Codec)
	if err := assets.DagTemplate.Execute(w, assets.DagTemplateData{
		Path:      contentPath.String(),
		CID:       resolvedPath.Cid().String(),
		CodecName: cidCodec.String(),
		CodecHex:  fmt.Sprintf("0x%x", uint64(cidCodec)),
	}); err != nil {
		webError(w, "failed to generate HTML listing for this DAG: try fetching raw block with ?format=raw", err, http.StatusInternalServerError)
		return false
	}

	return true
}

// serveCodecRaw returns the raw block without any conversion
func (i *handler) serveCodecRaw(ctx context.Context, w http.ResponseWriter, r *http.Request, resolvedPath ipath.Resolved, contentPath ipath.Path, name string, modtime, begin time.Time) bool {
	blockCid := resolvedPath.Cid()
	block, err := i.api.GetBlock(ctx, blockCid)
	if err != nil {
		webError(w, "ipfs block get "+blockCid.String(), err, http.StatusInternalServerError)
		return false
	}
	content := bytes.NewReader(block.RawData())

	// ServeContent will take care of
	// If-None-Match+Etag, Content-Length and range requests
	_, dataSent, _ := ServeContent(w, r, name, modtime, content)

	if dataSent {
		// Update metrics
		i.jsoncborDocumentGetMetric.WithLabelValues(contentPath.Namespace()).Observe(time.Since(begin).Seconds())
	}

	return dataSent
}

// serveCodecConverted returns payload converted to codec specified in toCodec
func (i *handler) serveCodecConverted(ctx context.Context, w http.ResponseWriter, r *http.Request, resolvedPath ipath.Resolved, contentPath ipath.Path, toCodec mc.Code, modtime, begin time.Time) bool {
	blockCid := resolvedPath.Cid()
	block, err := i.api.GetBlock(ctx, blockCid)
	if err != nil {
		webError(w, "ipfs block get "+html.EscapeString(resolvedPath.String()), err, http.StatusInternalServerError)
		return false
	}

	codec := blockCid.Prefix().Codec
	decoder, err := multicodec.LookupDecoder(codec)
	if err != nil {
		webError(w, err.Error(), err, http.StatusInternalServerError)
		return false
	}

	node := basicnode.Prototype.Any.NewBuilder()
	err = decoder(node, bytes.NewReader(block.RawData()))
	if err != nil {
		webError(w, err.Error(), err, http.StatusInternalServerError)
		return false
	}

	encoder, err := multicodec.LookupEncoder(uint64(toCodec))
	if err != nil {
		webError(w, err.Error(), err, http.StatusInternalServerError)
		return false
	}

	// Ensure IPLD node conforms to the codec specification.
	var buf bytes.Buffer
	err = encoder(node.Build(), &buf)
	if err != nil {
		webError(w, err.Error(), err, http.StatusInternalServerError)
		return false
	}

	// Sets correct Last-Modified header. This code is borrowed from the standard
	// library (net/http/server.go) as we cannot use serveFile.
	if !(modtime.IsZero() || modtime.Equal(unixEpochTime)) {
		w.Header().Set("Last-Modified", modtime.UTC().Format(http.TimeFormat))
	}

	_, err = w.Write(buf.Bytes())
	if err == nil {
		// Update metrics
		i.jsoncborDocumentGetMetric.WithLabelValues(contentPath.Namespace()).Observe(time.Since(begin).Seconds())
		return true
	}

	return false
}

func setCodecContentDisposition(w http.ResponseWriter, r *http.Request, resolvedPath ipath.Resolved, contentType string) string {
	var dispType, name string

	ext, ok := contentTypeToExtension[contentType]
	if !ok {
		// Should never happen.
		ext = ".bin"
	}

	if urlFilename := r.URL.Query().Get("filename"); urlFilename != "" {
		name = urlFilename
	} else {
		name = resolvedPath.Cid().String() + ext
	}

	// JSON should be inlined, but ?download=true should still override
	if r.URL.Query().Get("download") == "true" {
		dispType = "attachment"
	} else {
		switch ext {
		case ".json": // codecs that serialize to JSON can be rendered by browsers
			dispType = "inline"
		default: // everything else is assumed binary / opaque bytes
			dispType = "attachment"
		}
	}

	setContentDispositionHeader(w, name, dispType)
	return name
}

func getDagIndexEtag(dagCid cid.Cid) string {
	return `"DagIndex-` + assets.AssetHash + `_CID-` + dagCid.String() + `"`
}
