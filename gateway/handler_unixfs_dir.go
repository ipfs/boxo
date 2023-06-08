package gateway

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	gopath "path"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	ipath "github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/gateway/assets"
	path "github.com/ipfs/boxo/path"
	cid "github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// serveDirectory returns the best representation of UnixFS directory
//
// It will return index.html if present, or generate directory listing otherwise.
func (i *handler) serveDirectory(ctx context.Context, w http.ResponseWriter, r *http.Request, resolvedPath ipath.Resolved, contentPath ipath.Path, isHeadRequest bool, directoryMetadata *directoryMetadata, ranges []ByteRange, begin time.Time, logger *zap.SugaredLogger) bool {
	ctx, span := spanTrace(ctx, "Handler.ServeDirectory", trace.WithAttributes(attribute.String("path", resolvedPath.String())))
	defer span.End()

	// WithHostname might have constructed an IPNS/IPFS path using the Host header.
	// In this case, we need the original path for constructing redirects and links
	// that match the requested URL.
	// For example, http://example.net would become /ipns/example.net, and
	// the redirects and links would end up as http://example.net/ipns/example.net
	requestURI, err := url.ParseRequestURI(r.RequestURI)
	if err != nil {
		i.webError(w, r, fmt.Errorf("failed to parse request path: %w", err), http.StatusInternalServerError)
		return false
	}
	originalURLPath := requestURI.Path

	// Ensure directory paths end with '/'
	if originalURLPath[len(originalURLPath)-1] != '/' {
		// don't redirect to trailing slash if it's go get
		// https://github.com/ipfs/kubo/pull/3963
		goget := r.URL.Query().Get("go-get") == "1"
		if !goget {
			suffix := "/"
			// preserve query parameters
			if r.URL.RawQuery != "" {
				suffix = suffix + "?" + r.URL.RawQuery
			}
			// /ipfs/cid/foo?bar must be redirected to /ipfs/cid/foo/?bar
			redirectURL := originalURLPath + suffix
			logger.Debugw("directory location moved permanently", "status", http.StatusMovedPermanently)
			http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)
			return true
		}
	}

	// Check if directory has index.html, if so, serveFile
	idxPath := ipath.Join(contentPath, "index.html")
	imIndexPath, err := NewImmutablePath(ipath.Join(resolvedPath, "index.html"))
	if err != nil {
		i.webError(w, r, err, http.StatusInternalServerError)
		return false
	}

	// TODO: could/should this all be skipped to have HEAD requests just return html content type and save the complexity? If so can we skip the above code as well?
	var idxFile files.File
	if isHeadRequest {
		var idx files.Node
		_, idx, err = i.backend.Head(ctx, imIndexPath)
		if err == nil {
			f, ok := idx.(files.File)
			if !ok {
				i.webError(w, r, fmt.Errorf("%q could not be read: %w", imIndexPath, files.ErrNotReader), http.StatusUnprocessableEntity)
				return false
			}
			idxFile = f
		}
	} else {
		var getResp *GetResponse
		_, getResp, err = i.backend.Get(ctx, imIndexPath, ranges...)
		if err == nil {
			if getResp.bytes == nil {
				i.webError(w, r, fmt.Errorf("%q could not be read: %w", imIndexPath, files.ErrNotReader), http.StatusUnprocessableEntity)
				return false
			}
			idxFile = getResp.bytes
		}
	}

	if err == nil {
		logger.Debugw("serving index.html file", "path", idxPath)
		// write to request
		success := i.serveFile(ctx, w, r, resolvedPath, idxPath, idxFile, "text/html", begin)
		if success {
			i.unixfsDirIndexGetMetric.WithLabelValues(contentPath.Namespace()).Observe(time.Since(begin).Seconds())
		}
		return success
	}

	if isErrNotFound(err) {
		logger.Debugw("no index.html; noop", "path", idxPath)
	} else if err != nil {
		i.webError(w, r, err, http.StatusInternalServerError)
		return false
	}

	// A HTML directory index will be presented, be sure to set the correct
	// type instead of relying on autodetection (which may fail).
	w.Header().Set("Content-Type", "text/html")

	// Generated dir index requires custom Etag (output may change between go-libipfs versions)
	dirEtag := getDirListingEtag(resolvedPath.Cid())
	w.Header().Set("Etag", dirEtag)

	if r.Method == http.MethodHead {
		logger.Debug("return as request's HTTP method is HEAD")
		return true
	}

	var dirListing []assets.DirectoryItem
	for l := range directoryMetadata.entries {
		if l.Err != nil {
			i.webError(w, r, l.Err, http.StatusInternalServerError)
			return false
		}

		name := l.Link.Name
		sz := l.Link.Size
		linkCid := l.Link.Cid

		hash := linkCid.String()
		di := assets.DirectoryItem{
			Size:      humanize.Bytes(sz),
			Name:      name,
			Path:      gopath.Join(originalURLPath, name),
			Hash:      hash,
			ShortHash: assets.ShortHash(hash),
		}
		dirListing = append(dirListing, di)
	}

	// construct the correct back link
	// https://github.com/ipfs/kubo/issues/1365
	backLink := originalURLPath

	// don't go further up than /ipfs/$hash/
	pathSplit := path.SplitList(contentPath.String())
	switch {
	// skip backlink when listing a content root
	case len(pathSplit) == 3: // url: /ipfs/$hash
		backLink = ""

	// skip backlink when listing a content root
	case len(pathSplit) == 4 && pathSplit[3] == "": // url: /ipfs/$hash/
		backLink = ""

	// add the correct link depending on whether the path ends with a slash
	default:
		if strings.HasSuffix(backLink, "/") {
			backLink += ".."
		} else {
			backLink += "/.."
		}
	}

	size := humanize.Bytes(directoryMetadata.dagSize)
	hash := resolvedPath.Cid().String()
	globalData := i.getTemplateGlobalData(r, contentPath)

	// See comment above where originalUrlPath is declared.
	tplData := assets.DirectoryTemplateData{
		GlobalData:  globalData,
		Listing:     dirListing,
		Size:        size,
		Path:        contentPath.String(),
		Breadcrumbs: assets.Breadcrumbs(contentPath.String(), globalData.DNSLink),
		BackLink:    backLink,
		Hash:        hash,
	}

	logger.Debugw("request processed", "tplDataDNSLink", globalData.DNSLink, "tplDataSize", size, "tplDataBackLink", backLink, "tplDataHash", hash)

	if err := assets.DirectoryTemplate.Execute(w, tplData); err != nil {
		i.webError(w, r, err, http.StatusInternalServerError)
		return false
	}

	// Update metrics
	i.unixfsGenDirListingGetMetric.WithLabelValues(contentPath.Namespace()).Observe(time.Since(begin).Seconds())
	return true
}

func getDirListingEtag(dirCid cid.Cid) string {
	return `"DirIndex-` + assets.AssetHash + `_CID-` + dirCid.String() + `"`
}
