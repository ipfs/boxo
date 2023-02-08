package gateway

import (
	"context"
	"fmt"
	"html"
	"net/http"
	"time"

	"github.com/ipfs/go-libipfs/files"
	ipath "github.com/ipfs/interface-go-ipfs-core/path"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func (i *handler) serveUnixFS(ctx context.Context, w http.ResponseWriter, r *http.Request, resolvedPath ipath.Resolved, contentPath ipath.Path, begin time.Time, logger *zap.SugaredLogger) bool {
	ctx, span := spanTrace(ctx, "ServeUnixFS", trace.WithAttributes(attribute.String("path", resolvedPath.String())))
	defer span.End()

	// Handling UnixFS
	dr, err := i.api.GetUnixFsNode(ctx, resolvedPath)
	if err != nil {
		webError(w, "ipfs cat "+html.EscapeString(contentPath.String()), err, http.StatusBadRequest)
		return false
	}
	defer dr.Close()

	// Handling Unixfs file
	if f, ok := dr.(files.File); ok {
		logger.Debugw("serving unixfs file", "path", contentPath)
		i.serveFile(ctx, w, r, resolvedPath, contentPath, f, begin)
		return false
	}

	// Handling Unixfs directory
	dir, ok := dr.(files.Directory)
	if !ok {
		internalWebError(w, fmt.Errorf("unsupported UnixFS type"))
		return false
	}

	logger.Debugw("serving unixfs directory", "path", contentPath)
	i.serveDirectory(ctx, w, r, resolvedPath, contentPath, dir, begin, logger)
	return true
}
