package gateway

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/ipfs/go-libipfs/files"
	ipath "github.com/ipfs/interface-go-ipfs-core/path"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func (i *handler) serveUnixFS(ctx context.Context, w http.ResponseWriter, r *http.Request, resolvedPath ipath.Resolved, data files.Node, fileContentType string, contentPath ipath.Path, begin time.Time, logger *zap.SugaredLogger) bool {
	ctx, span := spanTrace(ctx, "ServeUnixFS", trace.WithAttributes(attribute.String("path", resolvedPath.String())))
	defer span.End()

	// Handling Unixfs file
	if f, ok := data.(files.File); ok {
		logger.Debugw("serving unixfs file", "path", contentPath)
		return i.serveFile(ctx, w, r, resolvedPath, contentPath, f, fileContentType, begin)
	}

	// Handling Unixfs directory
	dir, ok := data.(files.Directory)
	if !ok {
		webError(w, fmt.Errorf("unsupported UnixFS type"), http.StatusInternalServerError)
		return false
	}

	logger.Debugw("serving unixfs directory", "path", contentPath)
	return i.serveDirectory(ctx, w, r, resolvedPath, contentPath, dir, begin, logger)
}
