package gateway

import (
	"context"
	"fmt"
	"net/http"
	"time"

	ipath "github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/files"
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
		err = fmt.Errorf("error while getting UnixFS node: %w", err)
		webError(w, err, http.StatusInternalServerError)
		return false
	}
	defer dr.Close()

	// Handling Unixfs file
	if f, ok := dr.(files.File); ok {
		logger.Debugw("serving unixfs file", "path", contentPath)
		return i.serveFile(ctx, w, r, resolvedPath, contentPath, f, begin)
	}

	// Handling Unixfs directory
	dir, ok := dr.(files.Directory)
	if !ok {
		webError(w, fmt.Errorf("unsupported UnixFS type"), http.StatusInternalServerError)
		return false
	}

	logger.Debugw("serving unixfs directory", "path", contentPath)
	return i.serveDirectory(ctx, w, r, resolvedPath, contentPath, dir, begin, logger)
}
