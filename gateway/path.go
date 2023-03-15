package gateway

import (
	ipath "github.com/ipfs/interface-go-ipfs-core/path"
)

// Path, ResolvedPath, ImmutablePath and other path types → single ContentPath type that has sub-states inside (original, resolved mutable→immutable, and fully resolved to /ipfs/cid + optional remainder)
type contentPathRequest struct {
	originalRequestedPath          ipath.Path     // original path
	immutableRequestedPath         ImmutablePath  // path after resolving mutability
	immutablePartiallyResolvedPath ImmutablePath  // closest resolution to the final path we have so far
	finalResolvedPath              ipath.Resolved // the path after it has been resolved down to its final CID + remainder. Note: this may include non-standard IPLD resolution such as following 200s from UnixFS _redirects
}

func newPath(originalPath ipath.Path) contentPathRequest {
	return contentPathRequest{
		originalRequestedPath:          originalPath,
		immutableRequestedPath:         ImmutablePath{},
		immutablePartiallyResolvedPath: ImmutablePath{},
		finalResolvedPath:              nil,
	}
}
