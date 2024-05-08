package json

import "github.com/ipfs/boxo/routing/http/types"

// AnnounceProvidersRequest is the content of a POST Providers request.
type AnnounceProvidersRequest struct {
	Providers []*types.AnnouncementRecord
}

// AnnouncePeersRequest is the content of a POST Peers request.
type AnnouncePeersRequest struct {
	Peers []*types.AnnouncementRecord
}
