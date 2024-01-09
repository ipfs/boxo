package json

// AnnounceProvidersRequest is the content of a PUT Providers request.
type AnnounceProvidersRequest struct {
	Error     string
	Providers RecordsArray
}

// AnnouncePeersRequest is the content of a PUT Peers request.
// TODO: is the the same? Shouldn't Providers be Peers?
type AnnouncePeersRequest = AnnounceProvidersRequest
