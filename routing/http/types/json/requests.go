package json

// AnnounceProvidersRequest is the content of a POST Providers request.
type AnnounceProvidersRequest struct {
	Providers RecordsArray
}

// AnnouncePeersRequest is the content of a POST Peers request.
type AnnouncePeersRequest struct {
	Peers RecordsArray
}
