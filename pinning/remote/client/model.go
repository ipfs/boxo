package go_pinning_service_http_client

import (
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-pinning-service-http-client/openapi"
	"github.com/multiformats/go-multiaddr"
	"time"
)

// PinGetter Getter for Pin object
type PinGetter interface {
	// CID to be pinned recursively
	GetCid() cid.Cid
	// Optional name for pinned data; can be used for lookups later
	GetName() string
	// Optional list of multiaddrs known to provide the data
	GetOrigins() []string
	// Optional metadata for pin object
	GetMeta() map[string]string
}

type pinObject struct {
	openapi.Pin
}

func (p *pinObject) GetCid() cid.Cid {
	c, err := cid.Parse(p.Pin.Cid)
	if err != nil {
		return cid.Undef
	}
	return c
}

type Status = openapi.Status

const (
	StatusQueued  Status = openapi.QUEUED
	StatusPinning Status = openapi.PINNING
	StatusPinned  Status = openapi.PINNED
	StatusFailed  Status = openapi.FAILED
)

var validStatuses = []Status{"queued", "pinning", "pinned", "failed"}

// PinStatusGetter Getter for Pin object with status
type PinStatusGetter interface {
	// Globally unique ID of the pin request; can be used to check the status of ongoing pinning, modification of pin object, or pin removal
	GetId() string
	GetStatus() Status
	// Immutable timestamp indicating when a pin request entered a pinning service; can be used for filtering results and pagination
	GetCreated() time.Time
	GetPin() PinGetter
	// List of multiaddrs designated by pinning service for transferring any new data from external peers
	GetDelegates() []multiaddr.Multiaddr
	// Optional info for PinStatus response
	GetInfo() map[string]string
}

type pinStatusObject struct {
	openapi.PinStatus
}

func (p *pinStatusObject) GetDelegates() []multiaddr.Multiaddr {
	delegates := p.PinStatus.GetDelegates()
	addrs := make([]multiaddr.Multiaddr, 0, len(delegates))
	for _, d := range delegates {
		a, err := multiaddr.NewMultiaddr(d)
		if err != nil {
			logger.Errorf("returned delegate is an invalid multiaddr: %w", err)
			continue
		}
		addrs = append(addrs, a)
	}
	return addrs
}

func (p *pinStatusObject) GetPin() PinGetter {
	return &pinObject{p.Pin}
}
