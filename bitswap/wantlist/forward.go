package wantlist

import (
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/bitswap/client/wantlist"
)

type (
	// DEPRECATED use wantlist.Entry instead
	Entry = wantlist.Entry
	// DEPRECATED use wantlist.Wantlist instead
	Wantlist = wantlist.Wantlist
)

// DEPRECATED use wantlist.New instead
func New() *Wantlist {
	return wantlist.New()
}

// DEPRECATED use wantlist.NewRefEntry instead
func NewRefEntry(c cid.Cid, p int32) Entry {
	return wantlist.NewRefEntry(c, p)
}
