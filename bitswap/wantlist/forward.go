package wantlist

import (
	"github.com/ipfs/go-bitswap/client/wantlist"
	"github.com/ipfs/go-cid"
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
