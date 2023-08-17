package wantlist

import (
	"github.com/ipfs/boxo/bitswap/client/wantlist"
	"github.com/ipfs/go-cid"
)

// Deprecated: use wantlist.Entry instead
type Entry = wantlist.Entry

// Deprecated: use wantlist.Wantlist instead
type Wantlist = wantlist.Wantlist

// Deprecated: use wantlist.New instead
func New() *Wantlist {
	return wantlist.New()
}

// Deprecated: use wantlist.NewRefEntry instead
func NewRefEntry(c cid.Cid, p int32) Entry {
	return wantlist.NewRefEntry(c, p)
}
