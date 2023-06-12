package unixfs

import (
	"encoding/base64"
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

const someDagPBBlock = `EisKIhIgVuq+9ViNicx1O8bIsb978a8u1uoTjm4taEeNW7gcB+cSABiu1OAVEioKIhIg7XyJKU3lrLCYFLKmcNTtKc82BUBCi5ePAeAqz2M1pWYSABirmGcKEAgCGPGUxxYggIDgFSDxlGc=`

func BenchmarkPB(b *testing.B) {
	data, err := base64.StdEncoding.DecodeString(someDagPBBlock)
	if err != nil {
		b.Fatal(err)
	}
	mh, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		b.Fatal()
	}
	c := cid.NewCidV0(mh)

	b.ResetTimer()
	var out []FileEntry
	for i := b.N; i != 0; i-- {
		_, f, _, _, _ := parsePB(out[:0], nil, c, data)
		out = f.Childrens
	}
}

func FuzzPB(f *testing.F) {
	data, err := base64.StdEncoding.DecodeString(someDagPBBlock)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(data)
	f.Fuzz(func(_ *testing.T, b []byte) {
		if len(b) > 2*1024*1024 {
			// Assume a block limit is inplace.
			return
		}
		parsePB(nil, nil, cid.Undef, b)
	})
}
