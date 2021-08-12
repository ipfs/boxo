package merkledag_test

import (
	"bytes"
	"fmt"
	"testing"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
)

var benchInput []byte

func init() {
	someData := bytes.Repeat([]byte("some plaintext data\n"), 10)
	// make a test CID -- doesn't matter just to add as a link
	someCid, _ := cid.Cast([]byte{1, 85, 0, 5, 0, 1, 2, 3, 4})

	node := &merkledag.ProtoNode{}
	node.SetData(someData)
	for i := 0; i < 10; i++ {
		node.AddRawLink(fmt.Sprintf("%d", i), &ipld.Link{
			Size: 10,
			Cid:  someCid,
		})
	}

	enc, err := node.EncodeProtobuf(true)
	if err != nil {
		panic(err)
	}
	benchInput = enc
}

func BenchmarkRoundtrip(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			node, err := merkledag.DecodeProtobuf(benchInput)
			if err != nil {
				b.Fatal(err)
			}

			enc, err := node.EncodeProtobuf(true)
			if err != nil {
				b.Fatal(err)
			}
			_ = enc
		}
	})
}
