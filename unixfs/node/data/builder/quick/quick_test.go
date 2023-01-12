package quickbuilder_test

import (
	"testing"

	quickbuilder "github.com/ipfs/go-unixfsnode/data/builder/quick"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
)

func TestQuickBuilder(t *testing.T) {
	ls := cidlink.DefaultLinkSystem()
	store := memstore.Store{Bag: make(map[string][]byte)}
	ls.SetReadStorage(&store)
	ls.SetWriteStorage(&store)
	err := quickbuilder.Store(&ls, func(b *quickbuilder.Builder) error {
		b.NewMapDirectory(map[string]quickbuilder.Node{
			"file.txt": b.NewBytesFile([]byte("1")),
			"foo? #<'": b.NewMapDirectory(map[string]quickbuilder.Node{
				"file.txt": b.NewBytesFile([]byte("2")),
				"bar": b.NewMapDirectory(map[string]quickbuilder.Node{
					"file.txt": b.NewBytesFile([]byte("3")),
				}),
			}),
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(store.Bag) != 6 {
		t.Fatal("unexpected number of stored nodes")
	}
}
