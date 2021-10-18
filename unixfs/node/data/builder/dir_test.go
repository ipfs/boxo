package builder

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ipfs/go-unixfsnode"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

func mkEntries(cnt int, ls *ipld.LinkSystem) ([]dagpb.PBLink, error) {
	entries := make([]dagpb.PBLink, 0, cnt)
	for i := 0; i < cnt; i++ {
		r := bytes.NewBufferString(fmt.Sprintf("%d", i))
		f, s, err := BuildUnixFSFile(r, "", ls)
		if err != nil {
			return nil, err
		}
		e, err := BuildUnixFSDirectoryEntry(fmt.Sprintf("file %d", i), int64(s), f)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func TestBuildUnixFSDirectory(t *testing.T) {
	ls := cidlink.DefaultLinkSystem()
	storage := cidlink.Memory{}
	ls.StorageReadOpener = storage.OpenRead
	ls.StorageWriteOpener = storage.OpenWrite

	testSizes := []int{100, 1000, 50000}
	for _, cnt := range testSizes {
		entries, err := mkEntries(cnt, &ls)
		if err != nil {
			t.Fatal(err)
		}

		dl, err := BuildUnixFSDirectory(entries, &ls)
		if err != nil {
			t.Fatal(err)
		}

		pbn, err := ls.Load(ipld.LinkContext{}, dl, dagpb.Type.PBNode)
		if err != nil {
			t.Fatal(err)
		}
		ufd, err := unixfsnode.Reify(ipld.LinkContext{}, pbn, &ls)
		if err != nil {
			t.Fatal(err)
		}
		observedCnt := 0

		li := ufd.MapIterator()
		for !li.Done() {
			_, _, err := li.Next()
			if err != nil {
				t.Fatal(err)
			}
			observedCnt++
		}
		if observedCnt != cnt {
			fmt.Printf("%+v\n", ufd)
			t.Fatalf("unexpected number of dir entries %d vs %d", observedCnt, cnt)
		}
	}
}
