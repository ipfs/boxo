package message

import (
	"bytes"
	"testing"

	"github.com/ipfs/go-bitswap/client/wantlist"
	pb "github.com/ipfs/go-bitswap/message/pb"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
)

func mkFakeCid(s string) cid.Cid {
	return cid.NewCidV0(u.Hash([]byte(s)))
}

func TestAppendWanted(t *testing.T) {
	str := mkFakeCid("foo")
	m := New(true)
	m.AddEntry(str, 1, pb.Message_Wantlist_Block, true)

	if !wantlistContains(&m.ToProtoV0().Wantlist, str) {
		t.Fail()
	}
}

func TestNewMessageFromProto(t *testing.T) {
	str := mkFakeCid("a_key")
	protoMessage := new(pb.Message)
	protoMessage.Wantlist.Entries = []pb.Message_Wantlist_Entry{
		{Block: pb.Cid{Cid: str}},
	}
	if !wantlistContains(&protoMessage.Wantlist, str) {
		t.Fail()
	}
	m, err := newMessageFromProto(*protoMessage)
	if err != nil {
		t.Fatal(err)
	}

	if !wantlistContains(&m.ToProtoV0().Wantlist, str) {
		t.Fail()
	}
}

func TestAppendBlock(t *testing.T) {

	strs := make([]string, 2)
	strs = append(strs, "Celeritas")
	strs = append(strs, "Incendia")

	m := New(true)
	for _, str := range strs {
		block := blocks.NewBlock([]byte(str))
		m.AddBlock(block)
	}

	// assert strings are in proto message
	for _, blockbytes := range m.ToProtoV0().GetBlocks() {
		s := bytes.NewBuffer(blockbytes).String()
		if !contains(strs, s) {
			t.Fail()
		}
	}
}

func TestWantlist(t *testing.T) {
	keystrs := []cid.Cid{mkFakeCid("foo"), mkFakeCid("bar"), mkFakeCid("baz"), mkFakeCid("bat")}
	m := New(true)
	for _, s := range keystrs {
		m.AddEntry(s, 1, pb.Message_Wantlist_Block, true)
	}
	exported := m.Wantlist()

	for _, k := range exported {
		present := false
		for _, s := range keystrs {

			if s.Equals(k.Cid) {
				present = true
			}
		}
		if !present {
			t.Logf("%v isn't in original list", k.Cid)
			t.Fail()
		}
	}
}

func TestCopyProtoByValue(t *testing.T) {
	str := mkFakeCid("foo")
	m := New(true)
	protoBeforeAppend := m.ToProtoV0()
	m.AddEntry(str, 1, pb.Message_Wantlist_Block, true)
	if wantlistContains(&protoBeforeAppend.Wantlist, str) {
		t.Fail()
	}
}

func TestToNetFromNetPreservesWantList(t *testing.T) {
	original := New(true)
	original.AddEntry(mkFakeCid("M"), 1, pb.Message_Wantlist_Block, true)
	original.AddEntry(mkFakeCid("B"), 1, pb.Message_Wantlist_Block, true)
	original.AddEntry(mkFakeCid("D"), 1, pb.Message_Wantlist_Block, true)
	original.AddEntry(mkFakeCid("T"), 1, pb.Message_Wantlist_Block, true)
	original.AddEntry(mkFakeCid("F"), 1, pb.Message_Wantlist_Block, true)

	buf := new(bytes.Buffer)
	if err := original.ToNetV1(buf); err != nil {
		t.Fatal(err)
	}

	copied, err := FromNet(buf)
	if err != nil {
		t.Fatal(err)
	}

	if !copied.Full() {
		t.Fatal("fullness attribute got dropped on marshal")
	}

	keys := make(map[cid.Cid]bool)
	for _, k := range copied.Wantlist() {
		keys[k.Cid] = true
	}

	for _, k := range original.Wantlist() {
		if _, ok := keys[k.Cid]; !ok {
			t.Fatalf("Key Missing: \"%v\"", k)
		}
	}
}

func TestToAndFromNetMessage(t *testing.T) {

	original := New(true)
	original.AddBlock(blocks.NewBlock([]byte("W")))
	original.AddBlock(blocks.NewBlock([]byte("E")))
	original.AddBlock(blocks.NewBlock([]byte("F")))
	original.AddBlock(blocks.NewBlock([]byte("M")))

	buf := new(bytes.Buffer)
	if err := original.ToNetV1(buf); err != nil {
		t.Fatal(err)
	}

	m2, err := FromNet(buf)
	if err != nil {
		t.Fatal(err)
	}

	keys := make(map[cid.Cid]bool)
	for _, b := range m2.Blocks() {
		keys[b.Cid()] = true
	}

	for _, b := range original.Blocks() {
		if _, ok := keys[b.Cid()]; !ok {
			t.Fail()
		}
	}
}

func wantlistContains(wantlist *pb.Message_Wantlist, c cid.Cid) bool {
	for _, e := range wantlist.GetEntries() {
		if e.Block.Cid.Defined() && c.Equals(e.Block.Cid) {
			return true
		}
	}
	return false
}

func contains(strs []string, x string) bool {
	for _, s := range strs {
		if s == x {
			return true
		}
	}
	return false
}

func TestDuplicates(t *testing.T) {
	b := blocks.NewBlock([]byte("foo"))
	msg := New(true)

	msg.AddEntry(b.Cid(), 1, pb.Message_Wantlist_Block, true)
	msg.AddEntry(b.Cid(), 1, pb.Message_Wantlist_Block, true)
	if len(msg.Wantlist()) != 1 {
		t.Fatal("Duplicate in BitSwapMessage")
	}

	msg.AddBlock(b)
	msg.AddBlock(b)
	if len(msg.Blocks()) != 1 {
		t.Fatal("Duplicate in BitSwapMessage")
	}

	b2 := blocks.NewBlock([]byte("bar"))
	msg.AddBlockPresence(b2.Cid(), pb.Message_Have)
	msg.AddBlockPresence(b2.Cid(), pb.Message_Have)
	if len(msg.Haves()) != 1 {
		t.Fatal("Duplicate in BitSwapMessage")
	}
}

func TestBlockPresences(t *testing.T) {
	b1 := blocks.NewBlock([]byte("foo"))
	b2 := blocks.NewBlock([]byte("bar"))
	msg := New(true)

	msg.AddBlockPresence(b1.Cid(), pb.Message_Have)
	msg.AddBlockPresence(b2.Cid(), pb.Message_DontHave)
	if len(msg.Haves()) != 1 || !msg.Haves()[0].Equals(b1.Cid()) {
		t.Fatal("Expected HAVE")
	}
	if len(msg.DontHaves()) != 1 || !msg.DontHaves()[0].Equals(b2.Cid()) {
		t.Fatal("Expected HAVE")
	}

	msg.AddBlock(b1)
	if len(msg.Haves()) != 0 {
		t.Fatal("Expected block to overwrite HAVE")
	}

	msg.AddBlock(b2)
	if len(msg.DontHaves()) != 0 {
		t.Fatal("Expected block to overwrite DONT_HAVE")
	}

	msg.AddBlockPresence(b1.Cid(), pb.Message_Have)
	if len(msg.Haves()) != 0 {
		t.Fatal("Expected HAVE not to overwrite block")
	}

	msg.AddBlockPresence(b2.Cid(), pb.Message_DontHave)
	if len(msg.DontHaves()) != 0 {
		t.Fatal("Expected DONT_HAVE not to overwrite block")
	}
}

func TestAddWantlistEntry(t *testing.T) {
	b := blocks.NewBlock([]byte("foo"))
	msg := New(true)

	msg.AddEntry(b.Cid(), 1, pb.Message_Wantlist_Have, false)
	msg.AddEntry(b.Cid(), 2, pb.Message_Wantlist_Block, true)
	entries := msg.Wantlist()
	if len(entries) != 1 {
		t.Fatal("Duplicate in BitSwapMessage")
	}
	e := entries[0]
	if e.WantType != pb.Message_Wantlist_Block {
		t.Fatal("want-block should override want-have")
	}
	if e.SendDontHave != true {
		t.Fatal("true SendDontHave should override false SendDontHave")
	}
	if e.Priority != 1 {
		t.Fatal("priority should only be overridden if wants are of same type")
	}

	msg.AddEntry(b.Cid(), 2, pb.Message_Wantlist_Block, true)
	e = msg.Wantlist()[0]
	if e.Priority != 2 {
		t.Fatal("priority should be overridden if wants are of same type")
	}

	msg.AddEntry(b.Cid(), 3, pb.Message_Wantlist_Have, false)
	e = msg.Wantlist()[0]
	if e.WantType != pb.Message_Wantlist_Block {
		t.Fatal("want-have should not override want-block")
	}
	if e.SendDontHave != true {
		t.Fatal("false SendDontHave should not override true SendDontHave")
	}
	if e.Priority != 2 {
		t.Fatal("priority should only be overridden if wants are of same type")
	}

	msg.Cancel(b.Cid())
	e = msg.Wantlist()[0]
	if !e.Cancel {
		t.Fatal("cancel should override want")
	}

	msg.AddEntry(b.Cid(), 10, pb.Message_Wantlist_Block, true)
	if !e.Cancel {
		t.Fatal("want should not override cancel")
	}
}

func TestEntrySize(t *testing.T) {
	blockGenerator := blocksutil.NewBlockGenerator()
	c := blockGenerator.Next().Cid()
	e := Entry{
		Entry: wantlist.Entry{
			Cid:      c,
			Priority: 10,
			WantType: pb.Message_Wantlist_Have,
		},
		SendDontHave: true,
		Cancel:       false,
	}
	epb := e.ToPB()
	if e.Size() != epb.Size() {
		t.Fatal("entry size calculation incorrect", e.Size(), epb.Size())
	}
}
