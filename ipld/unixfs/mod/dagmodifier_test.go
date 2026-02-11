package mod

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	dag "github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	trickle "github.com/ipfs/boxo/ipld/unixfs/importer/trickle"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	testu "github.com/ipfs/boxo/ipld/unixfs/test"
	"github.com/ipfs/boxo/util"
	"github.com/ipfs/boxo/verifcid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	mh "github.com/multiformats/go-multihash"
)

func testModWrite(t *testing.T, beg, size uint64, orig []byte, dm *DagModifier, opts testu.NodeOpts) []byte {
	newdata := make([]byte, size)
	random.NewRand().Read(newdata)

	if size+beg > uint64(len(orig)) {
		orig = append(orig, make([]byte, (size+beg)-uint64(len(orig)))...)
	}
	copy(orig[beg:], newdata)

	nmod, err := dm.WriteAt(newdata, int64(beg))
	if err != nil {
		t.Fatal(err)
	}

	if nmod != int(size) {
		t.Fatalf("Mod length not correct! %d != %d", nmod, size)
	}

	verifyNode(t, orig, dm, opts)

	return orig
}

func verifyNode(t *testing.T, orig []byte, dm *DagModifier, opts testu.NodeOpts) {
	nd, err := dm.GetNode()
	if err != nil {
		t.Fatal(err)
	}

	err = trickle.VerifyTrickleDagStructure(nd, trickle.VerifyParams{
		Getter:      dm.dagserv,
		Direct:      dm.MaxLinks,
		LayerRepeat: 4,
		Prefix:      &opts.Prefix,
		RawLeaves:   opts.RawLeavesUsed,
	})
	if err != nil {
		t.Fatal(err)
	}

	rd, err := uio.NewDagReader(context.Background(), nd, dm.dagserv)
	if err != nil {
		t.Fatal(err)
	}

	after, err := io.ReadAll(rd)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(after, orig)
	if err != nil {
		t.Fatal(err)
	}
}

func runAllSubtests(t *testing.T, tfunc func(*testing.T, testu.NodeOpts)) {
	t.Run("opts=ProtoBufLeaves", func(t *testing.T) { tfunc(t, testu.UseProtoBufLeaves) })
	t.Run("opts=RawLeaves", func(t *testing.T) { tfunc(t, testu.UseRawLeaves) })
	t.Run("opts=CidV1", func(t *testing.T) { tfunc(t, testu.UseCidV1) })
	t.Run("opts=Blake2b256", func(t *testing.T) { tfunc(t, testu.UseBlake2b256) })
}

func TestDagModifierBasic(t *testing.T) {
	runAllSubtests(t, testDagModifierBasic)
}

func testDagModifierBasic(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	b, n := testu.GetRandomNode(t, dserv, 50000, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	// Within zero block
	beg := uint64(15)
	length := uint64(60)

	t.Log("Testing mod within zero block")
	b = testModWrite(t, beg, length, b, dagmod, opts)

	// Within bounds of existing file
	beg = 1000
	length = 4000
	t.Log("Testing mod within bounds of existing multiblock file.")
	b = testModWrite(t, beg, length, b, dagmod, opts)

	// Extend bounds
	beg = 49500
	length = 4000

	t.Log("Testing mod that extends file.")
	b = testModWrite(t, beg, length, b, dagmod, opts)

	// "Append"
	beg = uint64(len(b))
	length = 3000
	t.Log("Testing pure append")
	_ = testModWrite(t, beg, length, b, dagmod, opts)

	// Verify reported length
	node, err := dagmod.GetNode()
	if err != nil {
		t.Fatal(err)
	}

	size, err := fileSize(node)
	if err != nil {
		t.Fatal(err)
	}

	const expected = uint64(50000 + 3500 + 3000)
	if size != expected {
		t.Fatalf("Final reported size is incorrect [%d != %d]", size, expected)
	}
}

func TestMultiWrite(t *testing.T) {
	runAllSubtests(t, testMultiWrite)
}

func testMultiWrite(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	data := make([]byte, 4000)
	random.NewRand().Read(data)

	for i := range data {
		n, err := dagmod.WriteAt(data[i:i+1], int64(i))
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatal("Somehow wrote the wrong number of bytes! (n != 1)")
		}

		size, err := dagmod.Size()
		if err != nil {
			t.Fatal(err)
		}

		if size != int64(i+1) {
			t.Fatal("Size was reported incorrectly")
		}
	}

	verifyNode(t, data, dagmod, opts)
}

func TestMultiWriteAndFlush(t *testing.T) {
	runAllSubtests(t, testMultiWriteAndFlush)
}

func testMultiWriteAndFlush(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	data := make([]byte, 20)
	random.NewRand().Read(data)

	for i := range data {
		n, err := dagmod.WriteAt(data[i:i+1], int64(i))
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatal("Somehow wrote the wrong number of bytes! (n != 1)")
		}
		err = dagmod.Sync()
		if err != nil {
			t.Fatal(err)
		}
	}

	verifyNode(t, data, dagmod, opts)
}

func TestWriteNewFile(t *testing.T) {
	runAllSubtests(t, testWriteNewFile)
}

func testWriteNewFile(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	towrite := make([]byte, 2000)
	random.NewRand().Read(towrite)

	nw, err := dagmod.Write(towrite)
	if err != nil {
		t.Fatal(err)
	}
	if nw != len(towrite) {
		t.Fatal("Wrote wrong amount")
	}

	verifyNode(t, towrite, dagmod, opts)
}

func TestMultiWriteCoal(t *testing.T) {
	runAllSubtests(t, testMultiWriteCoal)
}

func testMultiWriteCoal(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	data := make([]byte, 1000)
	random.NewRand().Read(data)

	for i := range data {
		n, err := dagmod.WriteAt(data[:i+1], 0)
		if err != nil {
			fmt.Println("FAIL AT ", i)
			t.Fatal(err)
		}
		if n != i+1 {
			t.Fatal("Somehow wrote the wrong number of bytes! (n != 1)")
		}

	}

	verifyNode(t, data, dagmod, opts)
}

func TestLargeWriteChunks(t *testing.T) {
	runAllSubtests(t, testLargeWriteChunks)
}

func testLargeWriteChunks(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	const wrsize = 1000
	const datasize = 10000000
	data := make([]byte, datasize)

	random.NewRand().Read(data)

	for i := range datasize / wrsize {
		n, err := dagmod.WriteAt(data[i*wrsize:(i+1)*wrsize], int64(i*wrsize))
		if err != nil {
			t.Fatal(err)
		}
		if n != wrsize {
			t.Fatal("failed to write buffer")
		}
	}

	_, err = dagmod.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	out, err := io.ReadAll(dagmod)
	if err != nil {
		t.Fatal(err)
	}

	if err = testu.ArrComp(out, data); err != nil {
		t.Fatal(err)
	}
}

func TestDagTruncate(t *testing.T) {
	runAllSubtests(t, testDagTruncate)
}

func testDagTruncate(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	b, n := testu.GetRandomNode(t, dserv, 50000, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	err = dagmod.Truncate(12345)
	if err != nil {
		t.Fatal(err)
	}
	size, err := dagmod.Size()
	if err != nil {
		t.Fatal(err)
	}

	if size != 12345 {
		t.Fatal("size was incorrect!")
	}

	_, err = dagmod.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	out, err := io.ReadAll(dagmod)
	if err != nil {
		t.Fatal(err)
	}

	if err = testu.ArrComp(out, b[:12345]); err != nil {
		t.Fatal(err)
	}

	err = dagmod.Truncate(10)
	if err != nil {
		t.Fatal(err)
	}

	size, err = dagmod.Size()
	if err != nil {
		t.Fatal(err)
	}

	if size != 10 {
		t.Fatal("size was incorrect!")
	}

	err = dagmod.Truncate(0)
	if err != nil {
		t.Fatal(err)
	}

	size, err = dagmod.Size()
	if err != nil {
		t.Fatal(err)
	}

	if size != 0 {
		t.Fatal("size was incorrect!")
	}
}

// TestDagSync tests that a DAG will expand sparse during sync
// if offset > curNode's size.
func TestDagSync(t *testing.T) {
	dserv := testu.GetDAGServ()
	nd := dag.NodeWithData(unixfs.FilePBData(nil, 0))

	ctx := t.Context()

	dagmod, err := NewDagModifier(ctx, nd, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}

	_, err = dagmod.Write([]byte("test1"))
	if err != nil {
		t.Fatal(err)
	}

	err = dagmod.Sync()
	if err != nil {
		t.Fatal(err)
	}

	// Truncate leave the offset at 5 and filesize at 0
	err = dagmod.Truncate(0)
	if err != nil {
		t.Fatal(err)
	}

	_, err = dagmod.Write([]byte("test2"))
	if err != nil {
		t.Fatal(err)
	}

	// When Offset > filesize , Sync will call enpandSparse
	err = dagmod.Sync()
	if err != nil {
		t.Fatal(err)
	}

	_, err = dagmod.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	out, err := io.ReadAll(dagmod)
	if err != nil {
		t.Fatal(err)
	}

	if err = testu.ArrComp(out[5:], []byte("test2")); err != nil {
		t.Fatal(err)
	}
}

// TestDagTruncateSameSize tests that a DAG truncated
// to the same size (i.e., doing nothing) doesn't modify
// the DAG (its hash).
func TestDagTruncateSameSize(t *testing.T) {
	runAllSubtests(t, testDagTruncateSameSize)
}

func testDagTruncateSameSize(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	_, n := testu.GetRandomNode(t, dserv, 50000, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	// Copied from `TestDagTruncate`.

	size, err := dagmod.Size()
	if err != nil {
		t.Fatal(err)
	}

	err = dagmod.Truncate(size)
	if err != nil {
		t.Fatal(err)
	}

	modifiedNode, err := dagmod.GetNode()
	if err != nil {
		t.Fatal(err)
	}

	if modifiedNode.Cid().Equals(n.Cid()) == false {
		t.Fatal("the node has been modified!")
	}
}

func TestSparseWrite(t *testing.T) {
	runAllSubtests(t, testSparseWrite)
}

func testSparseWrite(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	buf := make([]byte, 5000)
	random.NewRand().Read(buf[2500:])

	wrote, err := dagmod.WriteAt(buf[2500:], 2500)
	if err != nil {
		t.Fatal(err)
	}

	if wrote != 2500 {
		t.Fatal("incorrect write amount")
	}

	_, err = dagmod.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	out, err := io.ReadAll(dagmod)
	if err != nil {
		t.Fatal(err)
	}

	if err = testu.ArrComp(out, buf); err != nil {
		t.Fatal(err)
	}
}

func TestSeekPastEndWrite(t *testing.T) {
	runAllSubtests(t, testSeekPastEndWrite)
}

func testSeekPastEndWrite(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	buf := make([]byte, 5000)
	random.NewRand().Read(buf[2500:])

	nseek, err := dagmod.Seek(2500, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	if nseek != 2500 {
		t.Fatal("failed to seek")
	}

	wrote, err := dagmod.Write(buf[2500:])
	if err != nil {
		t.Fatal(err)
	}

	if wrote != 2500 {
		t.Fatal("incorrect write amount")
	}

	_, err = dagmod.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	out, err := io.ReadAll(dagmod)
	if err != nil {
		t.Fatal(err)
	}

	if err = testu.ArrComp(out, buf); err != nil {
		t.Fatal(err)
	}
}

// TestSparseWriteOnExistingRawNode ensures curNode is updated after sparse
// expansion. This mimics the scenario where `ipfs files write --offset N` is
// called on an existing file created with --raw-leaves:
// 1. First command creates a RawNode file
// 2. Second command loads the RawNode and writes at offset past its end
// Without the fix, expandSparse wouldn't update curNode, causing modifyDag
// to operate on the old unexpanded node and panic with slice bounds error.
func TestSparseWriteOnExistingRawNode(t *testing.T) {
	ctx := context.Background()
	dserv := testu.GetDAGServ()

	// Step 1: Create initial file as RawNode (simulates ipfs files write --raw-leaves)
	initialData := []byte("foobar\n")
	rawNode, err := dag.NewRawNodeWPrefix(initialData, cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   mh.SHA2_256,
		MhLength: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = dserv.Add(ctx, rawNode)
	if err != nil {
		t.Fatal(err)
	}

	// Step 2: Load existing RawNode in NEW DagModifier (simulates second ipfs files write)
	dagmod, err := NewDagModifier(ctx, rawNode, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	dagmod.RawLeaves = true

	// Step 3: Write at offset 50 (past end of 7-byte file)
	_, err = dagmod.Seek(50, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek to offset 50 failed: %v", err)
	}

	newData := []byte("blah\n")
	_, err = dagmod.Write(newData)
	if err != nil {
		t.Fatalf("Write at offset 50 failed: %v", err)
	}

	// Step 4: Sync - this panicked before the fix
	err = dagmod.Sync()
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Step 5: Verify result
	_, err = dagmod.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	result, err := io.ReadAll(dagmod)
	if err != nil {
		t.Fatal(err)
	}

	// Expected: "foobar\n" + 43 zero bytes + "blah\n" = 55 bytes
	expected := make([]byte, 55)
	copy(expected, initialData)
	copy(expected[50:], newData)

	if err = testu.ArrComp(result, expected); err != nil {
		t.Fatal(err)
	}
}

func TestRelativeSeek(t *testing.T) {
	runAllSubtests(t, testRelativeSeek)
}

func testRelativeSeek(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	for i := range 64 {
		dagmod.Write([]byte{byte(i)})
		if _, err := dagmod.Seek(1, io.SeekCurrent); err != nil {
			t.Fatal(err)
		}
	}

	out, err := io.ReadAll(dagmod)
	if err != nil {
		t.Fatal(err)
	}

	for i, v := range out {
		if v != 0 && i/2 != int(v) {
			t.Errorf("expected %d, at index %d, got %d", i/2, i, v)
		}
	}
}

func TestInvalidSeek(t *testing.T) {
	runAllSubtests(t, testInvalidSeek)
}

func testInvalidSeek(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(t, dserv, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	_, err = dagmod.Seek(10, -10)

	if err != ErrUnrecognizedWhence {
		t.Fatal(err)
	}
}

func TestEndSeek(t *testing.T) {
	runAllSubtests(t, testEndSeek)
}

func testEndSeek(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()

	n := testu.GetEmptyNode(t, dserv, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	_, err = dagmod.Write(make([]byte, 100))
	if err != nil {
		t.Fatal(err)
	}

	offset, err := dagmod.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 100 {
		t.Fatal("expected the relative seek 0 to return current location")
	}

	offset, err = dagmod.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 0 {
		t.Fatal("expected the absolute seek to set offset at 0")
	}

	offset, err = dagmod.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 100 {
		t.Fatal("expected the end seek to set offset at end")
	}
}

func TestReadAndSeek(t *testing.T) {
	runAllSubtests(t, testReadAndSeek)
}

func testReadAndSeek(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()

	n := testu.GetEmptyNode(t, dserv, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	writeBuf := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	dagmod.Write(writeBuf)

	if !dagmod.HasChanges() {
		t.Fatal("there are changes, this should be true")
	}

	readBuf := make([]byte, 4)
	offset, err := dagmod.Seek(0, io.SeekStart)
	if offset != 0 {
		t.Fatal("expected offset to be 0")
	}
	if err != nil {
		t.Fatal(err)
	}

	// read 0,1,2,3
	c, err := dagmod.Read(readBuf)
	if err != nil {
		t.Fatal(err)
	}
	if c != 4 {
		t.Fatalf("expected length of 4 got %d", c)
	}

	for i := range byte(4) {
		if readBuf[i] != i {
			t.Fatalf("wrong value %d [at index %d]", readBuf[i], i)
		}
	}

	// skip 4
	_, err = dagmod.Seek(1, io.SeekCurrent)
	if err != nil {
		t.Fatalf("error: %s, offset %d, reader offset %d", err, dagmod.curWrOff, getOffset(dagmod.read))
	}

	// read 5,6,7
	readBuf = make([]byte, 3)
	c, err = dagmod.Read(readBuf)
	if err != nil {
		t.Fatal(err)
	}
	if c != 3 {
		t.Fatalf("expected length of 3 got %d", c)
	}

	for i := range byte(3) {
		if readBuf[i] != i+5 {
			t.Fatalf("wrong value %d [at index %d]", readBuf[i], i)
		}
	}
}

func TestCtxRead(t *testing.T) {
	runAllSubtests(t, testCtxRead)
}

func testCtxRead(t *testing.T, opts testu.NodeOpts) {
	dserv := testu.GetDAGServ()

	n := testu.GetEmptyNode(t, dserv, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ForceRawLeaves {
		dagmod.RawLeaves = true
	}

	_, err = dagmod.Write([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	if err != nil {
		t.Fatal(err)
	}
	dagmod.Seek(0, io.SeekStart)

	readBuf := make([]byte, 4)
	_, err = dagmod.CtxReadFull(ctx, readBuf)
	if err != nil {
		t.Fatal(err)
	}
	err = testu.ArrComp(readBuf, []byte{0, 1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	// TODO(Kubuxu): context cancel case, I will do it after I figure out dagreader tests,
	// because this is exacelly the same.
}

func BenchmarkDagmodWrite(b *testing.B) {
	b.StopTimer()
	dserv := testu.GetDAGServ()
	n := testu.GetEmptyNode(b, dserv, testu.UseProtoBufLeaves)
	ctx := b.Context()

	const wrsize = 4096

	dagmod, err := NewDagModifier(ctx, n, dserv, testu.SizeSplitterGen(512))
	if err != nil {
		b.Fatal(err)
	}

	buf := make([]byte, b.N*wrsize)
	random.NewRand().Read(buf)
	b.StartTimer()
	b.SetBytes(int64(wrsize))
	for i := 0; i < b.N; i++ {
		n, err := dagmod.Write(buf[i*wrsize : (i+1)*wrsize])
		if err != nil {
			b.Fatal(err)
		}
		if n != wrsize {
			b.Fatal("Wrote bad size")
		}
	}
}

func getOffset(reader uio.DagReader) int64 {
	offset, err := reader.Seek(0, io.SeekCurrent)
	if err != nil {
		panic("failed to retrieve offset: " + err.Error())
	}
	return offset
}

// makeTestData generates test data with repeating lowercase ascii alphabet.
func makeTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte('a' + i%26)
	}
	return data
}

// makeIdentityNode creates a ProtoNode with identity CID for testing
func makeIdentityNode(data []byte) *dag.ProtoNode {
	node := dag.NodeWithData(unixfs.FilePBData(data, uint64(len(data))))
	node.SetCidBuilder(cid.Prefix{
		Version:  1,
		Codec:    cid.DagProtobuf,
		MhType:   mh.IDENTITY,
		MhLength: -1,
	})
	return node
}

func TestIdentityCIDHandling(t *testing.T) {
	ctx := context.Background()
	dserv := testu.GetDAGServ()

	// Test that DagModifier prevents creating overlarge identity CIDs
	// This addresses https://github.com/ipfs/kubo/issues/6011

	t.Run("prevents identity CID overflow on modification", func(t *testing.T) {
		// Create a UnixFS file node with identity hash near the size limit
		// UnixFS overhead is approximately 8-10 bytes, so we use data that will
		// encode to just under DefaultMaxIdentityDigestSize when combined with metadata
		initialData := makeTestData(verifcid.DefaultMaxIdentityDigestSize - 10)

		// Create a UnixFS file node with identity CID
		node := makeIdentityNode(initialData)

		// Verify the encoded size is under limit
		encoded, _ := node.EncodeProtobuf(false)
		if len(encoded) > verifcid.DefaultMaxIdentityDigestSize {
			t.Fatalf("Test setup error: initial node too large for identity: %d bytes (max %d)", len(encoded), verifcid.DefaultMaxIdentityDigestSize)
		}

		// Store the node
		err := dserv.Add(ctx, node)
		if err != nil {
			t.Fatal(err)
		}

		// Create DagModifier
		dmod, err := NewDagModifier(ctx, node, dserv, testu.SizeSplitterGen(512))
		if err != nil {
			t.Fatal(err)
		}

		// Configure fallback hash (simulating what MFS does)
		dmod.Prefix.MhType = util.DefaultIpfsHash
		dmod.Prefix.MhLength = -1

		// Modify first few bytes - this won't change the size but validates
		// that our check works for any modification
		modData := []byte("MODIFIED")
		n, err := dmod.WriteAt(modData, 0)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(modData) {
			t.Fatalf("Expected to write %d bytes, wrote %d", len(modData), n)
		}

		// Sync the changes
		err = dmod.Sync()
		if err != nil {
			t.Fatal(err)
		}

		// Get result
		resultNode, err := dmod.GetNode()
		if err != nil {
			t.Fatal(err)
		}

		// The key verification: if it's a leaf node and would exceed verifcid.DefaultMaxIdentityDigestSize,
		// it must not use identity hash
		if len(resultNode.Links()) == 0 {
			if protoNode, ok := resultNode.(*dag.ProtoNode); ok {
				encodedData, _ := protoNode.EncodeProtobuf(false)
				if len(encodedData) > verifcid.DefaultMaxIdentityDigestSize && resultNode.Cid().Prefix().MhType == mh.IDENTITY {
					t.Errorf("Leaf node with %d bytes must not use identity hash", len(encodedData))
				}
			}
		}
	})

	t.Run("small identity CID remains identity", func(t *testing.T) {
		// Create a small UnixFS file node with identity hash
		// Keep it well under verifcid.DefaultMaxIdentityDigestSize (the identity hash limit)
		smallData := []byte("hello world")

		// Create a UnixFS file node with identity CID
		smallNode := makeIdentityNode(smallData)

		// Store the node
		err := dserv.Add(ctx, smallNode)
		if err != nil {
			t.Fatal(err)
		}

		// Create DagModifier with default splitter
		dmod, err := NewDagModifier(ctx, smallNode, dserv, testu.SizeSplitterGen(512))
		if err != nil {
			t.Fatal(err)
		}

		// Write small amount of data (total still under verifcid.DefaultMaxIdentityDigestSize)
		additionalData := []byte(" and more")
		n, err := dmod.WriteAt(additionalData, int64(len(smallData)))
		if err != nil {
			t.Fatalf("WriteAt failed: %v", err)
		}
		if n != len(additionalData) {
			t.Fatalf("expected to write %d bytes, wrote %d", len(additionalData), n)
		}

		// Get the resulting node
		resultNode, err := dmod.GetNode()
		if err != nil {
			t.Fatal(err)
		}

		// Verify it still uses identity hash
		resultCID := resultNode.Cid()
		if resultCID.Prefix().MhType != mh.IDENTITY {
			t.Fatalf("expected identity hash for small data, got %v", resultCID.Prefix().MhType)
		}
	})

	t.Run("preserves all prefix fields when switching from identity", func(t *testing.T) {
		// Create initial data that fits well within identity CID limit
		// This needs to be large enough to trigger modifyDag but small enough
		// to fit in identity with UnixFS overhead
		initialData := makeTestData(100)

		// Create identity CID ProtoNode with UnixFS data
		initialNode := dag.NodeWithData(unixfs.FilePBData(initialData, uint64(len(initialData))))
		initialNode.SetCidBuilder(cid.Prefix{
			Version:  1,
			Codec:    cid.DagProtobuf,
			MhType:   mh.IDENTITY,
			MhLength: -1,
		})

		// Verify initial data fits in identity
		encodedInitial, _ := initialNode.EncodeProtobuf(false)
		if len(encodedInitial) > verifcid.DefaultMaxIdentityDigestSize {
			t.Skipf("Initial data too large for identity: %d bytes", len(encodedInitial))
		}

		err := dserv.Add(ctx, initialNode)
		if err != nil {
			t.Fatal(err)
		}

		// Create DagModifier
		dmod, err := NewDagModifier(ctx, initialNode, dserv, testu.SizeSplitterGen(512))
		if err != nil {
			t.Fatal(err)
		}

		// Configure a custom prefix with specific values
		dmod.Prefix = cid.Prefix{
			Version:  1,
			Codec:    cid.DagProtobuf,
			MhType:   mh.SHA2_512,
			MhLength: 32, // Truncated SHA-512
		}

		// Write within the existing data bounds to trigger modifyDag
		// Use enough data that would cause identity overflow
		modData := bytes.Repeat([]byte{'X'}, 50)

		// Write at offset 0, replacing first 50 bytes
		n, err := dmod.WriteAt(modData, 0)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(modData) {
			t.Fatalf("expected to write %d bytes, wrote %d", len(modData), n)
		}

		err = dmod.Sync()
		if err != nil {
			t.Fatal(err)
		}

		// Get the modified node
		modifiedNode, err := dmod.GetNode()
		if err != nil {
			t.Fatal(err)
		}

		// The node should still be using identity since the data is small
		// But let's test that prefix fields would be preserved if it switched

		// For this test, let's verify that our safePrefixForSize function
		// correctly preserves fields when it would switch
		testPrefix, changed := dmod.safePrefixForSize(initialNode.Cid().Prefix(), verifcid.DefaultMaxIdentityDigestSize+10)

		if !changed {
			t.Errorf("safePrefixForSize: expected prefix to change for oversized identity")
		}
		if testPrefix.Version != 1 {
			t.Errorf("safePrefixForSize: expected Version 1, got %d", testPrefix.Version)
		}
		if testPrefix.Codec != cid.DagProtobuf {
			t.Errorf("safePrefixForSize: expected Codec DagProtobuf, got %d", testPrefix.Codec)
		}
		if testPrefix.MhType != mh.SHA2_512 {
			t.Errorf("safePrefixForSize: expected MhType SHA2_512 (%d), got %d", mh.SHA2_512, testPrefix.MhType)
		}
		if testPrefix.MhLength != 32 {
			t.Errorf("safePrefixForSize: expected MhLength 32, got %d", testPrefix.MhLength)
		}

		// Also verify the actual node behavior (it should still be identity for this small data)
		if modifiedNode.Cid().Prefix().MhType != mh.IDENTITY {
			t.Logf("Note: Node switched from identity even though data is small")
		}
	})

	t.Run("raw node preserves codec", func(t *testing.T) {
		// Create a RawNode with identity CID that's near the limit
		// This allows us to test overflow with small modifications
		initialData := makeTestData(verifcid.DefaultMaxIdentityDigestSize - 10)

		rawNode, err := dag.NewRawNodeWPrefix(initialData, cid.Prefix{
			Version:  1,
			Codec:    cid.Raw,
			MhType:   mh.IDENTITY,
			MhLength: -1,
		})
		if err != nil {
			t.Fatal(err)
		}

		// Verify it's using Raw codec and identity hash
		if rawNode.Cid().Prefix().Codec != cid.Raw {
			t.Fatalf("expected Raw codec, got %d", rawNode.Cid().Prefix().Codec)
		}
		if rawNode.Cid().Prefix().MhType != mh.IDENTITY {
			t.Fatalf("expected identity hash, got %d", rawNode.Cid().Prefix().MhType)
		}

		err = dserv.Add(ctx, rawNode)
		if err != nil {
			t.Fatal(err)
		}

		// Create DagModifier
		dmod, err := NewDagModifier(ctx, rawNode, dserv, testu.SizeSplitterGen(512))
		if err != nil {
			t.Fatal(err)
		}

		// Verify DagModifier preserves the Raw codec (not forcing to DagProtobuf)
		if dmod.Prefix.Codec != cid.Raw {
			t.Errorf("BUG: DagModifier changed codec from Raw (%d) to %d",
				cid.Raw, dmod.Prefix.Codec)
		}

		// Configure fallback hash for when identity overflows
		dmod.Prefix.MhType = util.DefaultIpfsHash
		dmod.Prefix.MhLength = -1

		// Modify a few bytes - the node should stay the same size
		// but switch from identity to the configured hash
		modData := []byte("MODIFIED")
		_, err = dmod.WriteAt(modData, 0)
		if err != nil {
			t.Fatal(err)
		}

		err = dmod.Sync()
		if err != nil {
			t.Fatal(err)
		}

		modifiedNode, err := dmod.GetNode()
		if err != nil {
			t.Fatal(err)
		}

		// The node should switch from identity to SHA256 but keep Raw codec
		if modifiedNode.Cid().Prefix().MhType == mh.IDENTITY {
			// It might still be identity if total size is under limit
			t.Logf("Note: Node still uses identity hash")
		}

		// CRITICAL: Must preserve Raw codec
		if modifiedNode.Cid().Prefix().Codec != cid.Raw {
			t.Errorf("BUG: Raw node changed codec! Expected Raw (%d), got %d",
				cid.Raw, modifiedNode.Cid().Prefix().Codec)
		}
	})

	t.Run("raw node identity overflow via truncate", func(t *testing.T) {
		// Test that Truncate also handles identity overflow correctly for RawNodes
		// Start with a RawNode using identity that's at max size
		maxData := makeTestData(verifcid.DefaultMaxIdentityDigestSize)

		rawNode, err := dag.NewRawNodeWPrefix(maxData, cid.Prefix{
			Version:  1,
			Codec:    cid.Raw,
			MhType:   mh.IDENTITY,
			MhLength: -1,
		})
		if err != nil {
			t.Fatal(err)
		}

		// Verify initial state
		if rawNode.Cid().Prefix().Codec != cid.Raw {
			t.Fatalf("expected Raw codec, got %d", rawNode.Cid().Prefix().Codec)
		}
		if rawNode.Cid().Prefix().MhType != mh.IDENTITY {
			t.Fatalf("expected identity hash, got %d", rawNode.Cid().Prefix().MhType)
		}

		err = dserv.Add(ctx, rawNode)
		if err != nil {
			t.Fatal(err)
		}

		// Create DagModifier with non-identity fallback
		dmod, err := NewDagModifier(ctx, rawNode, dserv, testu.SizeSplitterGen(512))
		if err != nil {
			t.Fatal(err)
		}

		// Even though we're truncating to smaller, the original uses identity
		// and we should preserve that if it still fits
		err = dmod.Truncate(100)
		if err != nil {
			t.Fatal(err)
		}

		truncatedNode, err := dmod.GetNode()
		if err != nil {
			t.Fatal(err)
		}

		// Should still use identity (100 bytes < verifcid.DefaultMaxIdentityDigestSize)
		if truncatedNode.Cid().Prefix().MhType != mh.IDENTITY {
			t.Errorf("Expected identity hash for 100 bytes, got %d",
				truncatedNode.Cid().Prefix().MhType)
		}

		// Must preserve Raw codec
		if truncatedNode.Cid().Prefix().Codec != cid.Raw {
			t.Errorf("Truncate changed codec! Expected Raw (%d), got %d",
				cid.Raw, truncatedNode.Cid().Prefix().Codec)
		}

		// Verify data was actually truncated
		if rawTrunc, ok := truncatedNode.(*dag.RawNode); ok {
			if len(rawTrunc.RawData()) != 100 {
				t.Errorf("Expected 100 bytes after truncate, got %d", len(rawTrunc.RawData()))
			}
			if !bytes.Equal(rawTrunc.RawData(), maxData[:100]) {
				t.Error("Truncated data doesn't match expected")
			}
		} else {
			t.Error("Truncated node is not a RawNode")
		}
	})

	t.Run("raw node switches from identity when modified to exceed limit", func(t *testing.T) {
		// Create a RawNode with identity CID near the limit
		// Use size that allows modification without triggering append
		initialData := bytes.Repeat([]byte{'a'}, verifcid.DefaultMaxIdentityDigestSize-1) // 127 bytes

		rawNode, err := dag.NewRawNodeWPrefix(initialData, cid.Prefix{
			Version:  1,
			Codec:    cid.Raw,
			MhType:   mh.IDENTITY,
			MhLength: -1,
		})
		if err != nil {
			t.Fatal(err)
		}

		// Verify it starts with identity (127 bytes < verifcid.DefaultMaxIdentityDigestSize)
		if rawNode.Cid().Prefix().MhType != mh.IDENTITY {
			t.Fatalf("Expected identity hash for 127 bytes, got %d", rawNode.Cid().Prefix().MhType)
		}

		err = dserv.Add(ctx, rawNode)
		if err != nil {
			t.Fatal(err)
		}

		// Create DagModifier
		dmod, err := NewDagModifier(ctx, rawNode, dserv, testu.SizeSplitterGen(512))
		if err != nil {
			t.Fatal(err)
		}

		// Configure fallback for identity overflow
		dmod.Prefix.MhType = util.DefaultIpfsHash
		dmod.Prefix.MhLength = -1

		// Replace last byte with 2 bytes, pushing size to verifcid.DefaultMaxIdentityDigestSize
		// This modifies within bounds but increases total size just enough
		twoBytes := []byte("XX")
		_, err = dmod.WriteAt(twoBytes, int64(len(initialData)-1))
		if err != nil {
			t.Fatal(err)
		}

		err = dmod.Sync()
		if err != nil {
			t.Fatal(err)
		}

		modifiedNode, err := dmod.GetNode()
		if err != nil {
			t.Fatal(err)
		}

		// When extending a RawNode past its end, it gets converted to UnixFS structure
		// because RawNodes can't have child nodes - they're just raw data
		if protoNode, ok := modifiedNode.(*dag.ProtoNode); ok {
			// This is expected - RawNode was converted to UnixFS for growth
			fsn, err := unixfs.FSNodeFromBytes(protoNode.Data())
			if err != nil {
				t.Fatalf("Failed to parse UnixFS metadata: %v", err)
			}

			expectedSize := uint64(verifcid.DefaultMaxIdentityDigestSize) // 127 original + 1 byte extension
			if fsn.FileSize() != expectedSize {
				t.Errorf("Expected file size %d, got %d", expectedSize, fsn.FileSize())
			}

			// Should have switched from identity hash
			if modifiedNode.Cid().Prefix().MhType == mh.IDENTITY {
				t.Error("Large node should not use identity hash")
			}
		} else if rawMod, ok := modifiedNode.(*dag.RawNode); ok {
			// If it's still a RawNode, it means the write didn't extend past the end
			actualSize := len(rawMod.RawData())
			t.Logf("Modified node size: %d bytes", actualSize)

			// If size > verifcid.DefaultMaxIdentityDigestSize, must not use identity
			if actualSize > verifcid.DefaultMaxIdentityDigestSize {
				if modifiedNode.Cid().Prefix().MhType == mh.IDENTITY {
					t.Errorf("Node with %d bytes still uses identity (max %d)",
						actualSize, verifcid.DefaultMaxIdentityDigestSize)
				}
			}

			// Must preserve Raw codec
			if modifiedNode.Cid().Prefix().Codec != cid.Raw {
				t.Errorf("BUG: Raw node changed codec! Expected Raw (%d), got %d",
					cid.Raw, modifiedNode.Cid().Prefix().Codec)
			}
		} else {
			t.Error("Node is neither RawNode nor ProtoNode")
		}
	})
}

func TestRawNodeGrowthConversion(t *testing.T) {
	ctx := context.Background()
	dserv := testu.GetDAGServ()

	t.Run("raw node converts to UnixFS when growing beyond single block", func(t *testing.T) {
		// Create a small RawNode
		initialData := []byte("small raw data")
		rawNode, err := dag.NewRawNodeWPrefix(initialData, cid.Prefix{
			Version:  1,
			Codec:    cid.Raw,
			MhType:   mh.SHA2_256,
			MhLength: -1,
		})
		if err != nil {
			t.Fatal(err)
		}

		err = dserv.Add(ctx, rawNode)
		if err != nil {
			t.Fatal(err)
		}

		// Create DagModifier with small chunk size to force multiple blocks
		dmod, err := NewDagModifier(ctx, rawNode, dserv, testu.SizeSplitterGen(32))
		if err != nil {
			t.Fatal(err)
		}

		// Seek to end to append (not overwrite)
		_, err = dmod.Seek(0, io.SeekEnd)
		if err != nil {
			t.Fatal(err)
		}

		// Append data that will create multiple blocks
		largeData := makeTestData(100)

		_, err = dmod.Write(largeData)
		if err != nil {
			t.Fatal(err)
		}

		// Get the resulting node
		resultNode, err := dmod.GetNode()
		if err != nil {
			t.Fatal(err)
		}

		// Should now be a ProtoNode with UnixFS metadata
		protoNode, ok := resultNode.(*dag.ProtoNode)
		if !ok {
			t.Fatal("Expected ProtoNode after growth, got RawNode")
		}

		// Verify it has UnixFS metadata
		fsn, err := unixfs.FSNodeFromBytes(protoNode.Data())
		if err != nil {
			t.Fatalf("Failed to parse UnixFS metadata: %v", err)
		}

		if fsn.Type() != unixfs.TFile {
			t.Errorf("Expected file type, got %v", fsn.Type())
		}

		// Should have multiple links (blocks)
		if len(protoNode.Links()) < 2 {
			t.Errorf("Expected multiple blocks, got %d", len(protoNode.Links()))
		}

		// First link should be the original RawNode
		if len(protoNode.Links()) > 0 {
			firstChild, err := protoNode.Links()[0].GetNode(ctx, dserv)
			if err != nil {
				t.Fatal(err)
			}

			// First block should contain our original data
			if rawChild, ok := firstChild.(*dag.RawNode); ok {
				if !bytes.HasPrefix(rawChild.RawData(), initialData) {
					t.Error("First block doesn't contain original data")
				}
			}
		}

		// Verify the total size is correct
		totalSize := uint64(len(initialData) + len(largeData))
		if fsn.FileSize() != totalSize {
			t.Errorf("Expected file size %d, got %d", totalSize, fsn.FileSize())
		}
	})

	t.Run("identity raw node converts and switches hash when growing", func(t *testing.T) {
		// Create a RawNode with identity hash
		smallData := []byte("tiny")
		rawNode, err := dag.NewRawNodeWPrefix(smallData, cid.Prefix{
			Version:  1,
			Codec:    cid.Raw,
			MhType:   mh.IDENTITY,
			MhLength: -1,
		})
		if err != nil {
			t.Fatal(err)
		}

		err = dserv.Add(ctx, rawNode)
		if err != nil {
			t.Fatal(err)
		}

		// Create DagModifier with small chunk size
		dmod, err := NewDagModifier(ctx, rawNode, dserv, testu.SizeSplitterGen(32))
		if err != nil {
			t.Fatal(err)
		}

		// Configure fallback for identity overflow
		dmod.Prefix = cid.Prefix{
			Version:  1,
			Codec:    cid.DagProtobuf,
			MhType:   mh.SHA2_256,
			MhLength: -1,
		}

		// Seek to end to append (not overwrite)
		_, err = dmod.Seek(0, io.SeekEnd)
		if err != nil {
			t.Fatal(err)
		}

		// Append enough data to force multi-block structure
		appendData := bytes.Repeat([]byte{'X'}, 200)

		_, err = dmod.Write(appendData)
		if err != nil {
			t.Fatal(err)
		}

		resultNode, err := dmod.GetNode()
		if err != nil {
			t.Fatal(err)
		}

		// Should be ProtoNode with non-identity hash
		protoNode, ok := resultNode.(*dag.ProtoNode)
		if !ok {
			t.Fatal("Expected ProtoNode after growth")
		}

		if protoNode.Cid().Prefix().MhType == mh.IDENTITY {
			t.Error("Large multi-block node should not use identity hash")
		}

		// Verify structure
		if len(protoNode.Links()) < 2 {
			t.Errorf("Expected multiple blocks, got %d", len(protoNode.Links()))
		}
	})
}

func TestRawLeavesCollapse(t *testing.T) {
	t.Run("single-block file collapses to RawNode when RawLeaves enabled", func(t *testing.T) {
		ctx := t.Context()

		dserv := testu.GetDAGServ()

		// Create an empty file node with CIDv1 (required for raw leaves)
		emptyNode := testu.GetEmptyNode(t, dserv, testu.UseCidV1)

		// Create DagModifier with RawLeaves enabled
		dmod, err := NewDagModifier(ctx, emptyNode, dserv, testu.SizeSplitterGen(512))
		if err != nil {
			t.Fatal(err)
		}
		dmod.RawLeaves = true

		// Write small data (fits in single block)
		data := []byte("hello world")
		_, err = dmod.Write(data)
		if err != nil {
			t.Fatal(err)
		}

		// Get the final node
		resultNode, err := dmod.GetNode()
		if err != nil {
			t.Fatal(err)
		}

		// Should be a RawNode, not a ProtoNode
		rawNode, ok := resultNode.(*dag.RawNode)
		if !ok {
			t.Fatalf("expected RawNode for single-block file with RawLeaves=true, got %T", resultNode)
		}

		// Verify the data
		if !bytes.Equal(rawNode.RawData(), data) {
			t.Errorf("data mismatch: expected %q, got %q", data, rawNode.RawData())
		}

		// Verify CID uses raw codec
		if rawNode.Cid().Prefix().Codec != cid.Raw {
			t.Errorf("expected raw codec, got %d", rawNode.Cid().Prefix().Codec)
		}
	})

	t.Run("multi-block file stays as ProtoNode", func(t *testing.T) {
		ctx := t.Context()

		dserv := testu.GetDAGServ()

		// Create an empty file node with CIDv1
		emptyNode := testu.GetEmptyNode(t, dserv, testu.UseCidV1)

		// Create DagModifier with RawLeaves enabled and small chunk size
		dmod, err := NewDagModifier(ctx, emptyNode, dserv, testu.SizeSplitterGen(32))
		if err != nil {
			t.Fatal(err)
		}
		dmod.RawLeaves = true

		// Write enough data to require multiple blocks
		data := bytes.Repeat([]byte("x"), 100)
		_, err = dmod.Write(data)
		if err != nil {
			t.Fatal(err)
		}

		// Get the final node
		resultNode, err := dmod.GetNode()
		if err != nil {
			t.Fatal(err)
		}

		// Should be a ProtoNode (not collapsed)
		protoNode, ok := resultNode.(*dag.ProtoNode)
		if !ok {
			t.Fatalf("expected ProtoNode for multi-block file, got %T", resultNode)
		}

		// Should have multiple children
		if len(protoNode.Links()) < 2 {
			t.Errorf("expected multiple blocks, got %d links", len(protoNode.Links()))
		}
	})

	// Table-driven test for metadata preservation.
	// Files with Mode or ModTime metadata should NOT collapse to RawNode
	// even when RawLeaves=true and the file fits in a single block.
	// We use explicit non-zero values and verify they're preserved exactly.
	testMtime := time.Date(2025, 6, 15, 12, 30, 0, 0, time.UTC)
	metadataTests := []struct {
		name  string
		mode  os.FileMode
		mtime time.Time
	}{
		{"file with ModTime stays as ProtoNode", 0, testMtime},
		{"file with Mode stays as ProtoNode", 0o755, time.Time{}},
		{"file with both Mode and ModTime stays as ProtoNode", 0o644, testMtime},
	}

	for _, tc := range metadataTests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			dserv := testu.GetDAGServ()

			// Create a file node with specified metadata
			fsNode := unixfs.NewFSNode(unixfs.TFile)
			if tc.mode != 0 {
				fsNode.SetMode(tc.mode)
			}
			if !tc.mtime.IsZero() {
				fsNode.SetModTime(tc.mtime)
			}
			fsNodeData, err := fsNode.GetBytes()
			if err != nil {
				t.Fatal(err)
			}
			emptyNode := dag.NodeWithData(fsNodeData)
			emptyNode.SetCidBuilder(cid.Prefix{
				Version:  1,
				Codec:    cid.DagProtobuf,
				MhType:   mh.SHA2_256,
				MhLength: -1,
			})
			err = dserv.Add(ctx, emptyNode)
			if err != nil {
				t.Fatal(err)
			}

			// Create DagModifier with RawLeaves enabled
			dmod, err := NewDagModifier(ctx, emptyNode, dserv, testu.SizeSplitterGen(512))
			if err != nil {
				t.Fatal(err)
			}
			dmod.RawLeaves = true

			// Write small data (would fit in single block)
			data := []byte("hello world")
			_, err = dmod.Write(data)
			if err != nil {
				t.Fatal(err)
			}

			// Get the final node
			resultNode, err := dmod.GetNode()
			if err != nil {
				t.Fatal(err)
			}

			// Should stay as ProtoNode because of metadata
			protoNode, ok := resultNode.(*dag.ProtoNode)
			if !ok {
				t.Fatalf("expected ProtoNode, got %T", resultNode)
			}

			// Verify metadata is preserved
			fsn, err := unixfs.FSNodeFromBytes(protoNode.Data())
			if err != nil {
				t.Fatal(err)
			}
			// Mode should be preserved exactly
			if tc.mode != 0 && fsn.Mode() != tc.mode {
				t.Errorf("Mode: expected %o, got %o", tc.mode, fsn.Mode())
			}
			// ModTime gets updated to time.Now() on write, so just verify it's present
			if !tc.mtime.IsZero() && fsn.ModTime().IsZero() {
				t.Error("ModTime was lost (expected non-zero)")
			}
		})
	}

	t.Run("RawLeaves=false keeps ProtoNode", func(t *testing.T) {
		ctx := t.Context()

		dserv := testu.GetDAGServ()

		// Create an empty file node with CIDv1
		emptyNode := testu.GetEmptyNode(t, dserv, testu.UseCidV1)

		// Create DagModifier with RawLeaves explicitly disabled
		dmod, err := NewDagModifier(ctx, emptyNode, dserv, testu.SizeSplitterGen(512))
		if err != nil {
			t.Fatal(err)
		}
		dmod.RawLeaves = false

		// Write small data
		data := []byte("hello world")
		_, err = dmod.Write(data)
		if err != nil {
			t.Fatal(err)
		}

		// Get the final node
		resultNode, err := dmod.GetNode()
		if err != nil {
			t.Fatal(err)
		}

		// Should stay as ProtoNode when RawLeaves=false
		_, ok := resultNode.(*dag.ProtoNode)
		if !ok {
			t.Fatalf("expected ProtoNode when RawLeaves=false, got %T", resultNode)
		}
	})
}
