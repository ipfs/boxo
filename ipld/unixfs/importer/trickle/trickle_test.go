package trickle

import (
	"bytes"
	"context"
	"fmt"
	"io"
	mrand "math/rand"
	"runtime"
	"testing"
	"time"

	chunker "github.com/ipfs/boxo/chunker"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	mdtest "github.com/ipfs/boxo/ipld/merkledag/test"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	h "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-test/random"
)

type UseRawLeaves bool

const (
	ProtoBufLeaves UseRawLeaves = false
	RawLeaves      UseRawLeaves = true
)

func runBothSubtests(t *testing.T, tfunc func(*testing.T, UseRawLeaves)) {
	t.Parallel()

	t.Run("leaves=ProtoBuf", func(t *testing.T) { t.Parallel(); tfunc(t, ProtoBufLeaves) })
	t.Run("leaves=Raw", func(t *testing.T) { t.Parallel(); tfunc(t, RawLeaves) })
}

func buildTestDag(ds ipld.DAGService, spl chunker.Splitter, rawLeaves UseRawLeaves) (*merkledag.ProtoNode, error) {
	dbp := h.DagBuilderParams{
		Dagserv:   ds,
		Maxlinks:  h.DefaultLinksPerBlock,
		RawLeaves: bool(rawLeaves),
	}

	return buildTestDagWithParams(ds, spl, dbp)
}

func buildTestDagWithParams(ds ipld.DAGService, spl chunker.Splitter, dbp h.DagBuilderParams) (*merkledag.ProtoNode, error) {
	db, err := dbp.New(spl)
	if err != nil {
		return nil, err
	}

	nd, err := Layout(db)
	if err != nil {
		return nil, err
	}

	pbnd, ok := nd.(*merkledag.ProtoNode)
	if !ok {
		return nil, merkledag.ErrNotProtobuf
	}

	return pbnd, VerifyTrickleDagStructure(pbnd, VerifyParams{
		Getter:      ds,
		Direct:      dbp.Maxlinks,
		LayerRepeat: depthRepeat,
		RawLeaves:   dbp.RawLeaves,
	})
}

// Test where calls to read are smaller than the chunk size
func TestSizeBasedSplit(t *testing.T) {
	runBothSubtests(t, testSizeBasedSplit)
}

func testSizeBasedSplit(t *testing.T, rawLeaves UseRawLeaves) {
	if testing.Short() {
		t.SkipNow()
	}
	bs := chunker.SizeSplitterGen(512)
	testFileConsistency(t, bs, 32*512, rawLeaves)

	bs = chunker.SizeSplitterGen(4096)
	testFileConsistency(t, bs, 32*4096, rawLeaves)

	// Uneven offset
	testFileConsistency(t, bs, 31*4095, rawLeaves)
}

func dup(b []byte) []byte {
	o := make([]byte, len(b))
	copy(o, b)
	return o
}

func testFileConsistency(t *testing.T, bs chunker.SplitterGen, nbytes int, rawLeaves UseRawLeaves) {
	should := make([]byte, nbytes)
	random.NewRand().Read(should)

	read := bytes.NewReader(should)
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, bs(read), rawLeaves)
	if err != nil {
		t.Fatal(err)
	}

	r, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	err = arrComp(out, should)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBuilderConsistency(t *testing.T) {
	runBothSubtests(t, testBuilderConsistency)
}

func testBuilderConsistency(t *testing.T, rawLeaves UseRawLeaves) {
	const nbytes = 100000
	buf := new(bytes.Buffer)
	io.CopyN(buf, random.NewRand(), int64(nbytes))

	should := dup(buf.Bytes())
	dagserv := mdtest.Mock()
	nd, err := buildTestDag(dagserv, chunker.DefaultSplitter(buf), rawLeaves)
	if err != nil {
		t.Fatal(err)
	}
	r, err := uio.NewDagReader(context.Background(), nd, dagserv)
	if err != nil {
		t.Fatal(err)
	}

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	err = arrComp(out, should)
	if err != nil {
		t.Fatal(err)
	}
}

func arrComp(a, b []byte) error {
	if len(a) != len(b) {
		return fmt.Errorf("arrays differ in length. %d != %d", len(a), len(b))
	}
	for i, v := range a {
		if v != b[i] {
			return fmt.Errorf("arrays differ at index: %d", i)
		}
	}
	return nil
}

func TestIndirectBlocks(t *testing.T) {
	runBothSubtests(t, testIndirectBlocks)
}

func testIndirectBlocks(t *testing.T, rawLeaves UseRawLeaves) {
	splitter := chunker.SizeSplitterGen(512)
	const nbytes = 1024 * 1024
	buf := make([]byte, nbytes)
	random.NewRand().Read(buf)

	read := bytes.NewReader(buf)

	ds := mdtest.Mock()
	dag, err := buildTestDag(ds, splitter(read), rawLeaves)
	if err != nil {
		t.Fatal(err)
	}

	reader, err := uio.NewDagReader(context.Background(), dag, ds)
	if err != nil {
		t.Fatal(err)
	}

	out, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(out, buf) {
		t.Fatal("Not equal!")
	}
}

func TestSeekingBasic(t *testing.T) {
	runBothSubtests(t, testSeekingBasic)
}

func testSeekingBasic(t *testing.T, rawLeaves UseRawLeaves) {
	const nbytes = 10 * 1024
	should := make([]byte, nbytes)
	random.NewRand().Read(should)

	read := bytes.NewReader(should)
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, chunker.NewSizeSplitter(read, 512), rawLeaves)
	if err != nil {
		t.Fatal(err)
	}

	rs, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	start := int64(4000)
	n, err := rs.Seek(start, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if n != start {
		t.Fatal("Failed to seek to correct offset")
	}

	out, err := io.ReadAll(rs)
	if err != nil {
		t.Fatal(err)
	}

	err = arrComp(out, should[start:])
	if err != nil {
		t.Fatal(err)
	}
}

func TestSeekToBegin(t *testing.T) {
	runBothSubtests(t, testSeekToBegin)
}

func testSeekToBegin(t *testing.T, rawLeaves UseRawLeaves) {
	const nbytes = 10 * 1024
	should := make([]byte, nbytes)
	random.NewRand().Read(should)

	read := bytes.NewReader(should)
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, chunker.NewSizeSplitter(read, 500), rawLeaves)
	if err != nil {
		t.Fatal(err)
	}

	rs, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	n, err := io.CopyN(io.Discard, rs, 1024*4)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4096 {
		t.Fatal("Copy didnt copy enough bytes")
	}

	seeked, err := rs.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if seeked != 0 {
		t.Fatal("Failed to seek to beginning")
	}

	out, err := io.ReadAll(rs)
	if err != nil {
		t.Fatal(err)
	}

	err = arrComp(out, should)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSeekToAlmostBegin(t *testing.T) {
	runBothSubtests(t, testSeekToAlmostBegin)
}

func testSeekToAlmostBegin(t *testing.T, rawLeaves UseRawLeaves) {
	const nbytes = 10 * 1024
	should := make([]byte, nbytes)
	random.NewRand().Read(should)

	read := bytes.NewReader(should)
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, chunker.NewSizeSplitter(read, 500), rawLeaves)
	if err != nil {
		t.Fatal(err)
	}

	rs, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	n, err := io.CopyN(io.Discard, rs, 1024*4)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4096 {
		t.Fatal("Copy didnt copy enough bytes")
	}

	seeked, err := rs.Seek(1, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if seeked != 1 {
		t.Fatal("Failed to seek to almost beginning")
	}

	out, err := io.ReadAll(rs)
	if err != nil {
		t.Fatal(err)
	}

	err = arrComp(out, should[1:])
	if err != nil {
		t.Fatal(err)
	}
}

func TestSeekEnd(t *testing.T) {
	runBothSubtests(t, testSeekEnd)
}

func testSeekEnd(t *testing.T, rawLeaves UseRawLeaves) {
	const nbytes = 50 * 1024
	should := make([]byte, nbytes)
	random.NewRand().Read(should)

	read := bytes.NewReader(should)
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, chunker.NewSizeSplitter(read, 500), rawLeaves)
	if err != nil {
		t.Fatal(err)
	}

	rs, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	seeked, err := rs.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}
	if seeked != nbytes {
		t.Fatal("Failed to seek to end")
	}
}

func TestSeekEndSingleBlockFile(t *testing.T) {
	runBothSubtests(t, testSeekEndSingleBlockFile)
}

func testSeekEndSingleBlockFile(t *testing.T, rawLeaves UseRawLeaves) {
	const nbytes = 100
	should := make([]byte, nbytes)
	random.NewRand().Read(should)

	read := bytes.NewReader(should)
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, chunker.NewSizeSplitter(read, 5000), rawLeaves)
	if err != nil {
		t.Fatal(err)
	}

	rs, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	seeked, err := rs.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}
	if seeked != nbytes {
		t.Fatal("Failed to seek to end")
	}
}

func TestSeekingStress(t *testing.T) {
	runBothSubtests(t, testSeekingStress)
}

func testSeekingStress(t *testing.T, rawLeaves UseRawLeaves) {
	const nbytes = 1024 * 1024
	should := make([]byte, nbytes)
	random.NewRand().Read(should)

	read := bytes.NewReader(should)
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, chunker.NewSizeSplitter(read, 1000), rawLeaves)
	if err != nil {
		t.Fatal(err)
	}

	rs, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	testbuf := make([]byte, nbytes)
	for i := 0; i < 50; i++ {
		offset := mrand.Intn(int(nbytes))
		l := int(nbytes) - offset
		n, err := rs.Seek(int64(offset), io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}
		if n != int64(offset) {
			t.Fatal("Seek failed to move to correct position")
		}

		nread, err := rs.Read(testbuf[:l])
		if err != nil {
			t.Fatal(err)
		}
		if nread != l {
			t.Fatal("Failed to read enough bytes")
		}

		err = arrComp(testbuf[:l], should[offset:offset+l])
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestSeekingConsistency(t *testing.T) {
	runBothSubtests(t, testSeekingConsistency)
}

func testSeekingConsistency(t *testing.T, rawLeaves UseRawLeaves) {
	const nbytes = 128 * 1024
	should := make([]byte, nbytes)
	random.NewRand().Read(should)

	read := bytes.NewReader(should)
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, chunker.NewSizeSplitter(read, 500), rawLeaves)
	if err != nil {
		t.Fatal(err)
	}

	rs, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	out := make([]byte, nbytes)

	for coff := int64(nbytes - 4096); coff >= 0; coff -= 4096 {
		t.Log(coff)
		n, err := rs.Seek(coff, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}
		if n != coff {
			t.Fatal("wasnt able to seek to the right position")
		}
		nread, err := rs.Read(out[coff : coff+4096])
		if err != nil {
			t.Fatal(err)
		}
		if nread != 4096 {
			t.Fatal("didnt read the correct number of bytes")
		}
	}

	err = arrComp(out, should)
	if err != nil {
		t.Fatal(err)
	}
}

func TestAppend(t *testing.T) {
	runBothSubtests(t, testAppend)
}

func testAppend(t *testing.T, rawLeaves UseRawLeaves) {
	const nbytes = 128 * 1024
	should := make([]byte, nbytes)
	random.NewRand().Read(should)

	// Reader for half the bytes
	read := bytes.NewReader(should[:nbytes/2])
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, chunker.NewSizeSplitter(read, 500), rawLeaves)
	if err != nil {
		t.Fatal(err)
	}

	dbp := &h.DagBuilderParams{
		Dagserv:   ds,
		Maxlinks:  h.DefaultLinksPerBlock,
		RawLeaves: bool(rawLeaves),
	}

	r := bytes.NewReader(should[nbytes/2:])

	ctx := context.Background()

	db, err := dbp.New(chunker.NewSizeSplitter(r, 500))
	if err != nil {
		t.Fatal(err)
	}

	nnode, err := Append(ctx, nd, db)
	if err != nil {
		t.Fatal(err)
	}

	err = VerifyTrickleDagStructure(nnode, VerifyParams{
		Getter:      ds,
		Direct:      dbp.Maxlinks,
		LayerRepeat: depthRepeat,
		RawLeaves:   bool(rawLeaves),
	})
	if err != nil {
		t.Fatal(err)
	}

	fread, err := uio.NewDagReader(ctx, nnode, ds)
	if err != nil {
		t.Fatal(err)
	}

	out, err := io.ReadAll(fread)
	if err != nil {
		t.Fatal(err)
	}

	err = arrComp(out, should)
	if err != nil {
		t.Fatal(err)
	}
}

// This test appends one byte at a time to an empty file
func TestMultipleAppends(t *testing.T) {
	runBothSubtests(t, testMultipleAppends)
}

func testMultipleAppends(t *testing.T, rawLeaves UseRawLeaves) {
	ds := mdtest.Mock()

	// TODO: fix small size appends and make this number bigger
	const nbytes = 1000
	should := make([]byte, nbytes)
	random.NewRand().Read(should)

	read := bytes.NewReader(nil)
	nd, err := buildTestDag(ds, chunker.NewSizeSplitter(read, 500), rawLeaves)
	if err != nil {
		t.Fatal(err)
	}

	dbp := &h.DagBuilderParams{
		Dagserv:   ds,
		Maxlinks:  4,
		RawLeaves: bool(rawLeaves),
	}

	spl := chunker.SizeSplitterGen(500)

	ctx := context.Background()
	for i := 0; i < len(should); i++ {

		db, err := dbp.New(spl(bytes.NewReader(should[i : i+1])))
		if err != nil {
			t.Fatal(err)
		}

		nnode, err := Append(ctx, nd, db)
		if err != nil {
			t.Fatal(err)
		}

		err = VerifyTrickleDagStructure(nnode, VerifyParams{
			Getter:      ds,
			Direct:      dbp.Maxlinks,
			LayerRepeat: depthRepeat,
			RawLeaves:   bool(rawLeaves),
		})
		if err != nil {
			t.Fatal(err)
		}

		fread, err := uio.NewDagReader(ctx, nnode, ds)
		if err != nil {
			t.Fatal(err)
		}

		out, err := io.ReadAll(fread)
		if err != nil {
			t.Fatal(err)
		}

		err = arrComp(out, should[:i+1])
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestAppendSingleBytesToEmpty(t *testing.T) {
	t.Parallel()

	ds := mdtest.Mock()

	data := []byte("AB")

	nd := new(merkledag.ProtoNode)
	nd.SetData(ft.FilePBData(nil, 0))

	dbp := &h.DagBuilderParams{
		Dagserv:  ds,
		Maxlinks: 4,
	}

	spl := chunker.SizeSplitterGen(500)

	ctx := context.Background()

	db, err := dbp.New(spl(bytes.NewReader(data[:1])))
	if err != nil {
		t.Fatal(err)
	}

	nnode, err := Append(ctx, nd, db)
	if err != nil {
		t.Fatal(err)
	}

	db, err = dbp.New(spl(bytes.NewReader(data[1:])))
	if err != nil {
		t.Fatal(err)
	}

	nnode, err = Append(ctx, nnode, db)
	if err != nil {
		t.Fatal(err)
	}

	fread, err := uio.NewDagReader(ctx, nnode, ds)
	if err != nil {
		t.Fatal(err)
	}

	out, err := io.ReadAll(fread)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(out, data)
	err = arrComp(out, data)
	if err != nil {
		t.Fatal(err)
	}
}

func TestAppendWithModTime(t *testing.T) {
	const nbytes = 128 * 1024

	timestamp := time.Now()
	buf := random.Bytes(nbytes)

	nd := new(merkledag.ProtoNode)
	nd.SetData(ft.FilePBDataWithStat(buf[:nbytes/2], nbytes/2, 0, timestamp))

	if runtime.GOOS == "windows" {
		time.Sleep(3 * time.Second) // for os with low-res mod time.
	}

	dbp := &h.DagBuilderParams{
		Dagserv:  mdtest.Mock(),
		Maxlinks: h.DefaultLinksPerBlock,
	}

	r := bytes.NewReader(buf[nbytes/2:])
	db, err := dbp.New(chunker.NewSizeSplitter(r, 500))
	if err != nil {
		t.Fatal(err)
	}

	nd2, err := Append(context.Background(), nd, db)
	if err != nil {
		t.Fatal(err)
	}

	fsn, _ := ft.ExtractFSNode(nd2)

	if !fsn.ModTime().After(timestamp) {
		t.Errorf("expected modification time to be updated")
	}
}

func TestAppendToEmptyWithModTime(t *testing.T) {
	timestamp := time.Now()
	nd := new(merkledag.ProtoNode)
	nd.SetData(ft.FilePBDataWithStat(nil, 0, 0, timestamp))

	if runtime.GOOS == "windows" {
		time.Sleep(3 * time.Second) // for os with low-res mod time.
	}

	dbp := &h.DagBuilderParams{
		Dagserv:  mdtest.Mock(),
		Maxlinks: h.DefaultLinksPerBlock,
	}

	db, err := dbp.New(chunker.DefaultSplitter(bytes.NewReader([]byte("test"))))
	if err != nil {
		t.Fatal(err)
	}

	nd2, err := Append(context.Background(), nd, db)
	if err != nil {
		t.Fatal(err)
	}

	fsn, _ := ft.ExtractFSNode(nd2)

	if !fsn.ModTime().After(timestamp) {
		t.Errorf("expected modification time to be updated")
	}
}

func TestMetadata(t *testing.T) {
	runBothSubtests(t, testMetadata)
}

func testMetadata(t *testing.T, rawLeaves UseRawLeaves) {
	const nbytes = 3 * chunker.DefaultBlockSize
	buf := new(bytes.Buffer)
	_, err := io.CopyN(buf, random.NewRand(), nbytes)
	if err != nil {
		t.Fatal(err)
	}

	dagserv := mdtest.Mock()
	dbp := h.DagBuilderParams{
		Dagserv:     dagserv,
		Maxlinks:    h.DefaultLinksPerBlock,
		RawLeaves:   bool(rawLeaves),
		FileMode:    0o522,
		FileModTime: time.Unix(1638111600, 76552),
	}

	nd, err := buildTestDagWithParams(dagserv, chunker.DefaultSplitter(buf), dbp)
	if err != nil {
		t.Fatal(err)
	}

	dr, err := uio.NewDagReader(context.Background(), nd, dagserv)
	if err != nil {
		t.Fatal(err)
	}

	if !dr.ModTime().Equal(dbp.FileModTime) {
		t.Errorf("got modtime %v, wanted %v", dr.ModTime(), dbp.FileModTime)
	}

	if dr.Mode() != dbp.FileMode {
		t.Errorf("got filemode %o, wanted %o", dr.Mode(), dbp.FileMode)
	}
}
