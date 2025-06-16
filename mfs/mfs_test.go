package mfs

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	gopath "path"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	bserv "github.com/ipfs/boxo/blockservice"
	bstore "github.com/ipfs/boxo/blockstore"
	chunker "github.com/ipfs/boxo/chunker"
	offline "github.com/ipfs/boxo/exchange/offline"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	importer "github.com/ipfs/boxo/ipld/unixfs/importer"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-test/random"
)

func emptyDirNode() *dag.ProtoNode {
	return dag.NodeWithData(ft.FolderPBData())
}

func getDagserv(t testing.TB) ipld.DAGService {
	t.Helper()
	db := dssync.MutexWrap(ds.NewMapDatastore())
	bs := bstore.NewBlockstore(db)
	blockserv := bserv.New(bs, offline.Exchange(bs))
	return dag.NewDAGService(blockserv)
}

func getRandFile(t *testing.T, ds ipld.DAGService, size int64) ipld.Node {
	r := io.LimitReader(random.NewRand(), size)
	return fileNodeFromReader(t, ds, r)
}

func fileNodeFromReader(t *testing.T, ds ipld.DAGService, r io.Reader) ipld.Node {
	nd, err := importer.BuildDagFromReader(ds, chunker.DefaultSplitter(r))
	if err != nil {
		t.Fatal(err)
	}
	return nd
}

func mkdirP(t *testing.T, root *Directory, pth string) *Directory {
	dirs := strings.Split(pth, "/")
	cur := root
	for _, d := range dirs {
		n, err := cur.Mkdir(d)
		if err != nil && err != os.ErrExist {
			t.Fatal(err)
		}
		if err == os.ErrExist {
			fsn, err := cur.Child(d)
			if err != nil {
				t.Fatal(err)
			}
			switch fsn := fsn.(type) {
			case *Directory:
				n = fsn
			case *File:
				t.Fatal("tried to make a directory where a file already exists")
			}
		}

		cur = n
	}
	return cur
}

func assertDirNotAtPath(root *Directory, pth string) error {
	_, err := DirLookup(root, pth)
	if err == nil {
		return fmt.Errorf("%s exists in %s", pth, root.name)
	}
	return nil
}

func assertDirAtPath(root *Directory, pth string, children []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fsn, err := DirLookup(root, pth)
	if err != nil {
		return err
	}

	dir, ok := fsn.(*Directory)
	if !ok {
		return fmt.Errorf("%s was not a directory", pth)
	}

	listing, err := dir.List(ctx)
	if err != nil {
		return err
	}

	var names []string
	for _, d := range listing {
		names = append(names, d.Name)
	}

	slices.Sort(children)
	slices.Sort(names)
	if !compStrArrs(children, names) {
		return errors.New("directories children did not match")
	}

	return nil
}

func compStrArrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func assertFileAtPath(ds ipld.DAGService, root *Directory, expn ipld.Node, pth string) error {
	exp, ok := expn.(*dag.ProtoNode)
	if !ok {
		return dag.ErrNotProtobuf
	}

	parts := strings.Split(pth, "/")
	cur := root
	for i, d := range parts[:len(parts)-1] {
		next, err := cur.Child(d)
		if err != nil {
			return fmt.Errorf("looking for %s failed: %s", pth, err)
		}

		nextDir, ok := next.(*Directory)
		if !ok {
			return fmt.Errorf("%s points to a non-directory", parts[:i+1])
		}

		cur = nextDir
	}

	last := parts[len(parts)-1]
	finaln, err := cur.Child(last)
	if err != nil {
		return err
	}

	file, ok := finaln.(*File)
	if !ok {
		return fmt.Errorf("%s was not a file", pth)
	}

	rfd, err := file.Open(Flags{Read: true})
	if err != nil {
		return err
	}

	out, err := io.ReadAll(rfd)
	if err != nil {
		return err
	}

	expbytes, err := catNode(ds, exp)
	if err != nil {
		return err
	}

	if !bytes.Equal(out, expbytes) {
		return errors.New("incorrect data at path")
	}
	return nil
}

func catNode(ds ipld.DAGService, nd *dag.ProtoNode) ([]byte, error) {
	r, err := uio.NewDagReader(context.TODO(), nd, ds)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}

func setupRoot(ctx context.Context, t testing.TB) (ipld.DAGService, *Root) {
	t.Helper()

	ds := getDagserv(t)

	root := emptyDirNode()
	rt, err := NewRoot(ctx, ds, root, func(ctx context.Context, c cid.Cid) error {
		fmt.Println("PUBLISHED: ", c)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	return ds, rt
}

func TestBasic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ds, rt := setupRoot(ctx, t)

	rootdir := rt.GetDirectory()

	// test making a basic dir
	_, err := rootdir.Mkdir("a")
	if err != nil {
		t.Fatal(err)
	}

	path := "a/b/c/d/e/f/g"
	d := mkdirP(t, rootdir, path)

	fi := getRandFile(t, ds, 1000)

	// test inserting that file
	err = d.AddChild("afile", fi)
	if err != nil {
		t.Fatal(err)
	}

	err = assertFileAtPath(ds, rootdir, fi, "a/b/c/d/e/f/g/afile")
	if err != nil {
		t.Fatal(err)
	}
}

func TestMkdir(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, rt := setupRoot(ctx, t)

	rootdir := rt.GetDirectory()

	dirsToMake := []string{"a", "B", "foo", "bar", "cats", "fish"}
	slices.Sort(dirsToMake) // sort for easy comparing later

	for _, d := range dirsToMake {
		_, err := rootdir.Mkdir(d)
		if err != nil {
			t.Fatal(err)
		}
	}

	err := assertDirAtPath(rootdir, "/", dirsToMake)
	if err != nil {
		t.Fatal(err)
	}

	for _, d := range dirsToMake {
		mkdirP(t, rootdir, "a/"+d)
	}

	err = assertDirAtPath(rootdir, "/a", dirsToMake)
	if err != nil {
		t.Fatal(err)
	}

	// mkdir over existing dir should fail
	_, err = rootdir.Mkdir("a")
	if err == nil {
		t.Fatal("should have failed!")
	}
}

func TestDirectoryLoadFromDag(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ds, rt := setupRoot(ctx, t)

	rootdir := rt.GetDirectory()

	nd := getRandFile(t, ds, 1000)
	err := ds.Add(ctx, nd)
	if err != nil {
		t.Fatal(err)
	}

	fihash := nd.Cid()

	dir := emptyDirNode()
	err = ds.Add(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}

	dirhash := dir.Cid()

	top := emptyDirNode()
	top.SetLinks([]*ipld.Link{
		{
			Name: "a",
			Cid:  fihash,
		},
		{
			Name: "b",
			Cid:  dirhash,
		},
	})

	err = rootdir.AddChild("foo", top)
	if err != nil {
		t.Fatal(err)
	}

	// get this dir
	topi, err := rootdir.Child("foo")
	if err != nil {
		t.Fatal(err)
	}

	topd := topi.(*Directory)

	path := topd.Path()
	if path != "/foo" {
		t.Fatalf("Expected path '/foo', got '%s'", path)
	}

	// mkdir over existing but unloaded child file should fail
	_, err = topd.Mkdir("a")
	if err == nil {
		t.Fatal("expected to fail!")
	}

	// mkdir over existing but unloaded child dir should fail
	_, err = topd.Mkdir("b")
	if err == nil {
		t.Fatal("expected to fail!")
	}

	// adding a child over an existing path fails
	err = topd.AddChild("b", nd)
	if err == nil {
		t.Fatal("expected to fail!")
	}

	err = assertFileAtPath(ds, rootdir, nd, "foo/a")
	if err != nil {
		t.Fatal(err)
	}

	err = assertDirAtPath(rootdir, "foo/b", nil)
	if err != nil {
		t.Fatal(err)
	}

	err = rootdir.Unlink("foo")
	if err != nil {
		t.Fatal(err)
	}

	err = assertDirAtPath(rootdir, "", nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMvFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dagService, rt := setupRoot(ctx, t)
	rootDir := rt.GetDirectory()

	fi := getRandFile(t, dagService, 1000)

	err := rootDir.AddChild("afile", fi)
	if err != nil {
		t.Fatal(err)
	}

	err = Mv(rt, "/afile", "/bfile")
	if err != nil {
		t.Fatal(err)
	}

	err = assertFileAtPath(dagService, rootDir, fi, "bfile")
	if err != nil {
		t.Fatal(err)
	}
}

func TestMvFileToSubdir(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dagService, rt := setupRoot(ctx, t)
	rootDir := rt.GetDirectory()

	_ = mkdirP(t, rootDir, "test1")

	fi := getRandFile(t, dagService, 1000)

	err := rootDir.AddChild("afile", fi)
	if err != nil {
		t.Fatal(err)
	}

	err = Mv(rt, "/afile", "/test1")
	if err != nil {
		t.Fatal(err)
	}

	err = assertFileAtPath(dagService, rootDir, fi, "test1/afile")
	if err != nil {
		t.Fatal(err)
	}
}

func TestMvFileToSubdirWithRename(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dagService, rt := setupRoot(ctx, t)
	rootDir := rt.GetDirectory()

	_ = mkdirP(t, rootDir, "test1")

	fi := getRandFile(t, dagService, 1000)

	err := rootDir.AddChild("afile", fi)
	if err != nil {
		t.Fatal(err)
	}

	err = Mv(rt, "/afile", "/test1/bfile")
	if err != nil {
		t.Fatal(err)
	}

	err = assertFileAtPath(dagService, rootDir, fi, "test1/bfile")
	if err != nil {
		t.Fatal(err)
	}
}

func TestMvDir(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dagService, rt := setupRoot(ctx, t)
	rootDir := rt.GetDirectory()

	_ = mkdirP(t, rootDir, "test1")
	d2 := mkdirP(t, rootDir, "test2")

	fi := getRandFile(t, dagService, 1000)

	err := d2.AddChild("afile", fi)
	if err != nil {
		t.Fatal(err)
	}

	err = Mv(rt, "/test2", "/test1")
	if err != nil {
		t.Fatal(err)
	}

	err = assertDirNotAtPath(rootDir, "test2")
	if err != nil {
		t.Fatal(err)
	}

	err = assertFileAtPath(dagService, rootDir, fi, "test1/test2/afile")
	if err != nil {
		t.Fatal(err)
	}
}

func TestMfsFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ds, rt := setupRoot(ctx, t)

	rootdir := rt.GetDirectory()

	fisize := 1000
	nd := getRandFile(t, ds, 1000)

	err := rootdir.AddChild("file", nd)
	if err != nil {
		t.Fatal(err)
	}

	fsn, err := rootdir.Child("file")
	if err != nil {
		t.Fatal(err)
	}

	fi := fsn.(*File)

	if fi.Type() != TFile {
		t.Fatal("something is seriously wrong here")
	}

	if m, err := fi.Mode(); err != nil {
		t.Fatal("failed to get file mode: ", err)
	} else if m != 0 {
		t.Fatal("mode should not be set on a new file")
	}

	if ts, err := fi.ModTime(); err != nil {
		t.Fatal("failed to get file mtime: ", err)
	} else if !ts.IsZero() {
		t.Fatal("modification time should not be set on a new file")
	}

	wfd, err := fi.Open(Flags{Read: true, Write: true, Sync: true})
	if err != nil {
		t.Fatal(err)
	}

	// assert size is as expected
	size, err := fi.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(fisize) {
		t.Fatal("size isnt correct")
	}

	// write to beginning of file
	b := []byte("THIS IS A TEST")
	n, err := wfd.Write(b)
	if err != nil {
		t.Fatal(err)
	}

	if n != len(b) {
		t.Fatal("didnt write correct number of bytes")
	}

	// make sure size hasnt changed
	size, err = wfd.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(fisize) {
		t.Fatal("size isnt correct")
	}

	// seek back to beginning
	ns, err := wfd.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	if ns != 0 {
		t.Fatal("didnt seek to beginning")
	}

	// read back bytes we wrote
	buf := make([]byte, len(b))
	n, err = wfd.Read(buf)
	if err != nil {
		t.Fatal(err)
	}

	if n != len(buf) {
		t.Fatal("didnt read enough")
	}

	if !bytes.Equal(buf, b) {
		t.Fatal("data read was different than data written")
	}

	// truncate file to ten bytes
	err = wfd.Truncate(10)
	if err != nil {
		t.Fatal(err)
	}

	size, err = wfd.Size()
	if err != nil {
		t.Fatal(err)
	}

	if size != 10 {
		t.Fatal("size was incorrect: ", size)
	}

	// 'writeAt' to extend it
	data := []byte("this is a test foo foo foo")
	nwa, err := wfd.WriteAt(data, 5)
	if err != nil {
		t.Fatal(err)
	}

	if nwa != len(data) {
		t.Fatal(err)
	}

	// assert size once more
	size, err = wfd.Size()
	if err != nil {
		t.Fatal(err)
	}

	if size != int64(5+len(data)) {
		t.Fatal("size was incorrect")
	}

	// close it out!
	err = wfd.Close()
	if err != nil {
		t.Fatal(err)
	}

	if ts, err := fi.ModTime(); err != nil {
		t.Fatal("failed to get file mtime: ", err)
	} else if !ts.IsZero() {
		t.Fatal("file with unset modification time should not update modification time")
	}

	// make sure we can get node. TODO: verify it later
	_, err = fi.GetNode()
	if err != nil {
		t.Fatal(err)
	}
}

func TestMfsModeAndModTime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, rt := setupRoot(ctx, t)
	rootdir := rt.GetDirectory()
	nd := getRandFile(t, ds, 1000)

	err := rootdir.AddChild("file", nd)
	if err != nil {
		t.Fatal(err)
	}

	fsn, err := rootdir.Child("file")
	if err != nil {
		t.Fatal(err)
	}

	fi := fsn.(*File)

	if fi.Type() != TFile {
		t.Fatal("something is seriously wrong here")
	}

	var mode os.FileMode
	ts, _ := time.Now(), time.Time{}

	// can set mode
	if err = fi.SetMode(0o644); err == nil {
		if mode, err = fi.Mode(); mode != 0o644 {
			t.Fatal("failed to get correct mode of file")
		}
	}
	if err != nil {
		t.Fatal("failed to check file mode: ", err)
	}

	// can set last modification time
	if err = fi.SetModTime(ts); err == nil {
		ts2, err := fi.ModTime()
		if err != nil {
			t.Fatal(err)
		}
		if !ts2.Equal(ts) {
			t.Fatal("failed to get correct modification time of file")
		}
	}
	if err != nil {
		t.Fatal("failed to check file modification time: ", err)
	}

	// test modification time update after write (on closing file)
	if runtime.GOOS == "windows" {
		time.Sleep(3 * time.Second) // for os with low-res mod time.
	}
	wfd, err := fi.Open(Flags{Read: false, Write: true, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = wfd.Write([]byte("test"))
	if err != nil {
		t.Fatal(err)
	}
	err = wfd.Close()
	if err != nil {
		t.Fatal(err)
	}
	ts2, err := fi.ModTime()
	if err != nil {
		t.Fatal(err)
	}
	if !ts2.After(ts) {
		t.Fatal("modification time should be updated after file write")
	}

	// writeAt
	ts = ts2
	if runtime.GOOS == "windows" {
		time.Sleep(3 * time.Second) // for os with low-res mod time.
	}
	wfd, err = fi.Open(Flags{Read: false, Write: true, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = wfd.WriteAt([]byte("test"), 42)
	if err != nil {
		t.Fatal(err)
	}
	err = wfd.Close()
	if err != nil {
		t.Fatal(err)
	}
	ts2, err = fi.ModTime()
	if err != nil {
		t.Fatal(err)
	}
	if !ts2.After(ts) {
		t.Fatal("modification time should be updated after file writeAt")
	}

	// truncate (shrink)
	ts = ts2
	if runtime.GOOS == "windows" {
		time.Sleep(3 * time.Second) // for os with low-res mod time.
	}
	wfd, err = fi.Open(Flags{Read: false, Write: true, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	err = wfd.Truncate(100)
	if err != nil {
		t.Fatal(err)
	}
	err = wfd.Close()
	if err != nil {
		t.Fatal(err)
	}
	ts2, err = fi.ModTime()
	if err != nil {
		t.Fatal(err)
	}
	if !ts2.After(ts) {
		t.Fatal("modification time should be updated after file truncate (shrink)")
	}

	// truncate (expand)
	ts = ts2
	if runtime.GOOS == "windows" {
		time.Sleep(3 * time.Second) // for os with low-res mod time.
	}
	wfd, err = fi.Open(Flags{Read: false, Write: true, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	err = wfd.Truncate(1500)
	if err != nil {
		t.Fatal(err)
	}
	err = wfd.Close()
	if err != nil {
		t.Fatal(err)
	}
	ts2, err = fi.ModTime()
	if err != nil {
		t.Fatal(err)
	}
	if !ts2.After(ts) {
		t.Fatal("modification time should be updated after file truncate (expand)")
	}
}

func TestMfsRawNodeSetModeAndMtime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, rt := setupRoot(ctx, t)
	rootdir := rt.GetDirectory()

	// Create raw-node file.
	nd := dag.NewRawNode(random.Bytes(256))
	_, err := ft.ExtractFSNode(nd)
	if !errors.Is(err, ft.ErrNotProtoNode) {
		t.Fatal("Expected non-proto node")
	}

	err = rootdir.AddChild("file", nd)
	if err != nil {
		t.Fatal(err)
	}

	fsn, err := rootdir.Child("file")
	if err != nil {
		t.Fatal(err)
	}

	fi := fsn.(*File)
	if fi.Type() != TFile {
		t.Fatal("something is seriously wrong here")
	}

	// Check for expected error when getting mode and mtime.
	_, err = fi.Mode()
	if !errors.Is(err, ft.ErrNotProtoNode) {
		t.Fatal("Expected non-proto node")
	}
	_, err = fi.ModTime()
	if !errors.Is(err, ft.ErrNotProtoNode) {
		t.Fatal("Expected non-proto node")
	}

	// Set and check mode.
	err = fi.SetMode(0o644)
	if err != nil {
		t.Fatalf("failed to set file mode: %s", err)
	}
	mode, err := fi.Mode()
	if err != nil {
		t.Fatalf("failed to check file mode: %s", err)
	}
	if mode != 0o644 {
		t.Fatal("failed to get correct mode of file, got", mode.String())
	}

	// Mtime should still be unset.
	mtime, err := fi.ModTime()
	if err != nil {
		t.Fatalf("failed to get file modification time: %s", err)
	}
	if !mtime.IsZero() {
		t.Fatalf("expected mtime to be unset")
	}

	// Set and check mtime.
	now := time.Now()
	err = fi.SetModTime(now)
	if err != nil {
		t.Fatalf("failed to set file modification time: %s", err)
	}
	mtime, err = fi.ModTime()
	if err != nil {
		t.Fatalf("failed to get file modification time: %s", err)
	}
	if !mtime.Equal(now) {
		t.Fatal("failed to get correct modification time of file")
	}
}

func TestMfsDirListNames(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ds, rt := setupRoot(ctx, t)

	rootdir := rt.GetDirectory()

	total := rand.Intn(10) + 1
	fNames := make([]string, 0, total)

	for i := 0; i < total; i++ {
		fn := randomName()
		fNames = append(fNames, fn)
		nd := getRandFile(t, ds, rand.Int63n(1000)+1)
		err := rootdir.AddChild(fn, nd)
		if err != nil {
			t.Fatal(err)
		}
	}

	list, err := rootdir.ListNames(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, lName := range list {
		found := false
		for _, fName := range fNames {
			if lName == fName {
				found = true
				break
			}
		}
		if !found {
			t.Fatal(lName + " not found in directory listing")
		}
	}
}

func randomWalk(d *Directory, n int) (*Directory, error) {
	for i := 0; i < n; i++ {
		dirents, err := d.List(context.Background())
		if err != nil {
			return nil, err
		}

		var childdirs []NodeListing
		for _, child := range dirents {
			if child.Type == int(TDir) {
				childdirs = append(childdirs, child)
			}
		}
		if len(childdirs) == 0 {
			return d, nil
		}

		next := childdirs[rand.Intn(len(childdirs))].Name

		nextD, err := d.Child(next)
		if err != nil {
			return nil, err
		}

		d = nextD.(*Directory)
	}
	return d, nil
}

func randomName() string {
	set := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
	length := rand.Intn(10) + 2
	var out string
	for i := 0; i < length; i++ {
		j := rand.Intn(len(set))
		out += set[j : j+1]
	}
	return out
}

func actorMakeFile(d *Directory) error {
	d, err := randomWalk(d, rand.Intn(7))
	if err != nil {
		return err
	}

	name := randomName()
	f, err := NewFile(name, dag.NodeWithData(ft.FilePBData(nil, 0)), d, d.dagService)
	if err != nil {
		return err
	}

	wfd, err := f.Open(Flags{Write: true, Sync: true})
	if err != nil {
		return err
	}

	rread := rand.New(rand.NewSource(time.Now().UnixNano()))
	r := io.LimitReader(rread, int64(77*rand.Intn(123)+1))
	_, err = io.Copy(wfd, r)
	if err != nil {
		return err
	}

	return wfd.Close()
}

func actorMkdir(d *Directory) error {
	d, err := randomWalk(d, rand.Intn(7))
	if err != nil {
		return err
	}

	_, err = d.Mkdir(randomName())

	return err
}

func randomFile(d *Directory) (*File, error) {
	d, err := randomWalk(d, rand.Intn(6))
	if err != nil {
		return nil, err
	}

	ents, err := d.List(context.Background())
	if err != nil {
		return nil, err
	}

	var files []string
	for _, e := range ents {
		if e.Type == int(TFile) {
			files = append(files, e.Name)
		}
	}

	if len(files) == 0 {
		return nil, nil
	}

	fname := files[rand.Intn(len(files))]
	fsn, err := d.Child(fname)
	if err != nil {
		return nil, err
	}

	fi, ok := fsn.(*File)
	if !ok {
		return nil, errors.New("file wasn't a file, race?")
	}

	return fi, nil
}

func actorWriteFile(d *Directory) error {
	fi, err := randomFile(d)
	if err != nil {
		return err
	}
	if fi == nil {
		return nil
	}

	size := rand.Intn(1024) + 1
	buf := make([]byte, size)
	if _, err := crand.Read(buf); err != nil {
		return err
	}

	s, err := fi.Size()
	if err != nil {
		return err
	}

	wfd, err := fi.Open(Flags{Write: true, Sync: true})
	if err != nil {
		return err
	}

	offset := rand.Int63n(s)

	n, err := wfd.WriteAt(buf, offset)
	if err != nil {
		return err
	}
	if n != size {
		return errors.New("didnt write enough")
	}

	return wfd.Close()
}

func actorReadFile(d *Directory) error {
	fi, err := randomFile(d)
	if err != nil {
		return err
	}
	if fi == nil {
		return nil
	}

	_, err = fi.Size()
	if err != nil {
		return err
	}

	rfd, err := fi.Open(Flags{Read: true})
	if err != nil {
		return err
	}

	_, err = io.ReadAll(rfd)
	if err != nil {
		return err
	}

	return rfd.Close()
}

func testActor(rt *Root, iterations int, errs chan error) {
	d := rt.GetDirectory()
	for i := 0; i < iterations; i++ {
		switch rand.Intn(5) {
		case 0:
			if err := actorMkdir(d); err != nil {
				errs <- err
				return
			}
		case 1, 2:
			if err := actorMakeFile(d); err != nil {
				errs <- err
				return
			}
		case 3:
			if err := actorWriteFile(d); err != nil {
				errs <- err
				return
			}
		case 4:
			if err := actorReadFile(d); err != nil {
				errs <- err
				return
			}
		}
	}
	errs <- nil
}

func TestMfsStress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, rt := setupRoot(ctx, t)

	numroutines := 10

	errs := make(chan error)
	for i := 0; i < numroutines; i++ {
		go testActor(rt, 50, errs)
	}

	for i := 0; i < numroutines; i++ {
		err := <-errs
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMfsHugeDir(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, rt := setupRoot(ctx, t)

	for i := 0; i < 10000; i++ {
		err := Mkdir(rt, fmt.Sprintf("/dir%d", i), MkdirOpts{Mkparents: false, Flush: false})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMkdirP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, rt := setupRoot(ctx, t)

	err := Mkdir(rt, "/a/b/c/d/e/f", MkdirOpts{Mkparents: true, Flush: true})
	if err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentWriteAndFlush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ds, rt := setupRoot(ctx, t)

	d := mkdirP(t, rt.GetDirectory(), "foo/bar/baz")
	fn := fileNodeFromReader(t, ds, bytes.NewBuffer(nil))
	err := d.AddChild("file", fn)
	if err != nil {
		t.Fatal(err)
	}

	nloops := 500

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < nloops; i++ {
			err := writeFile(rt, "/foo/bar/baz/file", func(_ []byte) []byte { return []byte("STUFF") })
			if err != nil {
				t.Error("file write failed: ", err)
				return
			}
		}
	}()

	for i := 0; i < nloops; i++ {
		_, err := rt.GetDirectory().GetNode()
		if err != nil {
			t.Fatal(err)
		}
	}

	wg.Wait()
}

func TestFlushing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, rt := setupRoot(ctx, t)

	dir := rt.GetDirectory()
	c := mkdirP(t, dir, "a/b/c")
	d := mkdirP(t, dir, "a/b/d")
	e := mkdirP(t, dir, "a/b/e")

	data := []byte("this is a test\n")
	nd1 := dag.NodeWithData(ft.FilePBData(data, uint64(len(data))))

	if err := c.AddChild("TEST", nd1); err != nil {
		t.Fatal(err)
	}
	if err := d.AddChild("TEST", nd1); err != nil {
		t.Fatal(err)
	}
	if err := e.AddChild("TEST", nd1); err != nil {
		t.Fatal(err)
	}
	if err := dir.AddChild("FILE", nd1); err != nil {
		t.Fatal(err)
	}

	nd, err := FlushPath(ctx, rt, "/a/b/c/TEST")
	if err != nil {
		t.Fatal(err)
	}
	if nd.Cid().String() != "QmYi7wrRFKVCcTB56A6Pep2j31Q5mHfmmu21RzHXu25RVR" {
		t.Fatalf("unexpected node from FlushPath: %s", nd.Cid())
	}

	if _, err := FlushPath(ctx, rt, "/a/b/d/TEST"); err != nil {
		t.Fatal(err)
	}

	if _, err := FlushPath(ctx, rt, "/a/b/e/TEST"); err != nil {
		t.Fatal(err)
	}

	if _, err := FlushPath(ctx, rt, "/FILE"); err != nil {
		t.Fatal(err)
	}

	rnd, err := dir.GetNode()
	if err != nil {
		t.Fatal(err)
	}

	pbrnd, ok := rnd.(*dag.ProtoNode)
	if !ok {
		t.Fatal(dag.ErrNotProtobuf)
	}

	fsnode, err := ft.FSNodeFromBytes(pbrnd.Data())
	if err != nil {
		t.Fatal(err)
	}

	if fsnode.Type() != ft.TDirectory {
		t.Fatal("root wasnt a directory")
	}

	rnk := rnd.Cid()
	exp := "QmWMVyhTuyxUrXX3ynz171jq76yY3PktfY9Bxiph7b9ikr"
	if rnk.String() != exp {
		t.Fatalf("dag looks wrong, expected %s, but got %s", exp, rnk.String())
	}
}

func readFile(rt *Root, path string, offset int64, buf []byte) error {
	n, err := Lookup(rt, path)
	if err != nil {
		return err
	}

	fi, ok := n.(*File)
	if !ok {
		return fmt.Errorf("%s was not a file", path)
	}

	fd, err := fi.Open(Flags{Read: true})
	if err != nil {
		return err
	}

	_, err = fd.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}

	nread, err := fd.Read(buf)
	if err != nil {
		return err
	}
	if nread != len(buf) {
		return errors.New("didn't read enough")
	}

	return fd.Close()
}

func TestConcurrentReads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, rt := setupRoot(ctx, t)

	rootdir := rt.GetDirectory()

	path := "a/b/c"
	d := mkdirP(t, rootdir, path)

	buf := make([]byte, 2048)
	if _, err := crand.Read(buf); err != nil {
		t.Fatal(err)
	}
	fi := fileNodeFromReader(t, ds, bytes.NewReader(buf))
	err := d.AddChild("afile", fi)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	nloops := 100
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(me int) {
			defer wg.Done()
			mybuf := make([]byte, len(buf))
			for j := 0; j < nloops; j++ {
				offset := rand.Intn(len(buf))
				length := rand.Intn(len(buf) - offset)

				err := readFile(rt, "/a/b/c/afile", int64(offset), mybuf[:length])
				if err != nil {
					t.Error("readfile failed: ", err)
					return
				}

				if !bytes.Equal(mybuf[:length], buf[offset:offset+length]) {
					t.Error("incorrect read!")
				}
			}
		}(i)
	}
	wg.Wait()
}

func writeFile(rt *Root, path string, transform func([]byte) []byte) error {
	n, err := Lookup(rt, path)
	if err != nil {
		return err
	}

	fi, ok := n.(*File)
	if !ok {
		return errors.New("expected to receive a file, but didnt get one")
	}

	fd, err := fi.Open(Flags{Read: true, Write: true, Sync: true})
	if err != nil {
		return err
	}
	defer fd.Close()

	data, err := io.ReadAll(fd)
	if err != nil {
		return err
	}
	data = transform(data)

	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	err = fd.Truncate(0)
	if err != nil {
		return err
	}

	nw, err := fd.Write(data)
	if err != nil {
		return err
	}

	if nw != len(data) {
		return fmt.Errorf("wrote incorrect amount: %d != 10", nw)
	}

	return nil
}

func TestConcurrentWrites(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, rt := setupRoot(ctx, t)

	rootdir := rt.GetDirectory()

	path := "a/b/c"
	d := mkdirP(t, rootdir, path)

	fi := fileNodeFromReader(t, ds, bytes.NewReader(make([]byte, 0)))
	err := d.AddChild("afile", fi)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	nloops := 100
	errs := make(chan error, 1000)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var lastSeen uint64
			for j := 0; j < nloops; j++ {
				err := writeFile(rt, "a/b/c/afile", func(buf []byte) []byte {
					if len(buf) == 0 {
						if lastSeen > 0 {
							errs <- fmt.Errorf("file corrupted, last seen: %d", lastSeen)
							return buf
						}
						buf = make([]byte, 8)
					} else if len(buf) != 8 {
						errs <- errors.New("buf not the right size")
						return buf
					}

					num := binary.LittleEndian.Uint64(buf)
					if num < lastSeen {
						errs <- fmt.Errorf("count decreased: was %d, is %d", lastSeen, num)
						return buf
					} else {
						t.Logf("count correct: was %d, is %d", lastSeen, num)
					}
					num++
					binary.LittleEndian.PutUint64(buf, num)
					lastSeen = num
					return buf
				})
				if err != nil {
					errs <- fmt.Errorf("writefile failed: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	buf := make([]byte, 8)
	if err := readFile(rt, "a/b/c/afile", 0, buf); err != nil {
		t.Fatal(err)
	}
	actual := binary.LittleEndian.Uint64(buf)
	expected := uint64(10 * nloops)
	if actual != expected {
		t.Fatalf("iteration mismatch: expect %d, got %d", expected, actual)
	}
}

func TestFileDescriptors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, rt := setupRoot(ctx, t)
	dir := rt.GetDirectory()

	nd := dag.NodeWithData(ft.FilePBData(nil, 0))
	fi, err := NewFile("test", nd, dir, ds)
	if err != nil {
		t.Fatal(err)
	}

	// test read only
	rfd1, err := fi.Open(Flags{Read: true})
	if err != nil {
		t.Fatal(err)
	}

	err = rfd1.Truncate(0)
	if err == nil {
		t.Fatal("shouldnt be able to truncate readonly fd")
	}

	_, err = rfd1.Write([]byte{})
	if err == nil {
		t.Fatal("shouldnt be able to write to readonly fd")
	}

	_, err = rfd1.Read([]byte{})
	if err != nil {
		t.Fatalf("expected to be able to read from file: %s", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		// can open second readonly file descriptor
		rfd2, err := fi.Open(Flags{Read: true})
		if err != nil {
			t.Error(err)
			return
		}

		rfd2.Close()
	}()

	select {
	case <-time.After(time.Second):
		t.Fatal("open second file descriptor failed")
	case <-done:
	}

	if t.Failed() {
		return
	}

	// test not being able to open for write until reader are closed
	done = make(chan struct{})
	go func() {
		defer close(done)
		wfd1, err := fi.Open(Flags{Write: true, Sync: true})
		if err != nil {
			t.Error(err)
		}

		wfd1.Close()
	}()

	select {
	case <-time.After(time.Millisecond * 200):
	case <-done:
		if t.Failed() {
			return
		}

		t.Fatal("shouldnt have been able to open file for writing")
	}

	err = rfd1.Close()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(time.Second):
		t.Fatal("should have been able to open write fd after closing read fd")
	case <-done:
	}

	wfd, err := fi.Open(Flags{Write: true, Sync: true})
	if err != nil {
		t.Fatal(err)
	}

	_, err = wfd.Read([]byte{})
	if err == nil {
		t.Fatal("shouldnt have been able to read from write only filedescriptor")
	}

	_, err = wfd.Write([]byte{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTruncateAtSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ds, rt := setupRoot(ctx, t)

	dir := rt.GetDirectory()

	nd := dag.NodeWithData(ft.FilePBData(nil, 0))
	fi, err := NewFile("test", nd, dir, ds)
	if err != nil {
		t.Fatal(err)
	}

	fd, err := fi.Open(Flags{Read: true, Write: true, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()
	_, err = fd.Write([]byte("test"))
	if err != nil {
		t.Fatal(err)
	}
	fd.Truncate(4)
}

func TestTruncateAndWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ds, rt := setupRoot(ctx, t)

	dir := rt.GetDirectory()

	nd := dag.NodeWithData(ft.FilePBData(nil, 0))
	fi, err := NewFile("test", nd, dir, ds)
	if err != nil {
		t.Fatal(err)
	}

	fd, err := fi.Open(Flags{Read: true, Write: true, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()
	for i := 0; i < 200; i++ {
		err = fd.Truncate(0)
		if err != nil {
			t.Fatal(err)
		}
		l, err := fd.Write([]byte("test"))
		if err != nil {
			t.Fatal(err)
		}
		if l != len("test") {
			t.Fatal("incorrect write length")
		}

		_, err = fd.Seek(0, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}

		data, err := io.ReadAll(fd)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "test" {
			t.Fatalf("read error at read %d, read: %v", i, data)
		}
	}
}

func TestFSNodeType(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ds, rt := setupRoot(ctx, t)

	// check for IsDir
	nd := dag.NodeWithData(ft.FolderPBData())
	di, err := NewDirectory(ctx, "test", nd, rt.GetDirectory(), ds)
	if err != nil {
		t.Fatal(err)
	}
	ret := IsDir(di)
	if !ret {
		t.Fatal("FSNode type should be dir, but not")
	}

	// check for IsFile
	fnd := dag.NodeWithData(ft.FilePBData(nil, 0))
	fi, err := NewFile("test", fnd, rt.GetDirectory(), ds)
	if err != nil {
		t.Fatal(err)
	}
	ret = IsFile(fi)
	if !ret {
		t.Fatal("FSNode type should be file, but not")
	}
}

func getParentDir(root *Root, dir string) (*Directory, error) {
	parent, err := Lookup(root, dir)
	if err != nil {
		return nil, err
	}

	pdir, ok := parent.(*Directory)
	if !ok {
		return nil, errors.New("expected *Directory, didn't get it. This is likely a race condition")
	}
	return pdir, nil
}

func getFileHandle(r *Root, path string, create bool, builder cid.Builder) (*File, error) {
	target, err := Lookup(r, path)
	switch err {
	case nil:
		fi, ok := target.(*File)
		if !ok {
			return nil, fmt.Errorf("%s was not a file", path)
		}
		return fi, nil

	case os.ErrNotExist:
		if !create {
			return nil, err
		}

		// if create is specified and the file doesn't exist, we create the file
		dirname, fname := gopath.Split(path)
		pdir, err := getParentDir(r, dirname)
		if err != nil {
			return nil, err
		}

		if builder == nil {
			builder = pdir.GetCidBuilder()
		}

		nd := dag.NodeWithData(ft.FilePBData(nil, 0))
		nd.SetCidBuilder(builder)
		err = pdir.AddChild(fname, nd)
		if err != nil {
			return nil, err
		}

		fsn, err := pdir.Child(fname)
		if err != nil {
			return nil, err
		}

		fi, ok := fsn.(*File)
		if !ok {
			return nil, errors.New("expected *File, didn't get it. This is likely a race condition")
		}
		return fi, nil

	default:
		return nil, err
	}
}

func FuzzMkdirAndWriteConcurrently(f *testing.F) {
	testCases := []struct {
		flush     bool
		mkparents bool
		dir       string

		filepath string
		content  []byte
	}{
		{
			flush:     true,
			mkparents: true,
			dir:       "/test/dir1",

			filepath: "/test/dir1/file.txt",
			content:  []byte("file content on dir 1"),
		},
		{
			flush:     true,
			mkparents: false,
			dir:       "/test/dir2",

			filepath: "/test/dir2/file.txt",
			content:  []byte("file content on dir 2"),
		},
		{
			flush:     false,
			mkparents: true,
			dir:       "/test/dir3",

			filepath: "/test/dir3/file.txt",
			content:  []byte("file content on dir 3"),
		},
		{
			flush:     false,
			mkparents: false,
			dir:       "/test/dir4",

			filepath: "/test/dir4/file.txt",
			content:  []byte("file content on dir 4"),
		},
	}

	for _, tc := range testCases {
		f.Add(tc.flush, tc.mkparents, tc.dir, tc.filepath, tc.content)
	}

	_, root := setupRoot(context.Background(), f)

	f.Fuzz(func(t *testing.T, flush bool, mkparents bool, dir string, filepath string, filecontent []byte) {
		err := Mkdir(root, dir, MkdirOpts{
			Mkparents: mkparents,
			Flush:     flush,
		})
		if err != nil {
			t.Logf("error making dir %s: %s", dir, err)
			return
		}

		fi, err := getFileHandle(root, filepath, true, nil)
		if err != nil {
			t.Logf("error getting file handle on path %s: %s", filepath, err)
			return
		}
		wfd, err := fi.Open(Flags{Write: true, Sync: flush})
		if err != nil {
			t.Logf("error opening file from filepath %s: %s", filepath, err)
			return
		}

		t.Cleanup(func() {
			wfd.Close()
		})

		_, err = wfd.Write(filecontent)
		if err != nil {
			t.Logf("error writing to file from filepath %s: %s", filepath, err)
		}
	})
}
