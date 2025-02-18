package unixfs

import (
	"bytes"
	"os"
	"testing"
	"time"

	pb "github.com/ipfs/boxo/ipld/unixfs/pb"
	"google.golang.org/protobuf/proto"
)

func TestFSNode(t *testing.T) {
	fsn := NewFSNode(TFile)
	for i := 0; i < 16; i++ {
		fsn.AddBlockSize(100)
	}
	fsn.RemoveBlockSize(15)

	fsn.SetData(make([]byte, 128))

	b, err := fsn.GetBytes()
	if err != nil {
		t.Fatal(err)
	}

	pbn := new(pb.Data)
	err = proto.Unmarshal(b, pbn)
	if err != nil {
		t.Fatal(err)
	}

	ds, err := DataSize(b)
	if err != nil {
		t.Fatal(err)
	}
	nKids := fsn.NumChildren()
	if nKids != 15 {
		t.Fatal("Wrong number of child nodes")
	}

	if ds != (100*15)+128 {
		t.Fatal("Datasize calculations incorrect!")
	}

	nfsn, err := FSNodeFromBytes(b)
	if err != nil {
		t.Fatal(err)
	}

	if nfsn.FileSize() != (100*15)+128 {
		t.Fatal("fsNode FileSize calculations incorrect")
	}
}

func TestPBdataTools(t *testing.T) {
	raw := []byte{0x00, 0x01, 0x02, 0x17, 0xA1}
	rawPB := WrapData(raw)

	pbDataSize, err := DataSize(rawPB)
	if err != nil {
		t.Fatal(err)
	}

	same := len(raw) == int(pbDataSize)
	if !same {
		t.Fatal("WrapData changes the size of data.")
	}

	rawPBBytes, err := UnwrapData(rawPB)
	if err != nil {
		t.Fatal(err)
	}

	same = bytes.Equal(raw, rawPBBytes)
	if !same {
		t.Fatal("Unwrap failed to produce the correct wrapped data.")
	}

	rawPBdata, err := FSNodeFromBytes(rawPB)
	if err != nil {
		t.Fatal(err)
	}

	isRaw := rawPBdata.Type() == TRaw
	if !isRaw {
		t.Fatal("WrapData does not create pb.Data_Raw!")
	}

	catFile := []byte("Mr_Meowgie.gif")
	catPBfile := FilePBData(catFile, 17)
	catSize, err := DataSize(catPBfile)
	if catSize != 17 {
		t.Fatal("FilePBData is the wrong size.")
	}
	if err != nil {
		t.Fatal(err)
	}

	dirPB := FolderPBData()
	dir, err := FSNodeFromBytes(dirPB)
	isDir := dir.Type() == TDirectory
	if !isDir {
		t.Fatal("FolderPBData does not create a directory!")
	}
	if err != nil {
		t.Fatal(err)
	}
	_, dirErr := DataSize(dirPB)
	if dirErr == nil {
		t.Fatal("DataSize didn't throw an error when taking the size of a directory.")
	}

	catSym, err := SymlinkData("/ipfs/adad123123/meowgie.gif")
	if err != nil {
		t.Fatal(err)
	}

	catSymPB, err := FSNodeFromBytes(catSym)
	isSym := catSymPB.Type() == TSymlink
	if !isSym {
		t.Fatal("Failed to make a Symlink.")
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestSymlinkFilesize(t *testing.T) {
	path := "/ipfs/adad123123/meowgie.gif"
	sym, err := SymlinkData(path)
	if err != nil {
		t.Fatal(err)
	}
	size, err := DataSize(sym)
	if err != nil {
		t.Fatal(err)
	}
	if int(size) != len(path) {
		t.Fatalf("size mismatch: %d != %d", size, len(path))
	}
}

func TestMetadata(t *testing.T) {
	meta := &Metadata{
		MimeType: "audio/aiff",
		Size:     12345,
	}

	_, err := meta.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	metaPB, err := BytesForMetadata(meta)
	if err != nil {
		t.Fatal(err)
	}

	meta, err = MetadataFromBytes(metaPB)
	if err != nil {
		t.Fatal(err)
	}

	mimeAiff := meta.MimeType == "audio/aiff"
	if !mimeAiff {
		t.Fatal("Metadata does not Marshal and Unmarshal properly!")
	}
}

func TestIsDir(t *testing.T) {
	prepares := map[pb.Data_DataType]bool{
		TDirectory: true,
		THAMTShard: true,
		TFile:      false,
		TMetadata:  false,
		TRaw:       false,
		TSymlink:   false,
	}
	for typ, v := range prepares {
		fsn := NewFSNode(typ)
		if fsn.IsDir() != v {
			t.Fatalf("type %v, IsDir() should be %v, but %v", typ, v, fsn.IsDir())
		}
	}
}

func (n *FSNode) getPbData(t *testing.T) *pb.Data {
	b, err := n.GetBytes()
	if err != nil {
		t.Fatal(err)
	}

	pbn := new(pb.Data)
	err = proto.Unmarshal(b, pbn)
	if err != nil {
		t.Fatal(err)
	}
	return pbn
}

func TestMode(t *testing.T) {
	fsn := NewFSNode(TDirectory)
	fsn.SetMode(1)
	if !fsn.Mode().IsDir() {
		t.Fatal("expected mode for directory")
	}

	fsn = NewFSNode(TSymlink)
	fsn.SetMode(1)
	if fsn.Mode()&os.ModeSymlink != os.ModeSymlink {
		t.Fatal("expected mode for symlink")
	}

	fsn = NewFSNode(TFile)

	// not stored
	if fsn.Mode() != 0 {
		t.Fatal("expected mode not to be set")
	}

	fileMode := os.FileMode(0o640)
	fsn.SetMode(fileMode)
	if !fsn.Mode().IsRegular() {
		t.Fatal("expected a regular file mode")
	}
	mode := fsn.Mode()

	if mode&os.ModePerm != fileMode {
		t.Fatalf("expected permissions to be %O but got %O", fileMode, mode&os.ModePerm)
	}
	if mode&0xFFFFF000 != 0 {
		t.Fatalf("expected high-order 20 bits of mode to be clear but got %b", (mode&0xFFFFF000)>>12)
	}

	fsn.SetMode(fileMode | os.ModeSticky)
	mode = fsn.Mode()
	if mode&os.ModePerm != fileMode&os.ModePerm {
		t.Fatalf("expected permissions to be %O but got %O", fileMode, mode&os.ModePerm)
	}
	if mode&os.ModeSticky == 0 {
		t.Fatal("expected permissions to have sticky bit set")
	}
	if mode&os.ModeSetuid != 0 {
		t.Fatal("expected permissions to have setuid bit unset")
	}
	if mode&os.ModeSetgid != 0 {
		t.Fatal("expected permissions to have setgid bit unset")
	}

	fsn.SetMode(fileMode | os.ModeSticky | os.ModeSetuid)
	mode = fsn.Mode()
	if mode&os.ModePerm != fileMode&os.ModePerm {
		t.Fatalf("expected permissions to be %O but got %O", fileMode, mode&os.ModePerm)
	}
	if mode&os.ModeSticky == 0 {
		t.Fatal("expected permissions to have sticky bit set")
	}
	if mode&os.ModeSetuid == 0 {
		t.Fatal("expected permissions to have setuid bit set")
	}
	if mode&os.ModeSetgid != 0 {
		t.Fatal("expected permissions to have setgid bit unset")
	}

	fsn.SetMode(fileMode | os.ModeSetuid | os.ModeSetgid)
	mode = fsn.Mode()
	if mode&os.ModePerm != fileMode&os.ModePerm {
		t.Fatalf("expected permissions to be %O but got %O", fileMode, mode&os.ModePerm)
	}
	if mode&os.ModeSticky != 0 {
		t.Fatal("expected permissions to have sticky bit unset")
	}
	if mode&os.ModeSetuid == 0 {
		t.Fatal("expected permissions to have setuid bit set")
	}
	if mode&os.ModeSetgid == 0 {
		t.Fatal("expected permissions to have setgid bit set")
	}

	// check the internal format (unix permissions)
	fsn.SetMode(fileMode | os.ModeSetuid | os.ModeSticky)
	pbn := fsn.getPbData(t)
	// unix perms setuid and sticky bits should also be set
	expected := uint32(0o5000 | (fileMode & os.ModePerm))
	if *pbn.Mode != expected {
		t.Fatalf("expected stored permissions to be %O but got %O", expected, *pbn.Mode)
	}

	fsn.SetMode(0)
	pbn = fsn.getPbData(t)
	if pbn.Mode != nil {
		t.Fatal("expected file mode to be unset")
	}

	fsn.SetExtendedMode(1)
	fsn.SetMode(0)
	pbn = fsn.getPbData(t)
	if pbn.Mode == nil {
		t.Fatal("expected extended mode to be preserved")
	}
}

func TestExtendedMode(t *testing.T) {
	fsn := NewFSNode(TFile)
	fsn.SetMode(os.ModePerm | os.ModeSetuid | os.ModeSticky)
	const expectedUnixMode = uint32(0o5777)

	expectedExtMode := uint32(0xAAAAA)
	fsn.SetExtendedMode(expectedExtMode)
	extMode := fsn.ExtendedMode()
	if extMode != expectedExtMode {
		t.Fatalf("expected extended mode to be %X but got %X", expectedExtMode, extMode)
	}
	pbn := fsn.getPbData(t)
	expectedPbMode := (expectedExtMode << 12) | (expectedUnixMode & 0xFFF)
	if *pbn.Mode != expectedPbMode {
		t.Fatalf("expected stored mode to be %b but got %b", expectedPbMode, *pbn.Mode)
	}

	expectedExtMode = uint32(0x55555)
	fsn.SetExtendedMode(expectedExtMode)
	extMode = fsn.ExtendedMode()
	if extMode != expectedExtMode {
		t.Fatalf("expected extended mode to be %X but got %X", expectedExtMode, extMode)
	}
	pbn = fsn.getPbData(t)
	expectedPbMode = (expectedExtMode << 12) | (expectedUnixMode & 0xFFF)
	if *pbn.Mode != expectedPbMode {
		t.Fatalf("expected stored mode to be %b but got %b", expectedPbMode, *pbn.Mode)
	}

	// ignore bits 21..32
	expectedExtMode = uint32(0xFFFFF)
	fsn.SetExtendedMode(0xAAAFFFFF)
	extMode = fsn.ExtendedMode()
	if extMode != expectedExtMode {
		t.Fatalf("expected extended mode to be %X but got %X", expectedExtMode, extMode)
	}
	pbn = fsn.getPbData(t)
	expectedPbMode = (expectedExtMode << 12) | (expectedUnixMode & 0xFFF)
	if *pbn.Mode != expectedPbMode {
		t.Fatalf("expected raw mode to be %b but got %b", expectedPbMode, *pbn.Mode)
	}

	fsn.SetMode(0)
	fsn.SetExtendedMode(0)
	pbn = fsn.getPbData(t)
	if pbn.Mode != nil {
		t.Fatal("expected file mode to be unset")
	}
}

func (n *FSNode) setPbModTime(seconds *int64, nanos *uint32) {
	if n.format.Mtime == nil {
		n.format.Mtime = &pb.IPFSTimestamp{}
	}

	n.format.Mtime.Seconds = seconds
	n.format.Mtime.Nanos = nanos
}

func TestModTime(t *testing.T) {
	tm := time.Now()
	expectedUnix := tm.Unix()
	n := NewFSNode(TFile)

	// not stored
	mt := n.ModTime()
	if !mt.IsZero() {
		t.Fatal("expected modification time not to be set")
	}

	// valid timestamps
	n.SetModTime(tm)
	mt = n.ModTime()
	if !mt.Equal(tm) {
		t.Fatalf("expected modification time to be %v but got %v", tm, mt)
	}

	tm = time.Unix(expectedUnix, 0)
	n.SetModTime(tm)
	pbn := n.getPbData(t)
	if pbn.Mtime.Nanos != nil {
		t.Fatal("expected nanoseconds to be nil")
	}
	mt = n.ModTime()
	if !mt.Equal(tm) {
		t.Fatalf("expected modification time to be %v but got %v", tm, mt)
	}

	tm = time.Unix(expectedUnix, 3489753)
	n.SetModTime(tm)
	mt = n.ModTime()
	if !mt.Equal(tm) {
		t.Fatalf("expected modification time to be %v but got %v", tm, mt)
	}

	tm = time.Time{}
	n.SetModTime(tm)
	pbn = n.getPbData(t)
	if pbn.Mtime != nil {
		t.Fatal("expected modification time to be unset")
	}
	mt = n.ModTime()
	if !mt.Equal(tm) {
		t.Fatalf("expected modification time to be %v but got %v", tm, mt)
	}

	n.setPbModTime(&expectedUnix, nil)
	mt = n.ModTime()
	if !mt.Equal(time.Unix(expectedUnix, 0)) {
		t.Fatalf("expected modification time to be %v but got %v", time.Unix(expectedUnix, 0), mt)
	}

	// invalid timestamps
	n.setPbModTime(nil, proto.Uint32(1000))
	mt = n.ModTime()
	if !mt.IsZero() {
		t.Fatal("expected modification time not to be set")
	}

	n.setPbModTime(&expectedUnix, proto.Uint32(0))
	mt = n.ModTime()
	if !mt.IsZero() {
		t.Fatal("expected modification time not to be set")
	}

	n.setPbModTime(&expectedUnix, proto.Uint32(1000000000))
	mt = n.ModTime()
	if !mt.IsZero() {
		t.Fatal("expected modification time not to be set")
	}
}
