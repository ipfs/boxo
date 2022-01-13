package builder

import (
	"fmt"
	"io/fs"
	"os"
	"path"

	"github.com/ipfs/go-unixfsnode/data"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
)

// https://github.com/ipfs/go-ipfs/pull/8114/files#diff-eec963b47a6e1080d9d8023b4e438e6e3591b4154f7379a7e728401d2055374aR319
const shardSplitThreshold = 262144

// https://github.com/ipfs/go-unixfs/blob/ec6bb5a4c5efdc3a5bce99151b294f663ee9c08d/io/directory.go#L29
const defaultShardWidth = 256

// BuildUnixFSRecursive returns a link pointing to the UnixFS node representing
// the file or directory tree pointed to by `root`
func BuildUnixFSRecursive(root string, ls *ipld.LinkSystem) (ipld.Link, uint64, error) {
	info, err := os.Lstat(root)
	if err != nil {
		return nil, 0, err
	}

	m := info.Mode()
	switch {
	case m.IsDir():
		entries, err := os.ReadDir(root)
		if err != nil {
			return nil, 0, err
		}
		lnks := make([]dagpb.PBLink, 0, len(entries))
		for _, e := range entries {
			lnk, sz, err := BuildUnixFSRecursive(path.Join(root, e.Name()), ls)
			if err != nil {
				return nil, 0, err
			}
			entry, err := BuildUnixFSDirectoryEntry(e.Name(), int64(sz), lnk)
			if err != nil {
				return nil, 0, err
			}
			lnks = append(lnks, entry)
		}
		outLnk, err := BuildUnixFSDirectory(lnks, ls)
		return outLnk, 0, err
	case m.Type() == fs.ModeSymlink:
		content, err := os.Readlink(root)
		if err != nil {
			return nil, 0, err
		}
		return BuildUnixFSSymlink(content, ls)
	case m.IsRegular():
		fp, err := os.Open(root)
		if err != nil {
			return nil, 0, err
		}
		defer fp.Close()
		return BuildUnixFSFile(fp, "", ls)
	default:
		return nil, 0, fmt.Errorf("cannot encode non regular file: %s", root)
	}
}

// estimateDirSize estimates if a directory is big enough that it warrents sharding.
// The estimate is the sum over the len(linkName) + bytelen(linkHash)
// https://github.com/ipfs/go-unixfs/blob/master/io/directory.go#L152-L162
func estimateDirSize(entries []dagpb.PBLink) int {
	s := 0
	for _, e := range entries {
		s += len(e.Name.Must().String())
		lnk := e.Hash.Link()
		cl, ok := lnk.(cidlink.Link)
		if ok {
			s += cl.ByteLen()
		} else if lnk == nil {
			s += 0
		} else {
			s += len(lnk.Binary())
		}
	}
	return s
}

// BuildUnixFSDirectory creates a directory link over a collection of entries.
func BuildUnixFSDirectory(entries []dagpb.PBLink, ls *ipld.LinkSystem) (ipld.Link, error) {
	if estimateDirSize(entries) > shardSplitThreshold {
		return BuildUnixFSShardedDirectory(defaultShardWidth, multihash.MURMUR3X64_64, entries, ls)
	}
	ufd, err := BuildUnixFS(func(b *Builder) {
		DataType(b, data.Data_Directory)
	})
	if err != nil {
		return nil, err
	}
	pbb := dagpb.Type.PBNode.NewBuilder()
	pbm, err := pbb.BeginMap(2)
	if err != nil {
		return nil, err
	}
	if err = pbm.AssembleKey().AssignString("Data"); err != nil {
		return nil, err
	}
	if err = pbm.AssembleValue().AssignBytes(data.EncodeUnixFSData(ufd)); err != nil {
		return nil, err
	}
	if err = pbm.AssembleKey().AssignString("Links"); err != nil {
		return nil, err
	}
	lnks, err := pbm.AssembleValue().BeginList(int64(len(entries)))
	if err != nil {
		return nil, err
	}
	// sorting happens in codec-dagpb
	for _, e := range entries {
		if err := lnks.AssembleValue().AssignNode(e); err != nil {
			return nil, err
		}
	}
	if err := lnks.Finish(); err != nil {
		return nil, err
	}
	if err := pbm.Finish(); err != nil {
		return nil, err
	}
	node := pbb.Build()
	return ls.Store(ipld.LinkContext{}, fileLinkProto, node)
}
