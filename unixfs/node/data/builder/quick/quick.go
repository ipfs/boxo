// Package quickbuilder is designed as a replacement for the existing ipfs-files
// constructor for a simple way to generate synthetic directory trees.
package quickbuilder

import (
	"bytes"

	"github.com/ipfs/go-unixfsnode/data/builder"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
)

// A Node represents the most basic form of a file or directory
type Node interface {
	Size() (int64, error)
	Link() ipld.Link
}

type lnkNode struct {
	link ipld.Link
	size int64
	ls   *ipld.LinkSystem
}

func (ln *lnkNode) Size() (int64, error) {
	return ln.size, nil
}

func (ln *lnkNode) Link() ipld.Link {
	return ln.link
}

// Builder provides the linksystem context for saving files & directories
type Builder struct {
	ls *ipld.LinkSystem
}

// NewMapDirectory creates a unixfs directory from a list of named entries
func (b *Builder) NewMapDirectory(entries map[string]Node) Node {
	lnks := make([]dagpb.PBLink, 0, len(entries))
	for name, e := range entries {
		sz, _ := e.Size()
		entry, err := builder.BuildUnixFSDirectoryEntry(name, sz, e.Link())
		if err != nil {
			return nil
		}
		lnks = append(lnks, entry)
	}
	n, size, err := builder.BuildUnixFSDirectory(lnks, b.ls)
	if err != nil {
		panic(err)
	}
	return &lnkNode{
		n,
		int64(size),
		b.ls,
	}
}

// NewBytesFile creates a unixfs file from byte contents
func (b *Builder) NewBytesFile(data []byte) Node {
	n, size, err := builder.BuildUnixFSFile(bytes.NewReader(data), "", b.ls)
	if err != nil {
		panic(err)
	}
	return &lnkNode{
		n,
		int64(size),
		b.ls,
	}
}

// Store provides a builder context for making unixfs files and directories
func Store(ls *ipld.LinkSystem, cb func(b *Builder) error) error {
	b := Builder{ls}
	return cb(&b)
}
