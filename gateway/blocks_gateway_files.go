package gateway

import (
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/go-unixfs"
)

type wrappedLink struct {
	link *format.Link
}

func (w *wrappedLink) Size() (int64, error) {
	return int64(w.link.Size), nil
}

func (w *wrappedLink) Cid() cid.Cid {
	return w.link.Cid
}

func (w *wrappedLink) Close() error {
	return nil
}

type wrappedDirLinkChan struct {
	dir  <-chan unixfs.LinkResult
	err  error
	name string
	node *wrappedLink
}

func (it *wrappedDirLinkChan) Next() bool {
	result, ok := <-it.dir
	if !ok {
		return false
	}

	if result.Err != nil {
		it.err = result.Err
		return false
	}

	it.name = result.Link.Name
	it.node = &wrappedLink{
		link: result.Link,
	}
	return true
}

func (it *wrappedDirLinkChan) Err() error {
	return it.err
}

func (it *wrappedDirLinkChan) Name() string {
	return it.name
}

func (e *wrappedDirLinkChan) Node() files.Node {
	return e.node
}

type wrappedDirectory struct {
	node files.Node
	dir  <-chan unixfs.LinkResult
}

func (d *wrappedDirectory) Close() error {
	return d.node.Close()
}

func (d *wrappedDirectory) Size() (int64, error) {
	return d.node.Size()
}

func (d *wrappedDirectory) Entries() files.DirIterator {
	return &wrappedDirLinkChan{dir: d.dir}
}

var _ files.Directory = &wrappedDirectory{}
