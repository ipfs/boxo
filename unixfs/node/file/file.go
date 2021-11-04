package file

import (
	"context"
	"io"

	"github.com/ipld/go-ipld-prime"
)

// NewUnixFSFile attempts to construct an ipld node from the base protobuf node representing the
// root of a unixfs File.
// It provides a `bytes` view over the file, along with access to io.Reader streaming access
// to file data.
func NewUnixFSFile(ctx context.Context, substrate ipld.Node, lsys *ipld.LinkSystem) (StreamableByteNode, error) {
	if substrate.Kind() == ipld.Kind_Bytes {
		// A raw / single-node file.
		return &singleNodeFile{substrate, 0}, nil
	}
	// see if it's got children.
	links, err := substrate.LookupByString("Links")
	if err != nil {
		return nil, err
	}
	if links.Length() == 0 {
		// no children.
		return newWrappedNode(substrate)
	}

	return &shardNodeFile{
		ctx:       ctx,
		lsys:      lsys,
		substrate: substrate,
		done:      false,
		rdr:       nil}, nil
}

// A StreamableByteNode is an ipld.Node that can be streamed over. It is guaranteed to have a Bytes type.
type StreamableByteNode interface {
	ipld.Node
	io.Reader
}

type singleNodeFile struct {
	ipld.Node
	offset int
}

func (f *singleNodeFile) Read(p []byte) (int, error) {
	buf, err := f.Node.AsBytes()
	if err != nil {
		return 0, err
	}
	if f.offset >= len(buf) {
		return 0, io.EOF
	}
	n := copy(p, buf[f.offset:])
	f.offset += n
	return n, nil
}
