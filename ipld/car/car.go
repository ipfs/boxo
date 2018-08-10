package car

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
)

func WriteCar(ctx context.Context, ds format.DAGService, root *cid.Cid, w io.Writer) error {
	tw := tar.NewWriter(w)

	rh := &tar.Header{
		Typeflag: tar.TypeSymlink,
		Name:     "root",
		Linkname: root.String(),
	}
	if err := tw.WriteHeader(rh); err != nil {
		return err
	}

	cw := &carWriter{ds: ds, tw: tw}

	seen := cid.NewSet()
	if err := dag.EnumerateChildren(ctx, cw.enumGetLinks, root, seen.Visit); err != nil {
		return err
	}

	return tw.Flush()
}

func LoadCar(ctx context.Context, bs bstore.Blockstore, r io.Reader) (*cid.Cid, error) {
	tr := tar.NewReader(r)
	root, err := tr.Next()
	if err != nil {
		return nil, err
	}

	if root.Name != "root" || root.Typeflag != tar.TypeSymlink {
		return nil, fmt.Errorf("expected first entry in CAR to by symlink named 'root'")
	}

	rootcid, err := cid.Decode(root.Linkname)
	if err != nil {
		return nil, err
	}

	for {
		obj, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		c, err := cid.Decode(obj.Name)
		if err != nil {
			return nil, err
		}

		// safety 1st
		limr := io.LimitReader(tr, 2<<20)
		data, err := ioutil.ReadAll(limr)
		if err != nil {
			return nil, err
		}

		hashed, err := c.Prefix().Sum(data)
		if err != nil {
			return nil, err
		}

		if !hashed.Equals(c) {
			return nil, fmt.Errorf("mismatch in content integrity, name: %s, data: %s", c, hashed)
		}

		blk, err := blocks.NewBlockWithCid(data, c)
		if err != nil {
			return nil, err
		}

		if err := bs.Put(blk); err != nil {
			return nil, err
		}
	}

	return rootcid, nil
}

type carWriter struct {
	ds format.DAGService
	tw *tar.Writer
}

func (cw *carWriter) enumGetLinks(ctx context.Context, c *cid.Cid) ([]*format.Link, error) {
	nd, err := cw.ds.Get(ctx, c)
	if err != nil {
		return nil, err
	}

	if err := cw.writeNode(ctx, nd); err != nil {
		return nil, err
	}

	return nd.Links(), nil
}

func (cw *carWriter) writeNode(ctx context.Context, nd format.Node) error {
	hdr := &tar.Header{
		Name:     nd.Cid().String(),
		Typeflag: tar.TypeReg,
		Size:     int64(len(nd.RawData())),
	}

	if err := cw.tw.WriteHeader(hdr); err != nil {
		return err
	}

	if _, err := cw.tw.Write(nd.RawData()); err != nil {
		return err
	}

	return nil
}
