package main

import (
	"bytes"
	"fmt"
	"io"
	"os"

	_ "github.com/ipld/go-codec-dagpb"
	_ "github.com/ipld/go-ipld-prime/codec/cbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	_ "github.com/ipld/go-ipld-prime/codec/json"
	_ "github.com/ipld/go-ipld-prime/codec/raw"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipfsbs "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorParser "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/urfave/cli/v2"
)

// GetCarBlock is a command to get a block out of a car
func GetCarBlock(c *cli.Context) error {
	if c.Args().Len() < 2 {
		return fmt.Errorf("usage: car get-block <file.car> <block cid> [output file]")
	}

	bs, err := blockstore.OpenReadOnly(c.Args().Get(0))
	if err != nil {
		return err
	}

	// string to CID
	blkCid, err := cid.Parse(c.Args().Get(1))
	if err != nil {
		return err
	}

	blk, err := bs.Get(blkCid)
	if err != nil {
		return err
	}

	outStream := os.Stdout
	if c.Args().Len() >= 3 {
		outStream, err = os.Create(c.Args().Get(2))
		if err != nil {
			return err
		}
		defer outStream.Close()
	}

	_, err = outStream.Write(blk.RawData())
	return err
}

// GetCarDag is a command to get a dag out of a car
func GetCarDag(c *cli.Context) error {
	if c.Args().Len() < 3 {
		return fmt.Errorf("usage: car get-dag [-s selector] <file.car> <root cid> <output file>")
	}

	bs, err := blockstore.OpenReadOnly(c.Args().Get(0))
	if err != nil {
		return err
	}

	// string to CID
	blkCid, err := cid.Parse(c.Args().Get(1))
	if err != nil {
		return err
	}

	outStore, err := blockstore.OpenReadWrite(c.Args().Get(2), []cid.Cid{blkCid})
	if err != nil {
		return err
	}

	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true
	ls.StorageReadOpener = func(_ linking.LinkContext, l datamodel.Link) (io.Reader, error) {
		if cl, ok := l.(cidlink.Link); ok {
			blk, err := bs.Get(cl.Cid)
			if err != nil {
				if err == ipfsbs.ErrNotFound {
					if c.Bool("strict") {
						return nil, err
					}
					return nil, traversal.SkipMe{}
				}
				return nil, err
			}
			return bytes.NewBuffer(blk.RawData()), nil
		}
		return nil, fmt.Errorf("unknown link type: %T", l)
	}
	ls.StorageWriteOpener = func(_ linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(l datamodel.Link) error {
			if cl, ok := l.(cidlink.Link); ok {
				blk, err := blocks.NewBlockWithCid(buf.Bytes(), cl.Cid)
				if err != nil {
					return err
				}
				return outStore.Put(blk)
			}
			return fmt.Errorf("unknown link type: %T", l)
		}, nil
	}

	rootlnk := cidlink.Link{
		Cid: blkCid,
	}
	node, err := ls.Load(linking.LinkContext{}, rootlnk, basicnode.Prototype.Any)
	if err != nil {
		return err
	}

	// selector traversal
	s, _ := selector.CompileSelector(selectorParser.CommonSelector_MatchAllRecursively)
	if c.IsSet("selector") {
		sn, err := selectorParser.ParseJSONSelector(c.String("selector"))
		if err != nil {
			return err
		}
		s, err = selector.CompileSelector(sn)
		if err != nil {
			return err
		}
	}

	lnkProto := cidlink.LinkPrototype{
		Prefix: blkCid.Prefix(),
	}
	err = traversal.WalkMatching(node, s, func(p traversal.Progress, n datamodel.Node) error {
		if p.LastBlock.Link != nil {
			if cl, ok := p.LastBlock.Link.(cidlink.Link); ok {
				lnkProto = cidlink.LinkPrototype{
					Prefix: cl.Prefix(),
				}
			}
		}
		_, err = ls.Store(linking.LinkContext{}, lnkProto, n)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	return outStore.Finalize()
}
