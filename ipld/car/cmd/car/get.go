package main

import (
	"bytes"
	"context"
	"fmt"

	"io"
	"os"

	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/cbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	_ "github.com/ipld/go-ipld-prime/codec/json"
	_ "github.com/ipld/go-ipld-prime/codec/raw"

	"github.com/ipfs/go-cid"
	ipldfmt "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car"
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

	blk, err := bs.Get(c.Context, blkCid)
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
	if c.Args().Len() < 2 {
		return fmt.Errorf("usage: car get-dag [-s selector] <file.car> [root cid] <output file>")
	}

	// if root cid is emitted we'll read it from the root of file.car.
	output := c.Args().Get(1)
	var rootCid cid.Cid

	bs, err := blockstore.OpenReadOnly(c.Args().Get(0))
	if err != nil {
		return err
	}

	if c.Args().Len() == 2 {
		roots, err := bs.Roots()
		if err != nil {
			return err
		}
		if len(roots) != 1 {
			return fmt.Errorf("car file has does not have exactly one root, dag root must be specified explicitly")
		}
		rootCid = roots[0]
	} else {
		rootCid, err = cid.Parse(output)
		if err != nil {
			return err
		}
		output = c.Args().Get(2)
	}

	strict := c.Bool("strict")

	// selector traversal, default to ExploreAllRecursively which only explores the DAG blocks
	// because we only care about the blocks loaded during the walk, not the nodes matched
	sel := selectorParser.CommonSelector_MatchAllRecursively
	if c.IsSet("selector") {
		sel, err = selectorParser.ParseJSONSelector(c.String("selector"))
		if err != nil {
			return err
		}
	}
	linkVisitOnlyOnce := !c.IsSet("selector") // if using a custom selector, this isn't as safe

	switch c.Int("version") {
	case 2:
		return writeCarV2(c.Context, rootCid, output, bs, strict, sel, linkVisitOnlyOnce)
	case 1:
		return writeCarV1(rootCid, output, bs, strict, sel, linkVisitOnlyOnce)
	default:
		return fmt.Errorf("invalid CAR version %d", c.Int("version"))
	}
}

func writeCarV2(ctx context.Context, rootCid cid.Cid, output string, bs *blockstore.ReadOnly, strict bool, sel datamodel.Node, linkVisitOnlyOnce bool) error {
	_ = os.Remove(output)

	outStore, err := blockstore.OpenReadWrite(output, []cid.Cid{rootCid}, blockstore.AllowDuplicatePuts(false))
	if err != nil {
		return err
	}

	ls := cidlink.DefaultLinkSystem()
	ls.KnownReifiers = map[string]linking.NodeReifier{"unixfs": unixfsnode.Reify}
	ls.TrustedStorage = true
	ls.StorageReadOpener = func(_ linking.LinkContext, l datamodel.Link) (io.Reader, error) {
		if cl, ok := l.(cidlink.Link); ok {
			blk, err := bs.Get(ctx, cl.Cid)
			if err != nil {
				if ipldfmt.IsNotFound(err) {
					if strict {
						return nil, err
					}
					return nil, traversal.SkipMe{}
				}
				return nil, err
			}
			if err := outStore.Put(ctx, blk); err != nil {
				return nil, err
			}
			return bytes.NewBuffer(blk.RawData()), nil
		}
		return nil, fmt.Errorf("unknown link type: %T", l)
	}

	nsc := func(lnk datamodel.Link, lctx ipld.LinkContext) (datamodel.NodePrototype, error) {
		if lnk, ok := lnk.(cidlink.Link); ok && lnk.Cid.Prefix().Codec == 0x70 {
			return dagpb.Type.PBNode, nil
		}
		return basicnode.Prototype.Any, nil
	}

	rootLink := cidlink.Link{Cid: rootCid}
	ns, _ := nsc(rootLink, ipld.LinkContext{})
	rootNode, err := ls.Load(ipld.LinkContext{}, rootLink, ns)
	if err != nil {
		return err
	}

	traversalProgress := traversal.Progress{
		Cfg: &traversal.Config{
			LinkSystem:                     ls,
			LinkTargetNodePrototypeChooser: nsc,
			LinkVisitOnlyOnce:              linkVisitOnlyOnce,
		},
	}

	s, err := selector.CompileSelector(sel)
	if err != nil {
		return err
	}

	err = traversalProgress.WalkMatching(rootNode, s, func(p traversal.Progress, n datamodel.Node) error {
		lb, ok := n.(datamodel.LargeBytesNode)
		if ok {
			rs, err := lb.AsLargeBytes()
			if err == nil {
				_, err := io.Copy(io.Discard, rs)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return outStore.Finalize()
}

func writeCarV1(rootCid cid.Cid, output string, bs *blockstore.ReadOnly, strict bool, sel datamodel.Node, linkVisitOnlyOnce bool) error {
	opts := make([]car.Option, 0)
	if linkVisitOnlyOnce {
		opts = append(opts, car.TraverseLinksOnlyOnce())
	}
	sc := car.NewSelectiveCar(context.Background(), bs, []car.Dag{{Root: rootCid, Selector: sel}}, opts...)
	f, err := os.Create(output)
	if err != nil {
		return err
	}
	defer f.Close()

	return sc.Write(f)
}
