package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
	data "github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/hamt"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/urfave/cli/v2"
)

// ListCar is a command to output the cids in a car.
func ListCar(c *cli.Context) error {
	var err error
	outStream := os.Stdout
	if c.Args().Len() >= 2 {
		outStream, err = os.Create(c.Args().Get(1))
		if err != nil {
			return err
		}
	}
	defer outStream.Close()

	if c.Bool("unixfs") {
		return listUnixfs(c, outStream)
	}

	inStream := os.Stdin
	if c.Args().Len() >= 1 {
		inStream, err = os.Open(c.Args().First())
		if err != nil {
			return err
		}
		defer inStream.Close()
	}

	rd, err := carv2.NewBlockReader(inStream)
	if err != nil {
		return err
	}

	for {
		blk, err := rd.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if c.Bool("verbose") {
			fmt.Fprintf(outStream, "%s: %s\n",
				multicodec.Code(blk.Cid().Prefix().Codec).String(),
				blk.Cid())
			if blk.Cid().Prefix().Codec == uint64(multicodec.DagPb) {
				// parse as dag-pb
				builder := dagpb.Type.PBNode.NewBuilder()
				if err := dagpb.DecodeBytes(builder, blk.RawData()); err != nil {
					fmt.Fprintf(outStream, "\tnot interpretable as dag-pb: %s\n", err)
					continue
				}
				n := builder.Build()
				pbn, ok := n.(dagpb.PBNode)
				if !ok {
					continue
				}
				dl := 0
				if pbn.Data.Exists() {
					dl = len(pbn.Data.Must().Bytes())
				}
				fmt.Fprintf(outStream, "\t%d links. %d bytes\n", pbn.Links.Length(), dl)
				// example link:
				li := pbn.Links.ListIterator()
				max := 3
				for !li.Done() {
					_, l, _ := li.Next()
					max--
					pbl, ok := l.(dagpb.PBLink)
					if ok && max >= 0 {
						hsh := "<unknown>"
						lnk, ok := pbl.Hash.Link().(cidlink.Link)
						if ok {
							hsh = lnk.Cid.String()
						}
						name := "<no name>"
						if pbl.Name.Exists() {
							name = pbl.Name.Must().String()
						}
						size := 0
						if pbl.Tsize.Exists() {
							size = int(pbl.Tsize.Must().Int())
						}
						fmt.Fprintf(outStream, "\t\t%s[%s] %s\n", name, humanize.Bytes(uint64(size)), hsh)
					}
				}
				if max < 0 {
					fmt.Fprintf(outStream, "\t\t(%d total)\n", 3-max)
				}
				// see if it's unixfs.
				ufd, err := data.DecodeUnixFSData(pbn.Data.Must().Bytes())
				if err != nil {
					fmt.Fprintf(outStream, "\tnot interpretable as unixfs: %s\n", err)
					continue
				}
				fmt.Fprintf(outStream, "\tUnixfs %s\n", data.DataTypeNames[ufd.FieldDataType().Int()])
			}
		} else {
			fmt.Fprintf(outStream, "%s\n", blk.Cid())
		}
	}

	return err
}

func listUnixfs(c *cli.Context, outStream io.Writer) error {
	if c.Args().Len() == 0 {
		return fmt.Errorf("must provide file to read from. unixfs reading requires random access")
	}

	bs, err := blockstore.OpenReadOnly(c.Args().First())
	if err != nil {
		return err
	}
	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true
	ls.StorageReadOpener = func(_ ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		cl, ok := l.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("not a cidlink")
		}
		blk, err := bs.Get(c.Context, cl.Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(blk.RawData()), nil
	}

	roots, err := bs.Roots()
	if err != nil {
		return err
	}
	for _, r := range roots {
		if err := printUnixFSNode(c, "", r, &ls, outStream); err != nil {
			return err
		}
	}
	return nil
}

func printUnixFSNode(c *cli.Context, prefix string, node cid.Cid, ls *ipld.LinkSystem, outStream io.Writer) error {
	// it might be a raw file (bytes) node. if so, not actually an error.
	if node.Prefix().Codec == cid.Raw {
		return nil
	}

	pbn, err := ls.Load(ipld.LinkContext{}, cidlink.Link{Cid: node}, dagpb.Type.PBNode)
	if err != nil {
		return err
	}

	pbnode := pbn.(dagpb.PBNode)

	ufd, err := data.DecodeUnixFSData(pbnode.Data.Must().Bytes())
	if err != nil {
		return err
	}

	if ufd.FieldDataType().Int() == data.Data_Directory {
		i := pbnode.Links.Iterator()
		for !i.Done() {
			_, l := i.Next()
			name := path.Join(prefix, l.Name.Must().String())
			fmt.Fprintf(outStream, "%s\n", name)
			// recurse into the file/directory
			cl, err := l.Hash.AsLink()
			if err != nil {
				return err
			}
			if cidl, ok := cl.(cidlink.Link); ok {
				if err := printUnixFSNode(c, name, cidl.Cid, ls, outStream); err != nil {
					return err
				}
			}

		}
	} else if ufd.FieldDataType().Int() == data.Data_HAMTShard {
		hn, err := hamt.AttemptHAMTShardFromNode(c.Context, pbn, ls)
		if err != nil {
			return err
		}
		i := hn.Iterator()
		for !i.Done() {
			n, l := i.Next()
			fmt.Fprintf(outStream, "%s\n", path.Join(prefix, n.String()))
			// recurse into the file/directory
			cl, err := l.AsLink()
			if err != nil {
				return err
			}
			if cidl, ok := cl.(cidlink.Link); ok {
				if err := printUnixFSNode(c, path.Join(prefix, n.String()), cidl.Cid, ls, outStream); err != nil {
					return err
				}
			}
		}
	} else {
		// file, file chunk, symlink, other un-named entities.
		return nil
	}

	return nil
}
