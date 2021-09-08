package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	icarv1 "github.com/ipld/go-car/v2/internal/carv1"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "car",
		Usage: "Utility for working with car files",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "codec",
				Aliases: []string{"c"},
				Usage:   "The type of index to write",
				Value:   multicodec.CarMultihashIndexSorted.String(),
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "index",
				Aliases: []string{"i"},
				Usage:   "write out the car with an index",
				Action: func(c *cli.Context) error {
					r, err := carv2.OpenReader(c.Args().Get(0))
					if err != nil {
						return err
					}
					defer r.Close()

					var idx index.Index
					if c.String("codec") != "none" {
						var mc multicodec.Code
						if err := mc.Set(c.String("codec")); err != nil {
							return err
						}
						idx, err = index.New(mc)
						if err != nil {
							return err
						}
					}

					outStream := os.Stdout
					if c.Args().Len() >= 2 {
						outStream, err = os.Create(c.Args().Get(1))
						if err != nil {
							return err
						}
					}
					defer outStream.Close()

					v1r := r.DataReader()

					v2Header := carv2.NewHeader(r.Header.DataSize)
					if c.String("codec") == "none" {
						v2Header.IndexOffset = 0
						if _, err := outStream.Write(carv2.Pragma); err != nil {
							return err
						}
						if _, err := v2Header.WriteTo(outStream); err != nil {
							return err
						}
						if _, err := io.Copy(outStream, v1r); err != nil {
							return err
						}
						return nil
					}

					if _, err := outStream.Write(carv2.Pragma); err != nil {
						return err
					}
					if _, err := v2Header.WriteTo(outStream); err != nil {
						return err
					}

					// collect records as we go through the v1r
					hdr, err := icarv1.ReadHeader(v1r)
					if err != nil {
						return fmt.Errorf("error reading car header: %w", err)
					}
					if err := icarv1.WriteHeader(hdr, outStream); err != nil {
						return err
					}

					records := make([]index.Record, 0)
					var sectionOffset int64
					if sectionOffset, err = v1r.Seek(0, io.SeekCurrent); err != nil {
						return err
					}

					br := bufio.NewReader(v1r)
					for {
						// Read the section's length.
						sectionLen, err := varint.ReadUvarint(br)
						if err != nil {
							if err == io.EOF {
								break
							}
							return err
						}
						if _, err := outStream.Write(varint.ToUvarint(sectionLen)); err != nil {
							return err
						}

						// Null padding; by default it's an error.
						// TODO: integrate corresponding ReadOption
						if sectionLen == 0 {
							// TODO: pad writer to expected length.
							break
						}

						// Read the CID.
						cidLen, c, err := cid.CidFromReader(br)
						if err != nil {
							return err
						}
						records = append(records, index.Record{Cid: c, Offset: uint64(sectionOffset)})
						if _, err := c.WriteBytes(outStream); err != nil {
							return err
						}

						// Seek to the next section by skipping the block.
						// The section length includes the CID, so subtract it.
						remainingSectionLen := int64(sectionLen) - int64(cidLen)
						if _, err := io.CopyN(outStream, br, remainingSectionLen); err != nil {
							return err
						}
						sectionOffset += int64(sectionLen) + int64(varint.UvarintSize(sectionLen))
					}

					if err := idx.Load(records); err != nil {
						return err
					}

					return index.WriteTo(idx, outStream)
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
