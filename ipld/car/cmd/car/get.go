package main

import (
	"fmt"
	"os"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/blockstore"
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
