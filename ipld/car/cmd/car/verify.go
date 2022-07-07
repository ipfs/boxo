package main

import (
	"fmt"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

// VerifyCar is a command to check a files validity
func VerifyCar(c *cli.Context) error {
	if c.Args().Len() == 0 {
		return fmt.Errorf("usage: car verify <file.car>")
	}

	// header
	rx, err := carv2.OpenReader(c.Args().First())
	if err != nil {
		return err
	}
	defer rx.Close()
	roots, err := rx.Roots()
	if err != nil {
		return err
	}
	if len(roots) == 0 {
		return fmt.Errorf("no roots listed in car header")
	}
	rootMap := make(map[cid.Cid]struct{})
	for _, r := range roots {
		rootMap[r] = struct{}{}
	}

	if rx.Version == 2 {
		if rx.Header.DataSize == 0 {
			return fmt.Errorf("size of wrapped v1 car listed as '0'")
		}

		flen, err := os.Stat(c.Args().First())
		if err != nil {
			return err
		}
		lengthToIndex := carv2.PragmaSize + carv2.HeaderSize + rx.Header.DataSize
		if uint64(flen.Size()) > lengthToIndex && rx.Header.IndexOffset == 0 {
			return fmt.Errorf("header claims no index, but extra bytes in file beyond data size")
		}
		if rx.Header.DataOffset < carv2.PragmaSize+carv2.HeaderSize {
			return fmt.Errorf("data offset places data within carv2 header")
		}
		if rx.Header.IndexOffset < lengthToIndex {
			return fmt.Errorf("index offset overlaps with data. data ends at %d. index offset of %d", lengthToIndex, rx.Header.IndexOffset)
		}
	}

	// blocks
	fd, err := os.Open(c.Args().First())
	if err != nil {
		return err
	}
	rd, err := carv2.NewBlockReader(fd)
	if err != nil {
		return err
	}

	cidList := make([]cid.Cid, 0)
	for {
		blk, err := rd.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		delete(rootMap, blk.Cid())
		cidList = append(cidList, blk.Cid())
	}

	if len(rootMap) > 0 {
		return fmt.Errorf("header lists root(s) not present as a block: %v", rootMap)
	}

	// index
	if rx.Version == 2 && rx.Header.HasIndex() {
		ir, err := rx.IndexReader()
		if err != nil {
			return err
		}
		idx, err := index.ReadFrom(ir)
		if err != nil {
			return err
		}
		for _, c := range cidList {
			cidHash, err := multihash.Decode(c.Hash())
			if err != nil {
				return err
			}
			if cidHash.Code == multihash.IDENTITY {
				continue
			}
			if err := idx.GetAll(c, func(_ uint64) bool {
				return true
			}); err != nil {
				return fmt.Errorf("could not look up known cid %s in index: %w", c, err)
			}
		}
	}

	return nil
}
