package main

import (
	"fmt"
	"io"
	"os"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

// DetachCar is a command to output the index part of a car.
func DetachCar(c *cli.Context) error {
	r, err := carv2.OpenReader(c.Args().Get(0))
	if err != nil {
		return err
	}
	defer r.Close()

	if !r.Header.HasIndex() {
		return fmt.Errorf("no index present")
	}

	outStream := os.Stdout
	if c.Args().Len() >= 2 {
		outStream, err = os.Create(c.Args().Get(1))
		if err != nil {
			return err
		}
	}
	defer outStream.Close()

	ir, err := r.IndexReader()
	if err != nil {
		return err
	}
	_, err = io.Copy(outStream, ir)
	return err
}

// DetachCarList prints a list of what's found in a detached index.
func DetachCarList(c *cli.Context) error {
	var err error

	inStream := os.Stdin
	if c.Args().Len() >= 1 {
		inStream, err = os.Open(c.Args().First())
		if err != nil {
			return err
		}
		defer inStream.Close()
	}

	idx, err := index.ReadFrom(inStream)
	if err != nil {
		return err
	}

	if iidx, ok := idx.(index.IterableIndex); ok {
		err := iidx.ForEach(func(mh multihash.Multihash, offset uint64) error {
			fmt.Printf("%s %d\n", mh, offset)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}

	return fmt.Errorf("index of codec %s is not iterable", idx.Codec())
}
