package main

import (
	"fmt"
	"io"
	"os"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/urfave/cli/v2"
)

// ListCar is a command to output the cids in a car.
func ListCar(c *cli.Context) error {
	inStream := os.Stdin
	var err error
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

	outStream := os.Stdout
	if c.Args().Len() >= 2 {
		outStream, err = os.Create(c.Args().Get(1))
		if err != nil {
			return err
		}
	}
	defer outStream.Close()

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
		fmt.Fprintf(outStream, "%s\n", blk.Cid())
	}

	return err
}
