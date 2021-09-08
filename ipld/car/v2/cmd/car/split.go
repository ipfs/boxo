package main

import (
	"fmt"
	"io"
	"os"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/urfave/cli/v2"
)

// SplitCar is a command to output the index part of a car.
func SplitCar(c *cli.Context) error {
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

	_, err = io.Copy(outStream, r.IndexReader())
	return err
}
