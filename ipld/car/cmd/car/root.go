package main

import (
	"fmt"
	"os"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/urfave/cli/v2"
)

// CarRoot prints the root CID in a car
func CarRoot(c *cli.Context) (err error) {
	inStream := os.Stdin
	if c.Args().Len() >= 1 {
		inStream, err = os.Open(c.Args().First())
		if err != nil {
			return err
		}
	}

	rd, err := carv2.NewBlockReader(inStream)
	if err != nil {
		return err
	}
	for _, r := range rd.Roots {
		fmt.Printf("%s\n", r.String())
	}

	return nil
}
