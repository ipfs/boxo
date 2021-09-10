package main

import (
	"log"
	"os"

	"github.com/multiformats/go-multicodec"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "car",
		Usage: "Utility for working with car files",
		Commands: []*cli.Command{
			{
				Name:    "index",
				Aliases: []string{"i"},
				Usage:   "write out the car with an index",
				Action:  IndexCar,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "codec",
						Aliases: []string{"c"},
						Usage:   "The type of index to write",
						Value:   multicodec.CarMultihashIndexSorted.String(),
					},
				},
			},
			{
				Name:   "detach-index",
				Usage:  "Detach an index to a detached file",
				Action: DetachCar,
			},
			{
				Name:    "list",
				Aliases: []string{"l"},
				Usage:   "List the CIDs in a car",
				Action:  ListCar,
			},
			{
				Name:    "filter",
				Aliases: []string{"f"},
				Usage:   "Filter the CIDs in a car",
				Action:  FilterCar,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:      "cid-file",
						Usage:     "A file to read CIDs from",
						TakesFile: true,
					},
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
