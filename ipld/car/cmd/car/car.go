package main

import (
	"log"
	"os"

	"github.com/multiformats/go-multicodec"
	"github.com/urfave/cli/v2"
)

func main() { os.Exit(main1()) }

func main1() int {
	app := &cli.App{
		Name:  "car",
		Usage: "Utility for working with car files",
		Commands: []*cli.Command{
			{
				Name:    "create",
				Usage:   "Create a car file",
				Aliases: []string{"c"},
				Action:  CreateCar,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:      "file",
						Aliases:   []string{"f", "output", "o"},
						Usage:     "The car file to write to",
						TakesFile: true,
					},
				},
			},
			{
				Name:   "detach-index",
				Usage:  "Detach an index to a detached file",
				Action: DetachCar,
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
					&cli.BoolFlag{
						Name:  "append",
						Usage: "Append cids to an existing output file",
					},
				},
			},
			{
				Name:    "get-block",
				Aliases: []string{"gb"},
				Usage:   "Get a block out of a car",
				Action:  GetCarBlock,
			},
			{
				Name:    "get-dag",
				Aliases: []string{"gd"},
				Usage:   "Get a dag out of a car",
				Action:  GetCarDag,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "selector",
						Aliases: []string{"s"},
						Usage:   "A selector over the dag",
					},
					&cli.BoolFlag{
						Name:  "strict",
						Usage: "Fail if the selector finds links to blocks not in the original car",
					},
				},
			},
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
					&cli.BoolFlag{
						Name:  "v1",
						Usage: "Write out only the carV1 file. Implies codec of 'none'",
					},
				},
			},
			{
				Name:    "list",
				Aliases: []string{"l"},
				Usage:   "List the CIDs in a car",
				Action:  ListCar,
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "verbose",
						Aliases: []string{"v"},
						Usage:   "Include verbose information about contained blocks",
					},
					&cli.BoolFlag{
						Name:  "unixfs",
						Usage: "List unixfs filesystem from the root of the car",
					},
				},
			},
			{
				Name:    "verify",
				Aliases: []string{"v"},
				Usage:   "Verify a CAR is wellformed",
				Action:  VerifyCar,
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Println(err)
		return 1
	}
	return 0
}
