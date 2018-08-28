package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"gx/ipfs/QmU9oYpqJsNWwAAJju8CzE7mv4NHAJUDWhoKHqgnhMCBy5/go-car"

	cli "github.com/urfave/cli"
)

var headerCmd = cli.Command{
	Name: "header",
	Action: func(c *cli.Context) error {
		if !c.Args().Present() {
			return fmt.Errorf("must pass a car file to inspect")
		}
		arg := c.Args().First()

		fi, err := os.Open(arg)
		if err != nil {
			return err
		}
		defer fi.Close()

		ch, err := car.ReadHeader(bufio.NewReader(fi))
		if err != nil {
			return err
		}

		b, err := json.MarshalIndent(ch, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))
		return nil
	},
}

var verifyCmd = cli.Command{
	Name: "verify",
	Action: func(c *cli.Context) error {
		if !c.Args().Present() {
			return fmt.Errorf("must pass a car file to inspect")
		}
		arg := c.Args().First()

		fi, err := os.Open(arg)
		if err != nil {
			return err
		}
		defer fi.Close()

		cr, err := car.NewCarReader(fi)
		if err != nil {
			return err
		}

		for {
			_, err := cr.Next()
			switch err {
			case io.EOF:
				return nil
			default:
				return err
			case nil:
			}
		}
	},
}

var lsCmd = cli.Command{
	Name: "ls",
	Action: func(c *cli.Context) error {
		if !c.Args().Present() {
			return fmt.Errorf("must pass a car file to inspect")
		}
		arg := c.Args().First()

		fi, err := os.Open(arg)
		if err != nil {
			return err
		}
		defer fi.Close()

		cr, err := car.NewCarReader(fi)
		if err != nil {
			return err
		}

		for {
			blk, err := cr.Next()
			switch err {
			case io.EOF:
				return nil
			default:
				return err
			case nil:
			}
			fmt.Println(blk.Cid())
		}
	},
}

func main() {
	app := cli.NewApp()
	app.Commands = []cli.Command{
		headerCmd,
		lsCmd,
		verifyCmd,
	}
	app.RunAndExitOnError()
}
