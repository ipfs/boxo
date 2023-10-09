package main

import (
	"fmt"
	"io"
	"os"

	"github.com/ipfs/boxo/unixfs/feather"
	"github.com/ipfs/go-cid"
)

func main() {
	err := mainRet()
	if err != nil {
		os.Stderr.WriteString(err.Error())
		os.Stderr.WriteString("\n")
		os.Exit(1)
	}
	os.Exit(0)
}

func parseArgs() (cid.Cid, error) {
	if len(os.Args) != 2 {
		return cid.Cid{}, fmt.Errorf("expected one argument")
	}

	return cid.Decode(os.Args[1])
}

func mainRet() error {
	c, err := parseArgs()
	if err != nil {
		return fmt.Errorf(`%w
Usage:
%s <CID>

Example:
%s bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi`, err, os.Args[0], os.Args[0])
	}

	r, err := feather.DownloadFile(c)
	if err != nil {
		return fmt.Errorf("error starting file download: %w", err)
	}
	defer r.Close()

	_, err = io.Copy(os.Stdout, r)
	if err != nil {
		return fmt.Errorf("error downloading file: %w", err)
	}
	return nil
}
