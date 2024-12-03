package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/routing/http/client"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

func main() {
	gatewayUrlPtr := flag.String("e", "", "routing v1 endpoint to use")
	timeoutPtr := flag.Int("t", 10, "timeout in seconds for lookup")
	cidPtr := flag.String("cid", "", "cid to find")
	pidPtr := flag.String("peer", "", "peer to find")
	namePtr := flag.String("ipns", "", "ipns name to retrieve record for")
	flag.Parse()

	timeout := time.Duration(*timeoutPtr) * time.Second
	if err := run(os.Stdout, *gatewayUrlPtr, *cidPtr, *pidPtr, *namePtr, timeout); err != nil {
		log.Fatal(err)
	}
}

func run(w io.Writer, gatewayURL, cidStr, pidStr, nameStr string, timeout time.Duration) error {
	// Creates a new Delegated Routing V1 client.
	client, err := client.New(gatewayURL)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if cidStr != "" {
		return findProviders(w, ctx, client, cidStr)
	}
	if pidStr != "" {
		return findPeers(w, ctx, client, pidStr)
	}
	if nameStr != "" {
		return findIPNS(w, ctx, client, nameStr)
	}
	return errors.New("cid or peer must be provided")
}

func findProviders(w io.Writer, ctx context.Context, client *client.Client, cidStr string) error {
	// Parses the given CID to lookup the providers for.
	contentCid, err := cid.Parse(cidStr)
	if err != nil {
		return err
	}

	// Ask for providers providing the given content CID.
	recordsIter, err := client.FindProviders(ctx, contentCid)
	if err != nil {
		return err
	}
	defer recordsIter.Close()
	return printIter(w, recordsIter)
}

func printIter(w io.Writer, iter iter.ResultIter[types.Record]) error {
	// The response is streamed. Alternatively, you could use [iter.ReadAll]
	// to fetch all the results all at once, instead of iterating as they are
	// streamed.
	for iter.Next() {
		res := iter.Val()

		// Check for error, but do not complain if we exceeded the timeout. We are
		// expecting that to happen: we explicitly defined a timeout.
		if res.Err != nil {
			if !errors.Is(res.Err, context.DeadlineExceeded) {
				return res.Err
			}

			return nil
		}

		switch res.Val.GetSchema() {
		case types.SchemaPeer:
			record := res.Val.(*types.PeerRecord)
			fmt.Fprintln(w, record.ID)
			fmt.Fprintln(w, "\tProtocols:", record.Protocols)
			fmt.Fprintln(w, "\tAddresses:", record.Addrs)
		default:
			// You may not want to fail here, it's up to you. You can just handle
			// the schemas you want, or that you know, but not fail.
			log.Printf("unrecognized schema: %s", res.Val.GetSchema())
		}
	}

	return nil
}

func findPeers(w io.Writer, ctx context.Context, client *client.Client, pidStr string) error {
	// Parses the given Peer ID to lookup the information for.
	pid, err := peer.Decode(pidStr)
	if err != nil {
		return err
	}

	// Ask for information about the peer with the given peer ID.
	recordsIter, err := client.FindPeers(ctx, pid)
	if err != nil {
		return err
	}
	defer recordsIter.Close()

	// The response is streamed. Alternatively, you could use [iter.ReadAll]
	// to fetch all the results all at once, instead of iterating as they are
	// streamed.
	for recordsIter.Next() {
		res := recordsIter.Val()

		// Check for error, but do not complain if we exceeded the timeout. We are
		// expecting that to happen: we explicitly defined a timeout.
		if res.Err != nil {
			if !errors.Is(res.Err, context.DeadlineExceeded) {
				return res.Err
			}

			return nil
		}

		fmt.Fprintln(w, res.Val.ID)
		fmt.Fprintln(w, "\tProtocols:", res.Val.Protocols)
		fmt.Fprintln(w, "\tAddresses:", res.Val.Addrs)
	}

	return nil
}

func findIPNS(w io.Writer, ctx context.Context, client *client.Client, nameStr string) error {
	// Parses the given name string to get a record for.
	name, err := ipns.NameFromString(nameStr)
	if err != nil {
		return err
	}

	// Fetch an IPNS record for the given name. [client.Client.GetIPNS] verifies
	// if the retrieved record is valid against the given name, and errors otherwise.
	record, err := client.GetIPNS(ctx, name)
	if err != nil {
		return err
	}

	fmt.Fprintf(w, "/ipns/%s\n", name)
	v, err := record.Value()
	if err != nil {
		return err
	}

	// Since [client.Client.GetIPNS] verifies if the retrieved record is valid, we
	// do not need to verify it again. However, if you were not using this specific
	// client, but using some other tool, you should always validate the IPNS Record
	// using the [ipns.Validate] or [ipns.ValidateWithName] functions.
	fmt.Fprintln(w, "\tSignature: VALID")
	fmt.Fprintln(w, "\tValue:", v.String())
	return nil
}
