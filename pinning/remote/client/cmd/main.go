package main

import (
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	pinclient "github.com/ipfs/go-pinning-service-http-client"
	"os"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	url, ok := os.LookupEnv("PS_URL")
	if !ok {
		panic("No Pinning Service URL found")
	}

	key, ok := os.LookupEnv("PS_KEY")
	if !ok {
		panic("No Pinning Service API Key found")
	}

	c := pinclient.NewClient(url, key)

	ipfsPgCid, err := cid.Parse("bafybeiayvrj27f65vbecspbnuavehcb3znvnt2strop2rfbczupudoizya")
	if err != nil {
		panic(err)
	}

	libp2pCid, err := cid.Parse("bafybeiejgrxo4p4uofgfzvlg5twrg5w7tfwpf7aciiswfacfbdpevg2xfy")
	if err != nil {
		panic(err)
	}
	_ = ipfsPgCid

	listPins(ctx, c)

	fmt.Println("Adding libp2p home page")
	ps, err := c.Add(ctx, libp2pCid, pinclient.PinOpts.WithName("libp2p"))
	if err == nil {
		fmt.Printf("PinStatus: %v \n", ps)
	} else {
		fmt.Println(err)
	}

	listPins(ctx, c)

	fmt.Println("Check on pin status")
	if ps == nil {
		panic("Skipping pin status check because the pin is null")
	}

	var pinned bool
	for !pinned {
		status, err := c.GetStatusByID(ctx, ps.GetRequestId())
		if err == nil {
			fmt.Println(status.GetStatus())
			pinned = status.GetStatus() == pinclient.StatusPinned
		} else {
			fmt.Println(err)
		}
		time.Sleep(time.Millisecond * 500)
	}

	listPins(ctx, c)

	fmt.Println("Delete pin")
	err = c.DeleteByID(ctx, ps.GetRequestId())
	if err == nil {
		fmt.Println("Successfully deleted pin")
	} else {
		fmt.Println(err)
	}

	listPins(ctx, c)
}

func listPins(ctx context.Context, c *pinclient.Client) {
	fmt.Println("List all pins")
	pins, err := c.LsSync(ctx)
	if err != nil {
		fmt.Println(err)
	} else {
		for _, p := range pins {
			fmt.Printf("Pin: %v \n", p)
		}
	}
}
