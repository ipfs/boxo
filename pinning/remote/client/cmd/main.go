package main

import (
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	pinclient "github.com/ipfs/go-pinning-service-http-client"
	"os"
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

	fmt.Println("Adding libp2p home page")
	ps, err := c.Add(ctx, libp2pCid, pinclient.PinOpts.WithName("libp2p_home"))
	if err == nil {
		fmt.Println(ps.GetStatus())
	} else {
		fmt.Println(err)
	}

	fmt.Println("List all pins")
	pins, err := c.LsSync(ctx)
	fmt.Println(err)

	for _, p := range pins {
		fmt.Printf("Pin Name: %s, CID: %s", p.GetPin().GetName(), p.GetPin().GetCid().String())
	}

	fmt.Println("Check on pin status")
	status, err := c.GetStatusByID(ctx, ps.GetId())
	if err == nil {
		fmt.Println(status.GetStatus())
	} else {
		fmt.Println(err)
	}

	fmt.Println("Delete pin")
	err = c.DeleteByID(ctx, ps.GetId())
	if err == nil {
		fmt.Println("Successfully deleted pin")
	} else {
		fmt.Println(err)
	}

	fmt.Println("List all pins")
	pins, err = c.LsSync(ctx)
	fmt.Println(err)

	for _, p := range pins {
		fmt.Printf("Pin Name: %s, CID: %s", p.GetPin().GetName(), p.GetPin().GetCid().String())
	}
}
