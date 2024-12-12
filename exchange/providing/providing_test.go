package providing

import (
	"context"
	"testing"
	"time"

	testinstance "github.com/ipfs/boxo/bitswap/testinstance"
	tn "github.com/ipfs/boxo/bitswap/testnet"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/provider"
	mockrouting "github.com/ipfs/boxo/routing/mock"
	delay "github.com/ipfs/go-ipfs-delay"
	"github.com/ipfs/go-test/random"
)

func TestExchange(t *testing.T) {
	ctx := context.Background()
	net := tn.VirtualNetwork(delay.Fixed(0))
	routing := mockrouting.NewServer()
	sg := testinstance.NewTestInstanceGenerator(net, routing, nil, nil)
	i := sg.Next()
	provFinder := routing.Client(i.Identity)
	prov, err := provider.New(i.Datastore,
		provider.Online(provFinder),
	)
	if err != nil {
		t.Fatal(err)
	}
	provExchange := New(i.Exchange, prov)
	// write-through so that we notify when re-adding block
	bs := blockservice.New(i.Blockstore, provExchange,
		blockservice.WriteThrough(true))
	block := random.BlocksOfSize(1, 10)[0]
	// put it on the blockstore of the first instance
	err = i.Blockstore.Put(ctx, block)
	if err != nil {
		t.Fatal()
	}

	// Trigger reproviding, otherwise it's not really provided.
	err = prov.Reprovide(ctx)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	providersChan := provFinder.FindProvidersAsync(ctx, block.Cid(), 1)
	_, ok := <-providersChan
	if ok {
		t.Fatal("there should be no providers yet for block")
	}

	// Now add it via BlockService. It should trigger NotifyNewBlocks
	// on the exchange and thus they should get announced.
	err = bs.AddBlock(ctx, block)
	if err != nil {
		t.Fatal()
	}
	// Trigger reproviding, otherwise it's not really provided.
	err = prov.Reprovide(ctx)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	providersChan = provFinder.FindProvidersAsync(ctx, block.Cid(), 1)
	_, ok = <-providersChan
	if !ok {
		t.Fatal("there should be one provider for the block")
	}
}
