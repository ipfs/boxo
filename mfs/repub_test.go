package mfs

import (
	"context"
	"testing"
	"time"

	cid "github.com/ipfs/go-cid"
	ci "github.com/libp2p/go-libp2p-testing/ci"
)

func TestRepublisher(t *testing.T) {
	if ci.IsRunning() {
		t.Skip("dont run timing tests in CI")
	}

	ctx := context.TODO()

	pub := make(chan struct{})

	pf := func(ctx context.Context, c cid.Cid) error {
		pub <- struct{}{}
		return nil
	}

	testCid1, _ := cid.Parse("QmeomffUNfmQy76CQGy9NdmqEnnHU9soCexBnGU3ezPHVH")
	testCid2, _ := cid.Parse("QmeomffUNfmQy76CQGy9NdmqEnnHU9soCexBnGU3ezPHVX")

	tshort := time.Millisecond * 50
	tlong := time.Second / 2

	rp := NewRepublisher(ctx, pf, tshort, tlong)
	go rp.Run(cid.Undef)

	rp.Update(testCid1)

	// should hit short timeout
	select {
	case <-time.After(tshort * 2):
		t.Fatal("publish didnt happen in time")
	case <-pub:
	}

	cctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			rp.Update(testCid2)
			time.Sleep(time.Millisecond * 10)
			select {
			case <-cctx.Done():
				return
			default:
			}
		}
	}()

	select {
	case <-pub:
		t.Fatal("shouldnt have received publish yet!")
	case <-time.After((tlong * 9) / 10):
	}
	select {
	case <-pub:
	case <-time.After(tlong / 2):
		t.Fatal("waited too long for pub!")
	}

	cancel()

	err := rp.Close()
	if err != nil {
		t.Fatal(err)
	}
}
