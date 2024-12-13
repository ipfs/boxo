package mfs

import (
	"context"
	"testing"
	"time"

	cid "github.com/ipfs/go-cid"
	ci "github.com/libp2p/go-libp2p-testing/ci"
	"github.com/stretchr/testify/require"
)

func TestRepublisher(t *testing.T) {
	if ci.IsRunning() {
		t.Skip("dont run timing tests in CI")
	}

	pub := make(chan struct{})

	pf := func(ctx context.Context, c cid.Cid) error {
		select {
		case pub <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	testCid1, _ := cid.Parse("QmeomffUNfmQy76CQGy9NdmqEnnHU9soCexBnGU3ezPHVH")
	testCid2, _ := cid.Parse("QmeomffUNfmQy76CQGy9NdmqEnnHU9soCexBnGU3ezPHVX")

	tshort := time.Millisecond * 50
	tlong := time.Second / 2

	rp := NewRepublisher(pf, tshort, tlong, cid.Undef)

	rp.Update(testCid1)

	// should hit short timeout
	select {
	case <-time.After(tshort * 2):
		t.Fatal("publish didnt happen in time")
	case <-pub:
	}

	stopUpdates := make(chan struct{})
	go func() {
		timer := time.NewTimer(time.Hour)
		defer timer.Stop()
		for {
			rp.Update(testCid2)
			timer.Reset(time.Millisecond * 10)
			select {
			case <-timer.C:
			case <-stopUpdates:
				return
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

	close(stopUpdates)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// Check that republishing update does not call pubfunc again
	rp.Update(testCid2)
	err := rp.WaitPub(context.Background())
	require.NoError(t, err)
	select {
	case <-pub:
		t.Fatal("pub func called again with repeated update")
	case <-time.After(tlong * 2):
	}

	// Check that waitpub times out when blocked pubfunc is called
	rp.Update(testCid1)
	err = rp.WaitPub(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Unblock pubfunc.
	<-pub

	err = rp.Close()
	require.NoError(t, err)

	// Check that additional call to Close is OK after republisher stopped.
	err = rp.Close()
	require.NoError(t, err)
}
