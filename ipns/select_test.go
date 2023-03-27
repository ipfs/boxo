package ipns

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	pb "github.com/ipfs/boxo/ipns/pb"

	"github.com/gogo/protobuf/proto"
	u "github.com/ipfs/boxo/util"
	ci "github.com/libp2p/go-libp2p/core/crypto"
)

func shuffle(a []*pb.IpnsEntry) {
	for n := 0; n < 5; n++ {
		for i := range a {
			j := rand.Intn(len(a))
			a[i], a[j] = a[j], a[i]
		}
	}
}

func AssertSelected(r *pb.IpnsEntry, from ...*pb.IpnsEntry) error {
	shuffle(from)
	var vals [][]byte
	for _, r := range from {
		data, err := proto.Marshal(r)
		if err != nil {
			return err
		}
		vals = append(vals, data)
	}

	i, err := selectRecord(from, vals)
	if err != nil {
		return err
	}

	if from[i] != r {
		return fmt.Errorf("selected incorrect record %d", i)
	}

	return nil
}

func TestOrdering(t *testing.T) {
	// select timestamp so selection is deterministic
	ts := time.Unix(1000000, 0)

	// generate a key for signing the records
	r := u.NewSeededRand(15) // generate deterministic keypair
	priv, _, err := ci.GenerateKeyPairWithReader(ci.RSA, 2048, r)
	if err != nil {
		t.Fatal(err)
	}

	e1, err := Create(priv, []byte("foo"), 1, ts.Add(time.Hour), 0)
	if err != nil {
		t.Fatal(err)
	}

	e2, err := Create(priv, []byte("bar"), 2, ts.Add(time.Hour), 0)
	if err != nil {
		t.Fatal(err)
	}

	e3, err := Create(priv, []byte("baz"), 3, ts.Add(time.Hour), 0)
	if err != nil {
		t.Fatal(err)
	}

	e4, err := Create(priv, []byte("cat"), 3, ts.Add(time.Hour*2), 0)
	if err != nil {
		t.Fatal(err)
	}

	e5, err := Create(priv, []byte("dog"), 4, ts.Add(time.Hour*3), 0)
	if err != nil {
		t.Fatal(err)
	}

	e6, err := Create(priv, []byte("fish"), 4, ts.Add(time.Hour*3), 0)
	if err != nil {
		t.Fatal(err)
	}

	// e1 is the only record, i hope it gets this right
	err = AssertSelected(e1, e1)
	if err != nil {
		t.Fatal(err)
	}

	// e2 has the highest sequence number
	err = AssertSelected(e2, e1, e2)
	if err != nil {
		t.Fatal(err)
	}

	// e3 has the highest sequence number
	err = AssertSelected(e3, e1, e2, e3)
	if err != nil {
		t.Fatal(err)
	}

	// e4 has a higher timeout
	err = AssertSelected(e4, e1, e2, e3, e4)
	if err != nil {
		t.Fatal(err)
	}

	// e5 has the highest sequence number
	err = AssertSelected(e5, e1, e2, e3, e4, e5)
	if err != nil {
		t.Fatal(err)
	}

	// e6 should be selected as its signauture will win in the comparison
	err = AssertSelected(e6, e1, e2, e3, e4, e5, e6)
	if err != nil {
		t.Fatal(err)
	}

	_ = []interface{}{e1, e2, e3, e4, e5, e6}
}
