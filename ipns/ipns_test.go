package ipns

import (
	"fmt"
	"testing"
	"time"

	u "github.com/ipfs/go-ipfs-util"
	ci "github.com/libp2p/go-libp2p-core/crypto"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func TestEmbedPublicKey(t *testing.T) {

	sr := u.NewTimeSeededRand()
	priv, pub, err := ci.GenerateKeyPairWithReader(ci.RSA, 2048, sr)
	if err != nil {
		t.Fatal(err)
	}

	pid, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}

	e, err := Create(priv, []byte("/a/b"), 0, time.Now().Add(1*time.Hour), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := EmbedPublicKey(pub, e); err != nil {
		t.Fatal(err)
	}
	embeddedPk, err := ci.UnmarshalPublicKey(e.PubKey)
	if err != nil {
		t.Fatal(err)
	}
	embeddedPid, err := peer.IDFromPublicKey(embeddedPk)
	if err != nil {
		t.Fatal(err)
	}
	if embeddedPid != pid {
		t.Fatalf("pid mismatch: %s != %s", pid, embeddedPid)
	}
}

func ExampleCreate() {
	// Generate a private key to sign the IPNS record with. Most of the time,
	// however, you'll want to retrieve an already-existing key from IPFS using
	// go-ipfs/core/coreapi CoreAPI.KeyAPI() interface.
	privateKey, _, err := ci.GenerateKeyPair(ci.RSA, 2048)
	if err != nil {
		panic(err)
	}

	// Create an IPNS record that expires in one hour and points to the IPFS address
	// /ipfs/Qme1knMqwt1hKZbc1BmQFmnm9f36nyQGwXxPGVpVJ9rMK5
	ipnsRecord, err := Create(privateKey, []byte("/ipfs/Qme1knMqwt1hKZbc1BmQFmnm9f36nyQGwXxPGVpVJ9rMK5"), 0, time.Now().Add(1*time.Hour), 0)
	if err != nil {
		panic(err)
	}

	fmt.Println(ipnsRecord)
}
