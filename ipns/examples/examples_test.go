package examples_test

import (
	"testing"

	"github.com/ipfs/go-ipns/examples"
)

var testPath = "/ipfs/Qme1knMqwt1hKZbc1BmQFmnm9f36nyQGwXxPGVpVJ9rMK5"

func TestKeyGeneration(t *testing.T) {
	_, err := generateRSAKey()
	if err != nil {
		t.Error(err)
	}

	_, err = generateEDKey()
	if err != nil {
		t.Error(err)
	}
}

func TestEmbeddedEntryCreation(t *testing.T) {
	rk, err := generateRSAKey()
	if err != nil {
		t.Fatal(err)
	}

	ek, err := generateEDKey()
	if err != nil {
		t.Fatal(err)
	}

	_, err = examples.CreateEntryWithEmbed(testPath, rk.Public, rk.Private)
	if err != nil {
		t.Error(err)
	}

	_, err = examples.CreateEntryWithEmbed(testPath, ek.Public, ek.Private)
	if err != nil {
		t.Error(err)
	}

}
func generateRSAKey() (*examples.KeyPair, error) {
	// DO NOT USE 1024 BITS IN PRODUCTION
	// THIS IS ONLY FOR TESTING PURPOSES
	kp, err := examples.GenerateRSAKeyPair(1024)
	if err != nil {
		return nil, err
	}
	return kp, nil
}

func generateEDKey() (*examples.KeyPair, error) {
	// DO NOT USE 1024 BITS IN PRODUCTION
	// THIS IS ONLY FOR TESTING PURPOSES
	kp, err := examples.GenerateEDKeyPair(1024)
	if err != nil {
		return nil, err
	}
	return kp, nil
}
