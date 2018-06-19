package examples

import (
	crypto "github.com/libp2p/go-libp2p-crypto"
)

// KeyPair is a helper struct used to contain the parts of a key
type KeyPair struct {
	Private crypto.PrivKey
	Public  crypto.PubKey
}

// GenerateRSAKeyPair is used to generate an RSA key pair
func GenerateRSAKeyPair(bits int) (*KeyPair, error) {
	var kp KeyPair
	priv, pub, err := crypto.GenerateKeyPair(crypto.RSA, bits)
	if err != nil {
		return nil, err
	}
	kp.Private = priv
	kp.Public = pub
	return &kp, nil
}

// GenerateEDKeyPair is used to generate an ED25519 keypair
func GenerateEDKeyPair(bits int) (*KeyPair, error) {
	var kp KeyPair
	priv, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, bits)
	if err != nil {
		return nil, err
	}
	kp.Private = priv
	kp.Public = pub
	return &kp, nil
}
