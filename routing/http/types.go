package delegatedrouting

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multicodec"
)

// TransferProtocol represents a data transfer protocol
type TransferProtocol struct {
	Codec   multicodec.Code
	Payload json.RawMessage
}

type ProvideRequestPayload struct {
	Keys        []string //cids
	Timestamp   int64
	AdvisoryTTL time.Duration
	Provider    Provider
}

type Provider struct {
	Peer      peer.AddrInfo
	Protocols []TransferProtocol
}

// ProvideRequest is a message indicating a provider can provide a Key for a given TTL
type ProvideRequest struct {
	Signature string
	Payload   string
}

// Sign a provide request
func (pr *ProvideRequest) Sign(peerID peer.ID, key crypto.PrivKey) error {
	if pr.IsSigned() {
		return errors.New("already Signed")
	}
	//	pr.Timestamp = time.Now().Unix()

	if key == nil {
		return errors.New("no key provided")
	}

	sid, err := peer.IDFromPrivateKey(key)
	if err != nil {
		return err
	}
	if sid != peerID {
		return errors.New("not the correct signing key")
	}

	out, err := json.Marshal(pr)
	if err != nil {
		return fmt.Errorf("marshaling provide request for signature: %w", err)
	}
	hash := sha256.New().Sum(out)
	sig, err := key.Sign(hash)
	if err != nil {
		return err
	}

	sigStr, err := multibase.Encode(multibase.Base64, sig)
	if err != nil {
		return fmt.Errorf("multibase-encoding signature: %w", err)
	}

	pr.Signature = sigStr
	return nil
}

func (pr *ProvideRequest) Verify() error {
	if !pr.IsSigned() {
		return errors.New("not signed")
	}

	_, payloadBytes, err := multibase.Decode(pr.Payload)
	if err != nil {
		return fmt.Errorf("multibase-decoding payload to verify: %w", err)
	}

	payload := ProvideRequestPayload{}
	err = json.Unmarshal(payloadBytes, &payload)
	if err != nil {
		return fmt.Errorf("unmarshaling payload to verify: %w", err)
	}

	pk, err := payload.Provider.Peer.ID.ExtractPublicKey()
	if err != nil {
		return err
	}

	_, sigBytes, err := multibase.Decode(pr.Signature)
	if err != nil {
		return fmt.Errorf("multibase-decoding signature to verify: %w", err)
	}

	ok, err := pk.Verify(payloadBytes, sigBytes)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("signature failed to verify")
	}

	return nil
}

// IsSigned indicates if the ProvideRequest has been signed
func (pr *ProvideRequest) IsSigned() bool {
	return pr.Signature != ""
}

type ProvideResult struct {
	AdvisoryTTL time.Duration
}
