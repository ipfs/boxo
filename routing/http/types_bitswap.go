package delegatedrouting

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-delegated-routing/internal"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multibase"
)

type BitswapReadProviderResponse struct {
	Protocol string
	ID       peer.ID
	Addrs    []multiaddr.Multiaddr
}

type BitswapWriteProviderRequest struct {
	BitswapWriteProviderRequestPayload
	Protocol  string
	Signature string

	rawPayload string
}

type BitswapWriteProviderRequestPayload struct {
	Keys        []CID
	Timestamp   Time
	AdvisoryTTL Duration
	ID          peer.ID
	Addrs       []multiaddr.Multiaddr
}

func (p *BitswapWriteProviderRequest) GetPayload() BitswapWriteProviderRequestPayload {
	return BitswapWriteProviderRequestPayload{}
}

func (p *BitswapWriteProviderRequest) MarshalJSON() ([]byte, error) {
	err := p.Verify()
	if err != nil {
		return nil, err
	}
	bwp := struct {
		Protocol  string
		Signature string
		Payload   string
	}{
		Protocol: p.Protocol,
	}

	bwp.Signature = p.Signature
	bwp.Payload = p.rawPayload

	return internal.MarshalJSONBytes(bwp)
}

func (p *BitswapWriteProviderRequest) UnmarshalJSON(b []byte) error {
	bwp := struct {
		Protocol  string
		Signature string
		Payload   string
	}{}
	err := json.Unmarshal(b, &bwp)
	if err != nil {
		return err
	}

	p.Protocol = bwp.Protocol
	p.Signature = bwp.Signature
	p.rawPayload = bwp.Payload

	return nil
}

func (p *BitswapWriteProviderRequest) IsSigned() bool {
	return p.Signature != ""
}

func (p *BitswapWriteProviderRequest) setRawPayload() error {
	payloadBytes, err := internal.MarshalJSONBytes(p.BitswapWriteProviderRequestPayload)
	if err != nil {
		return fmt.Errorf("marshaling bitswap write provider payload: %w", err)
	}
	p.rawPayload = string(payloadBytes)
	return nil
}

func (p *BitswapWriteProviderRequest) Sign(peerID peer.ID, key crypto.PrivKey) error {
	if p.IsSigned() {
		return errors.New("already signed")
	}

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

	err = p.setRawPayload()
	if err != nil {
		return err
	}
	hash := sha256.New().Sum([]byte(p.rawPayload))
	sig, err := key.Sign(hash)
	if err != nil {
		return err
	}

	sigStr, err := multibase.Encode(multibase.Base64, sig)
	if err != nil {
		return fmt.Errorf("multibase-encoding signature: %w", err)
	}

	p.Signature = sigStr
	return nil
}

func (p *BitswapWriteProviderRequest) Verify() error {
	if !p.IsSigned() {
		return errors.New("not signed")
	}

	// note that we only generate and set the payload if it hasn't already been set
	// to allow for passing through the payload untouched if it is already provided
	if p.rawPayload == "" {
		err := p.setRawPayload()
		if err != nil {
			return err
		}
	}

	pk, err := p.ID.ExtractPublicKey()
	if err != nil {
		return err
	}

	_, sigBytes, err := multibase.Decode(p.Signature)
	if err != nil {
		return fmt.Errorf("multibase-decoding signature to verify: %w", err)
	}

	hash := sha256.New().Sum([]byte(p.rawPayload))

	ok, err := pk.Verify(hash, sigBytes)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("signature failed to verify")
	}

	return nil
}

type BitswapWriteProviderResponse struct {
	AdvisoryTTL time.Duration
}
