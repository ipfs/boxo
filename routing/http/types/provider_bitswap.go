package types

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ipfs/go-libipfs/routing/http/internal/drjson"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multibase"
)

const SchemaBitswap = "bitswap"

var _ WriteProviderRecord = &WriteBitswapProviderRecord{}

// WriteBitswapProviderRecord is used when we want to add a new provider record that is using bitswap.
type WriteBitswapProviderRecord struct {
	Protocol  string
	Schema    string
	Signature string

	// this content must be untouched because it is signed and we need to verify it
	RawPayload json.RawMessage `json:"Payload"`
	Payload    BitswapPayload  `json:"-"`
}

type BitswapPayload struct {
	Keys        []CID
	Timestamp   *Time
	AdvisoryTTL *Duration
	ID          *peer.ID
	Addrs       []Multiaddr
}

func (*WriteBitswapProviderRecord) IsWriteProviderRecord() {}

type tmpBWPR WriteBitswapProviderRecord

func (p *WriteBitswapProviderRecord) UnmarshalJSON(b []byte) error {
	var bwp tmpBWPR
	err := json.Unmarshal(b, &bwp)
	if err != nil {
		return err
	}

	p.Protocol = bwp.Protocol
	p.Schema = bwp.Schema
	p.Signature = bwp.Signature
	p.RawPayload = bwp.RawPayload

	return json.Unmarshal(bwp.RawPayload, &p.Payload)
}

func (p *WriteBitswapProviderRecord) IsSigned() bool {
	return p.Signature != ""
}

func (p *WriteBitswapProviderRecord) setRawPayload() error {
	payloadBytes, err := drjson.MarshalJSONBytes(p.Payload)
	if err != nil {
		return fmt.Errorf("marshaling bitswap write provider payload: %w", err)
	}

	p.RawPayload = payloadBytes

	return nil
}

func (p *WriteBitswapProviderRecord) Sign(peerID peer.ID, key crypto.PrivKey) error {
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
	hash := sha256.Sum256([]byte(p.RawPayload))
	sig, err := key.Sign(hash[:])
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

func (p *WriteBitswapProviderRecord) Verify() error {
	if !p.IsSigned() {
		return errors.New("not signed")
	}

	if p.Payload.ID == nil {
		return errors.New("peer ID must be specified")
	}

	// note that we only generate and set the payload if it hasn't already been set
	// to allow for passing through the payload untouched if it is already provided
	if p.RawPayload == nil {
		err := p.setRawPayload()
		if err != nil {
			return err
		}
	}

	pk, err := p.Payload.ID.ExtractPublicKey()
	if err != nil {
		return fmt.Errorf("extracing public key from peer ID: %w", err)
	}

	_, sigBytes, err := multibase.Decode(p.Signature)
	if err != nil {
		return fmt.Errorf("multibase-decoding signature to verify: %w", err)
	}

	hash := sha256.Sum256([]byte(p.RawPayload))
	ok, err := pk.Verify(hash[:], sigBytes)
	if err != nil {
		return fmt.Errorf("verifying hash with signature: %w", err)
	}
	if !ok {
		return errors.New("signature failed to verify")
	}

	return nil
}

var _ ProviderResponse = &WriteBitswapProviderRecordResponse{}

// WriteBitswapProviderRecordResponse will be returned as a result of WriteBitswapProviderRecord
type WriteBitswapProviderRecordResponse struct {
	Protocol    string
	Schema      string
	AdvisoryTTL *Duration
}

func (wbprr *WriteBitswapProviderRecordResponse) GetProtocol() string {
	return wbprr.Protocol
}

func (wbprr *WriteBitswapProviderRecordResponse) GetSchema() string {
	return wbprr.Schema
}

var _ ReadProviderRecord = &ReadBitswapProviderRecord{}
var _ ProviderResponse = &ReadBitswapProviderRecord{}

// ReadBitswapProviderRecord is a provider result with parameters for bitswap providers
type ReadBitswapProviderRecord struct {
	Protocol string
	Schema   string
	ID       *peer.ID
	Addrs    []Multiaddr
}

func (rbpr *ReadBitswapProviderRecord) GetProtocol() string {
	return rbpr.Protocol
}

func (rbpr *ReadBitswapProviderRecord) GetSchema() string {
	return rbpr.Schema
}

func (*ReadBitswapProviderRecord) IsReadProviderRecord() {}
