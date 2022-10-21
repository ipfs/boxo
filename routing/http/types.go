package delegatedrouting

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multicodec"
)

var logger = logging.Logger("service/delegatedrouting")

// TransferProtocol represents a data transfer protocol
type TransferProtocol struct {
	Codec multicodec.Code
	// Payload optionally contains extra data about the transfer protocol
	Payload json.RawMessage
}

type ProvideResult struct {
	AdvisoryTTL time.Duration
}

type ProvideRequestPayload struct {
	Keys        []string //cids
	Timestamp   int64
	AdvisoryTTL time.Duration
	Provider    Provider
}

type FindProvidersResult struct {
	Providers []Provider
}

type Provider struct {
	PeerID    peer.ID
	Addrs     []multiaddr.Multiaddr
	Protocols []TransferProtocol
}

func (p *Provider) UnmarshalJSON(b []byte) error {
	type prov struct {
		PeerID    peer.ID
		Addrs     []string
		Protocols []TransferProtocol
	}
	tempProv := prov{}
	err := json.Unmarshal(b, &tempProv)
	if err != nil {
		return fmt.Errorf("unmarshaling provider: %w", err)
	}

	p.PeerID = tempProv.PeerID
	p.Protocols = tempProv.Protocols

	p.Addrs = nil
	for i, maStr := range tempProv.Addrs {
		ma, err := multiaddr.NewMultiaddr(maStr)
		if err != nil {
			return fmt.Errorf("parsing multiaddr %d: %w", i, err)
		}

		_, last := multiaddr.SplitLast(ma)
		if last != nil && last.Protocol().Code == multiaddr.P_P2P {
			logger.Infof("dropping provider multiaddress %v ending in /p2p/peerid", ma)
			continue
		}
		p.Addrs = append(p.Addrs, ma)
	}

	return nil
}

// ProvideRequest is a message indicating a provider can provide a Key for a given TTL
type ProvideRequest struct {
	Signature string
	Payload   string
}

// Sign a provide request
func (pr *ProvideRequest) Sign(peerID peer.ID, key crypto.PrivKey) error {
	if pr.IsSigned() {
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

	pk, err := payload.Provider.PeerID.ExtractPublicKey()
	if err != nil {
		return err
	}

	_, sigBytes, err := multibase.Decode(pr.Signature)
	if err != nil {
		return fmt.Errorf("multibase-decoding signature to verify: %w", err)
	}

	hash := sha256.New().Sum(payloadBytes)

	ok, err := pk.Verify(hash, sigBytes)
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
