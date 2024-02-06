package types

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/boxo/routing/http/internal/drjson"
	"github.com/ipfs/boxo/util"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multibase"
)

const SchemaAnnouncement = "announcement"
const announcementSignaturePrefix = "routing-record:"

var _ Record = &AnnouncementRecord{}

type AnnouncementScope string

const (
	AnnouncementBlock     AnnouncementScope = "block"
	AnnouncementEntity    AnnouncementScope = "entity"
	AnnouncementRecursive AnnouncementScope = "recursive"
)

// AnnouncementRequest represents a request to announce
type AnnouncementRequest struct {
	CID      cid.Cid
	Scope    AnnouncementScope
	TTL      time.Duration
	Metadata []byte
}

// AnnouncementPayload is the [AnnouncementRecord] payload.
type AnnouncementPayload struct {
	CID       cid.Cid
	Scope     AnnouncementScope
	Timestamp time.Time
	TTL       time.Duration
	ID        *peer.ID
	Addrs     []Multiaddr
	Protocols []string
	Metadata  string
}

func (ap AnnouncementPayload) asDagCbor() ([]byte, error) {
	m := make(map[string]ipld.Node)
	var err error

	if ap.CID.Defined() {
		m["CID"] = basicnode.NewString(ap.CID.String())
	}

	if ap.Scope != "" {
		m["Scope"] = basicnode.NewString(string(ap.Scope))
	}

	if !ap.Timestamp.IsZero() {
		m["Timestamp"] = basicnode.NewString(util.FormatRFC3339(ap.Timestamp))
	}

	if ap.TTL != 0 {
		m["TTL"] = basicnode.NewInt(ap.TTL.Milliseconds())
	}

	if ap.ID != nil {
		m["ID"] = basicnode.NewString(ap.ID.String())
	}

	if ap.Metadata != "" {
		m["Metadata"] = basicnode.NewString(ap.Metadata)
	}

	if len(ap.Addrs) != 0 {
		addrs := []ipld.Node{}
		for _, addr := range ap.Addrs {
			addrs = append(addrs, basicnode.NewString(addr.String()))
		}
		m["Addrs"], err = makeIPLDList(addrs)
		if err != nil {
			return nil, err
		}
	}

	if len(ap.Protocols) != 0 {
		protocols := []ipld.Node{}
		for _, protocol := range ap.Protocols {
			protocols = append(protocols, basicnode.NewString(protocol))
		}
		m["Protocols"], err = makeIPLDList(protocols)
		if err != nil {
			return nil, err
		}
	}

	// Make final IPLD node.
	nd, err := makeIPLDMap(m)
	if err != nil {
		return nil, err
	}

	// Encode it with the DAG-JSON encoder, which automatically sorts the fields
	// in a deterministic manner.
	var b bytes.Buffer
	err = dagcbor.Encode(nd, &b)
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

type announcementPayloadHelper struct {
	CID       string            `json:",omitempty"`
	ID        *peer.ID          `json:",omitempty"`
	Scope     AnnouncementScope `json:",omitempty"`
	Timestamp string            `json:",omitempty"`
	TTL       int64             `json:",omitempty"`
	Addrs     []Multiaddr       `json:",omitempty"`
	Protocols []string          `json:",omitempty"`
	Metadata  string            `json:",omitempty"`
}

func (ap AnnouncementPayload) MarshalJSON() ([]byte, error) {
	v := announcementPayloadHelper{
		ID:        ap.ID,
		Scope:     ap.Scope,
		Addrs:     ap.Addrs,
		Protocols: ap.Protocols,
		Metadata:  ap.Metadata,
	}

	if ap.CID.Defined() {
		v.CID = ap.CID.String()
	}

	if !ap.Timestamp.IsZero() {
		v.Timestamp = util.FormatRFC3339(ap.Timestamp)
	}

	if ap.TTL != 0 {
		v.TTL = ap.TTL.Milliseconds()
	}

	return drjson.MarshalJSONBytes(v)
}

func (ap *AnnouncementPayload) UnmarshalJSON(b []byte) error {
	v := announcementPayloadHelper{}
	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}

	if v.CID != "" {
		ap.CID, err = cid.Decode(v.CID)
		if err != nil {
			return err
		}
	}

	if v.Scope != "" {
		switch v.Scope {
		case AnnouncementBlock, AnnouncementEntity, AnnouncementRecursive, "":
			ap.Scope = v.Scope
		default:
			return fmt.Errorf("invalid announcement scope %q", ap.Scope)
		}
	}

	if v.Timestamp != "" {
		ap.Timestamp, err = util.ParseRFC3339(v.Timestamp)
		if err != nil {
			return err
		}
	}

	ap.TTL = time.Duration(v.TTL) * time.Millisecond
	ap.ID = v.ID
	ap.Addrs = v.Addrs
	ap.Protocols = v.Protocols
	ap.Metadata = v.Metadata
	return nil
}

// AnnouncementRecord is a [Record] of [SchemaAnnouncement].
type AnnouncementRecord struct {
	Schema    string
	Error     string `json:",omitempty"`
	Payload   AnnouncementPayload
	Signature string `json:",omitempty"`
}

func (ar *AnnouncementRecord) GetSchema() string {
	return ar.Schema
}

func (ar AnnouncementRecord) IsSigned() bool {
	return ar.Signature != ""
}

func (ar *AnnouncementRecord) Sign(peerID peer.ID, key crypto.PrivKey) error {
	if ar.IsSigned() {
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

	data, err := ar.Payload.asDagCbor()
	if err != nil {
		return err
	}

	dataForSig := []byte(announcementSignaturePrefix)
	dataForSig = append(dataForSig, data...)

	rawSignature, err := key.Sign(dataForSig)
	if err != nil {
		return err
	}

	signature, err := multibase.Encode(multibase.Base64, rawSignature)
	if err != nil {
		return err
	}

	ar.Signature = signature
	return nil
}

func (ar *AnnouncementRecord) Verify() error {
	if !ar.IsSigned() {
		return errors.New("not signed")
	}

	if ar.Payload.ID == nil {
		return errors.New("peer ID must be specified")
	}

	data, err := ar.Payload.asDagCbor()
	if err != nil {
		return err
	}

	pk, err := ar.Payload.ID.ExtractPublicKey()
	if err != nil {
		return fmt.Errorf("extracing public key from peer ID: %w", err)
	}

	_, sigBytes, err := multibase.Decode(ar.Signature)
	if err != nil {
		return fmt.Errorf("multibase-decoding signature to verify: %w", err)
	}

	dataForSig := []byte(announcementSignaturePrefix)
	dataForSig = append(dataForSig, data...)

	ok, err := pk.Verify(dataForSig, sigBytes)
	if err != nil {
		return fmt.Errorf("verifying hash with signature: %w", err)
	}
	if !ok {
		return errors.New("signature failed to verify")
	}

	return nil
}

func makeIPLDList(ls []ipld.Node) (datamodel.Node, error) {
	nd := basicnode.Prototype__List{}.NewBuilder()
	la, err := nd.BeginList(int64(len(ls)))
	if err != nil {
		return nil, err
	}

	for _, item := range ls {
		if err := la.AssembleValue().AssignNode(item); err != nil {
			return nil, err
		}
	}

	if err := la.Finish(); err != nil {
		return nil, err
	}

	return nd.Build(), nil
}

func makeIPLDMap(mp map[string]ipld.Node) (datamodel.Node, error) {
	nd := basicnode.Prototype__Map{}.NewBuilder()
	ma, err := nd.BeginMap(int64(len(mp)))
	if err != nil {
		return nil, err
	}

	for k, v := range mp {
		if err := ma.AssembleKey().AssignString(k); err != nil {
			return nil, err
		}
		if err := ma.AssembleValue().AssignNode(v); err != nil {
			return nil, err
		}
	}

	if err := ma.Finish(); err != nil {
		return nil, err
	}

	return nd.Build(), nil
}
