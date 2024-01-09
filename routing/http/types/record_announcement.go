package types

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/boxo/util"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multibase"
)

const SchemaAnnouncement = "announcement"
const announcementSignaturePrefix = "PUT /routing/v1 announcement:"

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

// MarshalJSON marshals the [AnnouncementPayload] into a canonical DAG-JSON
// representation of the payload.
func (ap AnnouncementPayload) MarshalJSON() ([]byte, error) {
	m := make(map[string]ipld.Node)
	var err error

	// TODO: or goes empty? Affects signature.
	if ap.CID.Defined() {
		m["CID"] = basicnode.NewString(ap.CID.String())
	}

	// TODO: is scope not set when empty? Affects signature.
	m["Scope"] = basicnode.NewString(string(ap.Scope))
	m["Timestamp"] = basicnode.NewString(util.FormatRFC3339(ap.Timestamp))
	m["TTL"] = basicnode.NewInt(ap.TTL.Milliseconds())
	m["ID"] = basicnode.NewString(ap.ID.String())
	m["Metadata"] = basicnode.NewString(ap.Metadata)

	// TODO: or goes empty if len == 0? Affects signature.
	addrs := []ipld.Node{}
	for _, addr := range ap.Addrs {
		addrs = append(addrs, basicnode.NewString(addr.String()))
	}
	m["Addrs"], err = makeIPLDList(addrs)
	if err != nil {
		return nil, err
	}

	// TODO: or goes empty if len == 0? Affects signature.
	protocols := []ipld.Node{}
	for _, protocol := range ap.Protocols {
		protocols = append(addrs, basicnode.NewString(protocol))
	}
	m["Protocols"], err = makeIPLDList(protocols)
	if err != nil {
		return nil, err
	}

	// Make final IPLD node.
	nd, err := makeIPLDMap(m)
	if err != nil {
		return nil, err
	}

	// Encode it with the DAG-JSON encoder, which automatically sorts the fields
	// in a deterministic manner.
	var b bytes.Buffer
	err = dagjson.Encode(nd, &b)
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (ap *AnnouncementPayload) UnmarshalJSON(b []byte) error {
	// TODO: this is the "simple" way of decoding the payload. I am assuming
	// we want to encode everything using strings (except for TTL). If we decide
	// to use DAG-JSON native types (e.g. Bytes), we have to convert this to IPLD code
	// in order to convert things properly.
	v := struct {
		CID       string
		Scope     AnnouncementScope
		Timestamp string
		TTL       int64
		ID        *peer.ID
		Addrs     []Multiaddr
		Protocols []string
		Metadata  string
	}{}
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

	ap.Timestamp, err = util.ParseRFC3339(v.Timestamp)
	if err != nil {
		return err
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
	Schema     string
	Error      string
	Payload    AnnouncementPayload
	RawPayload json.RawMessage
	Signature  string
}

func (ar *AnnouncementRecord) GetSchema() string {
	return ar.Schema
}

func (ar AnnouncementRecord) MarshalJSON() ([]byte, error) {
	if ar.RawPayload == nil {
		// This happens when [AnnouncementRecord] is used for response. In that
		// case it is not signed. Therefore, the RawPayload is not yet set.
		err := ar.setRawPayload()
		if err != nil {
			return nil, err
		}
	}

	v := struct {
		Schema    string
		Error     string `json:"Error,omitempty"`
		Payload   json.RawMessage
		Signature string `json:"Signature,omitempty"`
	}{
		Schema:    ar.Schema,
		Error:     ar.Error,
		Payload:   ar.RawPayload,
		Signature: ar.Signature,
	}

	return json.Marshal(v)
}

func (ar *AnnouncementRecord) UnmarshalJSON(b []byte) error {
	// Unmarshal all known fields and assign them.
	v := struct {
		Schema    string
		Error     string
		Payload   json.RawMessage
		Signature string
	}{}
	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}

	ar.Schema = v.Schema
	ar.Error = v.Error
	ar.RawPayload = v.Payload
	ar.Signature = v.Signature
	return (&ar.Payload).UnmarshalJSON(ar.RawPayload)
}

func (ar AnnouncementRecord) IsSigned() bool {
	return ar.Signature != ""
}

func (ar *AnnouncementRecord) setRawPayload() error {
	bytes, err := ar.Payload.MarshalJSON()
	if err != nil {
		return err
	}

	ar.RawPayload = bytes
	return nil
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

	err = ar.setRawPayload()
	if err != nil {
		return err
	}

	dataForSig := []byte(announcementSignaturePrefix)
	dataForSig = append(dataForSig, ar.RawPayload...)

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

	// note that we only generate and set the payload if it hasn't already been set
	// to allow for passing through the payload untouched if it is already provided
	if ar.RawPayload == nil {
		err := ar.setRawPayload()
		if err != nil {
			return err
		}
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
	dataForSig = append(dataForSig, ar.RawPayload...)

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
