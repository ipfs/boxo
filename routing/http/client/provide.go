package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-delegated-routing/gen/proto"
	"github.com/ipld/edelweiss/values"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"github.com/polydawn/refmt/cbor"
)

// Provider represents the source publishing one or more CIDs
type Provider struct {
	Peer          peer.AddrInfo
	ProviderProto []TransferProtocol
}

// ToProto convers a provider into the wire proto form
func (p *Provider) ToProto() *proto.Provider {
	pp := proto.Provider{
		ProviderNode: proto.Node{
			Peer: ToProtoPeer(p.Peer),
		},
		ProviderProto: proto.TransferProtocolList{},
	}
	for _, tp := range p.ProviderProto {
		pp.ProviderProto = append(pp.ProviderProto, tp.ToProto())
	}
	return &pp
}

// TransferProtocol represents a data transfer protocol
type TransferProtocol struct {
	Codec   multicodec.Code
	Payload []byte
}

// GraphSyncFILv1 is the current filecoin storage provider protocol.
type GraphSyncFILv1 struct {
	PieceCID      cid.Cid
	VerifiedDeal  bool
	FastRetrieval bool
}

// ToProto converts a TransferProtocol to the wire representation
func (tp *TransferProtocol) ToProto() proto.TransferProtocol {
	if tp.Codec == multicodec.TransportBitswap {
		return proto.TransferProtocol{
			Bitswap: &proto.BitswapProtocol{},
		}
	} else if tp.Codec == multicodec.TransportGraphsyncFilecoinv1 {
		into := GraphSyncFILv1{}
		if err := cbor.Unmarshal(cbor.DecodeOptions{}, tp.Payload, &into); err != nil {
			return proto.TransferProtocol{}
		}
		return proto.TransferProtocol{
			GraphSyncFILv1: &proto.GraphSyncFILv1Protocol{
				PieceCID:      proto.LinkToAny(into.PieceCID),
				VerifiedDeal:  values.Bool(into.VerifiedDeal),
				FastRetrieval: values.Bool(into.FastRetrieval),
			},
		}
	} else {
		return proto.TransferProtocol{}
	}
}

func parseProtocol(tp *proto.TransferProtocol) (TransferProtocol, error) {
	if tp.Bitswap != nil {
		return TransferProtocol{Codec: multicodec.TransportBitswap}, nil
	} else if tp.GraphSyncFILv1 != nil {
		pl := GraphSyncFILv1{
			PieceCID:      cid.Cid(tp.GraphSyncFILv1.PieceCID),
			VerifiedDeal:  bool(tp.GraphSyncFILv1.VerifiedDeal),
			FastRetrieval: bool(tp.GraphSyncFILv1.FastRetrieval),
		}
		plBytes, err := cbor.Marshal(&pl)
		if err != nil {
			return TransferProtocol{}, err
		}
		return TransferProtocol{
			Codec:   multicodec.TransportGraphsyncFilecoinv1,
			Payload: plBytes,
		}, nil
	}
	return TransferProtocol{}, nil
}

// ProvideRequest is a message indicating a provider can provide a Key for a given TTL
type ProvideRequest struct {
	Key cid.Cid
	*Provider
	Timestamp   int64
	AdvisoryTTL time.Duration
	Signature   []byte
}

var provideSchema, provideSchemaErr = ipld.LoadSchemaBytes([]byte(`
		type ProvideRequest struct {
			Key    &Any
			Provider  Provider
			Timestamp Int
			AdvisoryTTL Int
			Signature Bytes
		}
		type Provider struct {
			Peer          Peer
			ProviderProto [TransferProtocol]
		}
		type Peer struct {
			ID   String
			Multiaddresses [Bytes]
		}
		type TransferProtocol struct {
			Codec Int
			Payload Bytes
		}
	`))

func init() {
	if provideSchemaErr != nil {
		panic(provideSchemaErr)
	}
}

func bytesToMA(b []byte) (interface{}, error) {
	return multiaddr.NewMultiaddrBytes(b)
}
func maToBytes(iface interface{}) ([]byte, error) {
	if ma, ok := iface.(multiaddr.Multiaddr); ok {
		return ma.Bytes(), nil
	}
	return nil, fmt.Errorf("did not get expected MA type")
}

// Sign a provide request
func (pr *ProvideRequest) Sign(key crypto.PrivKey) error {
	if pr.IsSigned() {
		return errors.New("already Signed")
	}
	pr.Timestamp = time.Now().Unix()
	pr.Signature = []byte{}

	if key == nil {
		return errors.New("no key provided")
	}

	sid, err := peer.IDFromPrivateKey(key)
	if err != nil {
		return err
	}
	if sid != pr.Provider.Peer.ID {
		return errors.New("not the correct signing key")
	}

	ma, _ := multiaddr.NewMultiaddr("/")
	opts := []bindnode.Option{
		bindnode.TypedBytesConverter(&ma, bytesToMA, maToBytes),
	}

	node := bindnode.Wrap(pr, provideSchema.TypeByName("ProvideRequest"), opts...)
	nodeRepr := node.Representation()
	outBuf := bytes.NewBuffer(nil)
	if err = dagjson.Encode(nodeRepr, outBuf); err != nil {
		return err
	}
	hash := sha256.New().Sum(outBuf.Bytes())
	sig, err := key.Sign(hash)
	if err != nil {
		return err
	}
	pr.Signature = sig
	return nil
}

func (pr *ProvideRequest) Verify() error {
	if !pr.IsSigned() {
		return errors.New("not signed")
	}
	sig := pr.Signature
	pr.Signature = []byte{}
	defer func() {
		pr.Signature = sig
	}()

	ma, _ := multiaddr.NewMultiaddr("/")
	opts := []bindnode.Option{
		bindnode.TypedBytesConverter(&ma, bytesToMA, maToBytes),
	}

	node := bindnode.Wrap(pr, provideSchema.TypeByName("ProvideRequest"), opts...)
	nodeRepr := node.Representation()
	outBuf := bytes.NewBuffer(nil)
	if err := dagjson.Encode(nodeRepr, outBuf); err != nil {
		return err
	}
	hash := sha256.New().Sum(outBuf.Bytes())

	pk, err := pr.Peer.ID.ExtractPublicKey()
	if err != nil {
		return err
	}

	ok, err := pk.Verify(hash, sig)
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
	return pr.Signature != nil
}

func ParseProvideRequest(req *proto.ProvideRequest) (*ProvideRequest, error) {
	prov, err := parseProvider(&req.Provider)
	if err != nil {
		return nil, err
	}
	pr := ProvideRequest{
		Key:         cid.Cid(req.Key),
		Provider:    prov,
		AdvisoryTTL: time.Duration(req.AdvisoryTTL),
		Timestamp:   int64(req.Timestamp),
		Signature:   req.Signature,
	}

	if err := pr.Verify(); err != nil {
		return nil, err
	}
	return &pr, nil
}

func parseProvider(p *proto.Provider) (*Provider, error) {
	prov := Provider{
		Peer:          parseProtoNodeToAddrInfo(p.ProviderNode)[0],
		ProviderProto: make([]TransferProtocol, 0),
	}
	for _, tp := range p.ProviderProto {
		proto, err := parseProtocol(&tp)
		if err != nil {
			return nil, err
		}
		prov.ProviderProto = append(prov.ProviderProto, proto)
	}
	return &prov, nil
}

type ProvideAsyncResult struct {
	AdvisoryTTL time.Duration
	Err         error
}

func (fp *Client) Provide(ctx context.Context, key cid.Cid, ttl time.Duration) (time.Duration, error) {
	req := ProvideRequest{
		Key:         key,
		Provider:    fp.provider,
		AdvisoryTTL: ttl,
		Timestamp:   time.Now().Unix(),
	}

	if fp.identity != nil {
		if err := req.Sign(fp.identity); err != nil {
			return 0, err
		}
	}

	record, err := fp.ProvideSignedRecord(ctx, &req)
	if err != nil {
		return 0, err
	}

	var d time.Duration
	var set bool
	for resp := range record {
		if resp.Err == nil {
			set = true
			if resp.AdvisoryTTL > d {
				d = resp.AdvisoryTTL
			}
		} else if resp.Err != nil {
			err = resp.Err
		}
	}

	if set {
		return d, nil
	} else if err == nil {
		return 0, fmt.Errorf("no response")
	}
	return 0, err
}

func (fp *Client) ProvideAsync(ctx context.Context, key cid.Cid, ttl time.Duration) (<-chan time.Duration, error) {
	req := ProvideRequest{
		Key:         key,
		Provider:    fp.provider,
		AdvisoryTTL: ttl,
		Timestamp:   time.Now().Unix(),
	}
	ch := make(chan time.Duration, 1)

	if fp.identity != nil {
		if err := req.Sign(fp.identity); err != nil {
			close(ch)
			return ch, err
		}
	}

	record, err := fp.ProvideSignedRecord(ctx, &req)
	if err != nil {
		close(ch)
		return ch, err
	}
	go func() {
		defer close(ch)
		for resp := range record {
			if resp.Err != nil {
				logger.Infof("dropping partial provide failure (%v)", err)
			} else {
				ch <- resp.AdvisoryTTL
			}
		}
	}()
	return ch, nil
}

// ProvideAsync makes a provide request to a delegated router
func (fp *Client) ProvideSignedRecord(ctx context.Context, req *ProvideRequest) (<-chan ProvideAsyncResult, error) {
	if !req.IsSigned() {
		return nil, errors.New("request is not signed")
	}

	var providerProto proto.Provider
	if req.Provider != nil {
		providerProto = *req.Provider.ToProto()
	}
	ch0, err := fp.client.Provide_Async(ctx, &proto.ProvideRequest{
		Key:         proto.LinkToAny(req.Key),
		Provider:    providerProto,
		Timestamp:   values.Int(req.Timestamp),
		AdvisoryTTL: values.Int(req.AdvisoryTTL),
		Signature:   req.Signature,
	})
	if err != nil {
		return nil, err
	}
	ch1 := make(chan ProvideAsyncResult, 1)
	go func() {
		defer close(ch1)
		for {
			select {
			case <-ctx.Done():
				return
			case r0, ok := <-ch0:
				if !ok {
					return
				}

				var r1 ProvideAsyncResult

				if r0.Err != nil {
					r1.Err = r0.Err
					select {
					case <-ctx.Done():
						return
					case ch1 <- r1:
					}
					continue
				}

				if r0.Resp == nil {
					continue
				}

				r1.AdvisoryTTL = time.Duration(r0.Resp.AdvisoryTTL)

				select {
				case <-ctx.Done():
					return
				case ch1 <- r1:
				}
			}
		}
	}()
	return ch1, nil
}
