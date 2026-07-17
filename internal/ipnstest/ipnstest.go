// Package ipnstest builds IPNS records at the wire level for tests, bypassing
// [ipns.NewRecord] so callers can construct records that record creation would
// sanitize or that verification would reject (for example a negative TTL).
package ipnstest

import (
	"bytes"
	"time"

	ipns "github.com/ipfs/boxo/ipns"
	ipns_pb "github.com/ipfs/boxo/ipns/pb"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/util"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"google.golang.org/protobuf/proto"
)

// RawRecordWithTTL builds an unsigned EOL IPNS record whose DAG-CBOR data carries
// exactly the given TTL, without going through [ipns.NewRecord]. This lets a test
// produce a record that NewRecord would floor or that [ipns.Validate] would
// reject (such as a negative TTL). The record is unsigned and must only be fed to
// code paths that do not verify signatures.
func RawRecordWithTTL(value path.Path, eol time.Time, ttl time.Duration) (*ipns.Record, error) {
	// Keys are assembled in canonical DAG-CBOR order (by length, then bytewise).
	node, err := qp.BuildMap(basicnode.Prototype.Map, 5, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "TTL", qp.Int(int64(ttl)))
		qp.MapEntry(ma, "Value", qp.Bytes([]byte(value.String())))
		qp.MapEntry(ma, "Sequence", qp.Int(1))
		qp.MapEntry(ma, "Validity", qp.Bytes([]byte(util.FormatRFC3339(eol))))
		qp.MapEntry(ma, "ValidityType", qp.Int(0))
	})
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := dagcbor.Encode(node, &buf); err != nil {
		return nil, err
	}

	raw, err := proto.Marshal(&ipns_pb.IpnsRecord{Data: buf.Bytes()})
	if err != nil {
		return nil, err
	}
	return ipns.UnmarshalRecord(raw)
}
