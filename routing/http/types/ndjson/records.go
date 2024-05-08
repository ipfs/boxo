package ndjson

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
)

// NewRecordsIter returns an iterator that reads [types.Record] from the given [io.Reader].
func NewRecordsIter(r io.Reader) iter.ResultIter[types.Record] {
	jsonIter := iter.FromReaderJSON[types.UnknownRecord](r)
	mapFn := func(upr iter.Result[types.UnknownRecord]) iter.Result[types.Record] {
		var result iter.Result[types.Record]
		if upr.Err != nil {
			result.Err = upr.Err
			return result
		}
		switch upr.Val.Schema {
		case types.SchemaPeer:
			var prov types.PeerRecord
			err := json.Unmarshal(upr.Val.Bytes, &prov)
			if err != nil {
				result.Err = err
				return result
			}
			result.Val = &prov
		case types.SchemaAnnouncement:
			var prov types.AnnouncementRecord
			err := json.Unmarshal(upr.Val.Bytes, &prov)
			if err != nil {
				result.Err = err
				return result
			}
			result.Val = &prov
		default:
			result.Val = &upr.Val
		}
		return result
	}

	return iter.Map(jsonIter, mapFn)
}

// NewAnnouncementRecordsIter returns an iterator that reads [types.AnnouncementRecord]
// from the given [io.Reader]. Incompatible records result in an error within the iterator.
// To read all records, use [NewRecordsIter] instead.
func NewAnnouncementRecordsIter(r io.Reader) iter.ResultIter[*types.AnnouncementRecord] {
	return newTypedRecords[*types.AnnouncementRecord](r, types.SchemaAnnouncement)
}

// NewAnnouncementResponseRecordsIter returns an iterator that reads
// [types.AnnouncementResponseRecord] from the given [io.Reader]. Incompatible
// records result in an error within the iterator. To read all records, use
// [NewRecordsIter] instead.
func NewAnnouncementResponseRecordsIter(r io.Reader) iter.ResultIter[*types.AnnouncementResponseRecord] {
	return newTypedRecords[*types.AnnouncementResponseRecord](r, types.SchemaAnnouncementResponse)
}

// NewPeerRecordsIter returns an iterator that reads [types.PeerRecord] from the given
// [io.Reader]. Incompatible records result in an error within the iterator. To read all records,
// use [NewRecordsIter] instead.
func NewPeerRecordsIter(r io.Reader) iter.ResultIter[*types.PeerRecord] {
	return newTypedRecords[*types.PeerRecord](r, types.SchemaPeer)
}

func newTypedRecords[T any](r io.Reader, schema string) iter.ResultIter[T] {
	return iter.Map(
		NewRecordsIter(r),
		func(upr iter.Result[types.Record]) iter.Result[T] {
			var result iter.Result[T]
			if upr.Err != nil {
				result.Err = upr.Err
				return result
			}

			if upr.Val.GetSchema() != schema {
				result.Err = fmt.Errorf("unexpected schema %s, expected %s", upr.Val.GetSchema(), schema)
				return result
			}

			val, ok := upr.Val.(T)
			if !ok {
				result.Err = fmt.Errorf("type incompatible with schema %s", schema)
				return result
			}

			result.Val = val
			return result
		},
	)
}
