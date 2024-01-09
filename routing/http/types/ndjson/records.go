package ndjson

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
)

// NewRecordsIter returns an iterator that reads [types.Record] from the given [io.Reader].
func NewRecordsIter(r io.Reader) iter.Iter[iter.Result[types.Record]] {
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

	return iter.Map[iter.Result[types.UnknownRecord]](jsonIter, mapFn)
}

// NewAnnouncementRecordsIter returns an iterator that reads [types.AnnouncementRecord]
// from the given [io.Reader]. Records with a different schema are ignored. To read all
// records, use [NewRecordsIter] instead.
func NewAnnouncementRecordsIter(r io.Reader) iter.Iter[iter.Result[*types.AnnouncementRecord]] {
	return newFilteredRecords[*types.AnnouncementRecord](r, types.SchemaPeer)
}

// NewPeerRecordsIter returns an iterator that reads [types.PeerRecord] from the given
// [io.Reader]. Records with a different schema are ignored. To read all records, use
// [NewRecordsIter] instead.
func NewPeerRecordsIter(r io.Reader) iter.Iter[iter.Result[*types.PeerRecord]] {
	return newFilteredRecords[*types.PeerRecord](r, types.SchemaPeer)
}

func newFilteredRecords[T any](r io.Reader, schema string) iter.Iter[iter.Result[T]] {
	return iter.Map[iter.Result[types.Record]](
		iter.Filter(NewRecordsIter(r), func(t iter.Result[types.Record]) bool {
			return t.Val.GetSchema() == schema
		}),
		func(upr iter.Result[types.Record]) iter.Result[T] {
			var result iter.Result[T]
			if upr.Err != nil {
				result.Err = upr.Err
				return result
			}

			// Note that this should never happen unless [NewRecordsIter] is not well
			// is not well implemented.
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
