package ndjson

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRecordsIter_GenericRecordSizeLimit(t *testing.T) {
	t.Run("rejects generic record exceeding max size", func(t *testing.T) {
		// Build a generic record that exceeds MaxGenericRecordSize by padding
		// the ID field with enough data to push over 10 KiB.
		largeID := strings.Repeat("x", types.MaxGenericRecordSize)
		rec := map[string]any{
			"Schema": types.SchemaGeneric,
			"ID":     largeID,
		}
		data, err := json.Marshal(rec)
		require.NoError(t, err)
		require.Greater(t, len(data), types.MaxGenericRecordSize)

		r := strings.NewReader(string(data) + "\n")
		ri := NewRecordsIter(r)

		results := iter.ReadAll[iter.Result[types.Record]](ri)
		require.Len(t, results, 1)
		assert.Error(t, results[0].Err)
		assert.Contains(t, results[0].Err.Error(), "generic record too large")
	})

	t.Run("accepts generic record within max size", func(t *testing.T) {
		rec := map[string]any{
			"Schema":    types.SchemaGeneric,
			"ID":        "peer1",
			"Addrs":     []string{"https://example.com"},
			"Protocols": []string{"transport-ipfs-gateway-http"},
		}
		data, err := json.Marshal(rec)
		require.NoError(t, err)
		require.LessOrEqual(t, len(data), types.MaxGenericRecordSize)

		r := strings.NewReader(string(data) + "\n")
		ri := NewRecordsIter(r)

		results := iter.ReadAll[iter.Result[types.Record]](ri)
		require.Len(t, results, 1)
		require.NoError(t, results[0].Err)

		gr, ok := results[0].Val.(*types.GenericRecord)
		require.True(t, ok)
		assert.Equal(t, "peer1", gr.ID)
	})
}

// TestNewRecordsIter_GenericRecordRoundTrip verifies that all GenericRecord
// fields (Addrs, Protocols, Extra) survive the NDJSON deserialization path.
func TestNewRecordsIter_GenericRecordRoundTrip(t *testing.T) {
	rec := map[string]any{
		"Schema":    types.SchemaGeneric,
		"ID":        "did:key:z6Mkm1example",
		"Addrs":     []string{"https://trustless-gateway.example.com", "/ip4/1.2.3.4/tcp/5000"},
		"Protocols": []string{"transport-ipfs-gateway-http"},
	}
	data, err := json.Marshal(rec)
	require.NoError(t, err)

	r := strings.NewReader(string(data) + "\n")
	ri := NewRecordsIter(r)

	results := iter.ReadAll[iter.Result[types.Record]](ri)
	require.Len(t, results, 1)
	require.NoError(t, results[0].Err)

	gr, ok := results[0].Val.(*types.GenericRecord)
	require.True(t, ok, "expected *types.GenericRecord")

	assert.Equal(t, types.SchemaGeneric, gr.Schema)
	assert.Equal(t, types.SchemaGeneric, gr.GetSchema())
	assert.Equal(t, "did:key:z6Mkm1example", gr.ID)
	require.Len(t, gr.Addrs, 2)
	assert.True(t, gr.Addrs[0].IsURL(), "first addr should be URL")
	assert.Equal(t, "https://trustless-gateway.example.com", gr.Addrs[0].String())
	assert.True(t, gr.Addrs[1].IsMultiaddr(), "second addr should be multiaddr")
	assert.Equal(t, "/ip4/1.2.3.4/tcp/5000", gr.Addrs[1].String())
	assert.Equal(t, []string{"transport-ipfs-gateway-http"}, gr.Protocols)
}

// TestNewRecordsIter_GenericRecordExtraFields verifies that protocol-specific
// extra fields survive the NDJSON deserialization path. This matches the spec
// test fixture showing a record with custom metadata.
func TestNewRecordsIter_GenericRecordExtraFields(t *testing.T) {
	rec := map[string]any{
		"Schema":                  types.SchemaGeneric,
		"ID":                      "did:key:z6Mkm1example",
		"Addrs":                   []string{"https://provider.example.com"},
		"Protocols":               []string{"example-future-protocol"},
		"example-future-protocol": map[string]any{"version": 2, "features": []string{"foo"}},
	}
	data, err := json.Marshal(rec)
	require.NoError(t, err)

	r := strings.NewReader(string(data) + "\n")
	ri := NewRecordsIter(r)

	results := iter.ReadAll[iter.Result[types.Record]](ri)
	require.Len(t, results, 1)
	require.NoError(t, results[0].Err)

	gr, ok := results[0].Val.(*types.GenericRecord)
	require.True(t, ok)

	// Extra field must be preserved and known fields must not leak into it.
	require.Contains(t, gr.Extra, "example-future-protocol")
	assert.NotContains(t, gr.Extra, "Schema")
	assert.NotContains(t, gr.Extra, "ID")
	assert.NotContains(t, gr.Extra, "Addrs")
	assert.NotContains(t, gr.Extra, "Protocols")

	// Verify the extra field value round-trips correctly.
	var meta map[string]any
	err = json.Unmarshal(gr.Extra["example-future-protocol"], &meta)
	require.NoError(t, err)
	assert.Equal(t, float64(2), meta["version"])
}

// TestNewRecordsIter_MixedSchemaStream verifies that NewRecordsIter correctly
// deserializes a mixed NDJSON stream containing peer, generic, and bitswap
// records. Each record must come through as the correct Go type.
func TestNewRecordsIter_MixedSchemaStream(t *testing.T) {
	peerRec := map[string]any{
		"Schema":    types.SchemaPeer,
		"ID":        "12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn",
		"Addrs":     []string{"/ip4/127.0.0.1/tcp/4001"},
		"Protocols": []string{"transport-bitswap"},
	}
	genericRec := map[string]any{
		"Schema":    types.SchemaGeneric,
		"ID":        "gateway-provider",
		"Addrs":     []string{"https://gateway.example.com"},
		"Protocols": []string{"transport-ipfs-gateway-http"},
	}
	//nolint:staticcheck
	bitswapRec := map[string]any{
		//lint:ignore SA1019 // ignore staticcheck
		"Schema":   types.SchemaBitswap,
		"ID":       "12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn",
		"Protocol": "transport-bitswap",
		"Addrs":    []string{"/ip4/127.0.0.1/udp/4001/quic-v1"},
	}
	unknownRec := map[string]any{
		"Schema": "future-schema",
		"ID":     "something",
	}

	var lines []string
	for _, rec := range []any{peerRec, genericRec, bitswapRec, unknownRec} {
		data, err := json.Marshal(rec)
		require.NoError(t, err)
		lines = append(lines, string(data))
	}
	ndjson := strings.Join(lines, "\n") + "\n"

	ri := NewRecordsIter(strings.NewReader(ndjson))
	results := iter.ReadAll[iter.Result[types.Record]](ri)
	require.Len(t, results, 4)

	// Record 0: PeerRecord
	require.NoError(t, results[0].Err)
	_, ok := results[0].Val.(*types.PeerRecord)
	assert.True(t, ok, "first record should be *types.PeerRecord")

	// Record 1: GenericRecord
	require.NoError(t, results[1].Err)
	gr, ok := results[1].Val.(*types.GenericRecord)
	assert.True(t, ok, "second record should be *types.GenericRecord")
	assert.Equal(t, "gateway-provider", gr.ID)
	require.Len(t, gr.Addrs, 1)
	assert.Equal(t, "https://gateway.example.com", gr.Addrs[0].String())

	// Record 2: BitswapRecord
	require.NoError(t, results[2].Err)
	//nolint:staticcheck
	//lint:ignore SA1019 // ignore staticcheck
	_, ok = results[2].Val.(*types.BitswapRecord)
	assert.True(t, ok, "third record should be *types.BitswapRecord")

	// Record 3: unknown schema falls through as UnknownRecord
	require.NoError(t, results[3].Err)
	_, ok = results[3].Val.(*types.UnknownRecord)
	assert.True(t, ok, "fourth record should be *types.UnknownRecord")
}

// TestNewPeerRecordsIter_SkipsGenericRecord verifies that NewPeerRecordsIter
// silently skips GenericRecord entries and only returns PeerRecords.
func TestNewPeerRecordsIter_SkipsGenericRecord(t *testing.T) {
	peerRec := map[string]any{
		"Schema":    types.SchemaPeer,
		"ID":        "12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn",
		"Addrs":     []string{"/ip4/127.0.0.1/tcp/4001"},
		"Protocols": []string{"transport-bitswap"},
	}
	genericRec := map[string]any{
		"Schema":    types.SchemaGeneric,
		"ID":        "gateway-provider",
		"Addrs":     []string{"https://gateway.example.com"},
		"Protocols": []string{"transport-ipfs-gateway-http"},
	}
	peerRec2 := map[string]any{
		"Schema":    types.SchemaPeer,
		"ID":        "12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vz",
		"Addrs":     []string{"/ip4/127.0.0.2/tcp/4001"},
		"Protocols": []string{"transport-ipfs-gateway-http"},
	}

	var lines []string
	for _, rec := range []any{peerRec, genericRec, peerRec2} {
		data, err := json.Marshal(rec)
		require.NoError(t, err)
		lines = append(lines, string(data))
	}
	ndjson := strings.Join(lines, "\n") + "\n"

	ri := NewPeerRecordsIter(strings.NewReader(ndjson))
	results := iter.ReadAll[iter.Result[*types.PeerRecord]](ri)

	// GenericRecord produces a result with nil Val (skipped), so we collect
	// only non-nil PeerRecords.
	var peerResults []*types.PeerRecord
	for _, r := range results {
		require.NoError(t, r.Err)
		if r.Val != nil {
			peerResults = append(peerResults, r.Val)
		}
	}

	require.Len(t, peerResults, 2, "only PeerRecords should come through")
	assert.Equal(t, types.SchemaPeer, peerResults[0].Schema)
	assert.Equal(t, types.SchemaPeer, peerResults[1].Schema)
}
