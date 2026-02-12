package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenericRecord_GetSchema(t *testing.T) {
	gr := GenericRecord{Schema: SchemaGeneric}
	assert.Equal(t, SchemaGeneric, gr.GetSchema())
}

func TestGenericRecord_JSON(t *testing.T) {
	t.Run("round-trip with mixed addresses", func(t *testing.T) {
		// GenericRecord uses duck-typed Addrs that accept both multiaddrs and URIs.
		// This verifies the core IPIP-518 use case: a provider advertising both
		// libp2p multiaddrs and HTTP gateway URLs in a single record.
		jsonData := `{
			"Schema": "generic",
			"ID": "12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn",
			"Addrs": [
				"/ip4/192.168.1.1/tcp/4001",
				"https://trustless-gateway.example.com",
				"http://example.org:8080",
				"/dns4/libp2p.example.com/tcp/443/wss"
			],
			"Protocols": ["transport-bitswap", "transport-ipfs-gateway-http"]
		}`

		var gr GenericRecord
		err := json.Unmarshal([]byte(jsonData), &gr)
		require.NoError(t, err)

		assert.Equal(t, SchemaGeneric, gr.Schema)
		assert.Equal(t, "12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn", gr.ID)
		assert.Len(t, gr.Addrs, 4)
		assert.Equal(t, []string{"transport-bitswap", "transport-ipfs-gateway-http"}, gr.Protocols)

		// Verify address types were correctly parsed
		assert.True(t, gr.Addrs[0].IsMultiaddr(), "first addr should be multiaddr")
		assert.True(t, gr.Addrs[1].IsURL(), "second addr should be URL")
		assert.True(t, gr.Addrs[2].IsURL(), "third addr should be URL")
		assert.True(t, gr.Addrs[3].IsMultiaddr(), "fourth addr should be multiaddr")

		// Marshal back and verify fields are present
		data, err := json.Marshal(gr)
		require.NoError(t, err)

		var check map[string]any
		err = json.Unmarshal(data, &check)
		require.NoError(t, err)

		assert.Equal(t, "generic", check["Schema"])
		assert.Equal(t, "12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn", check["ID"])

		addrs, ok := check["Addrs"].([]any)
		require.True(t, ok)
		assert.Len(t, addrs, 4)
		assert.Equal(t, "/ip4/192.168.1.1/tcp/4001", addrs[0])
		assert.Equal(t, "https://trustless-gateway.example.com", addrs[1])
	})

	t.Run("extra fields are preserved", func(t *testing.T) {
		// Unknown fields must be preserved in Extra during deserialization and
		// included again when marshaling. This allows forward compatibility with
		// future spec extensions without data loss.
		jsonData := `{
			"Schema": "generic",
			"ID": "peer1",
			"Addrs": ["https://example.com"],
			"Protocols": ["transport-ipfs-gateway-http"],
			"CustomField": "custom-value",
			"AnotherField": 42
		}`

		var gr GenericRecord
		err := json.Unmarshal([]byte(jsonData), &gr)
		require.NoError(t, err)

		assert.Len(t, gr.Extra, 2)
		assert.Contains(t, gr.Extra, "CustomField")
		assert.Contains(t, gr.Extra, "AnotherField")

		// Known fields must not leak into Extra
		assert.NotContains(t, gr.Extra, "Schema")
		assert.NotContains(t, gr.Extra, "ID")
		assert.NotContains(t, gr.Extra, "Addrs")
		assert.NotContains(t, gr.Extra, "Protocols")

		// Marshal and verify extra fields survive the round-trip
		data, err := json.Marshal(gr)
		require.NoError(t, err)

		var check map[string]any
		err = json.Unmarshal(data, &check)
		require.NoError(t, err)
		assert.Equal(t, "custom-value", check["CustomField"])
		assert.Equal(t, float64(42), check["AnotherField"])
	})

	t.Run("nil Addrs and Protocols are omitted", func(t *testing.T) {
		// When Addrs or Protocols are nil (not just empty), they should be
		// omitted from the JSON output. This matches the behavior of PeerRecord
		// and avoids sending unnecessary null fields.
		gr := GenericRecord{
			Schema: SchemaGeneric,
			ID:     "peer1",
		}

		data, err := json.Marshal(gr)
		require.NoError(t, err)

		var check map[string]any
		err = json.Unmarshal(data, &check)
		require.NoError(t, err)

		assert.Equal(t, "generic", check["Schema"])
		assert.Equal(t, "peer1", check["ID"])
		_, hasAddrs := check["Addrs"]
		assert.False(t, hasAddrs, "nil Addrs should be omitted from JSON")
		_, hasProtos := check["Protocols"]
		assert.False(t, hasProtos, "nil Protocols should be omitted from JSON")
	})

	t.Run("empty Addrs are included", func(t *testing.T) {
		// An explicitly empty Addrs slice should be included in JSON output
		// (as opposed to nil which is omitted), because it signals "no addresses"
		// rather than "field not provided".
		gr := GenericRecord{
			Schema:    SchemaGeneric,
			ID:        "peer1",
			Addrs:     Addresses{},
			Protocols: []string{},
		}

		data, err := json.Marshal(gr)
		require.NoError(t, err)

		var check map[string]any
		err = json.Unmarshal(data, &check)
		require.NoError(t, err)

		_, hasAddrs := check["Addrs"]
		assert.True(t, hasAddrs, "empty Addrs should be included in JSON")
		_, hasProtos := check["Protocols"]
		assert.True(t, hasProtos, "empty Protocols should be included in JSON")
	})

	t.Run("non-standard URI schemes are valid addresses", func(t *testing.T) {
		// IPIP-518 is schema-agnostic: any absolute URI is a valid address.
		// This includes non-standard schemes like foo:// or tcp:// that may be
		// used by future transports. They must parse as valid URL addresses.
		jsonData := `{
			"Schema": "generic",
			"ID": "peer1",
			"Addrs": [
				"https://example.com",
				"foo://custom-transport.example.com/path",
				"tcp://192.168.1.1:4001",
				"/ip4/127.0.0.1/tcp/4001"
			],
			"Protocols": ["transport-foo"]
		}`

		var gr GenericRecord
		err := json.Unmarshal([]byte(jsonData), &gr)
		require.NoError(t, err)

		assert.Len(t, gr.Addrs, 4)

		assert.True(t, gr.Addrs[0].IsURL(), "https should be a valid URL")
		assert.True(t, gr.Addrs[1].IsURL(), "foo:// should be a valid URL")
		assert.True(t, gr.Addrs[2].IsURL(), "tcp:// should be a valid URL")
		assert.True(t, gr.Addrs[3].IsMultiaddr(), "multiaddr should be a valid multiaddr")

		// Verify protocol detection works for non-standard schemes
		assert.Equal(t, []string{"https"}, gr.Addrs[0].Protocols())
		assert.Equal(t, []string{"foo"}, gr.Addrs[1].Protocols())
		assert.Equal(t, []string{"tcp"}, gr.Addrs[2].Protocols())

		// Marshal round-trip preserves all addresses
		data, err := json.Marshal(gr)
		require.NoError(t, err)

		var check map[string]any
		err = json.Unmarshal(data, &check)
		require.NoError(t, err)

		addrs := check["Addrs"].([]any)
		assert.Len(t, addrs, 4)
		assert.Equal(t, "foo://custom-transport.example.com/path", addrs[1])
		assert.Equal(t, "tcp://192.168.1.1:4001", addrs[2])
	})

	t.Run("unparseable addresses are preserved", func(t *testing.T) {
		// Per IPIP-518, implementations MUST NOT fail on addresses they cannot
		// parse. Invalid addresses are stored as raw strings and survive the
		// round-trip, allowing forward compatibility with future address formats.
		// This is distinct from non-standard URI schemes (which ARE valid).
		jsonData := `{
			"Schema": "generic",
			"ID": "peer1",
			"Addrs": [
				"https://example.com",
				"foo://valid-but-unknown-scheme.example.com",
				"/invalid/multiaddr",
				"not-a-valid-address"
			]
		}`

		var gr GenericRecord
		err := json.Unmarshal([]byte(jsonData), &gr)
		require.NoError(t, err)

		assert.Len(t, gr.Addrs, 4)
		assert.True(t, gr.Addrs[0].IsValid(), "https URL should be valid")
		assert.True(t, gr.Addrs[1].IsValid(), "foo:// URI should be valid")
		assert.False(t, gr.Addrs[2].IsValid(), "invalid multiaddr should be marked invalid")
		assert.False(t, gr.Addrs[3].IsValid(), "bare string should be marked invalid")

		// All addresses survive marshal round-trip, even invalid ones
		data, err := json.Marshal(gr)
		require.NoError(t, err)

		var check map[string]any
		err = json.Unmarshal(data, &check)
		require.NoError(t, err)

		addrs := check["Addrs"].([]any)
		assert.Equal(t, "https://example.com", addrs[0])
		assert.Equal(t, "foo://valid-but-unknown-scheme.example.com", addrs[1])
		assert.Equal(t, "/invalid/multiaddr", addrs[2])
		assert.Equal(t, "not-a-valid-address", addrs[3])
	})
}
