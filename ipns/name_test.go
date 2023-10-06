package ipns

import (
	"encoding/json"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestName(t *testing.T) {
	t.Parallel()

	testFromCid := func(t *testing.T, name, input, expected string) {
		t.Run("NameFromCid method: "+name, func(t *testing.T) {
			t.Parallel()

			c, err := cid.Parse(input)
			require.NoError(t, err)

			name, err := NameFromCid(c)
			require.NoError(t, err)
			require.Equal(t, expected, name.String())
		})
	}

	testString := func(t *testing.T, name, input, expected string) {
		t.Run("String method: "+name, func(t *testing.T) {
			t.Parallel()

			name, err := NameFromString(input)
			require.NoError(t, err)
			require.Equal(t, expected, name.String())
		})
	}

	testPath := func(t *testing.T, name, input, expected string) {
		t.Run("AsPath method: "+name, func(t *testing.T) {
			t.Parallel()

			name, err := NameFromString(input)
			require.NoError(t, err)
			require.Equal(t, expected, name.AsPath().String())
		})
	}

	testMarshalJSON := func(t *testing.T, name, input, expected string) {
		t.Run("Marshal JSON: "+name, func(t *testing.T) {
			t.Parallel()

			name, err := NameFromString(input)
			require.NoError(t, err)
			raw, err := json.Marshal(name)
			require.NoError(t, err)
			require.Equal(t, expected, string(raw))
		})
	}

	testUnmarshalJSON := func(t *testing.T, name string, input []byte, expected string) {
		t.Run("Unmarshal JSON: "+name, func(t *testing.T) {
			t.Parallel()

			var name Name
			err := json.Unmarshal(input, &name)
			require.NoError(t, err)
			require.Equal(t, expected, name.String())
		})
	}

	for _, v := range [][]string{
		{"RSA", "QmRp2LvtSQtCkUWCpi92ph5MdQyRtfb9jHbkNgZzGExGuG", "k2k4r8kpauqq30hoj9oktej5btbgz1jeos16d3te36xd78trvak0jcor"},
		{"Ed25519", "12D3KooWSzRuSFHgLsKr6jJboAPdP7xMga2YBgBspYuErxswcgvt", "k51qzi5uqu5dmjjgoe7s21dncepi970722cn30qlhm9qridas1c9ktkjb6ejux"},
		{"ECDSA", "QmSBUTocZ9LxE53Br9PDDcPWnR1FJQRv94U96Wkt8eypAw", "k2k4r8ku8cnc1sl2h5xn7i07dma9abfnkqkxi4a6nd1xq0knoxe7b0y4"},
		{"Secp256k1", "16Uiu2HAmUymv6JpFwNZppdKUMxGJuHsTeicXgHGKbBasu4Ruj3K1", "kzwfwjn5ji4puw3jc1qw4b073j74xvq21iziuqw4rem21pr7f0l4dj8i9yb978s"},
	} {
		testFromCid(t, v[0], v[2], v[2])
		testString(t, v[0], v[1], v[2])
		testString(t, v[0], NamespacePrefix+v[1], v[2])
		testPath(t, v[0], v[1], NamespacePrefix+v[2])
		testMarshalJSON(t, v[0], v[1], `"`+v[2]+`"`)
		testMarshalJSON(t, v[0], NamespacePrefix+v[1], `"`+v[2]+`"`)
		testUnmarshalJSON(t, v[0], []byte(`"`+v[2]+`"`), v[2])
	}
}
