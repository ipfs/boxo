package unixfs_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/ipsl"
	"github.com/ipfs/go-libipfs/ipsl/helpers"
	. "github.com/ipfs/go-libipfs/ipsl/unixfs"
	"golang.org/x/exp/slices"
)

func TestEverythingThroughScope(t *testing.T) {
	var c ipsl.Compiler
	c.SetBuiltinScope(Name, Scope())

	var code = `[unixfs (load-builtin-scope ` + strconv.Quote(string(Name)) + `) (unixfs.everything)]`

	traversal, n, err := c.CompileToTraversal(strings.NewReader(code))
	if err != nil {
		t.Fatalf("CompileToTraversal: %s", err)
	}
	if n != len(code) {
		t.Errorf("wrong code length, expected %d; got %d", len(code), n)
	}

	bs, expectedOrder := getSmallTreeDatastore(t)
	root := expectedOrder[0]
	var result []cid.Cid
	err = helpers.SyncDFS(context.Background(), root, traversal, bs, 10, func(b blocks.Block) error {
		c := b.Cid()
		hashedData, err := c.Prefix().Sum(b.RawData())
		if err != nil {
			t.Errorf("error hashing data in callBack: %s", err)
		} else {
			if !hashedData.Equals(c) {
				t.Errorf("got wrong bytes in callBack: cid %s; hashedBytes %s", c, hashedData)
			}
		}

		result = append(result, c)

		return nil
	})
	if err != nil {
		t.Fatalf("SyncDFS: %s", err)
	}

	if !slices.Equal(result, expectedOrder) {
		t.Errorf("bad traversal order: expected: %v; got %v", expectedOrder, result)
	}

	ast, err := traversal.Serialize()
	if err != nil {
		t.Errorf("Serialize: %s", err)
	}

	serialized := ast.String()
	if serialized != code {
		t.Errorf("seriliazed code does not match original code: expected %q; got %q", code, serialized)
	}
}
