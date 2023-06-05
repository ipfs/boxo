package dsindex

import (
	"context"
	"testing"

	ds "github.com/ipfs/go-datastore"
)

func createIndexer() Indexer {
	dstore := ds.NewMapDatastore()
	nameIndex := New(dstore, ds.NewKey("/data/nameindex"))

	ctx := context.Background()
	nameIndex.Add(ctx, "alice", "a1")
	nameIndex.Add(ctx, "bob", "b1")
	nameIndex.Add(ctx, "bob", "b2")
	nameIndex.Add(ctx, "cathy", "c1")

	return nameIndex
}

func TestAdd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nameIndex := createIndexer()
	err := nameIndex.Add(ctx, "someone", "s1")
	if err != nil {
		t.Fatal(err)
	}
	err = nameIndex.Add(ctx, "someone", "s1")
	if err != nil {
		t.Fatal(err)
	}

	err = nameIndex.Add(ctx, "", "noindex")
	if err != ErrEmptyKey {
		t.Fatal("unexpected error:", err)
	}

	err = nameIndex.Add(ctx, "nokey", "")
	if err != ErrEmptyValue {
		t.Fatal("unexpected error:", err)
	}
}

func TestHasValue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nameIndex := createIndexer()

	ok, err := nameIndex.HasValue(ctx, "bob", "b1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing index")
	}

	ok, err = nameIndex.HasValue(ctx, "bob", "b3")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should not have index")
	}

	_, err = nameIndex.HasValue(ctx, "", "b1")
	if err != ErrEmptyKey {
		t.Fatal("unexpected error:", err)
	}

	_, err = nameIndex.HasValue(ctx, "bob", "")
	if err != ErrEmptyValue {
		t.Fatal("unexpected error:", err)
	}
}

func TestHasAny(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nameIndex := createIndexer()

	ok, err := nameIndex.HasAny(ctx, "nothere")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should return false")
	}

	for _, idx := range []string{"alice", "bob", ""} {
		ok, err = nameIndex.HasAny(ctx, idx)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("missing index", idx)
		}
	}

	count, err := nameIndex.DeleteAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Fatal("expected 4 deletions")
	}

	ok, err = nameIndex.HasAny(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should return false")
	}
}

func TestForEach(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nameIndex := createIndexer()

	found := make(map[string]struct{})
	err := nameIndex.ForEach(ctx, "bob", func(key, value string) bool {
		found[value] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, value := range []string{"b1", "b2"} {
		_, ok := found[value]
		if !ok {
			t.Fatal("missing key for value", value)
		}
	}

	values := map[string]string{}
	err = nameIndex.ForEach(ctx, "", func(key, value string) bool {
		values[value] = key
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 4 {
		t.Fatal("expected 4 keys")
	}

	if values["a1"] != "alice" {
		t.Error("expected a1: alice")
	}
	if values["b1"] != "bob" {
		t.Error("expected b1: bob")
	}
	if values["b2"] != "bob" {
		t.Error("expected b2: bob")
	}
	if values["c1"] != "cathy" {
		t.Error("expected c1: cathy")
	}
}

func TestSearch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nameIndex := createIndexer()

	ids, err := nameIndex.Search(ctx, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatal("wrong number of ids - expected 2 got", ids)
	}
	for _, id := range ids {
		if id != "b1" && id != "b2" {
			t.Fatal("wrong value in id set")
		}
	}
	if ids[0] == ids[1] {
		t.Fatal("duplicate id")
	}

	ids, err = nameIndex.Search(ctx, "cathy")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "c1" {
		t.Fatal("wrong ids")
	}

	ids, err = nameIndex.Search(ctx, "amit")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatal("unexpected ids returned")
	}
}

func TestDelete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nameIndex := createIndexer()

	err := nameIndex.Delete(ctx, "bob", "b3")
	if err != nil {
		t.Fatal(err)
	}

	err = nameIndex.Delete(ctx, "alice", "a1")
	if err != nil {
		t.Fatal(err)
	}

	ok, err := nameIndex.HasValue(ctx, "alice", "a1")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("index key should have been deleted")
	}

	count, err := nameIndex.DeleteKey(ctx, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatal("wrong deleted count")
	}
	ok, _ = nameIndex.HasValue(ctx, "bob", "b1")
	if ok {
		t.Fatal("index not deleted")
	}
}

func TestSyncIndex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nameIndex := createIndexer()

	dstore := ds.NewMapDatastore()
	refIndex := New(dstore, ds.NewKey("/ref"))
	refIndex.Add(ctx, "alice", "a1")
	refIndex.Add(ctx, "cathy", "zz")
	refIndex.Add(ctx, "dennis", "d1")

	changed, err := SyncIndex(ctx, refIndex, nameIndex)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Error("change was not indicated")
	}

	// Create map of id->index in sync target
	syncs := map[string]string{}
	err = nameIndex.ForEach(ctx, "", func(key, value string) bool {
		syncs[value] = key
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	// Iterate items in sync source and make sure they appear in target
	var itemCount int
	err = refIndex.ForEach(ctx, "", func(key, value string) bool {
		itemCount++
		syncKey, ok := syncs[value]
		if !ok || key != syncKey {
			t.Fatal("key", key, "-->", value, "was not synced")
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	if itemCount != len(syncs) {
		t.Fatal("different number of items in sync source and target")
	}
}
