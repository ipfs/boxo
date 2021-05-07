package io

import (
	"context"
	"fmt"
	"math"
	"testing"

	ipld "github.com/ipfs/go-ipld-format"
	mdag "github.com/ipfs/go-merkledag"
	mdtest "github.com/ipfs/go-merkledag/test"

	ft "github.com/ipfs/go-unixfs"
)

func TestEmptyNode(t *testing.T) {
	n := ft.EmptyDirNode()
	if len(n.Links()) != 0 {
		t.Fatal("empty node should have 0 links")
	}
}

func TestDirectoryGrowth(t *testing.T) {
	ds := mdtest.Mock()
	dir := NewDirectory(ds)
	ctx := context.Background()

	d := ft.EmptyDirNode()
	ds.Add(ctx, d)

	nelems := 10000

	for i := 0; i < nelems; i++ {
		err := dir.AddChild(ctx, fmt.Sprintf("dir%d", i), d)
		if err != nil {
			t.Fatal(err)
		}
	}

	_, err := dir.GetNode()
	if err != nil {
		t.Fatal(err)
	}

	links, err := dir.Links(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(links) != nelems {
		t.Fatal("didnt get right number of elements")
	}

	dirc := d.Cid()

	names := make(map[string]bool)
	for _, l := range links {
		names[l.Name] = true
		if !l.Cid.Equals(dirc) {
			t.Fatal("link wasnt correct")
		}
	}

	for i := 0; i < nelems; i++ {
		dn := fmt.Sprintf("dir%d", i)
		if !names[dn] {
			t.Fatal("didnt find directory: ", dn)
		}

		_, err := dir.Find(context.Background(), dn)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDuplicateAddDir(t *testing.T) {
	ds := mdtest.Mock()
	dir := NewDirectory(ds)
	ctx := context.Background()
	nd := ft.EmptyDirNode()

	err := dir.AddChild(ctx, "test", nd)
	if err != nil {
		t.Fatal(err)
	}

	err = dir.AddChild(ctx, "test", nd)
	if err != nil {
		t.Fatal(err)
	}

	lnks, err := dir.Links(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(lnks) != 1 {
		t.Fatal("expected only one link")
	}
}

// FIXME: Nothing blocking but nice to have:
//  * Check estimated size against link enumeration (indirectly done in the
//    restored node check from NewDirectoryFromNode).
//  * Check estimated size against encoded node (the difference should only be
//    a small percentage for a directory with 10s of entries).
func TestBasicDirectory_estimatedSize(t *testing.T) {
	ds := mdtest.Mock()
	ctx := context.Background()
	child := ft.EmptyFileNode()
	err := ds.Add(ctx, child)
	if err != nil {
		t.Fatal(err)
	}

	basicDir := newEmptyBasicDirectory(ds)

	// Several overwrites should not corrupt the size estimation.
	basicDir.AddChild(ctx, "child", child)
	basicDir.AddChild(ctx, "child", child)
	basicDir.AddChild(ctx, "child", child)
	basicDir.RemoveChild(ctx, "child")
	basicDir.AddChild(ctx, "child", child)
	basicDir.RemoveChild(ctx, "child")
	// FIXME: Check errors above (abstract adds/removals in iteration).
	if basicDir.estimatedSize != 0 {
		t.Fatal("estimated size is not zero after removing all entries")
	}

	for i := 0; i < 100; i++ {
		basicDir.AddChild(ctx, fmt.Sprintf("child-%03d", i), child) // e.g., "child-045"
	}
	// Estimated entry size: name (9) + CID (32 from hash and 2 extra for header)
	entrySize := 9 + 32 + 2
	expectedSize := 100 * entrySize
	if basicDir.estimatedSize != expectedSize {
		t.Fatalf("estimated size (%d) inaccurate after adding many entries (expected %d)",
			basicDir.estimatedSize, expectedSize)
	}

	basicDir.RemoveChild(ctx, "child-045") // just random values
	basicDir.RemoveChild(ctx, "child-063")
	basicDir.RemoveChild(ctx, "child-011")
	basicDir.RemoveChild(ctx, "child-000")
	basicDir.RemoveChild(ctx, "child-099")

	basicDir.RemoveChild(ctx, "child-045")        // already removed, won't impact size
	basicDir.RemoveChild(ctx, "nonexistent-name") // also doesn't count
	basicDir.RemoveChild(ctx, "child-100")        // same
	expectedSize -= 5 * entrySize
	if basicDir.estimatedSize != expectedSize {
		t.Fatalf("estimated size (%d) inaccurate after removing some entries (expected %d)",
			basicDir.estimatedSize, expectedSize)
	}

	// Restore a directory from original's node and check estimated size consistency.
	basicDirSingleNode, _ := basicDir.GetNode() // no possible error
	restoredBasicDir := newBasicDirectoryFromNode(ds, basicDirSingleNode.(*mdag.ProtoNode))
	if basicDir.estimatedSize != restoredBasicDir.estimatedSize {
		t.Fatalf("restored basic directory size (%d) doesn't match original estimate (%d)",
			basicDir.estimatedSize, restoredBasicDir.estimatedSize)
	}
}

// Basic test on extreme threshold to trigger switch. More fine-grained sizes
// are checked in TestBasicDirectory_estimatedSize (without the swtich itself
// but focusing on the size computation).
// FIXME: Ideally, instead of checking size computation on one test and directory
//  upgrade on another a better structured test should test both dimensions
//  simultaneously.
func TestUpgradeableDirectory(t *testing.T) {
	oldHamtOption := HAMTShardingSize
	defer func() { HAMTShardingSize = oldHamtOption }()

	ds := mdtest.Mock()
	dir := NewDirectory(ds)
	ctx := context.Background()
	child := ft.EmptyDirNode()
	err := ds.Add(ctx, child)
	if err != nil {
		t.Fatal(err)
	}

	HAMTShardingSize = 0 // Create a BasicDirectory.
	if _, ok := dir.(*UpgradeableDirectory).Directory.(*BasicDirectory); !ok {
		t.Fatal("UpgradeableDirectory doesn't contain BasicDirectory")
	}

	// Set a threshold so big a new entry won't trigger the change.
	HAMTShardingSize = math.MaxInt32

	err = dir.AddChild(ctx, "test", child)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := dir.(*UpgradeableDirectory).Directory.(*HAMTDirectory); ok {
		t.Fatal("UpgradeableDirectory was upgraded to HAMTDirectory for a large threshold")
	}

	// Now set it so low to make sure any new entry will trigger the upgrade.
	HAMTShardingSize = 1

	err = dir.AddChild(ctx, "test", child) // overwriting an entry should also trigger the switch
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := dir.(*UpgradeableDirectory).Directory.(*HAMTDirectory); !ok {
		t.Fatal("UpgradeableDirectory wasn't upgraded to HAMTDirectory for a low threshold")
	}
}

func TestDirBuilder(t *testing.T) {
	ds := mdtest.Mock()
	dir := NewDirectory(ds)
	ctx := context.Background()

	child := ft.EmptyDirNode()
	err := ds.Add(ctx, child)
	if err != nil {
		t.Fatal(err)
	}

	count := 5000

	for i := 0; i < count; i++ {
		err := dir.AddChild(ctx, fmt.Sprintf("entry %d", i), child)
		if err != nil {
			t.Fatal(err)
		}
	}

	dirnd, err := dir.GetNode()
	if err != nil {
		t.Fatal(err)
	}

	links, err := dir.Links(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(links) != count {
		t.Fatal("not enough links dawg", len(links), count)
	}

	adir, err := NewDirectoryFromNode(ds, dirnd)
	if err != nil {
		t.Fatal(err)
	}

	links, err = adir.Links(ctx)
	if err != nil {
		t.Fatal(err)
	}

	names := make(map[string]bool)
	for _, lnk := range links {
		names[lnk.Name] = true
	}

	for i := 0; i < count; i++ {
		n := fmt.Sprintf("entry %d", i)
		if !names[n] {
			t.Fatal("COULDNT FIND: ", n)
		}
	}

	if len(links) != count {
		t.Fatal("wrong number of links", len(links), count)
	}

	linkResults := dir.EnumLinksAsync(ctx)

	asyncNames := make(map[string]bool)
	var asyncLinks []*ipld.Link

	for linkResult := range linkResults {
		if linkResult.Err != nil {
			t.Fatal(linkResult.Err)
		}
		asyncNames[linkResult.Link.Name] = true
		asyncLinks = append(asyncLinks, linkResult.Link)
	}

	for i := 0; i < count; i++ {
		n := fmt.Sprintf("entry %d", i)
		if !asyncNames[n] {
			t.Fatal("COULDNT FIND: ", n)
		}
	}

	if len(asyncLinks) != count {
		t.Fatal("wrong number of links", len(asyncLinks), count)
	}
}
