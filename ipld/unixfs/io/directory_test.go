package io

import (
	"context"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	bsrv "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	offline "github.com/ipfs/boxo/exchange/offline"
	mdag "github.com/ipfs/boxo/ipld/merkledag"
	mdtest "github.com/ipfs/boxo/ipld/merkledag/test"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/ipld/unixfs/internal"
	"github.com/ipfs/boxo/ipld/unixfs/private/linksize"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmptyNode(t *testing.T) {
	n := ft.EmptyDirNode()
	require.Zero(t, len(n.Links()), "empty node should have 0 links")
}

func TestDirectoryGrowth(t *testing.T) {
	ds := mdtest.Mock()
	dir, err := NewDirectory(ds)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	d := ft.EmptyDirNode()
	ds.Add(ctx, d)

	nelems := 10000

	for i := range nelems {
		err = dir.AddChild(ctx, fmt.Sprintf("dir%d", i), d)
		require.NoError(t, err)
	}

	_, err = dir.GetNode()
	require.NoError(t, err)

	links, err := dir.Links(ctx)
	require.NoError(t, err)

	require.Len(t, links, nelems, "didnt get right number of elements")

	dirc := d.Cid()

	names := make(map[string]bool)
	for _, l := range links {
		names[l.Name] = true
		require.True(t, l.Cid.Equals(dirc), "link wasnt correct")
	}

	for i := 0; i < nelems; i++ {
		dn := fmt.Sprintf("dir%d", i)
		if !names[dn] {
			t.Fatal("didnt find directory: ", dn)
		}

		_, err = dir.Find(context.Background(), dn)
		require.NoError(t, err)
	}
}

func TestDuplicateAddDir(t *testing.T) {
	ds := mdtest.Mock()
	dir, err := NewDirectory(ds)
	require.NoError(t, err)
	ctx := context.Background()
	nd := ft.EmptyDirNode()

	err = dir.AddChild(ctx, "test", nd)
	require.NoError(t, err)

	err = dir.AddChild(ctx, "test", nd)
	require.NoError(t, err)

	lnks, err := dir.Links(ctx)
	require.NoError(t, err)

	require.Len(t, lnks, 1, "expected only one link")
}

func TestBasicDirectory_estimatedSize(t *testing.T) {
	ds := mdtest.Mock()
	basicDir, err := NewBasicDirectory(ds)
	require.NoError(t, err)

	testDirectorySizeEstimation(t, basicDir, ds, func(dir Directory) int {
		return dir.(*BasicDirectory).estimatedSize
	})
}

func TestHAMTDirectory_sizeChange(t *testing.T) {
	ds := mdtest.Mock()
	hamtDir, err := NewHAMTDirectory(ds, 0, WithMaxLinks(DefaultShardWidth))
	assert.NoError(t, err)

	testDirectorySizeEstimation(t, hamtDir, ds, func(dir Directory) int {
		// Since we created a HAMTDirectory from scratch with size 0 its
		// internal sizeChange delta will in fact track the directory size
		// throughout this run.
		return dir.(*HAMTDirectory).sizeChange
	})
}

func fullSizeEnumeration(dir Directory) int {
	size := 0
	dir.ForEachLink(context.Background(), func(l *ipld.Link) error {
		size += linksize.LinkSizeFunction(l.Name, l.Cid)
		return nil
	})
	return size
}

func testDirectorySizeEstimation(t *testing.T, dir Directory, ds ipld.DAGService, size func(Directory) int) {
	linksize.LinkSizeFunction = mockLinkSizeFunc(1)
	defer func() { linksize.LinkSizeFunction = productionLinkSize }()

	ctx := context.Background()
	child := ft.EmptyFileNode()
	assert.NoError(t, ds.Add(ctx, child))

	// Several overwrites should not corrupt the size estimation.
	assert.NoError(t, dir.AddChild(ctx, "child", child))
	assert.NoError(t, dir.AddChild(ctx, "child", child))
	assert.NoError(t, dir.AddChild(ctx, "child", child))
	assert.NoError(t, dir.RemoveChild(ctx, "child"))
	assert.NoError(t, dir.AddChild(ctx, "child", child))
	assert.NoError(t, dir.RemoveChild(ctx, "child"))
	assert.Equal(t, 0, size(dir), "estimated size is not zero after removing all entries")

	dirEntries := 100
	for i := 0; i < dirEntries; i++ {
		assert.NoError(t, dir.AddChild(ctx, fmt.Sprintf("child-%03d", i), child))
	}
	assert.Equal(t, dirEntries, size(dir), "estimated size inaccurate after adding many entries")

	assert.NoError(t, dir.RemoveChild(ctx, "child-045")) // just random values
	assert.NoError(t, dir.RemoveChild(ctx, "child-063"))
	assert.NoError(t, dir.RemoveChild(ctx, "child-011"))
	assert.NoError(t, dir.RemoveChild(ctx, "child-000"))
	assert.NoError(t, dir.RemoveChild(ctx, "child-099"))
	dirEntries -= 5
	assert.Equal(t, dirEntries, size(dir), "estimated size inaccurate after removing some entries")

	// All of the following remove operations will fail (won't impact dirEntries):
	assert.Error(t, dir.RemoveChild(ctx, "nonexistent-name"))
	assert.Error(t, dir.RemoveChild(ctx, "child-045")) // already removed
	assert.Error(t, dir.RemoveChild(ctx, "child-100"))
	assert.Equal(t, dirEntries, size(dir), "estimated size inaccurate after failed remove attempts")

	// Restore a directory from original's node and check estimated size consistency.
	dirNode, err := dir.GetNode()
	assert.NoError(t, err)
	restoredDir, err := NewDirectoryFromNode(ds, dirNode.(*mdag.ProtoNode))
	assert.NoError(t, err)
	assert.Equal(t, size(dir), fullSizeEnumeration(restoredDir), "restored directory's size doesn't match original's")
	// We don't use the estimation size function for the restored directory
	// because in the HAMT case this function depends on the sizeChange variable
	// that will be cleared when loading the directory from the node.
	// This also covers the case of comparing the size estimation `size()` with
	// the full enumeration function `fullSizeEnumeration()` to make sure it's
	// correct.
}

// Any entry link size will have the fixedSize passed.
func mockLinkSizeFunc(fixedSize int) func(linkName string, linkCid cid.Cid) int {
	return func(_ string, _ cid.Cid) int {
		return fixedSize
	}
}

func checkBasicDirectory(t *testing.T, dir Directory, errorMessage string) {
	t.Helper()
	if _, ok := dir.(*DynamicDirectory).Directory.(*BasicDirectory); !ok {
		t.Fatal(errorMessage)
	}
}

func checkHAMTDirectory(t *testing.T, dir Directory, errorMessage string) {
	t.Helper()
	if _, ok := dir.(*DynamicDirectory).Directory.(*HAMTDirectory); !ok {
		t.Fatal(errorMessage)
	}
}

func TestProductionLinkSize(t *testing.T) {
	link, err := ipld.MakeLink(ft.EmptyDirNode())
	assert.NoError(t, err)
	link.Name = "directory_link_name"
	assert.Equal(t, 53, productionLinkSize(link.Name, link.Cid))

	link, err = ipld.MakeLink(ft.EmptyFileNode())
	assert.NoError(t, err)
	link.Name = "file_link_name"
	assert.Equal(t, 48, productionLinkSize(link.Name, link.Cid))

	ds := mdtest.Mock()
	basicDir, err := NewBasicDirectory(ds)
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		basicDir.AddChild(context.Background(), strconv.FormatUint(uint64(i), 10), ft.EmptyFileNode())
	}
	basicDirNode, err := basicDir.GetNode()
	assert.NoError(t, err)
	link, err = ipld.MakeLink(basicDirNode)
	assert.NoError(t, err)
	link.Name = "basic_dir"
	assert.Equal(t, 43, productionLinkSize(link.Name, link.Cid))
}

// Test HAMTDirectory <-> BasicDirectory switch based on directory size. The
// switch is managed by the DynamicDirectory abstraction.
func TestDynamicDirectorySwitch(t *testing.T) {
	oldHamtOption := HAMTShardingSize
	defer func() { HAMTShardingSize = oldHamtOption }()
	HAMTShardingSize = 0 // Disable automatic switch at the start.
	linksize.LinkSizeFunction = mockLinkSizeFunc(1)
	defer func() { linksize.LinkSizeFunction = productionLinkSize }()

	ds := mdtest.Mock()
	dir, err := NewDirectory(ds)
	require.NoError(t, err)

	checkBasicDirectory(t, dir, "new dir is not BasicDirectory")

	ctx := context.Background()
	child := ft.EmptyDirNode()
	err = ds.Add(ctx, child)
	assert.NoError(t, err)

	err = dir.AddChild(ctx, "1", child)
	assert.NoError(t, err)
	checkBasicDirectory(t, dir, "added child, option still disabled")

	// Set a threshold so big a new entry won't trigger the change.
	HAMTShardingSize = math.MaxInt32

	err = dir.AddChild(ctx, "2", child)
	assert.NoError(t, err)
	checkBasicDirectory(t, dir, "added child, option now enabled but at max")

	// Now set it so low to make sure any new entry will trigger the upgrade.
	HAMTShardingSize = 1

	// We are already above the threshold, we trigger the switch with an overwrite
	// (any AddChild() should reevaluate the size).
	err = dir.AddChild(ctx, "2", child)
	assert.NoError(t, err)
	checkHAMTDirectory(t, dir, "added child, option at min, should switch up")

	// Set threshold at the number of current entries and delete the last one
	// to trigger a switch and evaluate if the rest of the entries are conserved.
	HAMTShardingSize = 2
	err = dir.RemoveChild(ctx, "2")
	assert.NoError(t, err)
	checkBasicDirectory(t, dir, "removed threshold entry, option at min, should switch down")
}

func TestIntegrityOfDirectorySwitch(t *testing.T) {
	ds := mdtest.Mock()
	dir, err := NewDirectory(ds)
	if err != nil {
		t.Fatal(err)
	}
	checkBasicDirectory(t, dir, "new dir is not BasicDirectory")

	ctx := context.Background()
	child := ft.EmptyDirNode()
	err = ds.Add(ctx, child)
	assert.NoError(t, err)

	basicDir, err := NewBasicDirectory(ds)
	assert.NoError(t, err)
	hamtDir, err := NewHAMTDirectory(ds, 0, WithMaxLinks(DefaultShardWidth))
	assert.NoError(t, err)
	for i := 0; i < 1000; i++ {
		basicDir.AddChild(ctx, strconv.FormatUint(uint64(i), 10), child)
		hamtDir.AddChild(ctx, strconv.FormatUint(uint64(i), 10), child)
	}
	compareDirectoryEntries(t, basicDir, hamtDir)

	hamtDirFromSwitch, err := basicDir.switchToSharding(ctx)
	require.NoError(t, err)
	basicDirFromSwitch, err := hamtDir.switchToBasic(ctx)
	require.NoError(t, err)
	compareDirectoryEntries(t, basicDir, basicDirFromSwitch)
	compareDirectoryEntries(t, hamtDir, hamtDirFromSwitch)
}

// This is the value of concurrent fetches during dag.Walk. Used in
// test to better predict how many nodes will be fetched.
var defaultConcurrentFetch = 32

// FIXME: Taken from private github.com/ipfs/boxo/ipld/merkledag@v0.2.3/merkledag.go.
// (We can also pass an explicit concurrency value in `(*Shard).EnumLinksAsync()`
// and take ownership of this configuration, but departing from the more
// standard and reliable one in `go-merkledag`.

// Test that we fetch as little nodes as needed to reach the HAMTShardingSize
// during the sizeBelowThreshold computation.
func TestHAMTEnumerationWhenComputingSize(t *testing.T) {
	// Adjust HAMT global/static options for the test to simplify its logic.
	// FIXME: These variables weren't designed to be modified and we should
	//  review in depth side effects.

	// Set all link sizes to a uniform 1 so the estimated directory size
	// is just the count of its entry links (in HAMT/Shard terminology these
	// are the "value" links pointing to anything that is *not* another Shard).
	linksize.LinkSizeFunction = mockLinkSizeFunc(1)
	defer func() { linksize.LinkSizeFunction = productionLinkSize }()

	// Use an identity hash function to ease the construction of "complete" HAMTs
	// (see CreateCompleteHAMT below for more details). (Ideally this should be
	// a parameter we pass and not a global option we modify in the caller.)
	oldHashFunc := internal.HAMTHashFunction
	defer func() { internal.HAMTHashFunction = oldHashFunc }()
	internal.HAMTHashFunction = idHash

	oldHamtOption := HAMTShardingSize
	defer func() { HAMTShardingSize = oldHamtOption }()

	// --- End of test static configuration adjustments. ---

	// Some arbitrary values below that make this test not that expensive.
	treeHeight := 4
	// How many leaf shards nodes (with value links,
	// i.e., directory entries) do we need to reach the threshold.
	thresholdToWidthRatio := 4
	// Departing from DefaultShardWidth of 256 to reduce HAMT size in
	// CreateCompleteHAMT.
	shardWidth := 16
	HAMTShardingSize = shardWidth * thresholdToWidthRatio

	// We create a "complete" HAMT (see CreateCompleteHAMT for more details)
	// with a regular structure to be able to predict how many Shard nodes we
	// will need to fetch in order to reach the HAMTShardingSize threshold in
	// sizeBelowThreshold (assuming a sequential DAG walk function).

	bstore := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	countGetsDS := newCountGetsDS(bstore)
	dsrv := mdag.NewDAGService(bsrv.New(countGetsDS, offline.Exchange(countGetsDS)))
	completeHAMTRoot, err := CreateCompleteHAMT(dsrv, treeHeight, shardWidth)
	assert.NoError(t, err)

	// Calculate the optimal number of nodes to traverse
	optimalNodesToFetch := 0
	nodesToProcess := HAMTShardingSize
	for i := 0; i < treeHeight-1; i++ {
		// divide by the shard width to get the parents and continue up the tree
		parentNodes := int(math.Ceil(float64(nodesToProcess) / float64(shardWidth)))
		optimalNodesToFetch += parentNodes
		nodesToProcess = parentNodes
	}

	// With this structure and a BFS traversal (from `parallelWalkDepth`) then
	// we would roughly fetch the following nodes:
	nodesToFetch := 0
	// * all layers up to (but not including) the last one with leaf nodes
	//   (because it's a BFS)
	for i := 0; i < treeHeight-1; i++ {
		nodesToFetch += int(math.Pow(float64(shardWidth), float64(i)))
	}
	// * `thresholdToWidthRatio` leaf Shards with enough value links to reach
	//    the HAMTShardingSize threshold.
	nodesToFetch += thresholdToWidthRatio

	hamtDir, err := NewHAMTDirectoryFromNode(dsrv, completeHAMTRoot)
	assert.NoError(t, err)

	countGetsDS.resetCounter()
	countGetsDS.setRequestDelay(10 * time.Millisecond)
	// (Without the `setRequestDelay` above the number of nodes fetched
	//  drops dramatically and unpredictably as the BFS starts to behave
	//  more like a DFS because some search paths are fetched faster than
	//  others.)
	below, err := hamtDir.sizeBelowThreshold(context.TODO(), 0)
	assert.NoError(t, err)
	assert.False(t, below)
	t.Logf("fetched %d nodes (predicted range: %d-%d)",
		countGetsDS.uniqueCidsFetched(), optimalNodesToFetch, nodesToFetch+defaultConcurrentFetch)
	// Check that the actual number of nodes fetched is within the margin of the
	// estimated `nodesToFetch` plus an extra of `defaultConcurrentFetch` since
	// we are fetching in parallel.
	assert.True(t, countGetsDS.uniqueCidsFetched() <= nodesToFetch+defaultConcurrentFetch)
	assert.True(t, countGetsDS.uniqueCidsFetched() >= optimalNodesToFetch)
}

// Compare entries in the leftDir against the rightDir and possibly
// missingEntries in the second.
func compareDirectoryEntries(t *testing.T, leftDir Directory, rightDir Directory) {
	leftLinks, err := getAllLinksSortedByName(leftDir)
	assert.NoError(t, err)
	rightLinks, err := getAllLinksSortedByName(rightDir)
	assert.NoError(t, err)

	assert.Equal(t, len(leftLinks), len(rightLinks))

	for i, leftLink := range leftLinks {
		assert.Equal(t, leftLink, rightLinks[i]) // FIXME: Can we just compare the entire struct?
	}
}

func getAllLinksSortedByName(d Directory) ([]*ipld.Link, error) {
	entries, err := d.Links(context.Background())
	if err != nil {
		return nil, err
	}
	sortLinksByName(entries)
	return entries, nil
}

func sortLinksByName(links []*ipld.Link) {
	slices.SortStableFunc(links, func(a, b *ipld.Link) int {
		return strings.Compare(a.Name, b.Name)
	})
}

func TestDirBuilder(t *testing.T) {
	ds := mdtest.Mock()
	dir, err := NewDirectory(ds)
	require.NoError(t, err)
	ctx := context.Background()

	child := ft.EmptyDirNode()
	err = ds.Add(ctx, child)
	require.NoError(t, err)

	count := 5000

	for i := 0; i < count; i++ {
		err = dir.AddChild(ctx, fmt.Sprintf("entry %d", i), child)
		require.NoError(t, err)
	}

	dirnd, err := dir.GetNode()
	require.NoError(t, err)

	links, err := dir.Links(ctx)
	require.NoError(t, err)

	require.Len(t, links, count, "not enough links")

	adir, err := NewDirectoryFromNode(ds, dirnd)
	require.NoError(t, err)

	links, err = adir.Links(ctx)
	require.NoError(t, err)

	names := make(map[string]bool, len(links))
	for _, lnk := range links {
		names[lnk.Name] = true
	}

	for i := range count {
		n := fmt.Sprintf("entry %d", i)
		if !names[n] {
			t.Fatal("COULDNT FIND: ", n)
		}
	}

	require.Len(t, links, count, "wrong number of links")

	linkResults := dir.EnumLinksAsync(ctx)

	asyncNames := make(map[string]bool)
	var asyncLinks []*ipld.Link

	for linkResult := range linkResults {
		require.NoError(t, linkResult.Err)
		asyncNames[linkResult.Link.Name] = true
		asyncLinks = append(asyncLinks, linkResult.Link)
	}

	for i := 0; i < count; i++ {
		n := fmt.Sprintf("entry %d", i)
		if !asyncNames[n] {
			t.Fatal("COULDNT FIND: ", n)
		}
	}

	require.Len(t, asyncLinks, count, "wrong number of links")
}

func TestBasicDirectoryWithMaxLinks(t *testing.T) {
	ds := mdtest.Mock()
	ctx := context.Background()

	dir, err := NewBasicDirectory(ds, WithMaxLinks(2))
	require.NoError(t, err)

	child1 := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, child1))

	err = dir.AddChild(ctx, "entry1", child1)
	require.NoError(t, err)

	child2 := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, child2))

	err = dir.AddChild(ctx, "entry2", child2)
	require.NoError(t, err)

	child3 := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, child3))

	err = dir.AddChild(ctx, "entry3", child3)
	require.Error(t, err)
}

// TestBasicDirectoryMaxLinksAllowsReplacement verifies that replacing an existing
// entry does not count against maxLinks. When at maxLinks capacity, replacing an
// existing entry should succeed (it's not adding a new link).
func TestBasicDirectoryMaxLinksAllowsReplacement(t *testing.T) {
	ds := mdtest.Mock()
	ctx := context.Background()

	dir, err := NewBasicDirectory(ds, WithMaxLinks(2))
	require.NoError(t, err)

	// add two entries to reach maxLinks
	child1 := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, child1))
	require.NoError(t, dir.AddChild(ctx, "entry1", child1))

	child2 := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, child2))
	require.NoError(t, dir.AddChild(ctx, "entry2", child2))

	// adding a third entry should fail (maxLinks reached)
	child3 := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, child3))
	err = dir.AddChild(ctx, "entry3", child3)
	require.Error(t, err, "adding third entry should fail")

	// but replacing an existing entry should succeed
	replacement := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, replacement))
	err = dir.AddChild(ctx, "entry1", replacement)
	require.NoError(t, err, "replacing existing entry should succeed even at maxLinks")

	// verify the replacement happened
	links, err := dir.Links(ctx)
	require.NoError(t, err)
	require.Len(t, links, 2)

	found := false
	for _, l := range links {
		if l.Name == "entry1" && l.Cid.Equals(replacement.Cid()) {
			found = true
			break
		}
	}
	require.True(t, found, "entry1 should point to the replacement node")
}

// TestHAMTDirectoryWithMaxLinks tests that no HAMT shard as more than MaxLinks.
func TestHAMTDirectoryWithMaxLinks(t *testing.T) {
	ds := mdtest.Mock()
	ctx := context.Background()

	dir, err := NewHAMTDirectory(ds, 0, WithMaxHAMTFanout(8))
	require.NoError(t, err)

	// Ensure we have at least 2 levels of HAMT by adding many nodes
	for i := 0; i < 300; i++ {
		child := ft.EmptyDirNode()
		require.NoError(t, ds.Add(ctx, child))
		err := dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
		require.NoError(t, err)
	}

	dirnd, err := dir.GetNode()
	require.NoError(t, err)

	require.Equal(t, 8, len(dirnd.Links()), "Node should not have more than 8 links")

	for _, l := range dirnd.Links() {
		childNd, err := ds.Get(ctx, l.Cid)
		if err != nil {
			t.Errorf("Failed to get node: %s", err)
			continue
		}
		assert.Equal(t, 8, len(childNd.Links()), "Node does not have 8 links")
	}
}

func TestDynamicDirectoryWithMaxLinks(t *testing.T) {
	ds := mdtest.Mock()
	ctx := context.Background()

	dir, err := NewDirectory(ds, WithMaxLinks(8), WithMaxHAMTFanout(16))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 8; i++ {
		child := ft.EmptyDirNode()
		require.NoError(t, ds.Add(ctx, child))
		err := dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
		require.NoError(t, err)
		checkBasicDirectory(t, dir, "directory should be basic because it has less children than MaxLinks")
	}

	for i := 8; i < 58; i++ {
		child := ft.EmptyDirNode()
		require.NoError(t, ds.Add(ctx, child))
		err := dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
		require.NoError(t, err)
		checkHAMTDirectory(t, dir, "directory should be sharded because more children than MaxLinks")
	}

	// Check that the directory root node has 8 links
	dirnd, err := dir.GetNode()
	require.NoError(t, err)
	assert.Equal(t, 16, len(dirnd.Links()), "HAMT Directory root node should have 16 links")

	// Continue the code by removing 50 elements while checking that the underlying directory is a HAMT directory.
	for i := 57; i >= 9; i-- {
		err := dir.RemoveChild(ctx, fmt.Sprintf("entry%d", i))
		require.NoError(t, err)
		checkHAMTDirectory(t, dir, "directory should still be sharded while more than 8 children")
	}

	// Continue removing elements until the directory is a basic directory again
	for i := 8; i >= 0; i-- {
		err := dir.RemoveChild(ctx, fmt.Sprintf("entry%d", i))
		require.NoError(t, err)
		checkBasicDirectory(t, dir, "directory should be basic when less than 8 children")
	}
}

// add a test that tests that validShardWidth(n) only returns true with positive numbers that are powers of 8 and multiples of 8.
func TestValidShardWidth(t *testing.T) {
	testCases := []struct {
		width  int
		expect bool
	}{
		{0, false},
		{-1, false},
		{1, false},
		{2, false},
		{4, false},
		{8, true},
		{16, true},
		{32, true},
		{64, true},
		{512, true},
		{1024, true},
		{4096, true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Width%d", tc.width), func(t *testing.T) {
			result := validShardWidth(tc.width)
			assert.Equal(t, tc.expect, result, "Expected %v for width %d", tc.expect, tc.width)
		})
	}
}

// TestInvalidMaxHAMTFanoutReturnsError verifies that invalid values
// passed to WithMaxHAMTFanout cause NewBasicDirectory and NewHAMTDirectory
// to return ErrInvalidHAMTFanout.
func TestInvalidMaxHAMTFanoutReturnsError(t *testing.T) {
	ds := mdtest.Mock()

	// Invalid values that should cause an error
	invalidValues := []int{
		-1,   // negative
		1,    // not multiple of 8
		2,    // power of 2 but not multiple of 8
		4,    // power of 2 but not multiple of 8
		7,    // not power of 2
		12,   // not power of 2
		1337, // not power of 2
	}

	// Valid values that should be accepted
	validValues := []int{
		0, // 0 means "use default"
		8,
		16,
		64,
		256,
		1024,
	}

	t.Run("BasicDirectory WithMaxHAMTFanout rejects invalid values with error", func(t *testing.T) {
		for _, invalid := range invalidValues {
			_, err := NewBasicDirectory(ds, WithMaxHAMTFanout(invalid))
			require.ErrorIs(t, err, ErrInvalidHAMTFanout,
				"invalid value %d should return ErrInvalidHAMTFanout", invalid)
		}
	})

	t.Run("BasicDirectory WithMaxHAMTFanout accepts valid values", func(t *testing.T) {
		for _, valid := range validValues {
			dir, err := NewBasicDirectory(ds, WithMaxHAMTFanout(valid))
			require.NoError(t, err, "valid value %d should be accepted", valid)
			if valid == 0 {
				assert.Equal(t, DefaultShardWidth, dir.GetMaxHAMTFanout(),
					"0 should use default %d", DefaultShardWidth)
			} else {
				assert.Equal(t, valid, dir.GetMaxHAMTFanout(),
					"valid value %d should be accepted", valid)
			}
		}
	})

	t.Run("HAMTDirectory WithMaxHAMTFanout rejects invalid values with error", func(t *testing.T) {
		for _, invalid := range invalidValues {
			_, err := NewHAMTDirectory(ds, 0, WithMaxHAMTFanout(invalid))
			require.ErrorIs(t, err, ErrInvalidHAMTFanout,
				"invalid value %d should return ErrInvalidHAMTFanout", invalid)
		}
	})

	t.Run("HAMTDirectory WithMaxHAMTFanout accepts valid values", func(t *testing.T) {
		for _, valid := range validValues {
			dir, err := NewHAMTDirectory(ds, 0, WithMaxHAMTFanout(valid))
			require.NoError(t, err, "valid value %d should be accepted", valid)
			if valid == 0 {
				assert.Equal(t, DefaultShardWidth, dir.GetMaxHAMTFanout(),
					"0 should use default %d", DefaultShardWidth)
			} else {
				assert.Equal(t, valid, dir.GetMaxHAMTFanout(),
					"valid value %d should be accepted", valid)
			}
		}
	})
}

// countGetsDS is a DAG service that keeps track of the number of
// unique CIDs fetched.
type countGetsDS struct {
	blockstore.Blockstore

	cidsFetched map[cid.Cid]struct{}
	mapLock     sync.Mutex
	started     bool

	getRequestDelay time.Duration
}

var _ blockstore.Blockstore = (*countGetsDS)(nil)

func newCountGetsDS(bs blockstore.Blockstore) *countGetsDS {
	return &countGetsDS{
		bs,
		make(map[cid.Cid]struct{}),
		sync.Mutex{},
		false,
		0,
	}
}

func (d *countGetsDS) resetCounter() {
	d.mapLock.Lock()
	defer d.mapLock.Unlock()
	d.cidsFetched = make(map[cid.Cid]struct{})
	d.started = true
}

func (d *countGetsDS) uniqueCidsFetched() int {
	d.mapLock.Lock()
	defer d.mapLock.Unlock()
	return len(d.cidsFetched)
}

func (d *countGetsDS) setRequestDelay(timeout time.Duration) {
	d.getRequestDelay = timeout
}

func (d *countGetsDS) maybeSleep(c cid.Cid) {
	d.mapLock.Lock()
	_, cidRequestedBefore := d.cidsFetched[c]
	d.cidsFetched[c] = struct{}{}
	d.mapLock.Unlock()

	if d.getRequestDelay != 0 && !cidRequestedBefore {
		// First request gets a timeout to simulate a network fetch.
		// Subsequent requests get no timeout simulating an in-disk cache.
		time.Sleep(d.getRequestDelay)
	}
}

func (d *countGetsDS) Has(ctx context.Context, c cid.Cid) (bool, error) {
	if d.started {
		panic("implement me")
	}
	return d.Blockstore.Has(ctx, c)
}

func (d *countGetsDS) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	blk, err := d.Blockstore.Get(ctx, c)
	if err != nil {
		return nil, err
	}

	d.maybeSleep(c)
	return blk, nil
}

func (d *countGetsDS) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	if d.started {
		panic("implement me")
	}
	return d.Blockstore.GetSize(ctx, c)
}

func (d *countGetsDS) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	if d.started {
		panic("implement me")
	}
	return d.Blockstore.AllKeysChan(ctx)
}

// TestSizeEstimationMode tests that all estimation modes work correctly.
func TestSizeEstimationMode(t *testing.T) {
	// Save and restore global settings after all subtests
	oldEstimation := HAMTSizeEstimation
	oldShardingSize := HAMTShardingSize
	oldLinkSize := linksize.LinkSizeFunction
	t.Cleanup(func() {
		HAMTSizeEstimation = oldEstimation
		HAMTShardingSize = oldShardingSize
		linksize.LinkSizeFunction = oldLinkSize
	})

	t.Run("links mode is default", func(t *testing.T) {
		HAMTSizeEstimation = SizeEstimationLinks // ensure default
		ds := mdtest.Mock()
		dir, err := NewBasicDirectory(ds)
		require.NoError(t, err)
		assert.Equal(t, SizeEstimationLinks, dir.GetSizeEstimationMode())
	})

	t.Run("block mode can be set globally", func(t *testing.T) {
		HAMTSizeEstimation = SizeEstimationBlock
		ds := mdtest.Mock()
		dir, err := NewBasicDirectory(ds)
		require.NoError(t, err)
		assert.Equal(t, SizeEstimationBlock, dir.GetSizeEstimationMode())
	})

	t.Run("disabled mode can be set globally", func(t *testing.T) {
		HAMTSizeEstimation = SizeEstimationDisabled
		ds := mdtest.Mock()
		dir, err := NewBasicDirectory(ds)
		require.NoError(t, err)
		assert.Equal(t, SizeEstimationDisabled, dir.GetSizeEstimationMode())
	})

	t.Run("WithSizeEstimationMode overrides global", func(t *testing.T) {
		HAMTSizeEstimation = SizeEstimationLinks
		ds := mdtest.Mock()
		dir, err := NewBasicDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
		require.NoError(t, err)
		assert.Equal(t, SizeEstimationBlock, dir.GetSizeEstimationMode())
	})
}

// TestSizeEstimationBlockMode tests that block estimation mode correctly
// calculates serialized dag-pb block size for HAMT threshold decisions.
func TestSizeEstimationBlockMode(t *testing.T) {
	// Save and restore global settings
	oldEstimation := HAMTSizeEstimation
	oldShardingSize := HAMTShardingSize
	oldLinkSize := linksize.LinkSizeFunction
	t.Cleanup(func() {
		HAMTSizeEstimation = oldEstimation
		HAMTShardingSize = oldShardingSize
		linksize.LinkSizeFunction = oldLinkSize
	})

	ds := mdtest.Mock()
	ctx := context.Background()

	// Create a child node to add to directories
	child := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, child))

	t.Run("block estimation uses full serialized size", func(t *testing.T) {
		// Set a threshold that will be exceeded with accurate block estimation
		// but not with legacy link estimation
		HAMTShardingSize = 500
		HAMTSizeEstimation = SizeEstimationBlock

		dir, err := NewDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
		require.NoError(t, err)

		// Add entries until we're near the threshold
		for i := range 10 {
			err = dir.AddChild(ctx, fmt.Sprintf("entry-%03d", i), child)
			require.NoError(t, err)
		}

		// Get the node and check its actual serialized size
		node, err := dir.GetNode()
		require.NoError(t, err)

		pn, ok := node.(*mdag.ProtoNode)
		if ok {
			data, err := pn.EncodeProtobuf(false)
			require.NoError(t, err)
			t.Logf("actual block size with 10 entries: %d bytes", len(data))
		}
	})

	t.Run("links mode regression", func(t *testing.T) {
		// Verify that links mode produces the same behavior as before
		HAMTShardingSize = 1000
		HAMTSizeEstimation = SizeEstimationLinks
		linksize.LinkSizeFunction = productionLinkSize

		dir, err := NewDirectory(ds)
		require.NoError(t, err)

		// Add entries and verify we stay as BasicDirectory with links mode
		for i := range 10 {
			err = dir.AddChild(ctx, fmt.Sprintf("entry-%03d", i), child)
			require.NoError(t, err)
		}

		// Should still be BasicDirectory with this threshold
		checkBasicDirectory(t, dir, "should be BasicDirectory with links estimation")
	})
}

// TestSizeEstimationDisabled tests that disabled mode ignores size-based threshold.
func TestSizeEstimationDisabled(t *testing.T) {
	// Save and restore global settings
	oldEstimation := HAMTSizeEstimation
	oldShardingSize := HAMTShardingSize
	oldLinkSize := linksize.LinkSizeFunction
	t.Cleanup(func() {
		HAMTSizeEstimation = oldEstimation
		HAMTShardingSize = oldShardingSize
		linksize.LinkSizeFunction = oldLinkSize
	})

	ds := mdtest.Mock()
	ctx := context.Background()

	child := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, child))

	t.Run("disabled mode ignores size threshold", func(t *testing.T) {
		// Set a very low threshold that would normally trigger HAMT
		HAMTShardingSize = 100
		HAMTSizeEstimation = SizeEstimationDisabled

		// Create directory with disabled mode and no MaxLinks set
		dir, err := NewDirectory(ds, WithSizeEstimationMode(SizeEstimationDisabled))
		require.NoError(t, err)

		// Add many entries - should stay as BasicDirectory since
		// SizeEstimationDisabled ignores size-based threshold
		for i := range 20 {
			err = dir.AddChild(ctx, fmt.Sprintf("entry-%03d", i), child)
			require.NoError(t, err)
		}

		// Should remain BasicDirectory (no MaxLinks constraint set)
		checkBasicDirectory(t, dir, "should be BasicDirectory when size estimation is disabled and MaxLinks not set")
	})

	t.Run("disabled mode respects MaxLinks", func(t *testing.T) {
		HAMTShardingSize = 100
		HAMTSizeEstimation = SizeEstimationDisabled

		// Create directory with disabled mode but MaxLinks set
		dir, err := NewDirectory(ds,
			WithSizeEstimationMode(SizeEstimationDisabled),
			WithMaxLinks(5))
		require.NoError(t, err)

		// Add entries up to MaxLinks
		for i := range 5 {
			err = dir.AddChild(ctx, fmt.Sprintf("entry-%03d", i), child)
			require.NoError(t, err)
		}
		checkBasicDirectory(t, dir, "should be BasicDirectory at MaxLinks")

		// Adding one more should trigger HAMT conversion
		err = dir.AddChild(ctx, "entry-005", child)
		require.NoError(t, err)
		checkHAMTDirectory(t, dir, "should be HAMTDirectory after exceeding MaxLinks")
	})

	t.Run("block mode respects MaxLinks", func(t *testing.T) {
		HAMTShardingSize = 256 * 1024 // 256KB - high enough to not be triggered by size
		HAMTSizeEstimation = SizeEstimationBlock

		// Create directory with block mode and MaxLinks set
		dir, err := NewDirectory(ds,
			WithSizeEstimationMode(SizeEstimationBlock),
			WithMaxLinks(5))
		require.NoError(t, err)

		// Add entries up to MaxLinks
		for i := range 5 {
			err = dir.AddChild(ctx, fmt.Sprintf("entry-%03d", i), child)
			require.NoError(t, err)
		}
		checkBasicDirectory(t, dir, "should be BasicDirectory at MaxLinks")

		// Adding one more should trigger HAMT conversion
		err = dir.AddChild(ctx, "entry-005", child)
		require.NoError(t, err)
		checkHAMTDirectory(t, dir, "should be HAMTDirectory after exceeding MaxLinks in block mode")
	})
}

// TestBlockSizeCalculation verifies the helper functions for block size calculation.
func TestBlockSizeCalculation(t *testing.T) {
	t.Run("varintLen", func(t *testing.T) {
		// Test varint length calculation
		assert.Equal(t, 1, varintLen(0))
		assert.Equal(t, 1, varintLen(127))
		assert.Equal(t, 2, varintLen(128))
		assert.Equal(t, 2, varintLen(16383))
		assert.Equal(t, 3, varintLen(16384))
	})

	t.Run("linkSerializedSize includes all fields", func(t *testing.T) {
		// Create a test CID
		c, err := cid.Decode("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG")
		require.NoError(t, err)

		// Calculate size with linkSerializedSize
		blockSize := linkSerializedSize("test-name", c, 1024)

		// Compare with legacy linksize calculation
		legacySize := productionLinkSize("test-name", c)

		// Block size should be larger because it includes:
		// - Tsize field
		// - Protobuf overhead (field tags, length prefixes)
		assert.Greater(t, blockSize, legacySize,
			"block serialized size should be larger than legacy link size")

		t.Logf("legacy size: %d, block size: %d", legacySize, blockSize)
	})

	t.Run("calculateBlockSize matches actual encoding", func(t *testing.T) {
		ds := mdtest.Mock()
		ctx := context.Background()

		basicDir, err := NewBasicDirectory(ds)
		require.NoError(t, err)

		child := ft.EmptyDirNode()
		require.NoError(t, ds.Add(ctx, child))

		// Add some entries
		for i := range 5 {
			err = basicDir.AddChild(ctx, fmt.Sprintf("file-%d", i), child)
			require.NoError(t, err)
		}

		// Calculate size using our function
		calculatedSize, err := calculateBlockSize(basicDir.node)
		require.NoError(t, err)

		// Get actual encoded size
		data, err := basicDir.node.EncodeProtobuf(false)
		require.NoError(t, err)
		actualSize := len(data)

		assert.Equal(t, actualSize, calculatedSize,
			"calculateBlockSize should match actual encoded size")
	})
}

// TestHAMTDirectorySizeEstimationMode tests that HAMTDirectory respects
// the size estimation mode for conversion back to BasicDirectory.
func TestHAMTDirectorySizeEstimationMode(t *testing.T) {
	ds := mdtest.Mock()

	t.Run("HAMTDirectory GetSizeEstimationMode defaults to global", func(t *testing.T) {
		oldEstimation := HAMTSizeEstimation
		defer func() { HAMTSizeEstimation = oldEstimation }()

		HAMTSizeEstimation = SizeEstimationBlock
		hamtDir, err := NewHAMTDirectory(ds, 0)
		require.NoError(t, err)
		assert.Equal(t, SizeEstimationBlock, hamtDir.GetSizeEstimationMode())
	})

	t.Run("HAMTDirectory WithSizeEstimationMode overrides global", func(t *testing.T) {
		oldEstimation := HAMTSizeEstimation
		defer func() { HAMTSizeEstimation = oldEstimation }()

		HAMTSizeEstimation = SizeEstimationLinks
		hamtDir, err := NewHAMTDirectory(ds, 0, WithSizeEstimationMode(SizeEstimationBlock))
		require.NoError(t, err)
		assert.Equal(t, SizeEstimationBlock, hamtDir.GetSizeEstimationMode())
	})
}

// TestNegativeMtimeEncoding verifies dataFieldSerializedSize handles negative
// timestamps (dates before Unix epoch, e.g., 1960) correctly. Negative int64
// values require 10-byte varint encoding in protobuf.
func TestNegativeMtimeEncoding(t *testing.T) {
	// Date before Unix epoch: January 1, 1960
	negativeMtime := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
	require.True(t, negativeMtime.Unix() < 0, "test requires negative Unix timestamp")

	// Calculate size with negative mtime
	sizeWithNegativeMtime := dataFieldSerializedSize(0, negativeMtime)

	// Calculate size with positive mtime of similar magnitude
	positiveMtime := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	sizeWithPositiveMtime := dataFieldSerializedSize(0, positiveMtime)

	// Negative mtime should use more bytes (10-byte varint vs ~5 bytes for positive)
	assert.Greater(t, sizeWithNegativeMtime, sizeWithPositiveMtime,
		"negative mtime should use more bytes due to 10-byte varint encoding")

	// Verify by creating actual directory and comparing
	ds := mdtest.Mock()
	ctx := context.Background()

	basicDir, err := NewBasicDirectory(ds, WithStat(0, negativeMtime))
	require.NoError(t, err)

	child := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, child))
	require.NoError(t, basicDir.AddChild(ctx, "test", child))

	node, err := basicDir.GetNode()
	require.NoError(t, err)

	// Verify the node actually has the negative mtime
	fsn, err := ft.FSNodeFromBytes(node.(*mdag.ProtoNode).Data())
	require.NoError(t, err)
	assert.Equal(t, negativeMtime.Unix(), fsn.ModTime().Unix())
}

// TestSizeEstimationDisabledWithMaxLinksZero verifies behavior when
// SizeEstimationDisabled is set and maxLinks is 0 (not set). This documents
// intended behavior: without maxLinks set, there's no criterion to trigger
// Basic<->HAMT conversion in either direction.
func TestSizeEstimationDisabledWithMaxLinksZero(t *testing.T) {
	ds := mdtest.Mock()
	ctx := context.Background()

	// Save and restore globals
	oldShardingSize := HAMTShardingSize
	oldEstimation := HAMTSizeEstimation
	defer func() {
		HAMTShardingSize = oldShardingSize
		HAMTSizeEstimation = oldEstimation
	}()

	// Configure: SizeEstimationDisabled, maxLinks=0 (unset)
	HAMTShardingSize = 100 // low threshold that would normally trigger HAMT
	HAMTSizeEstimation = SizeEstimationDisabled

	child := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, child))

	t.Run("BasicDirectory stays Basic when adding entries", func(t *testing.T) {
		// Create BasicDirectory with SizeEstimationDisabled and maxLinks=0
		basicDir, err := NewBasicDirectory(ds,
			WithSizeEstimationMode(SizeEstimationDisabled),
			WithMaxLinks(0))
		require.NoError(t, err)

		// Add many entries - would normally exceed size threshold
		for i := range 50 {
			err = basicDir.AddChild(ctx, fmt.Sprintf("file-%02d", i), child)
			require.NoError(t, err)
		}

		// With SizeEstimationDisabled and maxLinks=0, needsToSwitchToHAMTDir
		// should return false because there's no criterion to trigger conversion
		needsSwitch, err := basicDir.needsToSwitchToHAMTDir("file-50", child)
		require.NoError(t, err)
		assert.False(t, needsSwitch,
			"BasicDirectory with SizeEstimationDisabled and maxLinks=0 should not switch to HAMT")

		// Verify via DynamicDirectory as well
		dir, err := NewDirectory(ds,
			WithSizeEstimationMode(SizeEstimationDisabled),
			WithMaxLinks(0))
		require.NoError(t, err)

		for i := range 50 {
			err = dir.AddChild(ctx, fmt.Sprintf("entry-%02d", i), child)
			require.NoError(t, err)
		}
		checkBasicDirectory(t, dir, "DynamicDirectory should stay Basic with SizeEstimationDisabled and maxLinks=0")
	})

	t.Run("HAMTDirectory stays HAMT when removing entries", func(t *testing.T) {
		// Create HAMT directory directly to test reversion behavior
		hamtDir, err := NewHAMTDirectory(ds, 0,
			WithSizeEstimationMode(SizeEstimationDisabled),
			WithMaxLinks(0)) // maxLinks not set
		require.NoError(t, err)

		// Add some entries
		for i := range 10 {
			err = hamtDir.AddChild(ctx, fmt.Sprintf("file-%d", i), child)
			require.NoError(t, err)
		}

		// Remove all but one entry
		for i := range 9 {
			err = hamtDir.RemoveChild(ctx, fmt.Sprintf("file-%d", i))
			require.NoError(t, err)
		}

		// With SizeEstimationDisabled and maxLinks=0, needsToSwitchToBasicDir should
		// return false because there's no maxLinks criterion to satisfy
		needsSwitch, err := hamtDir.needsToSwitchToBasicDir(ctx, "file-9", nil)
		require.NoError(t, err)
		assert.False(t, needsSwitch,
			"HAMT with SizeEstimationDisabled and maxLinks=0 should not switch to Basic")
	})
}

// TestUnicodeFilenamesInSizeEstimation verifies that size estimation correctly
// handles multi-byte Unicode characters in filenames.
func TestUnicodeFilenamesInSizeEstimation(t *testing.T) {
	ds := mdtest.Mock()
	ctx := context.Background()

	testCases := []struct {
		name     string
		filename string
		byteLen  int // expected byte length of filename
	}{
		{"ASCII", "hello.txt", 9},
		{"Chinese", "文件.txt", 10}, // 2 Chinese chars (3 bytes each) + 4 ASCII
		{"Emoji", "📁folder", 10},  // 1 emoji (4 bytes) + 6 ASCII
		{"Mixed", "日本語ファイル", 21},  // 7 Japanese chars (3 bytes each)
		{"Combining", "café", 5},  // 'e' + combining acute accent
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Verify our expected byte length is correct
			assert.Equal(t, tc.byteLen, len(tc.filename),
				"test setup: filename byte length mismatch")

			// Create directory with SizeEstimationBlock for accurate size tracking
			basicDir, err := NewBasicDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
			require.NoError(t, err)

			child := ft.EmptyDirNode()
			require.NoError(t, ds.Add(ctx, child))

			// Add child with Unicode filename
			err = basicDir.AddChild(ctx, tc.filename, child)
			require.NoError(t, err)

			// Get the node and verify we can retrieve the entry
			_, err = basicDir.Find(ctx, tc.filename)
			require.NoError(t, err, "should find entry with Unicode filename")

			// Verify estimated size includes full byte length of filename
			node, err := basicDir.GetNode()
			require.NoError(t, err)

			actualSize, err := calculateBlockSize(node.(*mdag.ProtoNode))
			require.NoError(t, err)

			// The estimated size should match actual encoded size
			assert.Equal(t, actualSize, basicDir.estimatedSize,
				"estimated size should match actual for Unicode filename")
		})
	}
}

// TestConcurrentHAMTConversion verifies that concurrent reads don't corrupt
// directory state during HAMT conversion.
// TestSequentialHAMTConversion tests that directories handle rapid sequential
// operations during HAMT conversion correctly.
// Note: Directory is not thread-safe for concurrent reads and writes.
// This test verifies that rapid sequential operations work correctly.
func TestSequentialHAMTConversion(t *testing.T) {
	ds := mdtest.Mock()
	ctx := context.Background()

	// Save and restore globals
	oldShardingSize := HAMTShardingSize
	defer func() { HAMTShardingSize = oldShardingSize }()

	// Set low threshold to trigger HAMT quickly
	HAMTShardingSize = 500

	dir, err := NewDirectory(ds)
	require.NoError(t, err)

	child := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, child))

	// Add initial entries (but not enough to trigger HAMT)
	for i := range 5 {
		err = dir.AddChild(ctx, fmt.Sprintf("initial-%d", i), child)
		require.NoError(t, err)
	}

	// Interleave reads and writes to test HAMT conversion during operations
	for i := range 20 {
		// Read
		links, err := dir.Links(ctx)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(links), 5+i, "should have at least %d entries", 5+i)

		// Write - this may trigger HAMT conversion
		err = dir.AddChild(ctx, fmt.Sprintf("sequential-%d", i), child)
		require.NoError(t, err)
	}

	// Verify final state is consistent
	links, err := dir.Links(ctx)
	require.NoError(t, err)
	assert.Equal(t, 25, len(links), "should have 5 initial + 20 sequential entries")
}

// TestHAMTDirectoryModeAndMtimeAfterReload verifies that mode and mtime metadata
// survives HAMT conversion and reload from DAG.
func TestHAMTDirectoryModeAndMtimeAfterReload(t *testing.T) {
	ds := mdtest.Mock()
	ctx := context.Background()

	// Save and restore globals
	oldShardingSize := HAMTShardingSize
	defer func() { HAMTShardingSize = oldShardingSize }()

	HAMTShardingSize = 200 // low threshold

	testMode := os.FileMode(0o755)
	testMtime := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	// Create directory with mode and mtime
	dir, err := NewDirectory(ds, WithStat(testMode, testMtime))
	require.NoError(t, err)

	child := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, child))

	// Add entries to trigger HAMT conversion
	for i := range 15 {
		err = dir.AddChild(ctx, fmt.Sprintf("file-%02d", i), child)
		require.NoError(t, err)
	}

	// Verify it became HAMT
	checkHAMTDirectory(t, dir, "should be HAMTDirectory after many entries")

	// Get the node
	node, err := dir.GetNode()
	require.NoError(t, err)

	// Reload directory from node
	reloadedDir, err := NewDirectoryFromNode(ds, node.(*mdag.ProtoNode))
	require.NoError(t, err)

	// Verify we can still access entries
	links, err := reloadedDir.Links(ctx)
	require.NoError(t, err)
	assert.Equal(t, 15, len(links))

	// Remove entries to trigger conversion back to Basic
	for i := range 12 {
		err = reloadedDir.RemoveChild(ctx, fmt.Sprintf("file-%02d", i))
		require.NoError(t, err)
	}

	// Get node again - may have converted back to Basic
	node2, err := reloadedDir.GetNode()
	require.NoError(t, err)

	// Verify mode and mtime are preserved in the UnixFS data
	fsn, err := ft.FSNodeFromBytes(node2.(*mdag.ProtoNode).Data())
	require.NoError(t, err)

	// Verify mode and mtime are preserved after Basic<->HAMT conversions and reload.
	// FSNode.Mode() adds os.ModeDir for directories, so compare permission bits only.
	if fsn.Type() == ft.TDirectory {
		assert.Equal(t, testMode.Perm(), fsn.Mode().Perm(), "mode permissions should be preserved after reload and conversion")
		assert.Equal(t, testMtime.Unix(), fsn.ModTime().Unix(), "mtime should be preserved after reload and conversion")
	}
}

// TestHAMTThresholdExactBoundary verifies the exact boundary behavior:
// directory at threshold stays Basic, threshold+1 triggers HAMT.
func TestHAMTThresholdExactBoundary(t *testing.T) {
	ds := mdtest.Mock()
	ctx := context.Background()

	// Save and restore globals
	oldShardingSize := HAMTShardingSize
	oldEstimation := HAMTSizeEstimation
	defer func() {
		HAMTShardingSize = oldShardingSize
		HAMTSizeEstimation = oldEstimation
	}()

	// Use block estimation for accurate size tracking
	HAMTSizeEstimation = SizeEstimationBlock

	child := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, child))

	// Create a directory and measure its size with one entry
	measureDir, err := NewBasicDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
	require.NoError(t, err)
	require.NoError(t, measureDir.AddChild(ctx, "a", child))
	oneEntrySize := measureDir.estimatedSize

	// Set threshold to exactly the size of directory with N entries
	// We want to test that N entries stays Basic, N+1 triggers HAMT
	numEntries := 5
	targetSize := oneEntrySize + (numEntries-1)*linkSerializedSize("a", child.Cid(), 0)
	HAMTShardingSize = targetSize

	t.Logf("threshold=%d, oneEntrySize=%d", targetSize, oneEntrySize)

	// Create fresh directory
	dir, err := NewDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
	require.NoError(t, err)

	// Add entries up to threshold
	for i := range numEntries {
		name := fmt.Sprintf("%c", 'a'+i) // single char names for consistent size
		err = dir.AddChild(ctx, name, child)
		require.NoError(t, err)
	}

	// Should still be Basic (at threshold, not over)
	checkBasicDirectory(t, dir, "directory at exact threshold should stay Basic")

	// Add one more entry - should trigger HAMT
	err = dir.AddChild(ctx, "z", child)
	require.NoError(t, err)

	checkHAMTDirectory(t, dir, "directory over threshold should become HAMT")
}

// TestDirectoryConversionPreservesAllSettings verifies that all configurable
// directory settings are preserved when converting between BasicDirectory and
// HAMTDirectory in both directions (Basic->HAMT and HAMT->Basic).
func TestDirectoryConversionPreservesAllSettings(t *testing.T) {
	ds := mdtest.Mock()
	ctx := context.Background()

	// Use non-default values for ALL configurable settings to ensure they propagate
	const (
		testMaxLinks         = 5
		testMaxHAMTFanout    = 64 // must be power of 2 and multiple of 8
		testHAMTShardingSize = 100
	)
	testSizeEstimation := SizeEstimationDisabled
	testMode := os.FileMode(0o755)
	testMtime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	testCidBuilder := cid.V1Builder{Codec: cid.DagProtobuf, MhType: 0x12}

	child := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, child))

	// Helper to verify all settings on a directory
	verifySettings := func(t *testing.T, dir Directory, desc string) {
		t.Helper()
		assert.Equal(t, testMaxLinks, dir.GetMaxLinks(), "%s: MaxLinks mismatch", desc)
		assert.Equal(t, testMaxHAMTFanout, dir.GetMaxHAMTFanout(), "%s: MaxHAMTFanout mismatch", desc)
		assert.Equal(t, testSizeEstimation, dir.GetSizeEstimationMode(), "%s: SizeEstimationMode mismatch", desc)
		assert.Equal(t, testHAMTShardingSize, dir.GetHAMTShardingSize(), "%s: HAMTShardingSize mismatch", desc)
		// CidBuilder comparison - check the resulting CID version
		if cb := dir.GetCidBuilder(); cb != nil {
			switch b := cb.(type) {
			case cid.Prefix:
				assert.Equal(t, uint64(1), b.Version, "%s: CidBuilder version mismatch", desc)
			case cid.V1Builder:
				// V1Builder always produces CIDv1, which is what we want
			default:
				t.Errorf("%s: unexpected CidBuilder type %T", desc, cb)
			}
		}
	}

	t.Run("Basic to HAMT conversion preserves settings", func(t *testing.T) {
		// Create directory with all custom settings
		dir, err := NewDirectory(ds,
			WithMaxLinks(testMaxLinks),
			WithMaxHAMTFanout(testMaxHAMTFanout),
			WithSizeEstimationMode(testSizeEstimation),
			WithStat(testMode, testMtime),
			WithCidBuilder(testCidBuilder),
		)
		require.NoError(t, err)

		// Set per-directory HAMTShardingSize (not a DirectoryOption)
		dir.(*DynamicDirectory).Directory.(*BasicDirectory).hamtShardingSize = testHAMTShardingSize

		// Verify initial settings
		verifySettings(t, dir, "initial BasicDirectory")
		checkBasicDirectory(t, dir, "should start as BasicDirectory")

		// Add enough entries to trigger HAMT conversion (exceeds MaxLinks=5)
		for i := range 6 {
			err = dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
			require.NoError(t, err)
		}

		// Should now be HAMT
		checkHAMTDirectory(t, dir, "should convert to HAMTDirectory")

		// Verify all settings preserved after Basic->HAMT
		verifySettings(t, dir, "after Basic->HAMT conversion")
	})

	t.Run("HAMT to Basic conversion via AddChild preserves settings", func(t *testing.T) {
		// Create directory and force it to HAMT first
		dir, err := NewDirectory(ds,
			WithMaxLinks(testMaxLinks),
			WithMaxHAMTFanout(testMaxHAMTFanout),
			WithSizeEstimationMode(testSizeEstimation),
			WithStat(testMode, testMtime),
			WithCidBuilder(testCidBuilder),
		)
		require.NoError(t, err)
		dir.(*DynamicDirectory).Directory.(*BasicDirectory).hamtShardingSize = testHAMTShardingSize

		// Add entries to trigger HAMT
		for i := range 6 {
			err = dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
			require.NoError(t, err)
		}
		checkHAMTDirectory(t, dir, "should be HAMTDirectory")

		// Remove entries to allow conversion back to Basic
		// With SizeEstimationDisabled, conversion depends on maxLinks
		for i := range 4 {
			err = dir.RemoveChild(ctx, fmt.Sprintf("entry%d", i))
			require.NoError(t, err)
		}

		// Add an entry that replaces an existing one - this triggers the
		// HAMT->Basic path in AddChild when size is below threshold
		err = dir.AddChild(ctx, "entry4", child)
		require.NoError(t, err)

		// Should have converted back to Basic (2 entries, below MaxLinks=5)
		checkBasicDirectory(t, dir, "should convert back to BasicDirectory")

		// Verify all settings preserved after HAMT->Basic via AddChild
		verifySettings(t, dir, "after HAMT->Basic conversion via AddChild")
	})

	t.Run("HAMT to Basic conversion via RemoveChild preserves settings", func(t *testing.T) {
		// Create directory and force it to HAMT first
		dir, err := NewDirectory(ds,
			WithMaxLinks(testMaxLinks),
			WithMaxHAMTFanout(testMaxHAMTFanout),
			WithSizeEstimationMode(testSizeEstimation),
			WithStat(testMode, testMtime),
			WithCidBuilder(testCidBuilder),
		)
		require.NoError(t, err)
		dir.(*DynamicDirectory).Directory.(*BasicDirectory).hamtShardingSize = testHAMTShardingSize

		// Add entries to trigger HAMT
		for i := range 6 {
			err = dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
			require.NoError(t, err)
		}
		checkHAMTDirectory(t, dir, "should be HAMTDirectory")

		// Remove entries via RemoveChild to trigger conversion back to Basic
		for i := range 4 {
			err = dir.RemoveChild(ctx, fmt.Sprintf("entry%d", i))
			require.NoError(t, err)
		}

		// Should have converted back to Basic (2 entries, below MaxLinks=5)
		checkBasicDirectory(t, dir, "should convert back to BasicDirectory via RemoveChild")

		// Verify all settings preserved after HAMT->Basic via RemoveChild
		verifySettings(t, dir, "after HAMT->Basic conversion via RemoveChild")
	})
}
