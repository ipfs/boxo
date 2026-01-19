package io

import (
	"context"
	"fmt"
	"testing"

	mdag "github.com/ipfs/boxo/ipld/merkledag"
	mdtest "github.com/ipfs/boxo/ipld/merkledag/test"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/ipld/unixfs/private/linksize"
	cid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnixFSProfiles(t *testing.T) {
	t.Run("UnixFS_v0_2015 has correct values", func(t *testing.T) {
		assert.Equal(t, 0, UnixFS_v0_2015.CIDVersion, "CIDVersion should be 0")
		assert.Equal(t, uint64(mh.SHA2_256), UnixFS_v0_2015.MhType, "MhType should be SHA2_256")
		assert.Equal(t, int64(256*1024), UnixFS_v0_2015.ChunkSize, "ChunkSize should be 256 KiB")
		assert.Equal(t, 174, UnixFS_v0_2015.FileDAGWidth, "FileDAGWidth should be 174")
		assert.False(t, UnixFS_v0_2015.RawLeaves, "RawLeaves should be false for CIDv0")
		assert.Equal(t, 256*1024, UnixFS_v0_2015.HAMTShardingSize, "HAMTShardingSize should be 256 KiB")
		assert.Equal(t, SizeEstimationLinks, UnixFS_v0_2015.HAMTSizeEstimation, "should use links-based estimation")
		assert.Equal(t, 256, UnixFS_v0_2015.HAMTShardWidth, "HAMTShardWidth should be 256")
	})

	t.Run("UnixFS_v1_2025 has correct values", func(t *testing.T) {
		assert.Equal(t, 1, UnixFS_v1_2025.CIDVersion, "CIDVersion should be 1")
		assert.Equal(t, uint64(mh.SHA2_256), UnixFS_v1_2025.MhType, "MhType should be SHA2_256")
		assert.Equal(t, int64(1024*1024), UnixFS_v1_2025.ChunkSize, "ChunkSize should be 1 MiB")
		assert.Equal(t, 1024, UnixFS_v1_2025.FileDAGWidth, "FileDAGWidth should be 1024")
		assert.True(t, UnixFS_v1_2025.RawLeaves, "RawLeaves should be true for CIDv1")
		assert.Equal(t, 256*1024, UnixFS_v1_2025.HAMTShardingSize, "HAMTShardingSize should be 256 KiB")
		assert.Equal(t, SizeEstimationBlock, UnixFS_v1_2025.HAMTSizeEstimation, "should use block-based estimation")
		assert.Equal(t, 256, UnixFS_v1_2025.HAMTShardWidth, "HAMTShardWidth should be 256")
	})

	t.Run("CidBuilder returns correct prefix", func(t *testing.T) {
		t.Run("UnixFS_v0_2015", func(t *testing.T) {
			builder := UnixFS_v0_2015.CidBuilder()
			prefix := builder.(cid.Prefix)
			assert.Equal(t, uint64(0), prefix.Version)
			assert.Equal(t, uint64(cid.DagProtobuf), prefix.Codec)
			assert.Equal(t, uint64(mh.SHA2_256), prefix.MhType)
		})

		t.Run("UnixFS_v1_2025", func(t *testing.T) {
			builder := UnixFS_v1_2025.CidBuilder()
			prefix := builder.(cid.Prefix)
			assert.Equal(t, uint64(1), prefix.Version)
			assert.Equal(t, uint64(cid.DagProtobuf), prefix.Codec)
			assert.Equal(t, uint64(mh.SHA2_256), prefix.MhType)
		})
	})

	t.Run("ApplyGlobals sets global variables", func(t *testing.T) {
		// Save original values
		oldShardingSize := HAMTShardingSize
		oldEstimation := HAMTSizeEstimation
		oldShardWidth := DefaultShardWidth
		t.Cleanup(func() {
			HAMTShardingSize = oldShardingSize
			HAMTSizeEstimation = oldEstimation
			DefaultShardWidth = oldShardWidth
		})

		// Apply UnixFS_v1_2025
		UnixFS_v1_2025.ApplyGlobals()

		assert.Equal(t, UnixFS_v1_2025.HAMTShardingSize, HAMTShardingSize)
		assert.Equal(t, UnixFS_v1_2025.HAMTSizeEstimation, HAMTSizeEstimation)
		assert.Equal(t, UnixFS_v1_2025.HAMTShardWidth, DefaultShardWidth)

		// Apply UnixFS_v0_2015
		UnixFS_v0_2015.ApplyGlobals()

		assert.Equal(t, UnixFS_v0_2015.HAMTShardingSize, HAMTShardingSize)
		assert.Equal(t, UnixFS_v0_2015.HAMTSizeEstimation, HAMTSizeEstimation)
		assert.Equal(t, UnixFS_v0_2015.HAMTShardWidth, DefaultShardWidth)
	})
}

func TestProfileHAMTThresholdBehavior(t *testing.T) {
	// Use fixed link size for predictable testing
	const fixedLinkSize = 100

	saveAndRestoreGlobals := func(t *testing.T) {
		oldShardingSize := HAMTShardingSize
		oldEstimation := HAMTSizeEstimation
		oldShardWidth := DefaultShardWidth
		oldLinkSize := linksize.LinkSizeFunction
		t.Cleanup(func() {
			HAMTShardingSize = oldShardingSize
			HAMTSizeEstimation = oldEstimation
			DefaultShardWidth = oldShardWidth
			linksize.LinkSizeFunction = oldLinkSize
		})
	}

	t.Run("SizeEstimationLinks threshold behavior", func(t *testing.T) {
		saveAndRestoreGlobals(t)

		// Configure for links-based estimation with predictable sizes
		UnixFS_v0_2015.ApplyGlobals()
		linksize.LinkSizeFunction = func(name string, c cid.Cid) int {
			return fixedLinkSize
		}
		// Set threshold at exactly 300 bytes (3 links at 100 bytes each)
		HAMTShardingSize = 300

		ds := mdtest.Mock()
		ctx := context.Background()
		child := ft.EmptyDirNode()
		require.NoError(t, ds.Add(ctx, child))

		t.Run("below threshold stays BasicDirectory", func(t *testing.T) {
			dir, err := NewDirectory(ds)
			require.NoError(t, err)

			// Add 2 entries = 200 bytes, below 300 threshold
			for i := range 2 {
				err = dir.AddChild(ctx, fmt.Sprintf("e%d", i), child)
				require.NoError(t, err)
			}

			_, isHAMT := dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
			assert.False(t, isHAMT, "should remain BasicDirectory when below threshold (200 < 300)")
		})

		t.Run("at threshold stays BasicDirectory", func(t *testing.T) {
			dir, err := NewDirectory(ds)
			require.NoError(t, err)

			// Add 3 entries = 300 bytes, exactly at threshold
			// With > comparison, at threshold stays basic
			for i := range 3 {
				err = dir.AddChild(ctx, fmt.Sprintf("e%d", i), child)
				require.NoError(t, err)
			}

			_, isHAMT := dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
			assert.False(t, isHAMT, "should stay BasicDirectory when exactly at threshold (300 == 300)")
		})

		t.Run("above threshold switches to HAMTDirectory", func(t *testing.T) {
			dir, err := NewDirectory(ds)
			require.NoError(t, err)

			// Add 4 entries = 400 bytes, above 300 threshold
			for i := range 4 {
				err = dir.AddChild(ctx, fmt.Sprintf("e%d", i), child)
				require.NoError(t, err)
			}

			_, isHAMT := dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
			assert.True(t, isHAMT, "should switch to HAMTDirectory when above threshold (400 > 300)")
		})
	})

	t.Run("SizeEstimationBlock threshold behavior", func(t *testing.T) {
		saveAndRestoreGlobals(t)

		UnixFS_v1_2025.ApplyGlobals()

		ds := mdtest.Mock()
		ctx := context.Background()
		child := ft.EmptyDirNode()
		require.NoError(t, ds.Add(ctx, child))

		// First, measure actual block sizes to set precise threshold
		dir, err := NewDirectory(ds)
		require.NoError(t, err)

		// Add entries and measure actual block size
		for i := range 5 {
			err = dir.AddChild(ctx, fmt.Sprintf("e%d", i), child)
			require.NoError(t, err)
		}
		node, err := dir.GetNode()
		require.NoError(t, err)
		sizeWith5, err := calculateBlockSize(node.(*mdag.ProtoNode))
		require.NoError(t, err)
		t.Logf("block size with 5 entries: %d bytes", sizeWith5)

		// Add one more and measure
		err = dir.AddChild(ctx, "e5", child)
		require.NoError(t, err)
		node, err = dir.GetNode()
		require.NoError(t, err)
		sizeWith6, err := calculateBlockSize(node.(*mdag.ProtoNode))
		require.NoError(t, err)
		t.Logf("block size with 6 entries: %d bytes", sizeWith6)

		// Set threshold between 5 and 6 entries
		threshold := (sizeWith5 + sizeWith6) / 2
		t.Logf("setting threshold to %d bytes (between 5 and 6 entries)", threshold)

		t.Run("below threshold stays BasicDirectory", func(t *testing.T) {
			HAMTShardingSize = threshold

			dir, err := NewDirectory(ds)
			require.NoError(t, err)

			// Add 5 entries, should be below threshold
			for i := range 5 {
				err = dir.AddChild(ctx, fmt.Sprintf("e%d", i), child)
				require.NoError(t, err)
			}

			_, isHAMT := dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
			assert.False(t, isHAMT, "should remain BasicDirectory when below threshold (%d < %d)", sizeWith5, threshold)
		})

		t.Run("above threshold switches to HAMTDirectory", func(t *testing.T) {
			HAMTShardingSize = threshold

			dir, err := NewDirectory(ds)
			require.NoError(t, err)

			// Add 6 entries, should be above threshold
			for i := range 6 {
				err = dir.AddChild(ctx, fmt.Sprintf("e%d", i), child)
				require.NoError(t, err)
			}

			_, isHAMT := dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
			assert.True(t, isHAMT, "should switch to HAMTDirectory when above threshold (%d > %d)", sizeWith6, threshold)
		})
	})

	t.Run("SizeEstimationBlock is more accurate than SizeEstimationLinks", func(t *testing.T) {
		saveAndRestoreGlobals(t)

		ds := mdtest.Mock()
		ctx := context.Background()
		child := ft.EmptyDirNode()
		require.NoError(t, ds.Add(ctx, child))

		// Create a directory and add some entries
		dir, err := NewBasicDirectory(ds)
		require.NoError(t, err)

		for i := range 5 {
			err = dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
			require.NoError(t, err)
		}

		// Get actual serialized block size
		node, err := dir.GetNode()
		require.NoError(t, err)
		actualBlockSize, err := calculateBlockSize(node.(*mdag.ProtoNode))
		require.NoError(t, err)

		// Calculate links-based estimation (what SizeEstimationLinks uses)
		linksEstimate := 0
		for _, link := range node.Links() {
			linksEstimate += len(link.Name) + link.Cid.ByteLen()
		}

		t.Logf("actual block size: %d bytes", actualBlockSize)
		t.Logf("links-based estimate: %d bytes", linksEstimate)
		t.Logf("difference: %d bytes (%.1f%% underestimate)", actualBlockSize-linksEstimate, float64(actualBlockSize-linksEstimate)/float64(actualBlockSize)*100)

		// Links-based estimation should underestimate because it ignores:
		// - Tsize field
		// - Protobuf varints and tags
		// - UnixFS Data field
		assert.Greater(t, actualBlockSize, linksEstimate,
			"links-based estimation should underestimate actual block size")
	})

	t.Run("SizeEstimationBlock exact threshold boundary", func(t *testing.T) {
		saveAndRestoreGlobals(t)

		// Test that the HAMT switch happens exactly when size > threshold (not >=)
		HAMTSizeEstimation = SizeEstimationBlock

		ds := mdtest.Mock()
		ctx := t.Context()
		child := ft.EmptyDirNode()
		require.NoError(t, ds.Add(ctx, child))

		// First, determine the exact block size after adding entries
		testDir, err := NewDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
		require.NoError(t, err)

		// Add entries and track sizes at each step
		var sizes []int
		for i := 0; i < 10; i++ {
			err = testDir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
			require.NoError(t, err)

			node, err := testDir.GetNode()
			require.NoError(t, err)
			pn, ok := node.(*mdag.ProtoNode)
			require.True(t, ok)
			size, err := calculateBlockSize(pn)
			require.NoError(t, err)
			sizes = append(sizes, size)
		}

		// Set threshold to exactly the size after 5 entries
		// (entry0..entry4 = 5 entries)
		exactThreshold := sizes[4]
		t.Logf("threshold set to exactly %d bytes (size after 5 entries)", exactThreshold)

		t.Run("at exact threshold stays BasicDirectory", func(t *testing.T) {
			HAMTShardingSize = exactThreshold

			dir, err := NewDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
			require.NoError(t, err)

			// Add 5 entries to reach exactly threshold size
			for i := 0; i < 5; i++ {
				err = dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
				require.NoError(t, err)
			}

			node, err := dir.GetNode()
			require.NoError(t, err)
			pn := node.(*mdag.ProtoNode)
			actualSize, _ := calculateBlockSize(pn)
			t.Logf("actual size: %d, threshold: %d", actualSize, exactThreshold)

			_, isHAMT := dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
			assert.False(t, isHAMT, "should stay BasicDirectory when size equals threshold (%d == %d)", actualSize, exactThreshold)
		})

		t.Run("one byte over threshold switches to HAMTDirectory", func(t *testing.T) {
			// Set threshold to size[4] - 1 so that size[4] is > threshold
			HAMTShardingSize = sizes[4] - 1

			dir, err := NewDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
			require.NoError(t, err)

			// Add 5 entries - last one should trigger HAMT
			for i := 0; i < 5; i++ {
				err = dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
				require.NoError(t, err)
			}

			_, isHAMT := dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
			assert.True(t, isHAMT, "should switch to HAMTDirectory when size exceeds threshold (%d > %d)", sizes[4], HAMTShardingSize)
		})
	})
}
