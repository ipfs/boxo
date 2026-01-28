package io

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	mdag "github.com/ipfs/boxo/ipld/merkledag"
	mdtest "github.com/ipfs/boxo/ipld/merkledag/test"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/ipld/unixfs/private/linksize"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// calculateBlockSize returns the actual byte size of the serialized dag-pb block
// by performing real protobuf encoding. Used only in tests to verify that
// arithmetic size calculations (dataFieldSerializedSize + linkSerializedSize)
// match actual protobuf serialization.
func calculateBlockSize(node *mdag.ProtoNode) (int, error) {
	data, err := node.EncodeProtobuf(false)
	if err != nil {
		return 0, fmt.Errorf("failed to encode directory node: %w", err)
	}
	return len(data), nil
}

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

// saveAndRestoreGlobals saves the current global settings and restores them
// after the test completes. Use this in tests that modify HAMTShardingSize,
// HAMTSizeEstimation, DefaultShardWidth, or linksize.LinkSizeFunction.
func saveAndRestoreGlobals(t *testing.T) {
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

func TestProfileHAMTThresholdBehavior(t *testing.T) {
	// Use fixed link size for predictable testing
	const fixedLinkSize = 100

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

		t.Run("one byte over threshold switches to HAMTDirectory", func(t *testing.T) {
			// Set threshold to 299 so that 3 entries (300 bytes) = threshold + 1.
			// This also confirms HAMTShardingSize is read dynamically and not
			// hardcoded elsewhere in the implementation.
			HAMTShardingSize = 299

			dir, err := NewDirectory(ds)
			require.NoError(t, err)

			// Add 3 entries = 300 bytes = threshold + 1
			for i := range 3 {
				err = dir.AddChild(ctx, fmt.Sprintf("e%d", i), child)
				require.NoError(t, err)
			}

			_, isHAMT := dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
			assert.True(t, isHAMT, "should switch to HAMTDirectory when one byte over threshold (300 > 299)")
		})

		t.Run("well above threshold switches to HAMTDirectory", func(t *testing.T) {
			HAMTShardingSize = 300 // reset

			dir, err := NewDirectory(ds)
			require.NoError(t, err)

			// Add 4 entries = 400 bytes, well above 300 threshold
			for i := range 4 {
				err = dir.AddChild(ctx, fmt.Sprintf("e%d", i), child)
				require.NoError(t, err)
			}

			_, isHAMT := dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
			assert.True(t, isHAMT, "should switch to HAMTDirectory when well above threshold (400 > 300)")
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

func TestSizeEstimationBlockWithModeMtime(t *testing.T) {
	saveAndRestoreGlobals(t)
	HAMTSizeEstimation = SizeEstimationBlock

	ds := mdtest.Mock()
	ctx := t.Context()

	t.Run("mode increases block size", func(t *testing.T) {
		// Directory without mode
		dirNoMode, err := NewBasicDirectory(ds)
		require.NoError(t, err)

		nodeNoMode, err := dirNoMode.GetNode()
		require.NoError(t, err)
		sizeNoMode, err := calculateBlockSize(nodeNoMode.(*mdag.ProtoNode))
		require.NoError(t, err)

		// Directory with mode
		dirWithMode, err := NewBasicDirectory(ds, WithStat(os.FileMode(0o755), time.Time{}))
		require.NoError(t, err)

		nodeWithMode, err := dirWithMode.GetNode()
		require.NoError(t, err)
		sizeWithMode, err := calculateBlockSize(nodeWithMode.(*mdag.ProtoNode))
		require.NoError(t, err)

		// Mode field in protobuf: tag (1 byte) + varint value (2 bytes for 0755=493)
		// = 3 bytes overhead
		expectedModeOverhead := 3
		assert.Equal(t, expectedModeOverhead, sizeWithMode-sizeNoMode,
			"mode overhead should be exactly %d bytes", expectedModeOverhead)
	})

	t.Run("mtime increases block size", func(t *testing.T) {
		// Directory without mtime
		dirNoMtime, err := NewBasicDirectory(ds)
		require.NoError(t, err)

		nodeNoMtime, err := dirNoMtime.GetNode()
		require.NoError(t, err)
		sizeNoMtime, err := calculateBlockSize(nodeNoMtime.(*mdag.ProtoNode))
		require.NoError(t, err)

		// Directory with mtime (seconds only)
		mtime := time.Unix(1700000000, 0)
		dirWithMtime, err := NewBasicDirectory(ds, WithStat(0, mtime))
		require.NoError(t, err)

		nodeWithMtime, err := dirWithMtime.GetNode()
		require.NoError(t, err)
		sizeWithMtime, err := calculateBlockSize(nodeWithMtime.(*mdag.ProtoNode))
		require.NoError(t, err)

		// Mtime field in protobuf (seconds only):
		// - outer tag (1) + length prefix (1) + inner message
		// - inner: seconds tag (1) + varint (5 bytes for 1700000000)
		// = 8 bytes overhead
		expectedMtimeOverhead := 8
		assert.Equal(t, expectedMtimeOverhead, sizeWithMtime-sizeNoMtime,
			"mtime (seconds only) overhead should be exactly %d bytes", expectedMtimeOverhead)
	})

	t.Run("mtime with nanoseconds increases block size further", func(t *testing.T) {
		// Directory with mtime (seconds only)
		mtimeSeconds := time.Unix(1700000000, 0)
		dirSeconds, err := NewBasicDirectory(ds, WithStat(0, mtimeSeconds))
		require.NoError(t, err)

		nodeSeconds, err := dirSeconds.GetNode()
		require.NoError(t, err)
		sizeSeconds, err := calculateBlockSize(nodeSeconds.(*mdag.ProtoNode))
		require.NoError(t, err)

		// Directory with mtime (seconds + nanoseconds)
		mtimeNanos := time.Unix(1700000000, 123456789)
		dirNanos, err := NewBasicDirectory(ds, WithStat(0, mtimeNanos))
		require.NoError(t, err)

		nodeNanos, err := dirNanos.GetNode()
		require.NoError(t, err)
		sizeNanos, err := calculateBlockSize(nodeNanos.(*mdag.ProtoNode))
		require.NoError(t, err)

		// Nanoseconds field: tag (1) + varint (4 bytes for 123456789)
		// = 5 bytes additional overhead
		expectedNanosOverhead := 5
		assert.Equal(t, expectedNanosOverhead, sizeNanos-sizeSeconds,
			"nanoseconds overhead should be exactly %d bytes", expectedNanosOverhead)
	})

	t.Run("mode and mtime combined", func(t *testing.T) {
		// Directory without metadata
		dirPlain, err := NewBasicDirectory(ds)
		require.NoError(t, err)

		nodePlain, err := dirPlain.GetNode()
		require.NoError(t, err)
		sizePlain, err := calculateBlockSize(nodePlain.(*mdag.ProtoNode))
		require.NoError(t, err)

		// Directory with both mode and mtime
		mtime := time.Unix(1700000000, 123456789)
		dirFull, err := NewBasicDirectory(ds, WithStat(os.FileMode(0o755), mtime))
		require.NoError(t, err)

		nodeFull, err := dirFull.GetNode()
		require.NoError(t, err)
		sizeFull, err := calculateBlockSize(nodeFull.(*mdag.ProtoNode))
		require.NoError(t, err)

		// Total: mode (3) + mtime with nanos (8 + 5) = 16 bytes
		expectedTotalOverhead := 16
		assert.Equal(t, expectedTotalOverhead, sizeFull-sizePlain,
			"total mode+mtime overhead should be exactly %d bytes", expectedTotalOverhead)
	})

	t.Run("estimatedSize includes mode/mtime overhead", func(t *testing.T) {
		child := ft.EmptyDirNode()
		require.NoError(t, ds.Add(ctx, child))

		// Create directory with metadata
		mtime := time.Unix(1700000000, 123456789)
		dir, err := NewBasicDirectory(ds,
			WithStat(os.FileMode(0o755), mtime),
			WithSizeEstimationMode(SizeEstimationBlock),
		)
		require.NoError(t, err)

		// Add some entries
		for i := range 5 {
			err = dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
			require.NoError(t, err)
		}

		// Get actual block size
		node, err := dir.GetNode()
		require.NoError(t, err)
		actualSize, err := calculateBlockSize(node.(*mdag.ProtoNode))
		require.NoError(t, err)

		// Check estimated size matches actual size
		assert.Equal(t, actualSize, dir.estimatedSize,
			"estimatedSize should match actual block size (including mode/mtime)")
	})

	t.Run("HAMT threshold accounts for mode/mtime overhead", func(t *testing.T) {
		child := ft.EmptyDirNode()
		require.NoError(t, ds.Add(ctx, child))

		// Create directories with and without metadata, add entries until we find
		// a threshold where one switches to HAMT but the other doesn't
		mtime := time.Unix(1700000000, 123456789)

		// First, find the size difference due to metadata
		dirPlain, err := NewBasicDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
		require.NoError(t, err)
		dirMeta, err := NewBasicDirectory(ds,
			WithStat(os.FileMode(0o755), mtime),
			WithSizeEstimationMode(SizeEstimationBlock),
		)
		require.NoError(t, err)

		// Add entries to both
		for i := range 3 {
			err = dirPlain.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
			require.NoError(t, err)
			err = dirMeta.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
			require.NoError(t, err)
		}

		nodePlain, _ := dirPlain.GetNode()
		nodeMeta, _ := dirMeta.GetNode()
		sizePlain, _ := calculateBlockSize(nodePlain.(*mdag.ProtoNode))
		sizeMeta, _ := calculateBlockSize(nodeMeta.(*mdag.ProtoNode))

		t.Logf("size without metadata (3 entries): %d", sizePlain)
		t.Logf("size with metadata (3 entries): %d", sizeMeta)

		// Set threshold between the two sizes
		if sizeMeta > sizePlain {
			HAMTShardingSize = (sizePlain + sizeMeta) / 2
			t.Logf("threshold set to: %d (between plain and meta)", HAMTShardingSize)

			// Create fresh directories with this threshold
			dirPlain2, err := NewDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
			require.NoError(t, err)
			dirMeta2, err := NewDirectory(ds,
				WithStat(os.FileMode(0o755), mtime),
				WithSizeEstimationMode(SizeEstimationBlock),
			)
			require.NoError(t, err)

			// Add entries
			for i := range 3 {
				err = dirPlain2.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
				require.NoError(t, err)
				err = dirMeta2.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
				require.NoError(t, err)
			}

			// Plain should stay basic, meta should switch to HAMT
			_, plainIsHAMT := dirPlain2.(*DynamicDirectory).Directory.(*HAMTDirectory)
			_, metaIsHAMT := dirMeta2.(*DynamicDirectory).Directory.(*HAMTDirectory)

			assert.False(t, plainIsHAMT, "plain directory should stay BasicDirectory")
			assert.True(t, metaIsHAMT, "directory with metadata should switch to HAMTDirectory")
		}
	})
}

func TestEstimatedSizeAccuracy(t *testing.T) {
	saveAndRestoreGlobals(t)
	HAMTSizeEstimation = SizeEstimationBlock

	ds := mdtest.Mock()
	ctx := t.Context()

	t.Run("estimated size matches actual after multiple operations", func(t *testing.T) {
		child := ft.EmptyDirNode()
		require.NoError(t, ds.Add(ctx, child))

		dir, err := NewBasicDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
		require.NoError(t, err)

		// Add entries
		for i := range 10 {
			err = dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
			require.NoError(t, err)
		}

		// Remove some entries
		for i := range 5 {
			err = dir.RemoveChild(ctx, fmt.Sprintf("entry%d", i))
			require.NoError(t, err)
		}

		// Replace some entries
		for i := 5; i < 8; i++ {
			err = dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
			require.NoError(t, err)
		}

		// Verify estimated size matches actual
		node, err := dir.GetNode()
		require.NoError(t, err)
		actualSize, err := calculateBlockSize(node.(*mdag.ProtoNode))
		require.NoError(t, err)

		assert.Equal(t, actualSize, dir.estimatedSize,
			"estimatedSize should match actual after add/remove/replace operations")
	})

	t.Run("linkSerializedSize matches actual link contribution", func(t *testing.T) {
		child := ft.EmptyDirNode()
		require.NoError(t, ds.Add(ctx, child))

		// Create empty directory and get base size
		dir, err := NewBasicDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
		require.NoError(t, err)
		emptyNode, _ := dir.GetNode()
		emptySize, _ := calculateBlockSize(emptyNode.(*mdag.ProtoNode))

		// Add one entry
		err = dir.AddChild(ctx, "testentry", child)
		require.NoError(t, err)
		oneEntryNode, _ := dir.GetNode()
		oneEntrySize, _ := calculateBlockSize(oneEntryNode.(*mdag.ProtoNode))

		// Calculate expected link size
		link, _ := oneEntryNode.Links()[0].Cid, oneEntryNode.Links()[0]
		expectedLinkSize := linkSerializedSize("testentry", link, oneEntryNode.Links()[0].Size)

		actualLinkContribution := oneEntrySize - emptySize
		t.Logf("empty dir size: %d", emptySize)
		t.Logf("one entry size: %d", oneEntrySize)
		t.Logf("actual link contribution: %d", actualLinkContribution)
		t.Logf("linkSerializedSize result: %d", expectedLinkSize)

		assert.Equal(t, actualLinkContribution, expectedLinkSize,
			"linkSerializedSize should exactly match actual link contribution")
	})
}

func TestBlockSizeEstimationEdgeCases(t *testing.T) {
	saveAndRestoreGlobals(t)
	HAMTSizeEstimation = SizeEstimationBlock

	ds := mdtest.Mock()
	ctx := t.Context()

	t.Run("entry replacement updates estimated size correctly", func(t *testing.T) {
		child1 := ft.EmptyDirNode()
		require.NoError(t, ds.Add(ctx, child1))

		// Create a larger child node to have different CID/size
		child2 := ft.EmptyDirNode()
		child2.SetData(make([]byte, 100))
		require.NoError(t, ds.Add(ctx, child2))

		dir, err := NewBasicDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
		require.NoError(t, err)

		// Add entry
		err = dir.AddChild(ctx, "entry", child1)
		require.NoError(t, err)
		node1, _ := dir.GetNode()
		size1, _ := calculateBlockSize(node1.(*mdag.ProtoNode))
		estimated1 := dir.estimatedSize
		t.Logf("after add child1: actual=%d, estimated=%d", size1, estimated1)
		assert.Equal(t, size1, estimated1)

		// Replace with different child
		err = dir.AddChild(ctx, "entry", child2)
		require.NoError(t, err)
		node2, _ := dir.GetNode()
		size2, _ := calculateBlockSize(node2.(*mdag.ProtoNode))
		estimated2 := dir.estimatedSize
		t.Logf("after replace with child2: actual=%d, estimated=%d", size2, estimated2)
		assert.Equal(t, size2, estimated2)

		// Size should have changed due to different Tsize
		t.Logf("size changed by: %d bytes", size2-size1)
	})

	t.Run("long entry names handled correctly", func(t *testing.T) {
		child := ft.EmptyDirNode()
		require.NoError(t, ds.Add(ctx, child))

		dir, err := NewBasicDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
		require.NoError(t, err)

		// Add entries with varying name lengths
		names := []string{
			"a",
			"medium_length_name",
			"this_is_a_very_long_entry_name_that_should_still_work_correctly_for_size_estimation",
		}

		for _, name := range names {
			err = dir.AddChild(ctx, name, child)
			require.NoError(t, err)

			node, _ := dir.GetNode()
			actualSize, _ := calculateBlockSize(node.(*mdag.ProtoNode))
			assert.Equal(t, actualSize, dir.estimatedSize,
				"estimated size should match actual for name: %s", name)
		}
	})

	t.Run("threshold behavior with fast path optimization", func(t *testing.T) {
		child := ft.EmptyDirNode()
		require.NoError(t, ds.Add(ctx, child))

		// Calculate size for entries to set appropriate threshold
		testDir, err := NewBasicDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
		require.NoError(t, err)
		for i := range 20 {
			_ = testDir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
		}
		node, _ := testDir.GetNode()
		sizeAt20, _ := calculateBlockSize(node.(*mdag.ProtoNode))

		// Set threshold well below the current size (to test fast path for "clearly above")
		// The margin is 256 bytes, so set threshold such that size is > threshold + 256
		HAMTShardingSize = sizeAt20 - 500
		t.Logf("threshold: %d, size at 20 entries: %d, margin: 256", HAMTShardingSize, sizeAt20)

		// Create directory and add entries - should switch to HAMT via fast path
		dir, err := NewDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
		require.NoError(t, err)

		for i := range 20 {
			err = dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
			require.NoError(t, err)
		}

		_, isHAMT := dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
		assert.True(t, isHAMT, "should switch to HAMT when clearly above threshold")
	})

	t.Run("threshold behavior near boundary uses exact calculation", func(t *testing.T) {
		child := ft.EmptyDirNode()
		require.NoError(t, ds.Add(ctx, child))

		// Calculate exact sizes
		testDir, err := NewBasicDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
		require.NoError(t, err)
		for i := range 5 {
			_ = testDir.AddChild(ctx, fmt.Sprintf("e%d", i), child)
		}
		node, _ := testDir.GetNode()
		sizeAt5, _ := calculateBlockSize(node.(*mdag.ProtoNode))

		// Set threshold to exact size - should NOT switch because we use
		// "size > threshold" (not >=), so size == threshold stays basic
		HAMTShardingSize = sizeAt5
		t.Logf("threshold: %d (exact size at 5 entries)", HAMTShardingSize)

		dir, err := NewDirectory(ds, WithSizeEstimationMode(SizeEstimationBlock))
		require.NoError(t, err)

		for i := range 5 {
			err = dir.AddChild(ctx, fmt.Sprintf("e%d", i), child)
			require.NoError(t, err)
		}

		_, isHAMT := dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
		assert.False(t, isHAMT, "should stay BasicDirectory when size equals threshold")
	})
}

// TestHAMTToBasicDowngrade tests that HAMTDirectory converts back to BasicDirectory
// at precise byte boundaries when entries are removed.
func TestHAMTToBasicDowngrade(t *testing.T) {
	saveAndRestoreGlobals(t)

	ds := mdtest.Mock()
	ctx := t.Context()

	child := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, child))

	// Test both size estimation modes
	testCases := []struct {
		name string
		mode SizeEstimationMode
		// sizeFunc calculates the "size" for a given number of entries based on mode
		sizeFunc func(numEntries int) int
	}{
		{
			name: "SizeEstimationLinks",
			mode: SizeEstimationLinks,
			sizeFunc: func(numEntries int) int {
				// Each entry "entryN" has name length 6 (entry0-entry9) + CID length
				// Using the mock or calculating based on linksize function
				size := 0
				for i := range numEntries {
					name := fmt.Sprintf("entry%d", i)
					size += len(name) + child.Cid().ByteLen()
				}
				return size
			},
		},
		{
			name:     "SizeEstimationBlock",
			mode:     SizeEstimationBlock,
			sizeFunc: nil, // Will measure actual block sizes
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			HAMTSizeEstimation = tc.mode

			// Measure sizes at different entry counts
			var sizes []int
			if tc.mode == SizeEstimationBlock {
				// Measure actual block sizes
				measureDir, err := NewBasicDirectory(ds, WithSizeEstimationMode(tc.mode))
				require.NoError(t, err)
				for i := range 10 {
					err = measureDir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
					require.NoError(t, err)
					node, _ := measureDir.GetNode()
					size, _ := calculateBlockSize(node.(*mdag.ProtoNode))
					sizes = append(sizes, size)
				}
			} else {
				// Calculate link-based sizes
				for i := 1; i <= 10; i++ {
					sizes = append(sizes, tc.sizeFunc(i))
				}
			}
			t.Logf("sizes: 5 entries=%d, 6 entries=%d, 7 entries=%d",
				sizes[4], sizes[5], sizes[6])

			// Set threshold between 5 and 6 entries
			threshold := (sizes[4] + sizes[5]) / 2
			t.Logf("threshold set to %d (between 5 and 6 entries)", threshold)

			t.Run("HAMT stays HAMT when above threshold after remove", func(t *testing.T) {
				HAMTShardingSize = threshold

				dir, err := NewDirectory(ds, WithSizeEstimationMode(tc.mode))
				require.NoError(t, err)

				for i := range 8 {
					err = dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
					require.NoError(t, err)
				}

				_, isHAMT := dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
				require.True(t, isHAMT, "should be HAMTDirectory after adding 8 entries")

				// Remove entries down to 6 (still above threshold)
				err = dir.RemoveChild(ctx, "entry7")
				require.NoError(t, err)
				err = dir.RemoveChild(ctx, "entry6")
				require.NoError(t, err)

				_, isHAMT = dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
				assert.True(t, isHAMT,
					"should stay HAMTDirectory with 6 entries (size %d > threshold %d)",
					sizes[5], threshold)
			})

			t.Run("HAMT downgrades to Basic when at threshold after remove", func(t *testing.T) {
				// Set threshold to exactly the size at 5 entries
				HAMTShardingSize = sizes[4]
				t.Logf("threshold set to exactly %d (size at 5 entries)", HAMTShardingSize)

				dir, err := NewDirectory(ds, WithSizeEstimationMode(tc.mode))
				require.NoError(t, err)

				for i := range 8 {
					err = dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
					require.NoError(t, err)
				}

				_, isHAMT := dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
				require.True(t, isHAMT, "should be HAMTDirectory after adding 8 entries")

				// Remove down to 5 entries (at threshold)
				for i := 7; i >= 5; i-- {
					err = dir.RemoveChild(ctx, fmt.Sprintf("entry%d", i))
					require.NoError(t, err)
				}

				_, isHAMT = dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
				assert.False(t, isHAMT,
					"should downgrade to BasicDirectory when size equals threshold (%d == %d)",
					sizes[4], HAMTShardingSize)
			})

			t.Run("HAMT downgrades to Basic when below threshold after remove", func(t *testing.T) {
				HAMTShardingSize = threshold

				dir, err := NewDirectory(ds, WithSizeEstimationMode(tc.mode))
				require.NoError(t, err)

				for i := range 8 {
					err = dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
					require.NoError(t, err)
				}

				_, isHAMT := dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
				require.True(t, isHAMT, "should be HAMTDirectory after adding 8 entries")

				// Remove down to 5 entries (below threshold)
				for i := 7; i >= 5; i-- {
					err = dir.RemoveChild(ctx, fmt.Sprintf("entry%d", i))
					require.NoError(t, err)
				}

				_, isHAMT = dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
				assert.False(t, isHAMT,
					"should downgrade to BasicDirectory when below threshold (%d < %d)",
					sizes[4], threshold)

				// Verify all remaining entries are intact
				links, err := dir.Links(ctx)
				require.NoError(t, err)
				assert.Len(t, links, 5, "should have 5 entries after downgrade")
			})

			t.Run("precise boundary: one entry makes the difference", func(t *testing.T) {
				// Set threshold so that 6 entries is exactly at threshold
				HAMTShardingSize = sizes[5]
				t.Logf("threshold set to exactly %d (size at 6 entries)", HAMTShardingSize)

				dir, err := NewDirectory(ds, WithSizeEstimationMode(tc.mode))
				require.NoError(t, err)

				// Add 7 entries to get into HAMT territory
				for i := range 7 {
					err = dir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
					require.NoError(t, err)
				}

				_, isHAMT := dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
				require.True(t, isHAMT, "should be HAMTDirectory with 7 entries")

				// Remove one entry -> 6 entries, size == threshold -> should downgrade
				err = dir.RemoveChild(ctx, "entry6")
				require.NoError(t, err)

				_, isHAMT = dir.(*DynamicDirectory).Directory.(*HAMTDirectory)
				assert.False(t, isHAMT,
					"should downgrade to BasicDirectory when at exact threshold after remove")

				// Verify it's actually a BasicDirectory with correct entries
				_, ok := dir.(*DynamicDirectory).Directory.(*BasicDirectory)
				require.True(t, ok, "should be BasicDirectory")

				links, err := dir.Links(ctx)
				require.NoError(t, err)
				assert.Len(t, links, 6, "should have 6 entries after downgrade")
			})
		})
	}
}

// TestDataFieldSerializedSizeMatchesActual verifies that the arithmetic calculation
// in dataFieldSerializedSize matches the actual protobuf serialization. This test
// ensures the optimization doesn't drift from reality and will catch any future
// field additions to the UnixFS spec for directories.
func TestDataFieldSerializedSizeMatchesActual(t *testing.T) {
	testCases := []struct {
		name  string
		mode  os.FileMode
		mtime time.Time
	}{
		{"empty directory", 0, time.Time{}},
		{"with mode 0755", os.FileMode(0o755), time.Time{}},
		{"with mode 0644", os.FileMode(0o644), time.Time{}},
		{"with mtime seconds only", 0, time.Unix(1700000000, 0)},
		{"with mtime and nanos", 0, time.Unix(1700000000, 123456789)},
		{"with mode and mtime", os.FileMode(0o755), time.Unix(1700000000, 123456789)},
		{"with negative timestamp", 0, time.Unix(-1000000, 0)}, // before 1970
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create actual node and serialize
			var node *mdag.ProtoNode
			if tc.mode != 0 || !tc.mtime.IsZero() {
				node = ft.EmptyDirNodeWithStat(tc.mode, tc.mtime)
			} else {
				node = ft.EmptyDirNode()
			}

			actualSize, err := calculateBlockSize(node)
			require.NoError(t, err)

			// Compute using arithmetic function
			computedSize := dataFieldSerializedSize(tc.mode, tc.mtime)

			assert.Equal(t, actualSize, computedSize,
				"dataFieldSerializedSize must match actual serialized size; "+
					"if UnixFS spec adds new directory fields, update dataFieldSerializedSize()")
		})
	}
}

// TestHAMTAndBasicDirectorySizeConsistency verifies that HAMTDirectory.sizeBelowThreshold()
// and BasicDirectory.estimatedSize compute equivalent values for the same directory contents.
// This ensures both calculations use the same Data field overhead.
func TestHAMTAndBasicDirectorySizeConsistency(t *testing.T) {
	saveAndRestoreGlobals(t)

	ds := mdtest.Mock()
	ctx := t.Context()

	child := ft.EmptyDirNode()
	require.NoError(t, ds.Add(ctx, child))

	t.Run("SizeEstimationBlock mode/mtime consistency", func(t *testing.T) {
		HAMTSizeEstimation = SizeEstimationBlock

		mtime := time.Unix(1700000000, 123456789)
		mode := os.FileMode(0o755)

		// Create a BasicDirectory with mode/mtime and add some entries
		basicDir, err := NewBasicDirectory(ds,
			WithStat(mode, mtime),
			WithSizeEstimationMode(SizeEstimationBlock),
		)
		require.NoError(t, err)

		for i := range 5 {
			err = basicDir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
			require.NoError(t, err)
		}

		// Get BasicDirectory's estimated size
		basicEstimatedSize := basicDir.estimatedSize

		// Verify it matches actual serialized size
		node, err := basicDir.GetNode()
		require.NoError(t, err)
		actualSize, err := calculateBlockSize(node.(*mdag.ProtoNode))
		require.NoError(t, err)
		assert.Equal(t, actualSize, basicEstimatedSize,
			"BasicDirectory.estimatedSize should match actual block size")

		// Now create a HAMT and convert back to verify consistency
		// Set threshold low to force HAMT conversion
		HAMTShardingSize = 50

		hamtDir, err := NewDirectory(ds,
			WithStat(mode, mtime),
			WithSizeEstimationMode(SizeEstimationBlock),
		)
		require.NoError(t, err)

		// Add same entries to trigger HAMT conversion
		for i := range 5 {
			err = hamtDir.AddChild(ctx, fmt.Sprintf("entry%d", i), child)
			require.NoError(t, err)
		}

		// Should be a HAMT now
		hamt, ok := hamtDir.(*DynamicDirectory).Directory.(*HAMTDirectory)
		require.True(t, ok, "should be HAMTDirectory")

		// Set mode/mtime on HAMT (simulating what would happen during conversion)
		hamt.SetStat(mode, mtime)

		// Set threshold high to allow BasicDirectory and test sizeBelowThreshold
		HAMTShardingSize = 1000

		// sizeBelowThreshold should compute the same size as BasicDirectory
		// (since it's calculating the hypothetical BasicDirectory size)
		below, err := hamt.sizeBelowThreshold(ctx, 0)
		require.NoError(t, err)
		assert.True(t, below, "should be below threshold")

		// The BasicDirectory estimated size should equal what HAMT would calculate
		// for conversion decision. We can verify this by creating a fresh BasicDirectory
		// from the HAMT's contents and comparing sizes.
		freshBasic, err := NewBasicDirectory(ds,
			WithStat(mode, mtime),
			WithSizeEstimationMode(SizeEstimationBlock),
		)
		require.NoError(t, err)

		err = hamt.ForEachLink(ctx, func(link *ipld.Link) error {
			return freshBasic.addLinkChild(ctx, link.Name, link)
		})
		require.NoError(t, err)

		// The fresh BasicDirectory's estimated size should be consistent
		t.Logf("BasicDirectory estimated size: %d", basicEstimatedSize)
		t.Logf("Fresh BasicDirectory from HAMT estimated size: %d", freshBasic.estimatedSize)

		assert.Equal(t, basicEstimatedSize, freshBasic.estimatedSize,
			"BasicDirectory size should be consistent regardless of creation path")
	})
}
