package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	bsfetcher "github.com/ipfs/boxo/fetcher/impl/blockservice"
	"github.com/ipfs/boxo/verifcid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipld/go-car"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/multiformats/go-multihash"
)

type getBlock func(ctx context.Context, cid cid.Cid) (blocks.Block, error)

// ErrInvalidResponse can be returned from a DataCallback to indicate that the data provided for the
// requested resource was explicitly 'incorrect' - that blocks not in the requested dag, or non-car-conforming
// data was returned.
type ErrInvalidResponse struct {
	Message string
}

func (e ErrInvalidResponse) Error() string {
	return e.Message
}

// var ErrNilBlock = caboose.ErrInvalidResponse{Message: "received a nil block with no error"}
var ErrNilBlock = ErrInvalidResponse{Message: "received a nil block with no error"}

func carToLinearBlockGetter(ctx context.Context, reader io.Reader, metrics *GraphGatewayMetrics) (getBlock, error) {
	cr, err := car.NewCarReaderWithOptions(reader, car.WithErrorOnEmptyRoots(false))
	if err != nil {
		return nil, err
	}

	cbCtx, cncl := context.WithCancel(ctx)

	type blockRead struct {
		block blocks.Block
		err   error
	}

	blkCh := make(chan blockRead, 1)
	go func() {
		defer cncl()
		defer close(blkCh)
		for {
			blk, rdErr := cr.Next()
			select {
			case blkCh <- blockRead{blk, rdErr}:
				if rdErr != nil {
					cncl()
				}
			case <-cbCtx.Done():
				return
			}
		}
	}()

	isFirstBlock := true
	mx := sync.Mutex{}

	return func(ctx context.Context, c cid.Cid) (blocks.Block, error) {
		mx.Lock()
		defer mx.Unlock()
		if err := verifcid.ValidateCid(verifcid.DefaultAllowlist, c); err != nil {
			return nil, err
		}

		isId, bdata := extractIdentityMultihashCIDContents(c)
		if isId {
			return blocks.NewBlockWithCid(bdata, c)
		}

		// initially set a higher timeout here so that if there's an initial timeout error we get it from the car reader.
		var t *time.Timer
		if isFirstBlock {
			t = time.NewTimer(GetBlockTimeout * 2)
		} else {
			t = time.NewTimer(GetBlockTimeout)
		}
		var blkRead blockRead
		var ok bool
		select {
		case blkRead, ok = <-blkCh:
			if !t.Stop() {
				<-t.C
			}
			t.Reset(GetBlockTimeout)
		case <-t.C:
			return nil, ErrGatewayTimeout
		}
		if !ok || blkRead.err != nil {
			if !ok || errors.Is(blkRead.err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, GatewayError(blkRead.err)
		}
		if blkRead.block != nil {
			metrics.carBlocksFetchedMetric.Inc()
			if !blkRead.block.Cid().Equals(c) {
				return nil, ErrInvalidResponse{Message: fmt.Sprintf("received block with cid %s, expected %s", blkRead.block.Cid(), c)}
			}
			return blkRead.block, nil
		}
		return nil, ErrNilBlock
	}, nil
}

// extractIdentityMultihashCIDContents will check if a given CID has an identity multihash and if so return true and
// the bytes encoded in the digest, otherwise will return false.
// Taken from https://github.com/ipfs/boxo/blob/b96767cc0971ca279feb36e7844e527a774309ab/blockstore/idstore.go#L30
func extractIdentityMultihashCIDContents(k cid.Cid) (bool, []byte) {
	// Pre-check by calling Prefix(), this much faster than extracting the hash.
	if k.Prefix().MhType != multihash.IDENTITY {
		return false, nil
	}

	dmh, err := multihash.Decode(k.Hash())
	if err != nil || dmh.Code != multihash.IDENTITY {
		return false, nil
	}
	return true, dmh.Digest
}

func getLinksystem(fn getBlock) *ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(linkContext linking.LinkContext, link datamodel.Link) (io.Reader, error) {
		c := link.(cidlink.Link).Cid
		blk, err := fn(linkContext.Ctx, c)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(blk.RawData()), nil
	}
	lsys.TrustedStorage = true
	unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)
	return &lsys
}

// walkGatewaySimpleSelector2 walks the subgraph described by the path and terminal element parameters
func walkGatewaySimpleSelector2(ctx context.Context, terminalBlk blocks.Block, dagScope DagScope, entityRange *DagByteRange, lsys *ipld.LinkSystem) error {
	lctx := ipld.LinkContext{Ctx: ctx}
	var err error

	// If the scope is the block, we only need the root block of the last element of the path, which we have.
	if dagScope == DagScopeBlock {
		return nil
	}

	// decode the terminal block into a node
	pc := dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})

	pathTerminalCidLink := cidlink.Link{Cid: terminalBlk.Cid()}
	np, err := pc(pathTerminalCidLink, lctx)
	if err != nil {
		return err
	}

	decoder, err := lsys.DecoderChooser(pathTerminalCidLink)
	if err != nil {
		return err
	}
	nb := np.NewBuilder()
	blockData := terminalBlk.RawData()
	if err := decoder(nb, bytes.NewReader(blockData)); err != nil {
		return err
	}
	lastCidNode := nb.Build()

	// TODO: Evaluate:
	// Does it matter that we're ignoring the "remainder" portion of the traversal in GetCAR?
	// Does it matter that we're using a linksystem with the UnixFS reifier for dagscope=all?

	// If we're asking for everything then give it
	if dagScope == DagScopeAll {
		sel, err := selector.ParseSelector(selectorparse.CommonSelector_ExploreAllRecursively)
		if err != nil {
			return err
		}

		progress := traversal.Progress{
			Cfg: &traversal.Config{
				Ctx:                            ctx,
				LinkSystem:                     *lsys,
				LinkTargetNodePrototypeChooser: bsfetcher.DefaultPrototypeChooser,
				LinkVisitOnlyOnce:              false, // Despite being safe for the "all" selector we do this walk anyway since this is how we will be receiving the blocks
			},
		}

		if err := progress.WalkMatching(lastCidNode, sel, func(progress traversal.Progress, node datamodel.Node) error {
			return nil
		}); err != nil {
			return err
		}
		return nil
	}

	// From now on, dag-scope=entity!
	// Since we need more of the graph load it to figure out what we have
	// This includes determining if the terminal node is UnixFS or not
	if pbn, ok := lastCidNode.(dagpb.PBNode); !ok {
		// If it's not valid dag-pb then we're done
		return nil
	} else if !pbn.FieldData().Exists() {
		// If it's not valid UnixFS then we're done
		return nil
	} else if unixfsFieldData, decodeErr := data.DecodeUnixFSData(pbn.Data.Must().Bytes()); decodeErr != nil {
		// If it's not valid dag-pb and UnixFS then we're done
		return nil
	} else {
		switch unixfsFieldData.FieldDataType().Int() {
		case data.Data_Directory, data.Data_Symlink:
			// These types are non-recursive so we're done
			return nil
		case data.Data_Raw, data.Data_Metadata:
			// TODO: for now, we decided to return nil here. The different implementations are inconsistent
			// and UnixFS is not properly specified: https://github.com/ipfs/specs/issues/316.
			// 		- Is Data_Raw different from Data_File?
			//		- Data_Metadata is handled differently in boxo/ipld/unixfs and go-unixfsnode.
			return nil
		case data.Data_HAMTShard:
			// Return all elements in the map
			_, err := lsys.KnownReifiers["unixfs-preload"](lctx, lastCidNode, lsys)
			if err != nil {
				return err
			}
			return nil
		case data.Data_File:
			nd, err := unixfsnode.Reify(lctx, lastCidNode, lsys)
			if err != nil {
				return err
			}

			fnd, ok := nd.(datamodel.LargeBytesNode)
			if !ok {
				return fmt.Errorf("could not process file since it did not present as large bytes")
			}
			f, err := fnd.AsLargeBytes()
			if err != nil {
				return err
			}

			// Get the entity range. If it's empty, assume the defaults (whole file).
			effectiveRange := entityRange
			if effectiveRange == nil {
				effectiveRange = &DagByteRange{
					From: 0,
				}
			}

			from := effectiveRange.From

			// If we're starting to read based on the end of the file, find out where that is.
			var fileLength int64
			foundFileLength := false
			if effectiveRange.From < 0 {
				fileLength, err = f.Seek(0, io.SeekEnd)
				if err != nil {
					return err
				}
				from = fileLength + effectiveRange.From
				foundFileLength = true
			}

			// If we're reading until the end of the file then do it
			if effectiveRange.To == nil {
				if _, err := f.Seek(from, io.SeekStart); err != nil {
					return err
				}
				_, err = io.Copy(io.Discard, f)
				return err
			}

			to := *effectiveRange.To
			if (*effectiveRange.To) < 0 && !foundFileLength {
				fileLength, err = f.Seek(0, io.SeekEnd)
				if err != nil {
					return err
				}
				to = fileLength + *effectiveRange.To
				foundFileLength = true
			}

			numToRead := 1 + to - from
			if numToRead < 0 {
				return fmt.Errorf("tried to read less than zero bytes")
			}

			if _, err := f.Seek(from, io.SeekStart); err != nil {
				return err
			}
			_, err = io.CopyN(io.Discard, f, numToRead)
			return err
		default:
			// Not a supported type, so we're done
			return nil
		}
	}
}
