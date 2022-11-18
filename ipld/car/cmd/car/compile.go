package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode/utf8"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	carv1 "github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/polydawn/refmt/json"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slices"
)

var (
	plusLineRegex = regexp.MustCompile(`^\+\+\+ ([\w-]+) ([\S]+ )?([\w]+)$`)
)

// Compile is a command to translate between a human-debuggable patch-like format and a car file.
func CompileCar(c *cli.Context) error {
	var err error
	inStream := os.Stdin
	if c.Args().Len() >= 1 {
		inStream, err = os.Open(c.Args().First())
		if err != nil {
			return err
		}
	}

	//parse headers.
	br := bufio.NewReader(inStream)
	header, _, err := br.ReadLine()
	if err != nil {
		return err
	}

	v2 := strings.HasPrefix(string(header), "car compile --v2 ")
	rest := strings.TrimPrefix(string(header), "car compile ")
	if v2 {
		rest = strings.TrimPrefix(rest, "--v2 ")
	}
	carName := strings.TrimSpace(rest)

	roots := make([]cid.Cid, 0)
	for {
		peek, err := br.Peek(4)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if bytes.Equal(peek, []byte("--- ")) {
			break
		}
		rootLine, _, err := br.ReadLine()
		if err != nil {
			return err
		}
		if strings.HasPrefix(string(rootLine), "root ") {
			var rCidS string
			fmt.Sscanf(string(rootLine), "root %s", &rCidS)
			rCid, err := cid.Parse(rCidS)
			if err != nil {
				return err
			}
			roots = append(roots, rCid)
		}
	}

	//parse blocks.
	cidList := make([]cid.Cid, 0)
	rawBlocks := make(map[cid.Cid][]byte)
	rawCodecs := make(map[cid.Cid]string)

	for {
		nextCid, mode, nextBlk, err := parsePatch(br)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		rawBlocks[nextCid] = nextBlk
		rawCodecs[nextCid] = mode
		cidList = append(cidList, nextCid)
	}

	// Re-create the original IPLD encoded blocks, but allowing for modifications of the
	// patch data which may generate new CIDs; so we track the DAG relationships and
	// rewrite CIDs in other referring where they get updated.

	// structure as a tree
	childMap := make(map[cid.Cid][]cid.Cid)
	for c := range rawBlocks {
		if _, ok := childMap[c]; !ok {
			childMap[c] = make([]cid.Cid, 0)
		}
		for d, blk := range rawBlocks {
			if c.Equals(d) {
				continue
			}
			if strings.Contains(string(blk), c.String()) {
				if _, ok := childMap[d]; !ok {
					childMap[d] = make([]cid.Cid, 0)
				}
				childMap[d] = append(childMap[d], c)
			} else if strings.Contains(string(blk), string(c.Bytes())) {
				if _, ok := childMap[d]; !ok {
					childMap[d] = make([]cid.Cid, 0)
				}
				childMap[d] = append(childMap[d], c)
			}
		}
	}

	// re-parse/re-build CIDs
	outBlocks := make(map[cid.Cid][]byte)
	for len(childMap) > 0 {
		for origCid, kids := range childMap {
			if len(kids) == 0 {
				// compile to final cid
				blk := rawBlocks[origCid]
				finalCid, finalBlk, err := serializeBlock(c.Context, origCid.Prefix(), rawCodecs[origCid], blk)
				if err != nil {
					return err
				}
				outBlocks[finalCid] = finalBlk
				idx := slices.Index(cidList, origCid)
				cidList[idx] = finalCid

				// update other remaining nodes of the new cid.
				for otherCid, otherKids := range childMap {
					for i, otherKid := range otherKids {
						if otherKid.Equals(origCid) {
							if !finalCid.Equals(origCid) {
								// update block
								rawBlocks[otherCid] = bytes.ReplaceAll(rawBlocks[otherCid], origCid.Bytes(), finalCid.Bytes())
								rawBlocks[otherCid] = bytes.ReplaceAll(rawBlocks[otherCid], []byte(origCid.String()), []byte(finalCid.String()))
							}
							// remove from childMap
							nok := append(otherKids[0:i], otherKids[i+1:]...)
							childMap[otherCid] = nok
							break // to next child map entry.
						}
					}
				}

				delete(childMap, origCid)
			}
		}
	}

	if !v2 {
		// write output
		outStream := os.Stdout
		if c.IsSet("output") {
			outFileName := c.String("output")
			if outFileName == "" {
				outFileName = carName
			}
			outFile, err := os.Create(outFileName)
			if err != nil {
				return err
			}
			defer outFile.Close()
			outStream = outFile
		}

		if err := carv1.WriteHeader(&carv1.CarHeader{
			Roots:   roots,
			Version: 1,
		}, outStream); err != nil {
			return err
		}
		for c, blk := range outBlocks {
			if err := util.LdWrite(outStream, c.Bytes(), blk); err != nil {
				return err
			}
		}
	} else {
		outFileName := c.String("output")
		if outFileName == "" {
			outFileName = carName
		}

		if outFileName == "-" && !c.IsSet("output") {
			return fmt.Errorf("cannot stream carv2's to stdout")
		}
		bs, err := blockstore.OpenReadWrite(outFileName, roots)
		if err != nil {
			return err
		}
		for _, bc := range cidList {
			blk := outBlocks[bc]
			ob, _ := blocks.NewBlockWithCid(blk, bc)
			bs.Put(c.Context, ob)
		}
		return bs.Finalize()
	}

	return nil
}

func serializeBlock(ctx context.Context, codec cid.Prefix, encoding string, raw []byte) (cid.Cid, []byte, error) {
	ls := cidlink.DefaultLinkSystem()
	store := memstore.Store{Bag: map[string][]byte{}}
	ls.SetReadStorage(&store)
	ls.SetWriteStorage(&store)
	b := basicnode.Prototype.Any.NewBuilder()
	if encoding == "dag-json" {
		if err := dagjson.Decode(b, bytes.NewBuffer(raw)); err != nil {
			return cid.Undef, nil, err
		}
	} else if encoding == "raw" {
		if err := b.AssignBytes(raw); err != nil {
			return cid.Undef, nil, err
		}
	} else {
		return cid.Undef, nil, fmt.Errorf("unknown encoding: %s", encoding)
	}
	lnk, err := ls.Store(linking.LinkContext{Ctx: ctx}, cidlink.LinkPrototype{Prefix: codec}, b.Build())
	if err != nil {
		return cid.Undef, nil, err
	}
	outCid := lnk.(cidlink.Link).Cid
	outBytes, outErr := store.Get(ctx, outCid.KeyString())
	return outCid, outBytes, outErr
}

// DebugCar is a command to translate between a car file, and a human-debuggable patch-like format.
func DebugCar(c *cli.Context) error {
	var err error
	inStream := os.Stdin
	inFile := "-"
	if c.Args().Len() >= 1 {
		inFile = c.Args().First()
		inStream, err = os.Open(inFile)
		if err != nil {
			return err
		}
	}

	rd, err := carv2.NewBlockReader(inStream)
	if err != nil {
		return err
	}

	// patch the header.
	outStream := os.Stdout
	if c.IsSet("output") {
		outFileName := c.String("output")
		outFile, err := os.Create(outFileName)
		if err != nil {
			return err
		}
		defer outFile.Close()
		outStream = outFile
	}

	outStream.WriteString("car compile ")
	if rd.Version == 2 {
		outStream.WriteString("--v2 ")
	}

	outStream.WriteString(inFile + "\n")
	for _, rt := range rd.Roots {
		fmt.Fprintf(outStream, "root %s\n", rt.String())
	}

	// patch each block.
	nxt, err := rd.Next()
	if err != nil {
		return err
	}
	for nxt != nil {
		chunk, err := patch(c.Context, nxt.Cid(), nxt.RawData())
		if err != nil {
			return err
		}
		outStream.Write(chunk)

		nxt, err = rd.Next()
		if err == io.EOF {
			return nil
		}
	}

	return nil
}

func patch(ctx context.Context, c cid.Cid, blk []byte) ([]byte, error) {
	ls := cidlink.DefaultLinkSystem()
	store := memstore.Store{Bag: map[string][]byte{}}
	ls.SetReadStorage(&store)
	ls.SetWriteStorage(&store)
	store.Put(ctx, c.KeyString(), blk)
	node, err := ls.Load(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c}, basicnode.Prototype.Any)
	if err != nil {
		return nil, fmt.Errorf("could not load block: %q", err)
	}

	outMode := "dag-json"
	if node.Kind() == datamodel.Kind_Bytes && isPrintable(node) {
		outMode = "raw"
	}
	finalBuf := bytes.NewBuffer(nil)

	if outMode == "dag-json" {
		opts := dagjson.EncodeOptions{
			EncodeLinks: true,
			EncodeBytes: true,
			MapSortMode: codec.MapSortMode_Lexical,
		}
		if err := dagjson.Marshal(node, json.NewEncoder(finalBuf, json.EncodeOptions{Line: []byte{'\n'}, Indent: []byte{'\t'}}), opts); err != nil {
			return nil, err
		}
	} else if outMode == "raw" {
		nb, err := node.AsBytes()
		if err != nil {
			return nil, err
		}
		finalBuf.Write(nb)
	}

	// figure out number of lines.
	lcnt := strings.Count(finalBuf.String(), "\n")
	crStr := " (no-end-cr)"
	if finalBuf.Bytes()[len(finalBuf.Bytes())-1] == '\n' {
		crStr = ""
	}

	outBuf := bytes.NewBuffer(nil)
	outBuf.WriteString("--- " + c.String() + "\n")
	outBuf.WriteString("+++ " + outMode + crStr + " " + c.String() + "\n")
	outBuf.WriteString(fmt.Sprintf("@@ -%d,%d +%d,%d @@\n", 0, lcnt, 0, lcnt))
	outBuf.Write(finalBuf.Bytes())
	outBuf.WriteString("\n")
	return outBuf.Bytes(), nil
}

func isPrintable(n ipld.Node) bool {
	b, err := n.AsBytes()
	if err != nil {
		return false
	}
	if !utf8.Valid(b) {
		return false
	}
	if bytes.ContainsAny(b, string([]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x10, 0x11, 0x12, 0x13, 0x14, 0x16, 0x17, 0x18, 0x19, 0x1c, 0x1d, 0x1e, 0x1f})) {
		return false
	}
	// check if would confuse the 'end of patch' checker.
	if bytes.Contains(b, []byte("\n--- ")) {
		return false
	}
	return true
}

func parsePatch(br *bufio.Reader) (cid.Cid, string, []byte, error) {
	// read initial line to parse CID.
	l1, isPrefix, err := br.ReadLine()
	if err != nil {
		return cid.Undef, "", nil, err
	}
	if isPrefix {
		return cid.Undef, "", nil, fmt.Errorf("unexpected long header l1")
	}
	var cs string
	if _, err := fmt.Sscanf(string(l1), "--- %s", &cs); err != nil {
		return cid.Undef, "", nil, fmt.Errorf("could not parse patch cid line (%s): %q", l1, err)
	}
	l2, isPrefix, err := br.ReadLine()
	if err != nil {
		return cid.Undef, "", nil, err
	}
	if isPrefix {
		return cid.Undef, "", nil, fmt.Errorf("unexpected long header l2")
	}
	var mode string
	var noEndReturn bool
	matches := plusLineRegex.FindSubmatch(l2)
	if len(matches) >= 2 {
		mode = string(matches[1])
	}
	if len(matches) < 2 || string(matches[len(matches)-1]) != cs {
		return cid.Undef, "", nil, fmt.Errorf("mismatched cid lines: %v", string(l2))
	}
	if len(matches[2]) > 0 {
		noEndReturn = (string(matches[2]) == "(no-end-cr) ")
	}
	c, err := cid.Parse(cs)
	if err != nil {
		return cid.Undef, "", nil, err
	}

	// skip over @@ line.
	l3, isPrefix, err := br.ReadLine()
	if err != nil {
		return cid.Undef, "", nil, err
	}
	if isPrefix {
		return cid.Undef, "", nil, fmt.Errorf("unexpected long header l3")
	}
	if !strings.HasPrefix(string(l3), "@@") {
		return cid.Undef, "", nil, fmt.Errorf("unexpected missing chunk prefix")
	}

	// keep going until next chunk or end.
	outBuf := bytes.NewBuffer(nil)
	for {
		peek, err := br.Peek(4)
		if err != nil && err != io.EOF {
			return cid.Undef, "", nil, err
		}
		if bytes.Equal(peek, []byte("--- ")) {
			break
		}
		// accumulate to buffer.
		l, err := br.ReadBytes('\n')
		if l != nil {
			outBuf.Write(l)
		}
		if err == io.EOF {
			break
		} else if err != nil {
			return cid.Undef, "", nil, err
		}
	}

	ob := outBuf.Bytes()

	// remove the final line return
	if len(ob) > 2 && bytes.Equal(ob[len(ob)-2:], []byte("\r\n")) {
		ob = ob[:len(ob)-2]
	} else if len(ob) > 1 && bytes.Equal(ob[len(ob)-1:], []byte("\n")) {
		ob = ob[:len(ob)-1]
	}

	if noEndReturn && len(ob) > 2 && bytes.Equal(ob[len(ob)-2:], []byte("\r\n")) {
		ob = ob[:len(ob)-2]
	} else if noEndReturn && len(ob) > 1 && bytes.Equal(ob[len(ob)-1:], []byte("\n")) {
		ob = ob[:len(ob)-1]
	}

	return c, mode, ob, nil
}
