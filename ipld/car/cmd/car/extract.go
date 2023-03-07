package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/file"
	"github.com/ipld/go-car/v2"
	carstorage "github.com/ipld/go-car/v2/storage"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/storage"
	"github.com/urfave/cli/v2"
)

var ErrNotDir = fmt.Errorf("not a directory")

// ExtractCar pulls files and directories out of a car
func ExtractCar(c *cli.Context) error {
	outputDir, err := os.Getwd()
	if err != nil {
		return err
	}
	if c.Args().Present() {
		outputDir = c.Args().First()
	}

	var store storage.ReadableStorage
	var roots []cid.Cid

	if c.String("file") == "-" {
		var err error
		store, roots, err = NewStdinReadStorage(c.App.Reader)
		if err != nil {
			return err
		}
	} else {
		carFile, err := os.Open(c.String("file"))
		if err != nil {
			return err
		}
		store, err = carstorage.OpenReadable(carFile)
		if err != nil {
			return err
		}
		roots = store.(carstorage.ReadableCar).Roots()
	}

	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true
	ls.SetReadStorage(store)

	path, err := pathSegments(c.String("path"))
	if err != nil {
		return err
	}

	var extractedFiles int
	for _, root := range roots {
		count, err := extractRoot(c, &ls, root, outputDir, path)
		if err != nil {
			return err
		}
		extractedFiles += count
	}
	if extractedFiles == 0 {
		fmt.Fprintf(c.App.ErrWriter, "no files extracted\n")
	} else {
		fmt.Fprintf(c.App.ErrWriter, "extracted %d file(s)\n", extractedFiles)
	}

	return nil
}

func extractRoot(c *cli.Context, ls *ipld.LinkSystem, root cid.Cid, outputDir string, path []string) (int, error) {
	if root.Prefix().Codec == cid.Raw {
		if c.IsSet("verbose") {
			fmt.Fprintf(c.App.ErrWriter, "skipping raw root %s\n", root)
		}
		return 0, nil
	}

	pbn, err := ls.Load(ipld.LinkContext{}, cidlink.Link{Cid: root}, dagpb.Type.PBNode)
	if err != nil {
		return 0, err
	}
	pbnode := pbn.(dagpb.PBNode)

	ufn, err := unixfsnode.Reify(ipld.LinkContext{}, pbnode, ls)
	if err != nil {
		return 0, err
	}

	var outputResolvedDir string
	if outputDir != "-" {
		outputResolvedDir, err = filepath.EvalSymlinks(outputDir)
		if err != nil {
			return 0, err
		}
		if _, err := os.Stat(outputResolvedDir); os.IsNotExist(err) {
			if err := os.Mkdir(outputResolvedDir, 0755); err != nil {
				return 0, err
			}
		}
	}

	count, err := extractDir(c, ls, ufn, outputResolvedDir, "/", path)
	if err != nil {
		if !errors.Is(err, ErrNotDir) {
			return 0, fmt.Errorf("%s: %w", root, err)
		}

		// if it's not a directory, it's a file.
		ufsData, err := pbnode.LookupByString("Data")
		if err != nil {
			return 0, err
		}
		ufsBytes, err := ufsData.AsBytes()
		if err != nil {
			return 0, err
		}
		ufsNode, err := data.DecodeUnixFSData(ufsBytes)
		if err != nil {
			return 0, err
		}
		var outputName string
		if outputDir != "-" {
			outputName = filepath.Join(outputResolvedDir, "unknown")
		}
		if ufsNode.DataType.Int() == data.Data_File || ufsNode.DataType.Int() == data.Data_Raw {
			if err := extractFile(c, ls, pbnode, outputName); err != nil {
				return 0, err
			}
		}
		return 1, nil
	}

	return count, nil
}

func resolvePath(root, pth string) (string, error) {
	rp, err := filepath.Rel("/", pth)
	if err != nil {
		return "", fmt.Errorf("couldn't check relative-ness of %s: %w", pth, err)
	}
	joined := path.Join(root, rp)

	basename := path.Dir(joined)
	final, err := filepath.EvalSymlinks(basename)
	if err != nil {
		return "", fmt.Errorf("couldn't eval symlinks in %s: %w", basename, err)
	}
	if final != path.Clean(basename) {
		return "", fmt.Errorf("path attempts to redirect through symlinks")
	}
	return joined, nil
}

func extractDir(c *cli.Context, ls *ipld.LinkSystem, n ipld.Node, outputRoot, outputPath string, matchPath []string) (int, error) {
	if outputRoot != "" {
		dirPath, err := resolvePath(outputRoot, outputPath)
		if err != nil {
			return 0, err
		}
		// make the directory.
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			return 0, err
		}
	}

	if n.Kind() != ipld.Kind_Map {
		return 0, ErrNotDir
	}

	subPath := matchPath
	if len(matchPath) > 0 {
		subPath = matchPath[1:]
	}

	extractElement := func(name string, n ipld.Node) (int, error) {
		var nextRes string
		if outputRoot != "" {
			var err error
			nextRes, err = resolvePath(outputRoot, path.Join(outputPath, name))
			if err != nil {
				return 0, err
			}
			if c.IsSet("verbose") {
				fmt.Fprintf(c.App.Writer, "%s\n", nextRes)
			}
		}

		if n.Kind() != ipld.Kind_Link {
			return 0, fmt.Errorf("unexpected map value for %s at %s", name, outputPath)
		}
		// a directory may be represented as a map of name:<link> if unixADL is applied
		vl, err := n.AsLink()
		if err != nil {
			return 0, err
		}
		dest, err := ls.Load(ipld.LinkContext{}, vl, basicnode.Prototype.Any)
		if err != nil {
			if nf, ok := err.(interface{ NotFound() bool }); ok && nf.NotFound() {
				fmt.Fprintf(c.App.ErrWriter, "data for entry not found: %s (skipping...)\n", name)
				return 0, nil
			}
			return 0, err
		}
		// degenerate files are handled here.
		if dest.Kind() == ipld.Kind_Bytes {
			if err := extractFile(c, ls, dest, nextRes); err != nil {
				return 0, err
			}
			return 1, nil
		}

		// dir / pbnode
		pbb := dagpb.Type.PBNode.NewBuilder()
		if err := pbb.AssignNode(dest); err != nil {
			return 0, err
		}
		pbnode := pbb.Build().(dagpb.PBNode)

		// interpret dagpb 'data' as unixfs data and look at type.
		ufsData, err := pbnode.LookupByString("Data")
		if err != nil {
			return 0, err
		}
		ufsBytes, err := ufsData.AsBytes()
		if err != nil {
			return 0, err
		}
		ufsNode, err := data.DecodeUnixFSData(ufsBytes)
		if err != nil {
			return 0, err
		}

		switch ufsNode.DataType.Int() {
		case data.Data_Directory, data.Data_HAMTShard:
			ufn, err := unixfsnode.Reify(ipld.LinkContext{}, pbnode, ls)
			if err != nil {
				return 0, err
			}
			return extractDir(c, ls, ufn, outputRoot, path.Join(outputPath, name), subPath)
		case data.Data_File, data.Data_Raw:
			if err := extractFile(c, ls, pbnode, nextRes); err != nil {
				return 0, err
			}
			return 1, nil
		case data.Data_Symlink:
			if nextRes == "" {
				return 0, fmt.Errorf("cannot extract a symlink to stdout")
			}
			data := ufsNode.Data.Must().Bytes()
			if err := os.Symlink(string(data), nextRes); err != nil {
				return 0, err
			}
			return 1, nil
		default:
			return 0, fmt.Errorf("unknown unixfs type: %d", ufsNode.DataType.Int())
		}
	}

	// specific path segment
	if len(matchPath) > 0 {
		val, err := n.LookupByString(matchPath[0])
		if err != nil {
			return 0, err
		}
		return extractElement(matchPath[0], val)
	}

	if outputPath == "-" && len(matchPath) == 0 {
		return 0, fmt.Errorf("cannot extract a directory to stdout, use a path to extract a specific file")
	}

	// everything
	var count int
	mi := n.MapIterator()
	for !mi.Done() {
		key, val, err := mi.Next()
		if err != nil {
			return 0, err
		}
		ks, err := key.AsString()
		if err != nil {
			return 0, err
		}
		ecount, err := extractElement(ks, val)
		if err != nil {
			return 0, err
		}
		count += ecount
	}

	return count, nil
}

func extractFile(c *cli.Context, ls *ipld.LinkSystem, n ipld.Node, outputName string) error {
	node, err := file.NewUnixFSFile(c.Context, n, ls)
	if err != nil {
		return err
	}
	nlr, err := node.AsLargeBytes()
	if err != nil {
		return err
	}
	var f *os.File
	if outputName == "" {
		f = os.Stdout
	} else {
		f, err = os.Create(outputName)
		if err != nil {
			return err
		}
		defer f.Close()
	}
	_, err = io.Copy(f, nlr)
	return err
}

// TODO: dedupe this with lassie, probably into go-unixfsnode
func pathSegments(path string) ([]string, error) {
	segments := strings.Split(path, "/")
	filtered := make([]string, 0, len(segments))
	for i := 0; i < len(segments); i++ {
		if segments[i] == "" {
			// Allow one leading and one trailing '/' at most
			if i == 0 || i == len(segments)-1 {
				continue
			}
			return nil, fmt.Errorf("invalid empty path segment at position %d", i)
		}
		if segments[i] == "." || segments[i] == ".." {
			return nil, fmt.Errorf("'%s' is unsupported in paths", segments[i])
		}
		filtered = append(filtered, segments[i])
	}
	return filtered, nil
}

var _ storage.ReadableStorage = (*stdinReadStorage)(nil)

type stdinReadStorage struct {
	blocks map[string][]byte
	done   bool
	lk     *sync.RWMutex
	cond   *sync.Cond
}

func NewStdinReadStorage(reader io.Reader) (*stdinReadStorage, []cid.Cid, error) {
	var lk sync.RWMutex
	srs := &stdinReadStorage{
		blocks: make(map[string][]byte),
		lk:     &lk,
		cond:   sync.NewCond(&lk),
	}
	rdr, err := car.NewBlockReader(reader)
	if err != nil {
		return nil, nil, err
	}
	go func() {
		for {
			blk, err := rdr.Next()
			if err == io.EOF {
				srs.lk.Lock()
				srs.done = true
				srs.lk.Unlock()
				return
			}
			if err != nil {
				panic(err)
			}
			srs.lk.Lock()
			srs.blocks[string(blk.Cid().Hash())] = blk.RawData()
			srs.cond.Broadcast()
			srs.lk.Unlock()
		}
	}()
	return srs, rdr.Roots, nil
}

func (srs *stdinReadStorage) Has(ctx context.Context, key string) (bool, error) {
	_, err := srs.Get(ctx, key)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (srs *stdinReadStorage) Get(ctx context.Context, key string) ([]byte, error) {
	c, err := cid.Cast([]byte(key))
	if err != nil {
		return nil, err
	}
	srs.lk.Lock()
	defer srs.lk.Unlock()
	for {
		if data, ok := srs.blocks[string(c.Hash())]; ok {
			return data, nil
		}
		if srs.done {
			return nil, carstorage.ErrNotFound{Cid: c}
		}
		srs.cond.Wait()
	}
}
