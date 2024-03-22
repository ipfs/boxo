package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	bsfetcher "github.com/ipfs/boxo/fetcher/impl/blockservice"
	files "github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"
	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/namesys"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/path/resolver"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-unixfsnode"
	gocarv2 "github.com/ipld/go-car/v2"
	dagpb "github.com/ipld/go-codec-dagpb"
	madns "github.com/multiformats/go-multiaddr-dns"
)

// fetcher fetches files over HTTP using verifiable CAR archives.
type fetcher struct {
	gateway   string
	limit     int64
	userAgent string
}

type fetcherOption func(f *fetcher) error

// withUserAgent sets the user agent for the [Fetcher].
func withUserAgent(userAgent string) fetcherOption {
	return func(f *fetcher) error {
		f.userAgent = userAgent
		return nil
	}
}

// withLimit sets the limit for the [Fetcher].
func withLimit(limit int64) fetcherOption {
	return func(f *fetcher) error {
		f.limit = limit
		return nil
	}
}

// newFetcher creates a new [Fetcher]. Setting the gateway is mandatory.
func newFetcher(gateway string, options ...fetcherOption) (*fetcher, error) {
	if gateway == "" {
		return nil, errors.New("a gateway must be set")
	}

	f := &fetcher{
		gateway: strings.TrimRight(gateway, "/"),
	}

	for _, option := range options {
		if err := option(f); err != nil {
			return nil, err
		}
	}

	return f, nil
}

// fetch attempts to fetch the file at the given path, from the distribution
// site configured for this HttpFetcher.
func (f *fetcher) fetch(ctx context.Context, p path.Path, output string) error {
	imPath, err := f.resolvePath(ctx, p)
	if err != nil {
		return fmt.Errorf("path could not be resolved: %w", err)
	}

	rc, err := f.httpRequest(ctx, imPath, "application/vnd.ipld.car")
	if err != nil {
		return fmt.Errorf("failed to fetch CAR: %w", err)
	}

	rc, err = carToFileStream(ctx, rc, imPath)
	if err != nil {
		return fmt.Errorf("failed to read car stream: %w", err)
	}

	fd, err := os.OpenFile(output, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("failed to open output file: %w", err)
	}

	_, err = io.Copy(fd, rc)
	return err
}

func (f *fetcher) Close() error {
	return nil
}

func (f *fetcher) resolvePath(ctx context.Context, p path.Path) (path.ImmutablePath, error) {
	for p.Mutable() {
		// Download IPNS record and verify through the gateway, or resolve the
		// DNSLink with the default DNS resolver.
		name, err := ipns.NameFromString(p.Segments()[1])
		if err == nil {
			p, err = f.resolveIPNS(ctx, name)
		} else {
			p, err = f.resolveDNSLink(ctx, p)
		}

		if err != nil {
			return path.ImmutablePath{}, err
		}
	}

	return path.NewImmutablePath(p)
}

func (f *fetcher) resolveIPNS(ctx context.Context, name ipns.Name) (path.Path, error) {
	rc, err := f.httpRequest(ctx, name.AsPath(), "application/vnd.ipfs.ipns-record")
	if err != nil {
		return path.ImmutablePath{}, err
	}

	rc = newLimitReadCloser(rc, int64(ipns.MaxRecordSize))
	rawRecord, err := io.ReadAll(rc)
	if err != nil {
		return path.ImmutablePath{}, err
	}

	rec, err := ipns.UnmarshalRecord(rawRecord)
	if err != nil {
		return path.ImmutablePath{}, err
	}

	err = ipns.ValidateWithName(rec, name)
	if err != nil {
		return path.ImmutablePath{}, err
	}

	return rec.Value()
}

func (f *fetcher) resolveDNSLink(ctx context.Context, p path.Path) (path.Path, error) {
	dnsResolver := namesys.NewDNSResolver(madns.DefaultResolver.LookupTXT)
	res, err := dnsResolver.Resolve(ctx, p)
	if err != nil {
		return nil, err
	}
	return res.Path, nil
}

func (f *fetcher) httpRequest(ctx context.Context, p path.Path, accept string) (io.ReadCloser, error) {
	url := f.gateway + p.String()
	fmt.Printf("Fetching with HTTP: %q\n", url)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("http.NewRequest error: %w", err)
	}
	req.Header.Set("Accept", accept)

	if f.userAgent != "" {
		req.Header.Set("User-Agent", f.userAgent)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http.DefaultClient.Do error: %w", err)
	}

	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		mes, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading error body: %w", err)
		}
		return nil, fmt.Errorf("GET %s error: %s: %s", url, resp.Status, string(mes))
	}

	var rc io.ReadCloser
	if f.limit > 0 {
		rc = newLimitReadCloser(resp.Body, f.limit)
	} else {
		rc = resp.Body
	}

	return rc, nil
}

func carToFileStream(ctx context.Context, r io.ReadCloser, imPath path.ImmutablePath) (io.ReadCloser, error) {
	defer r.Close()

	// Create temporary block datastore and dag service.
	dataStore := dssync.MutexWrap(datastore.NewMapDatastore())
	blockStore := blockstore.NewBlockstore(dataStore)
	blockService := blockservice.New(blockStore, offline.Exchange(blockStore))
	dagService := merkledag.NewDAGService(blockService)

	defer dagService.Blocks.Close()
	defer dataStore.Close()

	// Create CAR reader
	car, err := gocarv2.NewBlockReader(r)
	if err != nil {
		fmt.Println(err)
		return nil, fmt.Errorf("error creating car reader: %s", err)
	}

	// Add all blocks to the blockstore.
	for {
		block, err := car.Next()
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("error reading block from car: %s", err)
		} else if block == nil {
			break
		}

		err = blockStore.Put(ctx, block)
		if err != nil {
			return nil, fmt.Errorf("error putting block in blockstore: %s", err)
		}
	}

	fetcherCfg := bsfetcher.NewFetcherConfig(blockService)
	fetcherCfg.PrototypeChooser = dagpb.AddSupportToChooser(bsfetcher.DefaultPrototypeChooser)
	fetcher := fetcherCfg.WithReifier(unixfsnode.Reify)
	resolver := resolver.NewBasicResolver(fetcher)

	cid, _, err := resolver.ResolveToLastNode(ctx, imPath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve: %w", err)
	}

	nd, err := dagService.Get(ctx, cid)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve: %w", err)
	}

	// Make UnixFS file out of the node.
	uf, err := unixfile.NewUnixfsFile(ctx, dagService, nd)
	if err != nil {
		return nil, fmt.Errorf("error building unixfs file: %s", err)
	}

	// Check if it's a file and return.
	if f, ok := uf.(files.File); ok {
		return f, nil
	}

	return nil, errors.New("unexpected unixfs node type")
}

type limitReadCloser struct {
	io.Reader
	io.Closer
}

// newLimitReadCloser returns a new [io.ReadCloser] with the reader wrapped in a
// [io.LimitedReader], limiting the reading to the specified amount.
func newLimitReadCloser(rc io.ReadCloser, limit int64) io.ReadCloser {
	return limitReadCloser{
		Reader: io.LimitReader(rc, limit),
		Closer: rc,
	}
}
