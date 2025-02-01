package gateway

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"
	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	carbs "github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/directory-with-multilayer-hamt-and-multiblock-files.car
var dirWithMultiblockHAMTandFiles []byte

func TestCarBackendTar(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requestNum := 0
	s := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNum++
		switch requestNum {
		case 1:
			// Expect the full request, but return one that terminates in the middle of the HAMT
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
				"bafybeifdv255wmsrh75vcsrtkcwyktvewgihegeeyhhj2ju4lzt4lqfoze", // basicDir
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // exampleA
				"bafkreie5noke3mb7hqxukzcy73nl23k6lxszxi5w3dtmuwz62wnvkpsscm",
				"bafkreih4ephajybraj6wnxsbwjwa77fukurtpl7oj7t7pfq545duhot7cq",
				"bafkreigu7buvm3cfunb35766dn7tmqyh2um62zcio63en2btvxuybgcpue",
				"bafkreicll3huefkc3qnrzeony7zcfo7cr3nbx64hnxrqzsixpceg332fhe",
				"bafkreifst3pqztuvj57lycamoi7z34b4emf7gawxs74nwrc2c7jncmpaqm",
				"bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", // exampleB
				"bafkreihgbi345degbcyxaf5b3boiyiaxhnuxdysvqmbdyaop2swmhh3s3m",
				"bafkreiaugmh5gal5rgiems6gslcdt2ixfncahintrmcqvrgxqamwtlrmz4",
				"bafkreiaxwwb7der2qvmteymgtlj7ww7w5vc44phdxfnexog3vnuwdkxuea",
				"bafkreic5zyan5rk4ccfum4d4mu3h5eqsllbudlj4texlzj6xdgxvldzngi",
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamtDir
			}); err != nil {
				panic(err)
			}
		case 2:
			// Expect a request for the HAMT only and give it
			// Note: this is an implementation detail, it could be in the future that we request less or more data
			// (e.g. requesting the blocks to fill out the HAMT, or with spec changes asking for HAMT ranges, or asking for the HAMT and its children)
			expectedUri := "/ipfs/bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamtDir
				"bafybeiccgo7euew77gkqkhezn3pozfrciiibqz2u3spdqmgjvd5wqskipm",
				"bafybeihjydob4eq5j4m43whjgf5cgftthc42kjno3g24sa3wcw7vonbmfy",
			}); err != nil {
				panic(err)
			}
		case 3:
			// Starting here expect requests for each file in the directory
			expectedUri := "/ipfs/bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", // exampleB
				"bafkreihgbi345degbcyxaf5b3boiyiaxhnuxdysvqmbdyaop2swmhh3s3m",
				"bafkreiaugmh5gal5rgiems6gslcdt2ixfncahintrmcqvrgxqamwtlrmz4",
				"bafkreiaxwwb7der2qvmteymgtlj7ww7w5vc44phdxfnexog3vnuwdkxuea",
				"bafkreic5zyan5rk4ccfum4d4mu3h5eqsllbudlj4texlzj6xdgxvldzngi",
			}); err != nil {
				panic(err)
			}
		case 4:
			// Expect a request for one of the directory items and give it
			expectedUri := "/ipfs/bafkreih2grj7p2bo5yk2guqazxfjzapv6hpm3mwrinv6s3cyayd72ke5he"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafkreih2grj7p2bo5yk2guqazxfjzapv6hpm3mwrinv6s3cyayd72ke5he", // exampleD
			}); err != nil {
				panic(err)
			}
		case 5:
			// Expect a request for one of the directory items and give it
			expectedUri := "/ipfs/bafkreidqhbqn5htm5qejxpb3hps7dookudo3nncfn6al6niqibi5lq6fee"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafkreidqhbqn5htm5qejxpb3hps7dookudo3nncfn6al6niqibi5lq6fee", // exampleC
			}); err != nil {
				panic(err)
			}
		case 6:
			// Expect a request for one of the directory items and give part of it
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // exampleA
				"bafkreie5noke3mb7hqxukzcy73nl23k6lxszxi5w3dtmuwz62wnvkpsscm",
				"bafkreih4ephajybraj6wnxsbwjwa77fukurtpl7oj7t7pfq545duhot7cq",
			}); err != nil {
				panic(err)
			}
		case 7:
			// Expect a partial request for one of the directory items and give it
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // exampleA
				"bafkreigu7buvm3cfunb35766dn7tmqyh2um62zcio63en2btvxuybgcpue",
				"bafkreicll3huefkc3qnrzeony7zcfo7cr3nbx64hnxrqzsixpceg332fhe",
				"bafkreifst3pqztuvj57lycamoi7z34b4emf7gawxs74nwrc2c7jncmpaqm",
			}); err != nil {
				panic(err)
			}
		default:
			t.Fatal("unsupported request number")
		}
	}))
	defer s.Close()

	bs, err := NewRemoteCarFetcher([]string{s.URL}, nil)
	require.NoError(t, err)

	fetcher, err := NewRetryCarFetcher(bs, 3)
	require.NoError(t, err)

	backend, err := NewCarBackend(fetcher)
	require.NoError(t, err)

	p := path.FromCid(cid.MustParse("bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi"))
	_, nd, err := backend.GetAll(ctx, p)
	require.NoError(t, err)

	assertNextEntryNameEquals := func(t *testing.T, dirIter files.DirIterator, expectedName string) {
		t.Helper()
		require.True(t, dirIter.Next(), dirIter.Err())
		require.Equal(t, expectedName, dirIter.Name())
	}

	robs, err := carbs.NewReadOnly(bytes.NewReader(dirWithMultiblockHAMTandFiles), nil)
	require.NoError(t, err)

	dsrv := merkledag.NewDAGService(blockservice.New(robs, offline.Exchange(robs)))
	assertFileEqual := func(t *testing.T, expectedCidString string, receivedFile files.File) {
		t.Helper()

		expected := cid.MustParse(expectedCidString)
		receivedFileData, err := io.ReadAll(receivedFile)
		require.NoError(t, err)
		nd, err := dsrv.Get(ctx, expected)
		require.NoError(t, err)
		expectedFile, err := unixfile.NewUnixfsFile(ctx, dsrv, nd)
		require.NoError(t, err)

		expectedFileData, err := io.ReadAll(expectedFile.(files.File))
		require.NoError(t, err)
		require.True(t, bytes.Equal(expectedFileData, receivedFileData))
	}

	rootDirIter := nd.(files.Directory).Entries()
	assertNextEntryNameEquals(t, rootDirIter, "basicDir")

	basicDirIter := rootDirIter.Node().(files.Directory).Entries()
	assertNextEntryNameEquals(t, basicDirIter, "exampleA")
	assertFileEqual(t, "bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", basicDirIter.Node().(files.File))

	assertNextEntryNameEquals(t, basicDirIter, "exampleB")
	assertFileEqual(t, "bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", basicDirIter.Node().(files.File))

	assertNextEntryNameEquals(t, rootDirIter, "hamtDir")
	hamtDirIter := rootDirIter.Node().(files.Directory).Entries()

	assertNextEntryNameEquals(t, hamtDirIter, "exampleB")
	assertFileEqual(t, "bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", hamtDirIter.Node().(files.File))

	assertNextEntryNameEquals(t, hamtDirIter, "exampleD-hamt-collide-exampleB-seed-364")
	assertFileEqual(t, "bafkreih2grj7p2bo5yk2guqazxfjzapv6hpm3mwrinv6s3cyayd72ke5he", hamtDirIter.Node().(files.File))

	assertNextEntryNameEquals(t, hamtDirIter, "exampleC-hamt-collide-exampleA-seed-52")
	assertFileEqual(t, "bafkreidqhbqn5htm5qejxpb3hps7dookudo3nncfn6al6niqibi5lq6fee", hamtDirIter.Node().(files.File))

	assertNextEntryNameEquals(t, hamtDirIter, "exampleA")
	assertFileEqual(t, "bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", hamtDirIter.Node().(files.File))

	require.False(t, rootDirIter.Next() || basicDirIter.Next() || hamtDirIter.Next())
}

func TestCarBackendTarAtEndOfPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requestNum := 0
	s := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNum++
		switch requestNum {
		case 1:
			// Expect the full request, but return one that terminates in the middle of the path
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi/hamtDir"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
			}); err != nil {
				panic(err)
			}
		case 2:
			// Expect the full request and give the path and the children from one of the HAMT nodes but not the other
			// Note: this is an implementation detail, it could be in the future that we request less or more data
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi/hamtDir"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamtDir
				"bafybeiccgo7euew77gkqkhezn3pozfrciiibqz2u3spdqmgjvd5wqskipm",
				"bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", // exampleB
				"bafkreihgbi345degbcyxaf5b3boiyiaxhnuxdysvqmbdyaop2swmhh3s3m",
				"bafkreiaugmh5gal5rgiems6gslcdt2ixfncahintrmcqvrgxqamwtlrmz4",
				"bafkreiaxwwb7der2qvmteymgtlj7ww7w5vc44phdxfnexog3vnuwdkxuea",
				"bafkreic5zyan5rk4ccfum4d4mu3h5eqsllbudlj4texlzj6xdgxvldzngi",
				"bafkreih2grj7p2bo5yk2guqazxfjzapv6hpm3mwrinv6s3cyayd72ke5he", // exampleD
			}); err != nil {
				panic(err)
			}
		case 3:
			// Expect a request for the HAMT only and give it
			// Note: this is an implementation detail, it could be in the future that we request less or more data
			// (e.g. requesting the blocks to fill out the HAMT, or with spec changes asking for HAMT ranges, or asking for the HAMT and its children)
			expectedUri := "/ipfs/bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamtDir
				"bafybeiccgo7euew77gkqkhezn3pozfrciiibqz2u3spdqmgjvd5wqskipm",
				"bafybeihjydob4eq5j4m43whjgf5cgftthc42kjno3g24sa3wcw7vonbmfy",
			}); err != nil {
				panic(err)
			}
		case 4:
			// Expect a request for one of the directory items and give it
			expectedUri := "/ipfs/bafkreidqhbqn5htm5qejxpb3hps7dookudo3nncfn6al6niqibi5lq6fee"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafkreidqhbqn5htm5qejxpb3hps7dookudo3nncfn6al6niqibi5lq6fee", // exampleC
			}); err != nil {
				panic(err)
			}
		case 5:
			// Expect a request for the multiblock file in the directory and give some of it
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // exampleA
				"bafkreie5noke3mb7hqxukzcy73nl23k6lxszxi5w3dtmuwz62wnvkpsscm",
				"bafkreih4ephajybraj6wnxsbwjwa77fukurtpl7oj7t7pfq545duhot7cq",
				"bafkreigu7buvm3cfunb35766dn7tmqyh2um62zcio63en2btvxuybgcpue",
			}); err != nil {
				panic(err)
			}
		case 6:
			// Expect a request for the rest of the multiblock file in the directory and give it
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa?format=car&dag-scope=entity&entity-bytes=768:*"
			if request.RequestURI != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // exampleA
				"bafkreicll3huefkc3qnrzeony7zcfo7cr3nbx64hnxrqzsixpceg332fhe",
				"bafkreifst3pqztuvj57lycamoi7z34b4emf7gawxs74nwrc2c7jncmpaqm",
			}); err != nil {
				panic(err)
			}
		default:
			t.Fatal("unsupported request number")
		}
	}))
	defer s.Close()

	bs, err := NewRemoteCarFetcher([]string{s.URL}, nil)
	require.NoError(t, err)
	fetcher, err := NewRetryCarFetcher(bs, 3)
	require.NoError(t, err)

	backend, err := NewCarBackend(fetcher)
	require.NoError(t, err)

	p, err := path.Join(path.FromCid(cid.MustParse("bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi")), "hamtDir")
	require.NoError(t, err)

	imPath, err := path.NewImmutablePath(p)
	require.NoError(t, err)

	_, nd, err := backend.GetAll(ctx, imPath)
	require.NoError(t, err)

	assertNextEntryNameEquals := func(t *testing.T, dirIter files.DirIterator, expectedName string) {
		t.Helper()
		require.True(t, dirIter.Next())
		require.Equal(t, expectedName, dirIter.Name())
	}

	robs, err := carbs.NewReadOnly(bytes.NewReader(dirWithMultiblockHAMTandFiles), nil)
	require.NoError(t, err)

	dsrv := merkledag.NewDAGService(blockservice.New(robs, offline.Exchange(robs)))
	assertFileEqual := func(t *testing.T, expectedCidString string, receivedFile files.File) {
		t.Helper()

		expected := cid.MustParse(expectedCidString)
		receivedFileData, err := io.ReadAll(receivedFile)
		require.NoError(t, err)
		nd, err := dsrv.Get(ctx, expected)
		require.NoError(t, err)
		expectedFile, err := unixfile.NewUnixfsFile(ctx, dsrv, nd)
		require.NoError(t, err)

		expectedFileData, err := io.ReadAll(expectedFile.(files.File))
		require.NoError(t, err)
		require.True(t, bytes.Equal(expectedFileData, receivedFileData))
	}

	hamtDirIter := nd.(files.Directory).Entries()

	assertNextEntryNameEquals(t, hamtDirIter, "exampleB")
	assertFileEqual(t, "bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", hamtDirIter.Node().(files.File))

	assertNextEntryNameEquals(t, hamtDirIter, "exampleD-hamt-collide-exampleB-seed-364")
	assertFileEqual(t, "bafkreih2grj7p2bo5yk2guqazxfjzapv6hpm3mwrinv6s3cyayd72ke5he", hamtDirIter.Node().(files.File))

	assertNextEntryNameEquals(t, hamtDirIter, "exampleC-hamt-collide-exampleA-seed-52")
	assertFileEqual(t, "bafkreidqhbqn5htm5qejxpb3hps7dookudo3nncfn6al6niqibi5lq6fee", hamtDirIter.Node().(files.File))

	assertNextEntryNameEquals(t, hamtDirIter, "exampleA")
	assertFileEqual(t, "bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", hamtDirIter.Node().(files.File))

	require.False(t, hamtDirIter.Next())
}

func sendBlocks(ctx context.Context, carFixture []byte, writer io.Writer, cidStrList []string) error {
	rd, err := storage.OpenReadable(bytes.NewReader(carFixture))
	if err != nil {
		return err
	}

	cw, err := storage.NewWritable(writer, []cid.Cid{cid.MustParse("bafkqaaa")}, carv2.WriteAsCarV1(true), carv2.AllowDuplicatePuts(true))
	if err != nil {
		return err
	}

	for _, s := range cidStrList {
		c := cid.MustParse(s)
		blockData, err := rd.Get(ctx, c.KeyString())
		if err != nil {
			return err
		}

		if err := cw.Put(ctx, c.KeyString(), blockData); err != nil {
			return err
		}
	}
	return nil
}

func TestCarBackendGetFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requestNum := 0
	s := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNum++
		switch requestNum {
		case 1:
			// Expect the full request, but return one that terminates in the middle of the path
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi/hamtDir/exampleA"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
			}); err != nil {
				panic(err)
			}
		case 2:
			// Expect the full request, but return one that terminates in the middle of the file
			// Note: this is an implementation detail, it could be in the future that we request less data (e.g. partial path)
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi/hamtDir/exampleA"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamt root
			}); err != nil {
				panic(err)
			}

		case 3:
			// Expect the full request and return the path and most of the file
			// Note: this is an implementation detail, it could be in the future that we request less data (e.g. partial path and file range)
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi/hamtDir/exampleA"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamt root
				"bafybeihjydob4eq5j4m43whjgf5cgftthc42kjno3g24sa3wcw7vonbmfy", // inner hamt
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // file root
				"bafkreie5noke3mb7hqxukzcy73nl23k6lxszxi5w3dtmuwz62wnvkpsscm", // file chunks start here
				"bafkreih4ephajybraj6wnxsbwjwa77fukurtpl7oj7t7pfq545duhot7cq",
			}); err != nil {
				panic(err)
			}

		case 4:
			// Expect a request for the remainder of the file
			// Note: this is an implementation detail, it could be that the requester really asks for more information
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // file root
				"bafkreigu7buvm3cfunb35766dn7tmqyh2um62zcio63en2btvxuybgcpue", // middle of the file starts here
				"bafkreicll3huefkc3qnrzeony7zcfo7cr3nbx64hnxrqzsixpceg332fhe",
				"bafkreifst3pqztuvj57lycamoi7z34b4emf7gawxs74nwrc2c7jncmpaqm",
			}); err != nil {
				panic(err)
			}

		default:
			t.Fatal("unsupported request number")
		}
	}))
	defer s.Close()

	bs, err := NewRemoteCarFetcher([]string{s.URL}, nil)
	require.NoError(t, err)
	fetcher, err := NewRetryCarFetcher(bs, 3)
	require.NoError(t, err)

	backend, err := NewCarBackend(fetcher)
	require.NoError(t, err)

	trustedGatewayServer := httptest.NewServer(NewHandler(Config{DeserializedResponses: true}, backend))
	defer trustedGatewayServer.Close()

	resp, err := http.Get(trustedGatewayServer.URL + "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi/hamtDir/exampleA")
	require.NoError(t, err)

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	robs, err := carbs.NewReadOnly(bytes.NewReader(dirWithMultiblockHAMTandFiles), nil)
	require.NoError(t, err)

	dsrv := merkledag.NewDAGService(blockservice.New(robs, offline.Exchange(robs)))
	fileRootNd, err := dsrv.Get(ctx, cid.MustParse("bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"))
	require.NoError(t, err)
	uio, err := unixfile.NewUnixfsFile(ctx, dsrv, fileRootNd)
	require.NoError(t, err)
	f := uio.(files.File)
	expectedFileData, err := io.ReadAll(f)
	require.NoError(t, err)
	require.True(t, bytes.Equal(data, expectedFileData))
}

func TestCarBackendGetFileRangeRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requestNum := 0
	s := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNum++
		switch requestNum {
		case 1:
			// Expect the full request, but return one that terminates at the root block
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // file root
			}); err != nil {
				panic(err)
			}
		case 2:
			// Expect the full request, and return the whole file which should be invalid
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // file root
				"bafkreie5noke3mb7hqxukzcy73nl23k6lxszxi5w3dtmuwz62wnvkpsscm", // file chunks start here
				"bafkreih4ephajybraj6wnxsbwjwa77fukurtpl7oj7t7pfq545duhot7cq",
				"bafkreigu7buvm3cfunb35766dn7tmqyh2um62zcio63en2btvxuybgcpue",
				"bafkreicll3huefkc3qnrzeony7zcfo7cr3nbx64hnxrqzsixpceg332fhe",
				"bafkreifst3pqztuvj57lycamoi7z34b4emf7gawxs74nwrc2c7jncmpaqm",
			}); err != nil {
				panic(err)
			}
		case 3:
			// Expect the full request and return the first block
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // file root
				"bafkreih4ephajybraj6wnxsbwjwa77fukurtpl7oj7t7pfq545duhot7cq",
			}); err != nil {
				panic(err)
			}

		case 4:
			// Expect a request for the remainder of the file
			// Note: this is an implementation detail, it could be that the requester really asks for more information
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // file root
				"bafkreigu7buvm3cfunb35766dn7tmqyh2um62zcio63en2btvxuybgcpue",
				"bafkreicll3huefkc3qnrzeony7zcfo7cr3nbx64hnxrqzsixpceg332fhe",
			}); err != nil {
				panic(err)
			}

		default:
			t.Fatal("unsupported request number")
		}
	}))
	defer s.Close()

	bs, err := NewRemoteCarFetcher([]string{s.URL}, nil)
	require.NoError(t, err)
	fetcher, err := NewRetryCarFetcher(bs, 3)
	require.NoError(t, err)

	backend, err := NewCarBackend(fetcher)
	require.NoError(t, err)

	trustedGatewayServer := httptest.NewServer(NewHandler(Config{DeserializedResponses: true}, backend))
	defer trustedGatewayServer.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, trustedGatewayServer.URL+"/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", nil)
	require.NoError(t, err)
	startIndex := 256
	endIndex := 750
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", startIndex, endIndex))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	robs, err := carbs.NewReadOnly(bytes.NewReader(dirWithMultiblockHAMTandFiles), nil)
	require.NoError(t, err)

	dsrv := merkledag.NewDAGService(blockservice.New(robs, offline.Exchange(robs)))
	fileRootNd, err := dsrv.Get(ctx, cid.MustParse("bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"))
	require.NoError(t, err)
	uio, err := unixfile.NewUnixfsFile(ctx, dsrv, fileRootNd)
	require.NoError(t, err)
	f := uio.(files.File)
	_, err = f.Seek(int64(startIndex), io.SeekStart)
	require.NoError(t, err)
	expectedFileData, err := io.ReadAll(io.LimitReader(f, int64(endIndex)-int64(startIndex)+1))
	require.NoError(t, err)
	require.True(t, bytes.Equal(data, expectedFileData))
	require.Equal(t, 4, requestNum)
}

func TestCarBackendGetFileWithBadBlockReturned(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requestNum := 0
	s := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNum++
		switch requestNum {
		case 1:
			// Expect the full request, but return one that terminates at the root block
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // file root
			}); err != nil {
				panic(err)
			}
		case 2:
			// Expect the full request, but return a totally unrelated block
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // file root
			}); err != nil {
				panic(err)
			}
		case 3:
			// Expect the full request and return most of the file
			// Note: this is an implementation detail, it could be in the future that we request less data (e.g. partial path and file range)
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // file root
				"bafkreie5noke3mb7hqxukzcy73nl23k6lxszxi5w3dtmuwz62wnvkpsscm", // file chunks start here
				"bafkreih4ephajybraj6wnxsbwjwa77fukurtpl7oj7t7pfq545duhot7cq",
			}); err != nil {
				panic(err)
			}

		case 4:
			// Expect a request for the remainder of the file
			// Note: this is an implementation detail, it could be that the requester really asks for more information
			expectedUri := "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // file root
				"bafkreigu7buvm3cfunb35766dn7tmqyh2um62zcio63en2btvxuybgcpue", // middle of the file starts here
				"bafkreicll3huefkc3qnrzeony7zcfo7cr3nbx64hnxrqzsixpceg332fhe",
				"bafkreifst3pqztuvj57lycamoi7z34b4emf7gawxs74nwrc2c7jncmpaqm",
			}); err != nil {
				panic(err)
			}

		default:
			t.Fatal("unsupported request number")
		}
	}))
	defer s.Close()

	bs, err := NewRemoteCarFetcher([]string{s.URL}, nil)
	require.NoError(t, err)
	fetcher, err := NewRetryCarFetcher(bs, 3)
	require.NoError(t, err)

	backend, err := NewCarBackend(fetcher)
	require.NoError(t, err)

	trustedGatewayServer := httptest.NewServer(NewHandler(Config{DeserializedResponses: true}, backend))
	defer trustedGatewayServer.Close()

	resp, err := http.Get(trustedGatewayServer.URL + "/ipfs/bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa")
	require.NoError(t, err)

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	robs, err := carbs.NewReadOnly(bytes.NewReader(dirWithMultiblockHAMTandFiles), nil)
	require.NoError(t, err)

	dsrv := merkledag.NewDAGService(blockservice.New(robs, offline.Exchange(robs)))
	fileRootNd, err := dsrv.Get(ctx, cid.MustParse("bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa"))
	require.NoError(t, err)
	uio, err := unixfile.NewUnixfsFile(ctx, dsrv, fileRootNd)
	require.NoError(t, err)
	f := uio.(files.File)
	expectedFileData, err := io.ReadAll(f)
	require.NoError(t, err)
	require.True(t, bytes.Equal(data, expectedFileData))
}

func TestCarBackendGetHAMTDirectory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requestNum := 0
	s := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNum++
		fmt.Println(requestNum, request.URL.Path)
		switch requestNum {
		case 1:
			// Expect the full request, but return one that terminates in the middle of the path
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi/hamtDir/"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
			}); err != nil {
				panic(err)
			}
		case 2:
			// Expect the full request, but return one that terminates in the middle of the HAMT
			// Note: this is an implementation detail, it could be in the future that we request less data (e.g. partial path)
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi/hamtDir/"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamt root
				"bafybeiccgo7euew77gkqkhezn3pozfrciiibqz2u3spdqmgjvd5wqskipm", // inner hamt nodes start here
			}); err != nil {
				panic(err)
			}
		case 3:
			// Expect a request for a non-existent index.html file
			// Note: this is an implementation detail related to the directory request above
			// Note: the order of cases 3 and 4 here are implementation specific as well
			expectedUri := "/ipfs/bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm/index.html"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamt root
				"bafybeiccgo7euew77gkqkhezn3pozfrciiibqz2u3spdqmgjvd5wqskipm", // inner hamt nodes start here
			}); err != nil {
				panic(err)
			}
		case 4:
			// Expect a request for the full HAMT and return it
			// Note: this is an implementation detail, it could be in the future that we request more or less data
			// (e.g. ask for the full path, ask for index.html first, make a spec change to allow asking for index.html with a fallback to the directory, etc.)
			expectedUri := "/ipfs/bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamt root
				"bafybeiccgo7euew77gkqkhezn3pozfrciiibqz2u3spdqmgjvd5wqskipm", // inner hamt nodes start here
				"bafybeihjydob4eq5j4m43whjgf5cgftthc42kjno3g24sa3wcw7vonbmfy",
			}); err != nil {
				panic(err)
			}

		default:
			t.Fatal("unsupported request number")
		}
	}))
	defer s.Close()

	bs, err := NewRemoteCarFetcher([]string{s.URL}, nil)
	require.NoError(t, err)
	fetcher, err := NewRetryCarFetcher(bs, 3)
	require.NoError(t, err)

	backend, err := NewCarBackend(fetcher)
	require.NoError(t, err)

	trustedGatewayServer := httptest.NewServer(NewHandler(Config{DeserializedResponses: true}, backend))
	defer trustedGatewayServer.Close()

	resp, err := http.Get(trustedGatewayServer.URL + "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi/hamtDir/")
	require.NoError(t, err)

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	if strings.Count(string(data), ">exampleD-hamt-collide-exampleB-seed-364<") == 1 &&
		strings.Count(string(data), ">exampleC-hamt-collide-exampleA-seed-52<") == 1 &&
		strings.Count(string(data), ">exampleA<") == 1 &&
		strings.Count(string(data), ">exampleB<") == 1 {
		return
	}
	t.Fatal("directory does not contain the expected links")
}

func TestCarBackendGetCAR(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requestNum := 0
	s := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNum++
		switch requestNum {
		case 1:
			// Expect the full request, but return one that terminates in the middle of the path
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
			}); err != nil {
				panic(err)
			}
		case 2:
			// Expect the full request, but return one that terminates in the middle of the HAMT
			// Note: this is an implementation detail, it could be in the future that we request less data (e.g. partial path)
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamt root
			}); err != nil {
				panic(err)
			}

		case 3:
			// Expect the full request and return the full HAMT
			// Note: this is an implementation detail, it could be in the future that we request less data (e.g. requesting the blocks to fill out the HAMT, or with spec changes asking for HAMT ranges)
			expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi"
			if request.URL.Path != expectedUri {
				panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
			}

			if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
				"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
				"bafybeifdv255wmsrh75vcsrtkcwyktvewgihegeeyhhj2ju4lzt4lqfoze", // basicDir
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // exampleA
				"bafkreie5noke3mb7hqxukzcy73nl23k6lxszxi5w3dtmuwz62wnvkpsscm",
				"bafkreih4ephajybraj6wnxsbwjwa77fukurtpl7oj7t7pfq545duhot7cq",
				"bafkreigu7buvm3cfunb35766dn7tmqyh2um62zcio63en2btvxuybgcpue",
				"bafkreicll3huefkc3qnrzeony7zcfo7cr3nbx64hnxrqzsixpceg332fhe",
				"bafkreifst3pqztuvj57lycamoi7z34b4emf7gawxs74nwrc2c7jncmpaqm",
				"bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", // exampleB
				"bafkreihgbi345degbcyxaf5b3boiyiaxhnuxdysvqmbdyaop2swmhh3s3m",
				"bafkreiaugmh5gal5rgiems6gslcdt2ixfncahintrmcqvrgxqamwtlrmz4",
				"bafkreiaxwwb7der2qvmteymgtlj7ww7w5vc44phdxfnexog3vnuwdkxuea",
				"bafkreic5zyan5rk4ccfum4d4mu3h5eqsllbudlj4texlzj6xdgxvldzngi",
				"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamtDir
				"bafybeiccgo7euew77gkqkhezn3pozfrciiibqz2u3spdqmgjvd5wqskipm",
				"bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", // exampleB
				"bafkreihgbi345degbcyxaf5b3boiyiaxhnuxdysvqmbdyaop2swmhh3s3m",
				"bafkreiaugmh5gal5rgiems6gslcdt2ixfncahintrmcqvrgxqamwtlrmz4",
				"bafkreiaxwwb7der2qvmteymgtlj7ww7w5vc44phdxfnexog3vnuwdkxuea",
				"bafkreic5zyan5rk4ccfum4d4mu3h5eqsllbudlj4texlzj6xdgxvldzngi",
				"bafkreih2grj7p2bo5yk2guqazxfjzapv6hpm3mwrinv6s3cyayd72ke5he", // exampleD
				"bafybeihjydob4eq5j4m43whjgf5cgftthc42kjno3g24sa3wcw7vonbmfy",
				"bafkreidqhbqn5htm5qejxpb3hps7dookudo3nncfn6al6niqibi5lq6fee", // exampleC
				"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // exampleA
				"bafkreie5noke3mb7hqxukzcy73nl23k6lxszxi5w3dtmuwz62wnvkpsscm",
				"bafkreih4ephajybraj6wnxsbwjwa77fukurtpl7oj7t7pfq545duhot7cq",
				"bafkreigu7buvm3cfunb35766dn7tmqyh2um62zcio63en2btvxuybgcpue",
				"bafkreicll3huefkc3qnrzeony7zcfo7cr3nbx64hnxrqzsixpceg332fhe",
				"bafkreifst3pqztuvj57lycamoi7z34b4emf7gawxs74nwrc2c7jncmpaqm",
			}); err != nil {
				panic(err)
			}

		default:
			t.Fatal("unsupported request number")
		}
	}))
	defer s.Close()

	bs, err := NewRemoteCarFetcher([]string{s.URL}, nil)
	require.NoError(t, err)
	fetcher, err := NewRetryCarFetcher(bs, 3)
	require.NoError(t, err)

	backend, err := NewCarBackend(fetcher)
	require.NoError(t, err)

	p := path.FromCid(cid.MustParse("bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi"))
	var carReader io.Reader
	_, carReader, err = backend.GetCAR(ctx, p, CarParams{Scope: DagScopeAll})
	require.NoError(t, err)

	carBytes, err := io.ReadAll(carReader)
	require.NoError(t, err)
	carReader = bytes.NewReader(carBytes)

	blkReader, err := carv2.NewBlockReader(carReader)
	require.NoError(t, err)

	responseCarBlock := []string{
		"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
		"bafybeifdv255wmsrh75vcsrtkcwyktvewgihegeeyhhj2ju4lzt4lqfoze", // basicDir
		"bafybeigcisqd7m5nf3qmuvjdbakl5bdnh4ocrmacaqkpuh77qjvggmt2sa", // exampleA
		"bafkreie5noke3mb7hqxukzcy73nl23k6lxszxi5w3dtmuwz62wnvkpsscm",
		"bafkreih4ephajybraj6wnxsbwjwa77fukurtpl7oj7t7pfq545duhot7cq",
		"bafkreigu7buvm3cfunb35766dn7tmqyh2um62zcio63en2btvxuybgcpue",
		"bafkreicll3huefkc3qnrzeony7zcfo7cr3nbx64hnxrqzsixpceg332fhe",
		"bafkreifst3pqztuvj57lycamoi7z34b4emf7gawxs74nwrc2c7jncmpaqm",
		"bafybeid3trcauvcp7fxaai23gkz3qexmlfxnnejgwm57hdvre472dafvha", // exampleB
		"bafkreihgbi345degbcyxaf5b3boiyiaxhnuxdysvqmbdyaop2swmhh3s3m",
		"bafkreiaugmh5gal5rgiems6gslcdt2ixfncahintrmcqvrgxqamwtlrmz4",
		"bafkreiaxwwb7der2qvmteymgtlj7ww7w5vc44phdxfnexog3vnuwdkxuea",
		"bafkreic5zyan5rk4ccfum4d4mu3h5eqsllbudlj4texlzj6xdgxvldzngi",
		"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamtDir
		"bafybeiccgo7euew77gkqkhezn3pozfrciiibqz2u3spdqmgjvd5wqskipm",
		"bafkreih2grj7p2bo5yk2guqazxfjzapv6hpm3mwrinv6s3cyayd72ke5he", // exampleD
		"bafybeihjydob4eq5j4m43whjgf5cgftthc42kjno3g24sa3wcw7vonbmfy",
		"bafkreidqhbqn5htm5qejxpb3hps7dookudo3nncfn6al6niqibi5lq6fee", // exampleC
	}

	for i := 0; i < len(responseCarBlock); i++ {
		expectedCid := cid.MustParse(responseCarBlock[i])
		blk, err := blkReader.Next()
		require.NoError(t, err)
		require.True(t, blk.Cid().Equals(expectedCid))
	}
	_, err = blkReader.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestCarBackendPassthroughErrors(t *testing.T) {
	t.Run("PathTraversalError", func(t *testing.T) {
		pathTraversalTest := func(t *testing.T, traversal func(ctx context.Context, p path.ImmutablePath, backend *CarBackend) error) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var requestNum int
			s := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				requestNum++
				switch requestNum {
				case 1:
					// Expect the full request, but return one that terminates in the middle of the path
					expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi/hamtDir/exampleA"
					if request.URL.Path != expectedUri {
						panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
					}

					if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
						"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
					}); err != nil {
						panic(err)
					}
				case 2:
					// Expect the full request, but return one that terminates in the middle of the file
					// Note: this is an implementation detail, it could be in the future that we request less data (e.g. partial path)
					expectedUri := "/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi/hamtDir/exampleA"
					if request.URL.Path != expectedUri {
						panic(fmt.Errorf("expected URI %s, got %s", expectedUri, request.RequestURI))
					}

					if err := sendBlocks(ctx, dirWithMultiblockHAMTandFiles, writer, []string{
						"bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi", // root dir
						"bafybeignui4g7l6cvqyy4t6vnbl2fjtego4ejmpcia77jhhwnksmm4bejm", // hamt root
					}); err != nil {
						panic(err)
					}
				default:
					t.Fatal("unsupported request number")
				}
			}))
			defer s.Close()

			bs, err := NewRemoteCarFetcher([]string{s.URL}, nil)
			require.NoError(t, err)

			p, err := path.NewPath("/ipfs/bafybeid3fd2xxdcd3dbj7trb433h2aqssn6xovjbwnkargjv7fuog4xjdi/hamtDir/exampleA")
			require.NoError(t, err)

			imPath, err := path.NewImmutablePath(p)
			require.NoError(t, err)

			bogusErr := NewErrorStatusCode(fmt.Errorf("this is a test error"), 418)

			clientRequestNum := 0

			fetcher, err := NewRetryCarFetcher(&fetcherWrapper{fn: func(ctx context.Context, path path.ImmutablePath, params CarParams, cb DataCallback) error {
				clientRequestNum++
				if clientRequestNum > 2 {
					return bogusErr
				}
				return bs.Fetch(ctx, path, params, cb)
			}}, 3)
			require.NoError(t, err)

			backend, err := NewCarBackend(fetcher)
			require.NoError(t, err)

			err = traversal(ctx, imPath, backend)
			parsedErr := &ErrorStatusCode{}
			if errors.As(err, &parsedErr) {
				if parsedErr.StatusCode == bogusErr.StatusCode {
					return
				}
			}
			t.Fatal("error did not pass through")
		}
		t.Run("Block", func(t *testing.T) {
			pathTraversalTest(t, func(ctx context.Context, p path.ImmutablePath, backend *CarBackend) error {
				_, _, err := backend.GetBlock(ctx, p)
				return err
			})
		})
		t.Run("File", func(t *testing.T) {
			pathTraversalTest(t, func(ctx context.Context, p path.ImmutablePath, backend *CarBackend) error {
				_, _, err := backend.Get(ctx, p)
				return err
			})
		})
	})
}

type fetcherWrapper struct {
	fn func(ctx context.Context, path path.ImmutablePath, params CarParams, cb DataCallback) error
}

func (w *fetcherWrapper) Fetch(ctx context.Context, path path.ImmutablePath, params CarParams, cb DataCallback) error {
	return w.fn(ctx, path, params, cb)
}

type testErr struct {
	message    string
	retryAfter time.Duration
}

func (e *testErr) Error() string {
	return e.message
}

func (e *testErr) RetryAfter() time.Duration {
	return e.retryAfter
}

func TestGatewayErrorRetryAfter(t *testing.T) {
	originalErr := &testErr{message: "test", retryAfter: time.Minute}
	var (
		convertedErr error
		gatewayErr   *ErrorRetryAfter
	)

	// Test unwrapped
	convertedErr = blockstoreErrToGatewayErr(originalErr)
	ok := errors.As(convertedErr, &gatewayErr)
	assert.True(t, ok)
	assert.EqualValues(t, originalErr.retryAfter, gatewayErr.RetryAfter)

	// Test wrapped.
	convertedErr = blockstoreErrToGatewayErr(fmt.Errorf("wrapped error: %w", originalErr))
	ok = errors.As(convertedErr, &gatewayErr)
	assert.True(t, ok)
	assert.EqualValues(t, originalErr.retryAfter, gatewayErr.RetryAfter)
}
