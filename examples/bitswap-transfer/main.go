package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	mrand "math/rand"
	"strconv"
	"strings"

	"github.com/ipfs/go-datastore"
	dsync "github.com/ipfs/go-datastore/sync"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"

	"github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	chunker "github.com/ipfs/boxo/chunker"
	offline "github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	uih "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"

	bsclient "github.com/ipfs/boxo/bitswap/client"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	bsserver "github.com/ipfs/boxo/bitswap/server"
	"github.com/ipfs/boxo/files"
)

const exampleBinaryName = "bitswap-transfer"

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Parse options from the command line
	targetF := flag.String("d", "", "target peer to dial")
	seedF := flag.Int64("seed", 0, "set random seed for id generation")
	flag.Parse()

	// For this example we are going to be transferring data using Bitswap over libp2p
	// This means we need to create a libp2p host first

	// Make a host that listens on the given multiaddress
	h, err := makeHost(0, *seedF)
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()

	fullAddr := getHostAddress(h)
	log.Printf("I am %s\n", fullAddr)

	if *targetF == "" {
		c, bs, err := startDataServer(ctx, h)
		if err != nil {
			log.Fatal(err)
		}
		defer bs.Close()
		log.Printf("hosting UnixFS file with CID: %s\n", c)
		log.Println("listening for inbound connections and Bitswap requests")
		log.Printf("Now run \"./%s -d %s\" on a different terminal\n", exampleBinaryName, fullAddr)

		// Run until canceled.
		<-ctx.Done()
	} else {
		log.Printf("downloading UnixFS file with CID: %s\n", fileCid)
		fileData, err := runClient(ctx, h, cid.MustParse(fileCid), *targetF)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("found the data")
		log.Println(string(fileData))
		log.Println("the file was all the numbers from 0 to 100k!")
	}
}

// makeHost creates a libP2P host with a random peer ID listening on the
// given multiaddress.
func makeHost(listenPort int, randseed int64) (host.Host, error) {
	var r io.Reader
	if randseed == 0 {
		r = rand.Reader
	} else {
		r = mrand.New(mrand.NewSource(randseed))
	}

	// Generate a key pair for this host. We will use it at least
	// to obtain a valid host ID.
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, r)
	if err != nil {
		return nil, err
	}

	// Some basic libp2p options, see the go-libp2p docs for more details
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", listenPort)), // port we are listening on, limiting to a single interface and protocol for simplicity
		libp2p.Identity(priv),
	}

	return libp2p.New(opts...)
}

func getHostAddress(h host.Host) string {
	// Build host multiaddress
	hostAddr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/p2p/%s", h.ID().String()))

	// Now we can build a full multiaddress to reach this host
	// by encapsulating both addresses:
	addr := h.Addrs()[0]
	return addr.Encapsulate(hostAddr).String()
}

// The CID of the file with the number 0 to 100k, built with the parameters:
// CIDv1 links, a 256bit sha2-256 hash function, raw-leaves, a balanced layout, 256kiB chunks, and 174 max links per block
const fileCid = "bafybeiecq2irw4fl5vunnxo6cegoutv4de63h7n27tekkjtak3jrvrzzhe"

// createFile0to100k creates a file with the number 0 to 100k
func createFile0to100k() ([]byte, error) {
	b := strings.Builder{}
	for i := 0; i <= 100000; i++ {
		s := strconv.Itoa(i)
		_, err := b.WriteString(s)
		if err != nil {
			return nil, err
		}
	}
	return []byte(b.String()), nil
}

func startDataServer(ctx context.Context, h host.Host) (cid.Cid, *bsserver.Server, error) {
	fileBytes, err := createFile0to100k()
	if err != nil {
		return cid.Undef, nil, err
	}
	fileReader := bytes.NewReader(fileBytes)

	ds := dsync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	bs = blockstore.NewIdStore(bs) // handle identity multihashes, these don't require doing any actual lookups

	bsrv := blockservice.New(bs, offline.Exchange(bs))
	dsrv := merkledag.NewDAGService(bsrv)

	// Create a UnixFS graph from our file, parameters described here but can be visualized at https://dag.ipfs.tech/
	ufsImportParams := uih.DagBuilderParams{
		Maxlinks:  uih.DefaultLinksPerBlock, // Default max of 174 links per block
		RawLeaves: true,                     // Leave the actual file bytes untouched instead of wrapping them in a dag-pb protobuf wrapper
		CidBuilder: cid.V1Builder{ // Use CIDv1 for all links
			Codec:    uint64(multicodec.DagPb),
			MhType:   uint64(multicodec.Sha2_256), // Use SHA2-256 as the hash function
			MhLength: -1,                          // Use the default hash length for the given hash function (in this case 256 bits)
		},
		Dagserv: dsrv,
		NoCopy:  false,
	}
	ufsBuilder, err := ufsImportParams.New(chunker.NewSizeSplitter(fileReader, chunker.DefaultBlockSize)) // Split the file up into fixed sized 256KiB chunks
	if err != nil {
		return cid.Undef, nil, err
	}
	nd, err := balanced.Layout(ufsBuilder) // Arrange the graph with a balanced layout
	if err != nil {
		return cid.Undef, nil, err
	}

	// Start listening on the Bitswap protocol
	// For this example we're not leveraging any content routing (DHT, IPNI, delegated routing requests, etc.) as we know the peer we are fetching from
	n := bsnet.NewFromIpfsHost(h)
	bswap := bsserver.New(ctx, n, bs)
	n.Start(bswap)
	return nd.Cid(), bswap, nil
}

func runClient(ctx context.Context, h host.Host, c cid.Cid, targetPeer string) ([]byte, error) {
	n := bsnet.NewFromIpfsHost(h)
	bswap := bsclient.New(ctx, n, nil, blockstore.NewBlockstore(datastore.NewNullDatastore()))
	n.Start(bswap)
	defer bswap.Close()

	// Turn the targetPeer into a multiaddr.
	maddr, err := multiaddr.NewMultiaddr(targetPeer)
	if err != nil {
		return nil, err
	}

	// Extract the peer ID from the multiaddr.
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return nil, err
	}

	// Directly connect to the peer that we know has the content
	// Generally this peer will come from whatever content routing system is provided, however go-bitswap will also
	// ask peers it is connected to for content so this will work
	if err := h.Connect(ctx, *info); err != nil {
		return nil, err
	}

	dserv := merkledag.NewReadOnlyDagService(merkledag.NewSession(ctx, merkledag.NewDAGService(blockservice.New(blockstore.NewBlockstore(datastore.NewNullDatastore()), bswap))))
	nd, err := dserv.Get(ctx, c)
	if err != nil {
		return nil, err
	}

	unixFSNode, err := unixfile.NewUnixfsFile(ctx, dserv, nd)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if f, ok := unixFSNode.(files.File); ok {
		if _, err := io.Copy(&buf, f); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}
