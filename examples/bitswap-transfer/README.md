# Transferring UnixFS file with Bitswap

This is an example that quickly shows how to use IPFS tooling to move around a file.

This example can be started in either server mode, or client mode.

In server mode, it will sit and wait for data to be requested via the [Bitswap](https://docs.ipfs.tech/concepts/bitswap/#bitswap) protocol.

In client mode, it will start up, connect to the server, request the data needed via Bitswap, write it out and shut down.

## Build

From the `boxo/examples` directory run the following:

```
> cd bitswap-transfer/
> go build
```

## Usage

```
> ./bitswap-transfer
2023/01/30 21:34:11 I am /ip4/127.0.0.1/tcp/53935/p2p/QmUtp8xEVgWC5dNPthF2g37eVvCdrqY1FPxLxXZoKkPbdp
2023/01/30 21:34:11 hosting UnixFS file with CID: bafybeiecq2irw4fl5vunnxo6cegoutv4de63h7n27tekkjtak3jrvrzzhe
2023/01/30 21:34:11 listening for inbound connections and Bitswap requests
2023/01/30 21:34:11 Now run "./bitswap-transfer -d /ip4/127.0.0.1/tcp/53935/p2p/QmUtp8xEVgWC5dNPthF2g37eVvCdrqY1FPxLxXZoKkPbdp" on a different terminal
```

The IPFS server hosting the data over libp2p will print out its `Multiaddress`, which indicates how it can be reached (ip4+tcp) and its randomly generated ID (`QmUtp8xEV...`)

Now, launch another node that talks to the hosting node:

```
> ./bitswap-transfer -d /ip4/127.0.0.1/tcp/53935/p2p/QmUtp8xEVgWC5dNPthF2g37eVvCdrqY1FPxLxXZoKkPbdp
```

The IPFS client will then download the file from the server peer and let you know that it's been received.

## Details

The `makeHost()` function creates a go-libp2p host that can make and receive connections and is usable by various protocols such as Bitswap.

Both the client and the server have their own libp2p hosts which have 
- A [libp2p Peer ID](https://godoc.org/github.com/libp2p/go-libp2p-peer#ID) like `QmNtX1cvrm2K6mQmMEaMxAuB4rTexhd87vpYVot4sEZzxc`. The example autogenerates a key pair on every run and uses an ID extracted from the public key (the hash of the public key).
- A [Multiaddress](https://godoc.org/github.com/multiformats/go-multiaddr), which indicates how to reach this peer. There can be several of them (using different protocols or locations for example). Example: `/ip4/127.0.0.1/tcp/1234`.

The `startDataServer` function creates some local storage and then processes the file data into [UnixFS](https://docs.ipfs.tech/concepts/file-systems/#unix-file-system-unixfs) graph.
There are many ways to turn a file into a UnixFS graph the ones selected in the example correspond to parameters commonly seen in the IPFS ecosystem, but are just one possible set. They correspond to [kubo](https://github.com/ipfs/kubo)'s `ipfs add --cid-version=1 <file>`.
It then starts a Bitswap server and waits for requests.

The `runClient` function connects to the data server we started earlier and then uses UnixFS tooling to get the parts of the file we need (in this case all of it, but getting ranges is valid as well).
As we read more of the file the parts of the graph we need are being requested using Bitswap from the data server.

Some important notes:
- The way in which a client discovers which peers to ask for data is highly situational. In this case we knew who we wanted to fetch the data from. In others we might use some system like a DHT, a coordination server, etc. to find that information.
- Downloading data using libp2p and Bitswap is just one way you can fetch data. You could also leverage other techniques including GraphSync, HTTP requests for a CAR file of your graph, or something else.
- UnixFS is only one type of data that can be moved around using IPFS tooling. A lot of IPFS tooling and infrastructure is built to work more generically with content addressable data. Other examples include data from BitTorrent, Filecoin, Git, etc.
