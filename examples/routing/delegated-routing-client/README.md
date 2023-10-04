# Delegated Routing V1 Command Line Client

This is an example of how to use the Delegated Routing V1 HTTP client from Boxo.
In this package, we build a small command line tool that allows you to connect to
a Routing V1 endpoint and fetch content providers, peer information, as well as
IPNS records for a certain IPNS name.

## Build

```bash
> go build -o delegated-routing-client
```

## Usage

First, you will need a HTTP endpoint compatible  with  [Delegated Routing V1 Specification][Specification].
For that, you can potentially use [Kubo], which supports [exposing][kubo-conf]
a  `/routing/v1` endpoint. For the commands below, we assume the HTTP server that
provides the endpoint  is `http://127.0.0.1:8080`.

### Find CID Providers

To find providers, provide the flag `-cid` with the [CID] of the content you're looking for:

```console
$ ./delegated-routing-client -e http://127.0.0.1:8080 -cid bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4

12D3KooWEfL19QqRGGLraaAYw1XA3dtDdVRYaHt6jymFxcuQo3Zm
	Protocols: []
	Addresses: [/ip4/163.47.51.218/tcp/28131]
12D3KooWK53GAx2g2UUYfJHHjxDbVLeDgGxNMHXDWeJa5KgMhTD2
	Protocols: []
	Addresses: [/ip4/195.167.147.43/udp/8888/quic /ip4/195.167.147.43/tcp/8888]
12D3KooWCpr8kACTRLKrPy4LPpSX7LXvKQ7eYqTmY8CBvgK5HZgB
	Protocols: []
	Addresses: [/ip4/163.47.49.234/tcp/28102]
12D3KooWC9L4RjPGgqpzBUBkcVpKjJYofCkC5i5QdQftg1LdsFb2
	Protocols: []
	Addresses: [/ip4/198.244.201.187/tcp/4001]
```

### Find Peer Information

To find a peer, provide the flag `-peer` with the [Peer ID] of the peer you're looking for:


```console
$ ./delegated-routing-client -e http://127.0.0.1:8080 -peer 12D3KooWC9L4RjPGgqpzBUBkcVpKjJYofCkC5i5QdQftg1LdsFb2

12D3KooWC9L4RjPGgqpzBUBkcVpKjJYofCkC5i5QdQftg1LdsFb2
	Protocols: []
	Addresses: [/ip4/198.244.201.187/tcp/4001]
```

### Get an IPNS Record

To find an IPNS record, provide the flag `-ipns` with the [IPNS Name] you're trying to find a record for:

```console
$ ./delegated-routing-client -e http://127.0.0.1:8080 -ipns /ipns/k51qzi5uqu5diuz0h5tjqama8qbmyxusvqz2hfgn5go5l07l9k2ubqa09m7toe

/ipns/k51qzi5uqu5diuz0h5tjqama8qbmyxusvqz2hfgn5go5l07l9k2ubqa09m7toe
	Value: /ipfs/QmUGMoVz62ZARyxkrdEiwmFZanTwVWLLu6EAWvbWHNcwR8
```

[Specification]: https://specs.ipfs.tech/routing/http-routing-v1/
[Kubo]: https://github.com/ipfs/kubo
[kubo-conf]: https://github.com/ipfs/kubo/blob/master/docs/config.md#gatewayexposeroutingapi
[CID]: https://docs.ipfs.tech/concepts/content-addressing/#what-is-a-cid
[Peer ID]: https://docs.libp2p.io/concepts/fundamentals/peers/#peer-id
[IPNS Name]: https://specs.ipfs.tech/ipns/ipns-record/#ipns-name
