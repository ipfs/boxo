# Gateway as a Verifying Proxy for Untrusted Remote Blockstore

This is an example of building a Gateway that uses  `application/vnd.ipld.raw`
responses from another gateway acting as a remote blockstore and IPNS resolver.

Key benefits:
1. Verifies raw blocks and IPNS records fetched from untrusted third-party gateways.
2. The proxy provides web gateway functionalities: returns deserialized files and websites, including index.html support, while the remote gateway only needs to support block responses.

In this example, we implement two major structures:

- [Block Store](./blockstore.go), which forwards the block requests to the backend
gateway using `?format=raw`, and
- [Routing System](./routing.go), which forwards the IPNS requests to the backend
gateway using `?format=ipns-record`. In addition, DNSLink lookups are done locally.
  - Note: `ipns-record` was introduced just recently in [IPIP-351](https://github.com/ipfs/specs/pull/351) and reference support for it will ship in Kubo 0.19. Until that happens, it may not be supported by public gateways yet. 

## Build

```bash
> go build -o verifying-proxy
```

## Usage

First, you need a compliant gateway that supports both [RAW Block](https://www.iana.org/assignments/media-types/application/vnd.ipld.raw) and IPNS Record response
types. Once you have it, run the proxy gateway with its address as the host parameter:


```
./verifying-proxy -h https://ipfs.io -p 8040
```

Now you can access the gateway in [127.0.0.1:8040](http://127.0.0.1:8040). It will
behave like a regular IPFS Gateway, except for the fact that it runs no libp2p, and has no local blockstore.
All contents are provided by a remote  gateway and fetched as RAW Blocks and Records, and verified locally.
