# CAR File Fetcher

This example shows how to download a UnixFS file or directory from a gateway that implements
[application/vnd.ipld.car](https://www.iana.org/assignments/media-types/application/vnd.ipld.car)
responses of the [Trustless Gateway](https://specs.ipfs.tech/http-gateways/trustless-gateway/)
specification, in a trustless, verifiable manner.

It relies on [IPIP-402](https://specs.ipfs.tech/ipips/ipip-0402/) to retrieve
the file entity via a single CAR request with all blocks required for end-to-end
verification.

## Build

```bash
> go build -o fetcher
```

## Usage

First, you need a gateway that complies with the Trustless Gateway specification.
In our specific case, we need that the gateway supports CAR response type.

As an example, you can verifiably fetch a `hello.txt` file from IPFS gateway at `https://trustless-gateway.link`:

```
./fetcher -g https://trustless-gateway.link -o hello.txt /ipfs/bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e
```
