# Verified File Fetch

This example shows how to download a UnixFS file from a gateway that implements
the [Trustless Gateway](https://specs.ipfs.tech/http-gateways/trustless-gateway/)
specification, in a trustless, verifiable manner.

This example does not yet support downloading UnixFS directories, since that becomes
more complex. For now, we would suggest reading the [`extract.go`](https://github.com/ipld/go-car/blob/master/cmd/car/extract.go)
file from `go-car` in order to understand how to convert a directory into a file system.

## Build

```bash
> go build -o verified-fetch
```

## Usage

First, you need a gateway that complies with the Trustless Gateway specification.
In our specific case, we need that the gateway supports both the CAR file format,
as well as verifiable IPNS records, in the case we fetch from an `/ipns` URL.

As an example, you can verifiably fetch a `hello.txt` file:

```
./verified-fetch -o hello.txt /ipfs/bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e
```
