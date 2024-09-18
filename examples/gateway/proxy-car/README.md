# Gateway as Proxy for Trustless CAR Remote Backend

This is an example of building a "verifying proxy" Gateway that has no
local on-disk blockstore, but instead, uses `application/vnd.ipld.car` and
`application/vnd.ipfs.ipns-record` responses from a remote HTTP server that
implements CAR support from [Trustless Gateway
Specification](https://specs.ipfs.tech/http-gateways/trustless-gateway/).

**NOTE:** the remote CAR backend MUST implement [IPIP-0402: Partial CAR Support on Trustless Gateways](https://specs.ipfs.tech/ipips/ipip-0402/)

## Build

```bash
> go build -o gateway
```

## Usage

First, you need a compliant gateway that supports both [CAR requests](https://www.iana.org/assignments/media-types/application/vnd.ipld.car) and IPNS Record response
types. Once you have it, run the proxy gateway with its address as the host parameter:

```
./gateway -g https://trustless-gateway.link -p 8040
```

### Subdomain gateway

Now you can access the gateway in [`localhost:8040`](http://localhost:8040/ipfs/bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze). It will
behave like a regular [subdomain gateway](https://docs.ipfs.tech/how-to/address-ipfs-on-web/#subdomain-gateway),
except for the fact that it runs no libp2p, and has no local blockstore.
All data is provided by a remote trustless gateway, fetched as CAR files and IPNS Records, and verified locally.

### Path gateway

If you don't need Origin isolation and only care about hosting flat files,
a plain [path gateway](https://docs.ipfs.tech/how-to/address-ipfs-on-web/#path-gateway) at
[`127.0.0.1:8040`](http://127.0.0.1:8040/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi)
may suffice.

### DNSLink gateway

Gateway supports hosting of [DNSLink](https://dnslink.dev/) websites. All you need is to pass `Host` header with FQDN that has DNSLink set up:

```console
$ curl -sH 'Host: en.wikipedia-on-ipfs.org' 'http://127.0.0.1:8080/wiki/' | head -3
<!DOCTYPE html><html class="client-js"><head>
  <meta charset="UTF-8">
  <title>Wikipedia, the free encyclopedia</title>
```

Put it behind a reverse proxy terminating TLS (like Nginx) and voila!
