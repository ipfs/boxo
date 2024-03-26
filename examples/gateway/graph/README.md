# Gateway as Proxy for Trustless Remote Backend

This is an example of building a Gateway that uses `application/vnd.ipld.car`
responses from another gateway acting as a remote Trustless Gateway and IPNS resolver.

## Build

```bash
> go build -o graph-proxy
```

## Usage

First, you need a compliant gateway that supports both [CAR requests](https://www.iana.org/assignments/media-types/application/vnd.ipld.car) and IPNS Record response
types. Once you have it, run the proxy gateway with its address as the host parameter:

```
./graph-proxy -g https://ipfs.io -p 8040
```

### Subdomain gateway

Now you can access the gateway in [localhost:8040](http://localhost:8040). It will
behave like a regular [Subdomain IPFS Gateway](https://docs.ipfs.tech/how-to/address-ipfs-on-web/#subdomain-gateway),
except for the fact that it runs no libp2p, and has no local blockstore.
All contents are provided by a remote  gateway and fetched as CAR files and IPNS Records, and verified locally.

### Path gateway

If you don't need Origin isolation and only care about hosting flat files,
a plain [path gateway](https://docs.ipfs.tech/how-to/address-ipfs-on-web/#path-gateway) at [127.0.0.1:8040](http://127.0.0.1:8040)
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
