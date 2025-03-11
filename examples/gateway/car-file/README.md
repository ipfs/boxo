# HTTP Gateway backed by a CAR File as BlocksBackend

This is an example that shows how to build a Gateway backed by the contents of
a CAR file. A [CAR file](https://ipld.io/specs/transport/car/) is a Content
Addressable archive that contains blocks.

The `main.go` sets up a `blockService` backed by a static CAR file,
and then uses it to initialize `gateway.NewBlocksBackend(blockService)`.

## Build

```bash
> go build -o gateway
```

## Usage

First of all, you will need some content stored as a CAR file. You can easily
export your favorite website or content, using:

```
ipfs dag export <CID> > data.car
```

Then, you can start the gateway with:


```
./gateway -c data.car -p 8040
```

### Subdomain gateway

Now you can access the gateway in [localhost:8040](http://localhost:8040). It will
behave like a regular [Subdomain IPFS Gateway](https://docs.ipfs.tech/how-to/address-ipfs-on-web/#subdomain-gateway),
except for the fact that all contents are provided
from the CAR file. Therefore, things such as IPNS resolution and fetching contents
from nodes in the IPFS network won't work.

### Path gateway

If you don't need Origin isolation and only care about hosting flat files,
a plain [path gateway](https://docs.ipfs.tech/how-to/address-ipfs-on-web/#path-gateway) at [127.0.0.1:8040](http://127.0.0.1:8040)
may suffice.
