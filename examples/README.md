# Boxo Examples and Tutorials

In this directory, you can find some examples to help you get started using Boxo and its associated libraries in your applications.

Let us know if you find any issue or if you want to contribute and add a new tutorial, feel welcome to submit a PR, thank you!

## How To Use the Examples

The examples are designed to give users a starting point to create certain things using Boxo. However, this directory is not meant to be directly copied out of Boxo, since it uses a replacement directive in the [`go.mod`](go.mod) file. You can also not `go install`.

If you want to copy of the examples out of Boxo in order to use it in your own product, you need to remove the replacement directive and ensure you're using the latest Boxo version:

```bash
> go mod edit -dropreplace=github.com/ipfs/boxo
> go get github.com/ipfs/boxo@latest
> go mod tidy
```

## How To Create an Example

All examples are self-contained inside the same module ([`examples`](go.mod)). To create an example, clone Boxo, navigate to this directory and create a sub-directory with a descriptive name for the example. If the example pertains a topic that has multiple examples, such as `gateway`, create a sub-directory there.

The new example must contain a descriptive `README.md` file, which explains what the example is, how to build and use it. See the existing examples to have an idea of how extensive it should be. In addition, your code must be properly documented.

Once you have your example finished, do not forget to run `go mod tidy` and adding a link to the example in the section [Examples and Tutorials](#examples-and-tutorials) below.

## Examples and Tutorials

- [Transferring UnixFS file data with Bitswap](./bitswap-transfer)
- [Gateway backed by a local blockstore in form of a CAR file](./gateway/car-file)
- [Gateway backed by a remote (HTTP) blockstore and IPNS resolver](./gateway/proxy-blocks)
- [Gateway backed by a remote (HTTP) CAR Gateway](./gateway/proxy-car)
- [Delegated Routing V1 Command Line Client](./routing/delegated-routing-client/)
