# RAPIDO

RAPIDO is an example CLI which download an unordered `.car` for some unixfs data from multiple gateways using [`rapide`](../../rapide).

This code is not maintained up to "production ready" standars, this is an example demonstrating how to use RAPIDE.

## Usage

```console
$ rapido -gw https://ipfs.io/ipfs/ -gw https://strn.pl/ipfs/ QmT2EHPdRvUDxiuZBbYg5ZHy1f8L6MY1HxD45zHycLobMJ | pv > 2023-02-03.ipfs.io.car
```