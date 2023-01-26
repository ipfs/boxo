<h1 align="center">
go-libipfs 🍌
<br>
<img src="https://raw.githubusercontent.com/ipfs/go-libipfs/main/logo.svg" alt="go-libipfs logo" title="go-libipfs logo" width="200">
<br>
</h1>
<p align="center" style="font-size: 1.2rem;">A library for building IPFS applications and implementations.</p>

<hr />

[![Go Test](https://github.com/ipfs/go-libipfs/actions/workflows/go-test.yml/badge.svg)](https://github.com/ipfs/go-libipfs/actions/workflows/go-test.yml)
[![Go Docs](https://img.shields.io/badge/godoc-reference-blue.svg)](https://pkg.go.dev/github.com/ipfs/go-libipfs)
[![codecov](https://codecov.io/gh/ipfs/go-libipfs/branch/main/graph/badge.svg?token=9eG7d8fbCB)](https://codecov.io/gh/ipfs/go-libipfs)

## 

Go-libips is a component library for building IPFS applications and implementations in Go.

Some scenarios in which you may find go-libipfs helpful:

* You are building an application that interacts with the IPFS network
* You are building an IPFS implementation
* You want to reuse some components of IPFS such as its Kademlia DHT, Bitswap, data encoding, etc.
* You want to experiment with IPFS

Go-libipfs powers [Kubo](https://github.com/ipfs/kubo), which is the most popular IPFS implementation, so its code has been battle-tested on the IPFS network for years, and is well-understood by the community.

## What kind of components does go-libipfs have?

Go-libipfs includes high-quality components useful for interacting with IPFS protocols, public and private IPFS networks, and content-addressed data, such as:

- Content routing (DHT, delegated content routing, providing)
- Data transfer (gateways, Bitswap, incremental verification)
- Naming and mutability (name resolution, IPNS)
- Interacting with public and private IPFS networks
- Working with content-addressed data

Go-libipfs aims to provide a cohesive interface into these components. Note that not all of the underlying components necessarily reside in this respository.

## Getting started
TODO

## Should I add my IPFS component to go-libipfs?
We happily accept external contributions! However, go-libipfs maintains a high quality bar, so code accepted into go-libipfs must meet some minimum maintenance criteria:

* Actively maintained
  * Must be actively used by, or will be included in software that is actively used by, a significant number of users or production systems. Code that is not actively used cannot be properly maintained.
  * Must have multiple engineers who are willing and able to maintain the relevant code in go-libipfs for a long period of time.
  * If either of these changes, go-libipfs maintainers will consider removing the component from go-libipfs.
* Adequately tested
  * At least with unit tests
  * Ideally also including integration tests with other components
* Adequately documented
  * Godocs at minimum
  * Complex components should have their own doc.go or README.md describing the component, its use cases, tradeoffs, design rationale, etc.
* If the maintainers are not go-libipfs maintainers, then the component must include a CODEOWNERS file with at least two code owners who can commit to reviewing PRs

If you have some experimental component that you think would benefit the IPFS community, we suggest you build the component in your own repository until it's clear that there's community demand for it, and then open an issue in this repository to discuss including it in go-libipfs.

## Help

If you have questions, feel free to open an issue. You can also find the go-libipfs maintainers in [Slack](https://filecoin.io/slack/) at #go-libipfs-maintainers.


## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
