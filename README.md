<h1 align="center">
Boxo 🍌
<br>
<img src="https://raw.githubusercontent.com/ipfs/boxo/main/logo.svg" alt="Boxo logo" title="Boxo logo" width="200">
<br>
</h1>
<p align="center" style="font-size: 1.2rem;">A library for building IPFS applications and implementations.</p>

<hr />

[![Go Test](https://github.com/ipfs/boxo/actions/workflows/go-test.yml/badge.svg)](https://github.com/ipfs/boxo/actions/workflows/go-test.yml)
[![Go Docs](https://img.shields.io/badge/godoc-reference-blue.svg)](https://pkg.go.dev/github.com/ipfs/boxo)
[![codecov](https://codecov.io/gh/ipfs/boxo/branch/main/graph/badge.svg?token=9eG7d8fbCB)](https://codecov.io/gh/ipfs/boxo)

<!-- TOC -->

- [About](#about)
    - [Motivation](#motivation)
- [What kind of components does Boxo have?](#what-kind-of-components-does-boxo-have)
    - [Does Boxo == IPFS?](#does-boxo--ipfs)
    - [Is everything related to IPFS in the Go ecosystem in this repo?](#is-everything-related-to-ipfs-in-the-go-ecosystem-in-this-repo)
- [Getting started](#getting-started)
- [Should I add my IPFS component to Boxo?](#should-i-add-my-ipfs-component-to-boxo)
- [Help](#help)
- [Governance and Access](#governance-and-access)
- [Release Process](#release-process)
- [Related Items](#related-items)
- [License](#license)

<!-- /TOC -->

## About

Boxo is a component library for building IPFS applications and implementations in Go.

Some scenarios in which you may find Boxo helpful:

* You are building an application that interacts with the IPFS network
* You are building an IPFS implementation
* You want to reuse some components of IPFS such as its Kademlia DHT, Bitswap, data encoding, etc.
* You want to experiment with IPFS

Boxo powers [Kubo](https://github.com/ipfs/kubo), which is [the most popular IPFS implementation](https://github.com/protocol/network-measurements/tree/master/reports),
so its code has been battle-tested on the IPFS network for years, and is well-understood by the community.

### Motivation
**TL;DR** The goal of this repo is to help people build things.  Previously users struggled to find existing useful code or to figure out how to use what they did find.  We observed many running Kubo and using its HTTP RPC API.  This repo aims to do better.  We're taking the libraries that many were already effectively relying on in production and making them more easily discoverable and usable.

The maintainers primarily aim to help people trying to build with IPFS in Go that were previously either giving up or relying on the [Kubo HTTP RPC API](https://docs.ipfs.tech/reference/kubo/rpc/). Some of these people will end up being better served by IPFS tooling in other languages (e.g., Javascript, Rust, Java, Python), but for those who are either looking to write in Go or to leverage the set of IPFS tooling we already have in Go we’d like to make their lives easier.

We’d also like to make life easier on ourselves as the maintainers by reducing the maintenance burden that comes from being the owners on [many repos](https://github.com/ipfs/kubo/issues/8543) and then use that time to contribute more to the community in the form of easier to use libraries, better implementations, improved protocols, new protocols, etc.

Boxo is not exhaustive nor comprehensive--there are plenty of useful IPFS protocols, specs, libraries, etc. that are not in Boxo. The goal of Boxo is to provide cohesive and well-maintained components for common IPFS use cases.

## What kind of components does Boxo have?

Boxo includes high-quality components useful for interacting with IPFS protocols, public and private IPFS networks, and content-addressed data, such as:

- Content routing (DHT, delegated content routing, providing)
- Data transfer (gateways, Bitswap, incremental verification)
- Naming and mutability (name resolution, IPNS)
- Interacting with public and private IPFS networks
- Working with content-addressed data

Boxo aims to provide a cohesive interface into these components. Note that not all of the underlying components necessarily reside in this respository.

### Does Boxo == IPFS?
No.  This repo houses some IPFS functionality written in Go that has been useful in practice, and is maintained by a group that has long term commitments to the IPFS project

### Is everything related to IPFS in the Go ecosystem in this repo?

No.  Not everything related to IPFS is intended to be in Boxo. View it as a starter toolbox (potentially among multiple).  If you’d like to build an IPFS implementation with Go, here are some tools you might want that are maintained by a group that has long term commitments to the IPFS project.  There are certainly repos that others maintainer that aren't included here (e.g., ipfs/go-car) which are still useful to IPFS implementations. It's expected and fine for new IPFS functionality to be developed that won't be part of Boxo.  

## Getting started
See [examples](./examples/README.md).

## Should I add my IPFS component to Boxo?
We happily accept external contributions! However, Boxo maintains a high quality bar, so code accepted into Boxo must meet some minimum maintenance criteria:

* Actively maintained
  * Must be actively used by, or will be included in software that is actively used by, a significant number of users or production systems. Code that is not actively used cannot be properly maintained.
  * Must have multiple engineers who are willing and able to maintain the relevant code in Boxo for a long period of time.
  * If either of these changes, Boxo maintainers will consider removing the component from Boxo.
* Adequately tested
  * At least with unit tests
  * Ideally also including integration tests with other components
* Adequately documented
  * Godocs at minimum
  * Complex components should have their own doc.go or README.md describing the component, its use cases, tradeoffs, design rationale, etc.
* If the maintainers are not Boxo maintainers, then the component must include a CODEOWNERS file with at least two code owners who can commit to reviewing PRs

If you have some experimental component that you think would benefit the IPFS community, we suggest you build the component in your own repository until it's clear that there's community demand for it, and then open an issue/PR in this repository to discuss including it in Boxo.

## Help

If you have questions, feel free to open an issue. You can also find the Boxo maintainers in [Filecoin Slack](https://filecoin.io/slack/) at #Boxo-maintainers.  (If you would like to engage via IPFS Discord or ipfs.io Matrix, please drop into the #ipfs-implementers channel/room or file an issue, and we'll get bridging from #Boxo-maintainers to these other chat platforms.)

## Governance and Access
See [CODEOWNERS](./docs/CODEOWNERS) for the current maintainers list.  Governance for graduating additional maintainers hasn't been established.  Repo permissions are all managed through [ipfs/github-mgmt](https://github.com/ipfs/github-mgmt).

## Release Process
To be documented: https://github.com/ipfs/boxo/issues/170

## Related Items
* [Initial proposal for "Consolidate IPFS Repositories" that spawned this project](https://github.com/ipfs/kubo/issues/8543)

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
