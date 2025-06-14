<h1 align="center">
  <br>
  <a href="#readme"><img src="https://github.com/ipfs/boxo/assets/157609/3c5e7391-fbc2-405b-9efc-920f4fd13b39" alt="Boxo logo" title="Boxo logo" width="200"></a>
  <br>
  BOXO: IPFS SDK for GO
  <br>
</h1>

<p align="center" style="font-size: 1.2rem;">A set of libraries for building IPFS applications and implementations in GO.</p>

<p align="center">
  <a href="https://ipfs.tech"><img src="https://img.shields.io/badge/project-IPFS-blue.svg?style=flat-square" alt="Official Part of IPFS Project"></a>
  <a href="https://discuss.ipfs.tech"><img alt="Discourse Forum" src="https://img.shields.io/discourse/posts?server=https%3A%2F%2Fdiscuss.ipfs.tech"></a>
  <a href="https://matrix.to/#/#ipfs-space:ipfs.io"><img alt="Matrix" src="https://img.shields.io/matrix/ipfs-space%3Aipfs.io?server_fqdn=matrix.org"></a>
  <a href="https://github.com/ipfs/boxo/actions"><img src="https://img.shields.io/github/actions/workflow/status/ipfs/boxo/go-test.yml?branch=main" alt="ci"></a>
  <a href="https://codecov.io/gh/ipfs/boxo"><img src="https://codecov.io/gh/ipfs/boxo/branch/main/graph/badge.svg?token=9eG7d8fbCB" alt="coverage"></a>
  <a href="https://github.com/ipfs/boxo/releases"><img alt="GitHub release" src="https://img.shields.io/github/v/release/ipfs/boxo?filter=!*rc*"></a>
  <a href="https://godoc.org/github.com/ipfs/boxo"><img src="https://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square" alt="godoc reference"></a>  
</p>

<hr />

<!-- TOC -->

- [About](#about)
  - [Motivation](#motivation)
- [Scope](#scope)
  - [What kind of components does Boxo have?](#what-kind-of-components-does-boxo-have)
  - [Does Boxo == IPFS?](#does-boxo--ipfs)
  - [Is everything related to IPFS in the Go ecosystem in this repo?](#is-everything-related-to-ipfs-in-the-go-ecosystem-in-this-repo)
- [Consuming](#consuming)
  - [Getting started](#getting-started)
  - [Migrating to Boxo](#migrating-to-boxo)
  - [Deprecations & Breaking Changes](#deprecations--breaking-changes)
- [Development](#development)
  - [Should I add my IPFS component to Boxo?](#should-i-add-my-ipfs-component-to-boxo)
  - [Release Process](#release-process)
  - [Why is the code coverage so bad?](#why-is-the-code-coverage-so-bad)
- [General](#general)
  - [Help](#help)
  - [What is the response time for issues or PRs filed?](#what-is-the-response-time-for-issues-or-prs-filed)
  - [What are some projects that depend on this project?](#what-are-some-projects-that-depend-on-this-project)
  - [Governance and Access](#governance-and-access)
  - [Why is this named "Boxo"?](#why-is-this-named-boxo)
  - [Additional Docs & FAQs](#additional-docs--faqs)
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

More details can also be found in the [Rationale FAQ](./docs/FAQ.md#rationale-faq).

## Scope

### What kind of components does Boxo have?

Boxo includes high-quality components useful for interacting with IPFS protocols, public and private IPFS networks, and content-addressed data, such as:

- Content routing (DHT, delegated content routing, providing)
- Data transfer (gateways, Bitswap, incremental verification)
- Naming and mutability (name resolution, IPNS)
- Interacting with public and private IPFS networks
- Working with content-addressed data

Boxo aims to provide a cohesive interface into these components. Note that not all of the underlying components necessarily reside in this repository.

### Does Boxo == IPFS?

No. This repo houses some IPFS functionality written in Go that has been useful in practice, and is maintained by a group that has long term commitments to the IPFS project

### Is everything related to IPFS in the Go ecosystem in this repo?

No. Not everything related to IPFS is intended to be in Boxo. View it as a starter toolbox (potentially among multiple). If you’d like to build an IPFS implementation with Go, here are some tools you might want that are maintained by a group that has long term commitments to the IPFS project. There are certainly repos that others maintain that aren't included here (e.g., ipfs/go-car) which are still useful to IPFS implementations. It's expected and fine for new IPFS functionality to be developed that won't be part of Boxo.

## Consuming

### Getting started

See [examples](./examples/README.md).

If you are migrating to Boxo, see [Migrating to Boxo](#migrating-to-boxo).

### Migrating to Boxo

Many Go modules under github.com/ipfs have moved here. Boxo provides a tool to ease this migration, which does most of the work for you:

* `cd` into the root directory of your module (where the `go.mod` file is)
* Run: `go run github.com/ipfs/boxo/cmd/boxo-migrate@latest update-imports`
  * This will upgrade your module to Boxo v0.8.0 and rewrite your import paths
* Run: `go run github.com/ipfs/boxo/cmd/boxo-migrate@latest check-dependencies`
  * This will print unmaintained dependencies you still have
  * These aren't necessarily an immediate problem, but you should eventually get them out of your dependency graph
  
This tool only upgrades your module to Boxo v0.8.0, to minimize backwards-incompatible changes. Depending on the versions of IPFS modules before the upgrade, your code may require additional changes to build.

We recommend upgrading to v0.8.0 first, and _then_ upgrading to the latest Boxo release.

If you encounter any challenges, please [open an issue](https://github.com/ipfs/boxo/issues/new/choose) and Boxo maintainers will help you.

### Deprecations & Breaking Changes

See [RELEASE.md](./RELEASE.md).

## Development

### Should I add my IPFS component to Boxo?

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

### Release Process

See [RELEASE.md](./RELEASE.md).

### Why is the code coverage so bad?

The code coverage of this repo is not currently representative of the actual test coverage of this code. Much of the code in this repo is currently covered by integration tests in [Kubo](https://github.com/ipfs/kubo). We are in the process of moving those tests here, and as that continues the code coverage will significantly increase.

## General

### Help

If you suspect a bug or have technical questions, feel free to open an issue. 

For regular support, try [Community chat](https://docs.ipfs.tech/community/#chat)'s `#ipfs-implementers` room or [help/boxo at IPFS discussion forums](https://discuss.ipfs.tech/c/help/boxo/51).

### What is the response time for issues or PRs filed?

New issues and PRs to this repo are usually looked at on a weekly basis as part of [Shipyard's GO Triage triage](https://ipshipyard.notion.site/IPFS-Go-Triage-Boxo-Kubo-Rainbow-0ddee6b7f28d412da7dabe4f9107c29a). However, the response time may vary.

### What are some projects that depend on this project?

The exhaustive list is https://github.com/ipfs/boxo/network/dependents. Some notable projects include:

1. [Kubo](https://github.com/ipfs/kubo), an IPFS implementation in Go
2. [Lotus](https://github.com/filecoin-project/lotus), a Filecoin implementation in Go
3. [rainbow](https://github.com/ipfs/rainbow), a specialized IPFS gateway
4. [ipfs-check](https://github.com/ipfs/ipfs-check), checks IPFS data availability
5. [someguy](https://github.com/ipfs/someguy), a dedicated Delegated Routing V1 server and client

### Governance and Access

See [CODEOWNERS](./docs/CODEOWNERS) for the current maintainers list. Governance for graduating additional maintainers hasn't been established. Repo permissions are all managed through [ipfs/github-mgmt](https://github.com/ipfs/github-mgmt).

### Why is this named "Boxo"?

See https://github.com/ipfs/boxo/issues/215. 

### Additional Docs & FAQs

See [the wiki](https://github.com/ipfs/boxo/wiki).

### License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
