go-car (go!)
==================

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](https://protocol.ai)
[![](https://img.shields.io/badge/project-ipld-orange.svg?style=flat-square)](https://github.com/ipld/ipld)
[![](https://img.shields.io/badge/freenode-%23ipld-orange.svg?style=flat-square)](https://webchat.freenode.net/?channels=%23ipld)
[![Go Reference](https://pkg.go.dev/badge/github.com/ipld/go-car.svg)](https://pkg.go.dev/github.com/ipld/go-car)
[![Coverage Status](https://codecov.io/gh/ipld/go-car/branch/master/graph/badge.svg)](https://codecov.io/gh/ipld/go-car/branch/master)

> A library to interact with merkledags stored as a single file

This is an implementation in Go of the [CAR spec](https://ipld.io/specs/transport/car/).

Note that there are two major module versions:

* [go-car/v2](v2/) is geared towards reading and writing CARv2 files, and also
  supports consuming CARv1 files and using CAR files as an IPFS blockstore.
* go-car v0, in the root directory, just supports reading and writing CARv1 files.

Most users should attempt to use v2, especially for new software.

## Maintainers

[Daniel Martí](https://github.com/mvdan) and [Masih Derkani](https://github.com/masih).

## Contribute

PRs are welcome!

Small note: If editing the Readme, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

Apache-2.0/MIT © Protocol Labs
