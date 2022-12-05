go-libipfs
=======================

> A library for building IPFS implementations

Go-libips is a library for building IPFS implementations and tools in Go. It contains reusable functionality useful for interacting and experimenting with IPFS.

This is also used by [Kubo](https://github.com/ipfs/kubo) for its core functionality.

Currently this library is a target for consolidating Go IPFS repositories, and will receive minor version releases as repositories are consolidated into it. We are initially focused on merely consolidating repositories, *not* refactoring across packages. Once repositories are mostly consolidated, *then* we will begin refactoring this library holistically. Individual components can still be worked on and refactored individually, but please refrain from trying to refactor across components.

## Contributing

Contributions are welcome! This repository is part of the IPFS project and therefore governed by our [contributing guidelines](https://github.com/ipfs/community/blob/master/CONTRIBUTING.md).

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
