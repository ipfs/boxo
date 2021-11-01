car - The CLI tool
==================

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](https://protocol.ai)
[![](https://img.shields.io/badge/project-ipld-orange.svg?style=flat-square)](https://github.com/ipld/ipld)
[![](https://img.shields.io/badge/matrix-%23ipld-blue.svg?style=flat-square)](https://matrix.to/#/#ipld:ipfs.io)

> A CLI to interact with car files

## Usage

```
USAGE:
   car [global options] command [command options] [arguments...]

COMMANDS:
   create, c      Create a car file
   detach-index   Detach an index to a detached file
   filter, f      Filter the CIDs in a car
   get-block, gb  Get a block out of a car
   get-dag, gd    Get a dag out of a car
   index, i       write out the car with an index
   list, l        List the CIDs in a car
   verify, v      Verify a CAR is wellformed
   help, h        Shows a list of commands or help for one command
```

## Install

To install the latest version of `car` module, run:
```shell script
go install github.com/ipld/go-car/cmd/car
```
