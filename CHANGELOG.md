# Changelog

All notable changes to this project will be documented in this file.

Note:
* The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
* This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
( More notes about versioning and our release policies are [here](./RELEASE.md).

## Legend
The following emojis are used to highlight certain changes:
* 🛠 - BREAKING CHANGE.  Action is required if you use this functionality.
* ✨ - Noteworthy change to be aware of.

## [Unreleased]

### Added

### Changed

### Removed

### Fixed

- Handle `_redirects` file when `If-None-Match` header is present ([#412](https://github.com/ipfs/boxo/pull/412))

### Security

## [0.10.2] - 2023-06-29

### Fixed

- Gateway: include CORS on subdomain redirects.
- Gateway: ensure 'X-Ipfs-Root' header is valid.

## [0.10.1] - 2023-06-19

### Added

None.

### Changed

None.

### Removed

None.

### Fixed

- Allow CAR requests with a path when `DeserializedResponses` is `false`.

### Security

None.

## [0.10.0] - 2023-06-09

### Added

* ✨ The gateway now supports partial CAR exports via query parameters from [IPIP-402](https://github.com/ipfs/specs/pull/402).

### Changed

* 🛠 A few trivial breaking changes have been done to the gateway:
  * The signature of `IPFSBackend.GetCAR` has been adapted to support [IPIP-402](https://github.com/ipfs/specs/pull/402) CAR Parameters.
  * A few variables have been renamed for consistency:
    * `WithHostname` -> `NewHostnameHandler`
    * `Specification` -> `PublicGateway`
    * `NewErrorResponse` -> `NewErrorStatusCode`
    * `NewErrorResponseForCode` -> `NewErrorStatusCodeFromStatus`
    * `BlocksGateway` -> `BlocksBackend`
    * `BlocksGatewayOption` -> `BlocksBackendOption`
    * `NewBlocksGateway` -> `NewBlocksBackend`
  * Some functions that are not supposed to be outside of the package were removed: `ServeContent`.

### Removed

None.

### Fixed

None.

### Security

None.

## [0.9.0] - 2023-06-08

### Added

- ✨ `gateway` The gateway were updated to provide better features for users and gateway implementers:
  - New human-friendly error messages.
  - Updated, higher-definition icons in directory listings.
  - Customizable menu items next to "About IPFS" and "Install IPFS".
  - Valid DAG-CBOR and DAG-JSON blocks now provide a preview, where links can be followed.
- `ipns` add `ValidateWithPeerID` and `UnmarshalIpnsEntry` helpers. (https://github.com/ipfs/boxo/pulls/292)
- 🛠 `coreiface/tests` add `*testing.T` argument to the swarm provider. (https://github.com/ipfs/boxo/pulls/321)

### Changed

- 🛠 `boxo/pinner` some listing methods have been changed to now return a `<-chan StreamedCid`.  This allows the consumption of pins *while* the pinner is listing them, which for large pinset can take a long time. (https://github.com/ipfs/boxo/pulls/336)
  The concerned methods are:
  - `DirectKeys`
  - `RecursiveKeys`
  - `InternalKeys`
- 🛠 `provider/batched.New` has been moved to `provider.New` and arguments has been changed. (https://github.com/ipfs/boxo/pulls/273)
  - a routing system is now passed with the `provider.Online` option, by default the system run in offline mode (push stuff onto the queue); and
  - you do not have to pass a queue anymore, you pass a `datastore.Datastore` exclusively.
- 🛠 `provider.NewOfflineProvider` has been renamed to `provider.NewNoopProvider` to show more clearly that is does nothing. (https://github.com/ipfs/boxo/pulls/273)
- 🛠 `provider.Provider` and `provider.Reprovider` has been merged under one `provider.System`. (https://github.com/ipfs/boxo/pulls/273)
- 🛠 `routing/http` responses now return a streaming `iter.ResultIter` generic interface. (https://github.com/ipfs/boxo/pulls/18)
- 🛠 `coreiface` add options and `AllowOffline` option to `RoutingAPI.Put`. (https://github.com/ipfs/boxo/pulls/278)
- 🛠 `gateway` now has deserialized responses turned off by default. This can be configured via `DeserializedResponses`. (https://github.com/ipfs/boxo/pull/252)

### Removed

- 🛠 `provider/queue` has been moved to `provider/internal/queue`. (https://github.com/ipfs/boxo/pulls/273)
- 🛠 `provider/simple` has been removed, now instead you can use `provider.New` because it accept non batched routing systems and use type assertion for the `ProvideMany` call, giving a single implementation. (https://github.com/ipfs/boxo/pulls/273)
- 🛠 `provider.NewSystem` has been removed, `provider.New` now returns a `provider.System` directly. (https://github.com/ipfs/boxo/pulls/273)

### Fixed

- `gateway` fix panics by returning in all error cases. (https://github.com/ipfs/boxo/pulls/314)
- `gateway` avoid duplicate payload during subdomain redirects. (https://github.com/ipfs/boxo/pulls/326)
- `gateway` correctly handle question marks in URL when redirecting. (https://github.com/ipfs/boxo/pulls/#313)

### Security

None

## [0.8.1] - 2023-04-25

### Added

- `gateway` trace context header support (https://github.com/ipfs/boxo/pull/256)

### Changed

- `gateway` widen duration histograms and cleanup (https://github.com/ipfs/boxo/pull/265)

### Deprecated

None

### Removed

None

### Fixed

- `gateway` panic on path without enough components (https://github.com/ipfs/boxo/pull/272)

### Security

None

## [0.8.0] - 2023-04-05
### Added

- ✨ Migrated repositories into Boxo (https://github.com/ipfs/boxo/pull/220)
  - github.com/ipfs/interface-go-ipfs-core => ./coreiface
  - github.com/ipfs/go-pinning-service-http-client => ./pinning/remote/client
  - github.com/ipfs/go-path => ./path
  - github.com/ipfs/go-namesys => ./namesys
  - github.com/ipfs/go-mfs => ./mfs
  - github.com/ipfs/go-ipfs-provider => ./provider
  - github.com/ipfs/go-ipfs-pinner => ./pinning/pinner
  - github.com/ipfs/go-ipfs-keystore => ./keystore
  - github.com/ipfs/go-filestore => ./filestore
  - github.com/ipfs/go-ipns => ./ipns
  - github.com/ipfs/go-blockservice => ./blockservice
  - github.com/ipfs/go-ipfs-chunker => ./chunker
  - github.com/ipfs/go-fetcher => ./fetcher
  - github.com/ipfs/go-ipfs-blockstore => ./blockstore
  - github.com/ipfs/go-ipfs-posinfo => ./filestore/posinfo
  - github.com/ipfs/go-ipfs-util => ./util
  - github.com/ipfs/go-ipfs-ds-help => ./datastore/dshelp
  - github.com/ipfs/go-verifcid => ./verifcid
  - github.com/ipfs/go-ipfs-exchange-offline => ./exchange/offline
  - github.com/ipfs/go-ipfs-routing => ./routing
  - github.com/ipfs/go-ipfs-exchange-interface => ./exchange
  - github.com/ipfs/go-unixfs => ./ipld/unixfs
  - github.com/ipfs/go-merkledag => ./ipld/merkledag
  - github.com/ipld/go-car => ./ipld/car
- ✨ Added a migration tool to aid in migrating from the migrated repositories to Boxo, see the documentation here: https://github.com/ipfs/boxo/blob/main/README.md#migrating-to-boxo (https://github.com/ipfs/boxo/pull/226)
  - Added a check to ensure the migration tool is only run in a Git repository (with an optional override flag)
- ✨ Added tracing and metrics to the refactored gateway for its IPFS backend


### Changed

- Removed a mention of "bitswap" in blockservice debug logs
- Changed the Bitswap message package from "bitswap.message.pb" to "bitswap.message.v1.pb" to avoid protobuf panics due to duplicate registration with [go-bitswap](https://github.com/ipfs/go-bitswap) (https://github.com/ipfs/boxo/pull/212)
- ✨ Remove a busyloop in blockservice getBlocks by removing batching when caching (https://github.com/ipfs/boxo/pull/232)

### Deprecated

None

### Removed

None

### Fixed

- Ensure dag-cbor/json codecs are registered in the gateway handler (https://github.com/ipfs/boxo/pull/223)
- ✨ Refactor the Gateway API to operate on higher level semantics (https://github.com/ipfs/boxo/pull/176)
- Fixed a panic in the gateway handler when returning errors (https://github.com/ipfs/boxo/pull/255)

### Security

None
