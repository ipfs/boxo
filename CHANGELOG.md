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

- `bitswap/server` minor memory use and performance improvements
- `bitswap` unify logger names to use uniform format bitswap/path/pkgname
- `gateway` now always returns meaningful cache-control headers for generated HTML listings of UnixFS directories

### Removed

### Fixed

- `boxo/gateway` now returns 404 Status Not Found instead of 500 when the requested data cannot be found, without a fallback on bitswap or similar restriction.
- `bitswap/client` fix memory leak in BlockPresenceManager due to unlimited map growth.

### Security

## [v0.21.0]

### Changed

- `boxo/gateway` is now tested against [gateway-conformance v6](https://github.com/ipfs/gateway-conformance/releases/tag/v0.6.0)
- `bitswap/client` supports additional tracing

### Removed

* 🛠 `routing/none` removed `ConstructNilRouting`, if you need this functionality you can use the Null Router from [go-libp2p-routing-helpers](https://github.com/libp2p/go-libp2p-routing-helpers).

### Fixed

- `routing/http`: the `FindPeer` now returns `routing.ErrNotFound` when no addresses are found
- `routing/http`: the `FindProvidersAsync` no longer causes a goroutine buildup
- `bitswap`: wantlist overflow handling now cancels existing entries to make room for newer entries. This fix prevents the wantlist from filling up with CIDs that the server does not have.

## [v0.20.0]

### Added

* ✨ `gateway` has new backend possibilities:
  * `NewRemoteBlocksBackend` allows you to create a gateway backend that uses one or multiple other gateways as backend. These gateways must support RAW block requests (`application/vnd.ipld.raw`), as well as IPNS Record requests (`application/vnd.ipfs.ipns-record`). With this, we also introduced `NewCacheBlockStore`, `NewRemoteBlockstore` and `NewRemoteValueStore`.
  * `NewRemoteCarBackend` allows you to create a gateway backend that uses one or multiple Trustless Gateways as backend. These gateways must support CAR requests (`application/vnd.ipld.car`), as well as the extensions describe in [IPIP-402](https://specs.ipfs.tech/ipips/ipip-0402/). With this, we also introduced `NewCarBackend`, `NewRemoteCarFetcher` and `NewRetryCarFetcher`.
* `gateway` now sets the [`Content-Location`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Location) header for requests with non-default content format, as a result of content negotiation. This allows generic and misconfigured HTTP caches to store Deserialized, CAR and Block responses separately, under distinct cache keys.
* `gateway` now supports `car-dups`, `car-order` and `car-version` as query parameters in addition to the `application/vnd.ipld.car` parameters sent via `Accept` header. The parameters in the `Accept` header have always priority, but including them in URL simplifies HTTP caching and allows use in `Content-Location` header on CAR responses to maximize interoperability with wide array of HTTP caches.
* `bitswap/server` now allows to override the default peer ledger with `WithPeerLedger`.

### Fixed

* `routing/http/server` now returns 404 Status Not Found when no records can be found.
* `routing/http/server` now supports legacy RSA PeerIDs encoded as Base58 Multihash

## [v0.19.0]

### Added

* `routing/http/server` now adds `Cache-Control` HTTP header to GET requests: 15 seconds for empty responses, or 5 minutes for responses with providers.
* `routing/http/server` the `/ipns` endpoint is more friendly to users opening URL in web browsers: returns `Content-Disposition` header and defaults to `application/vnd.ipfs.ipns-record` response when `Accept` is missing.
* `provider`:
  * Exports a `NewPrioritizedProvider`, which can be used to prioritize certain providers while ignoring duplicates.
  * 🛠️ `NewPinnedProvider` now prioritizes root blocks, even if `onlyRoots` is set to `false`.

### Changed

* `go` version changed to 1.21

### Fixed

- 🛠️`routing/http/server`: delegated peer routing endpoint now supports both [PeerID string notaitons from libp2p specs](https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md#string-representation).
- `bitswap`: add missing client `WithBlockReceivedNotifier` and `WithoutDuplicatedBlockStats` options to the exchange.

## [v0.18.0]

### Added

- `blockservice` now has `ContextWithSession` and `EmbedSessionInContext` functions, which allows to embed a session in a context. Future calls to `BlockGetter.GetBlock`, `BlockGetter.GetBlocks` and `NewSession` will use the session in the context.
- `blockservice.NewWritethrough` deprecated function has been removed, instead you can do `blockservice.New(..., ..., WriteThrough())` like previously.
- `gateway`: a new header configuration middleware has been added to replace the existing header configuration, which can be used more generically.
- `namesys` now has a `WithMaxCacheTTL` option, which allows you to define a maximum TTL that will be used for caching IPNS entries.

### Fixed

- 🛠 `boxo/gateway`: when making a trustless CAR request with the "entity-bytes" parameter, using a negative index greater than the underlying entity length could trigger reading more data than intended
- 🛠 `boxo/gateway`: the header configuration `Config.Headers` and `AddAccessControlHeaders` has been replaced by the new middleware provided by `NewHeaders`.
- 🛠 `routing/http/client`: the default HTTP client is no longer a global singleton. Therefore, using `WithUserAgent` won't modify the user agent of existing routing clients. This will also prevent potential race conditions. In addition, incompatible options will now return errors instead of silently failing.

## [v0.17.0]

### Added

* 🛠 `pinning/pinner`: you can now give a custom name when pinning a CID. To reflect this, the `Pinner` has been adjusted. Note that calling `Pin` for the same CID with a different name will replace its current name by the newly given name.

### Removed

- 🛠 `tracing` `jaeger` exporter has been removed due to it's deprecation and removal from upstream, you should use `otlp` exporter instead. See the [docs](./docs/tracing.md) for an example.

## [v0.16.0]

### Changed

* 🛠 `boxo/namesys`: now fails when multiple valid DNSLink entries are found for the same domain. This used to cause undefined behavior before. Now, we return an error, according to the [specification](https://dnslink.dev/).

### Removed

* 🛠 `boxo/gateway`: removed support for undocumented legacy `ipfs-404.html`. Use [`_redirects`](https://specs.ipfs.tech/http-gateways/web-redirects-file/) instead.
* 🛠 `boxo/namesys`: removed support for legacy DNSLink entries at the root of the domain. Use [`_dnslink.` TXT record](https://docs.ipfs.tech/concepts/dnslink/) instead.
* 🛠 `boxo/coreapi`, an intrinsic part of Kubo, has been removed and moved to `kubo/core/coreiface`.

### Fixed

* `boxo/gateway`
  * a panic (which is recovered) could sporadically be triggered inside a CAR request, if the right [conditions were met](https://github.com/ipfs/boxo/pull/511).
  * no longer emits `http: superfluous response.WriteHeader` warnings when an error happens.

## [v0.15.0]

### Changed

* 🛠 Bumped to [`go-libp2p` 0.32](https://github.com/libp2p/go-libp2p/releases/tag/v0.32.0).

## [v0.14.0]

### Added

* `boxo/gateway`:
  * A new `WithResolver(...)` option can be used with `NewBlocksBackend(...)` allowing the user to pass their custom `Resolver` implementation.
  * The gateway now sets a `Cache-Control` header for requests under the `/ipns/` namespace if the TTL for the corresponding IPNS Records or DNSLink entities is known.
* `boxo/bitswap/client`:
  * A new `WithoutDuplicatedBlockStats()` option can be used with `bitswap.New` and `bsclient.New`. This disable accounting for duplicated blocks, which requires a `blockstore.Has()` lookup for every received block and thus, can impact performance.
* ✨ Migrated repositories into Boxo
  * [`github.com/ipfs/kubo/peering`](https://pkg.go.dev/github.com/ipfs/kubo/peering) => [`./peering`](./peering)
    A service which establish, overwatch and maintain long lived connections.
  * [`github.com/ipfs/kubo/core/bootstrap`](https://pkg.go.dev/github.com/ipfs/kubo/core/bootstrap) => [`./bootstrap](./bootstrap)
    A service that maintains connections to a number of bootstrap peers.

### Changed

* `boxo/gateway`
  * 🛠 The `IPFSBackend` interface was updated to make the responses of the
    `Head` method more explicit. It now returns a `HeadResponse` instead of a
    `files.Node`.
* `boxo/routing/http/client.Client` is now exported. This means you can now pass
  it around functions, or add it to a struct if you want.
* 🛠 The `path` package has been massively refactored. With this refactor, we have
  condensed the different path-related and/or Kubo-specific packages under a single generic one. Therefore, there
  are many breaking changes. Please consult the [documentation](https://pkg.go.dev/github.com/ipfs/boxo/path)
  for more details on how to use the new package.
  * Note: content paths created with `boxo/path` are automatically normalized:
    - Replace multiple slashes with a single slash.
    - Eliminate each `.` path name element (the current directory).
    - Eliminate each inner `..` path name element (the parent directory) along with the non-`..` element that precedes it.
    - Eliminate `..` elements that begin a rooted path: that is, replace "`/..`" by "`/`" at the beginning of a path.
* 🛠 The signature of `CoreAPI.ResolvePath` in  `coreiface` has changed to now return
  the remainder segments as a second return value, matching the signature of `resolver.ResolveToLastNode`.
* 🛠 `routing/http/client.FindPeers` now returns `iter.ResultIter[types.PeerRecord]` instead of `iter.ResultIter[types.Record]`. The specification indicates that records for this method will always be Peer Records.
* 🛠 The `namesys` package has been refactored. The following are the largest modifications:
  * The options in `coreiface/options/namesys` have been moved to `namesys` and their names
    have been made more consistent.
  * Many of the exported structs and functions have been renamed in order to be consistent with
    the remaining packages.
  * `namesys.Resolver.Resolve` now returns a TTL, in addition to the resolved path. If the
    TTL is unknown, 0 is returned. `IPNSResolver` is able to resolve a TTL, while `DNSResolver`
    is not.
  * `namesys/resolver.ResolveIPNS` has been moved to `namesys.ResolveIPNS` and now returns a TTL
    in addition to the resolved path.
* ✨ `boxo/ipns` record defaults follow recommendations from [IPNS Record Specification](https://specs.ipfs.tech/ipns/ipns-record/#ipns-record):
    * `DefaultRecordTTL` is now set to `1h`
    * `DefaultRecordLifetime` follows the increased expiration window of Amino DHT ([go-libp2p-kad-dht#793](https://github.com/libp2p/go-libp2p-kad-dht/pull/793)) and is set to `48h`
* 🛠 The `gateway`'s `IPFSBackend.ResolveMutable` is now expected to return a TTL in addition to
    the resolved path. If the TTL is unknown, 0 should be returned.

### Removed

* 🛠 `util.MultiErr` has been removed. Please use Go's native support for wrapping errors, or `errors.Join` instead.

### Fixed

### Security

## [v0.13.1]

### Added

* An option `DisableHTMLErrors` has been added to `gateway.Config`. When this option
  is `true`, pretty HTML error pages for web browsers are disabled. Instead, a
  `text/plain` page with the raw error message as the body is returned.

### Changed

### Removed

### Fixed

### Security

## [v0.13.0]

### Added

* ✨ The `routing/http` implements Delegated Peer Routing introduced in [IPIP-417](https://github.com/ipfs/specs/pull/417).

### Changed

* 🛠 The `routing/http` package received the following modifications:
  * Client `GetIPNSRecord` and `PutIPNSRecord` have been renamed to `GetIPNS` and
    `PutIPNS`, respectively. Similarly, the required function names in the server
    `ContentRouter` have also been updated.
  * `ReadBitswapProviderRecord` has been renamed to `BitswapRecord` and marked as deprecated.
    From now on, please use the protocol-agnostic `PeerRecord` for most use cases. The new
    Peer Schema has been introduced in [IPIP-417](https://github.com/ipfs/specs/pull/417).

### Removed

* 🛠 The `routing/http` package experienced following removals:
  * Server and client no longer support the experimental `Provide` method.
    `ProvideBitswap` is still usable, but marked as deprecated. A protocol-agnostic
    provide mechanism is being worked on in [IPIP-378](https://github.com/ipfs/specs/pull/378).
  * Server no longer exports `FindProvidersPath` and `ProvidePath`.

### Fixed

* The normalization of DNSLink identifiers in `gateway` has been corrected in the edge
  case where the value passed to the path component of the URL is already normalized.

### Security

## [v0.12.0]

### Added

* The `routing/http` client and server now support Delegated IPNS at `/routing/v1`
  as per [IPIP-379](https://specs.ipfs.tech/ipips/ipip-0379/).
* 🛠 The `verifycid` package has been updated with the new Allowlist interface as part of
  reducing globals efforts.
* The `blockservice` and `provider` packages has been updated to accommodate for
  changes in `verifycid`.

### Changed

* 🛠 `blockservice.New` now accepts a variadic of func options following the [Functional
  Options pattern](https://www.sohamkamani.com/golang/options-pattern/).

### Removed

### Fixed

- HTTP Gateway API: Not having a block will result in a 5xx error rather than 404
- HTTP Gateway API: CAR requests will return 200s and a CAR file proving a requested path does not exist rather than returning an error
- 🛠 `MultiFileReader` has been updated with a new header with the encoded file name instead of the plain filename, due to a regression found in  [`net/textproto`](https://github.com/golang/go/issues/60674). This only affects files with binary characters in their name. By keeping the old header, we maximize backwards compatibility.
  |            | New Client | Old Client  |
  |------------|------------|-------------|
  | New Server | ✅         | 🟡*         |
  | Old Server | ✅         | ✅          |
   *Old clients can only send Unicode file paths to the server.

### Security

## [v0.11.0]

### Added

* ✨ The gateway now supports the optional `order` and `dups` CAR parameters
  from [IPIP-412](https://github.com/ipfs/specs/pull/412).
  * The `BlocksBackend` only implements `order=dfs` (Depth-First Search)
    ordering, which was already the default behavior.
  * If a request specifies no `dups`, response with `dups=n` is returned, which
    was already the default behavior.
  * If a request explicitly specifies a CAR `order` other than `dfs`, it will
    result in an error.
  * The only change to the default behavior on CAR responses is that we follow
    IPIP-412 and make `order=dfs;dups=n` explicit in the returned
    `Content-Type` HTTP header.
* ✨ While the call signature remains the same, the blocks that Bitswap returns can now be cast to [traceability.Block](./bitswap/client/traceability/block.go), which will additionally tell you where the Block came from and how long it took to fetch. This helps consumers of Bitswap collect better metrics on Bitswap behavior.

### Changed

* 🛠 The `ipns` package has been refactored.
  * You should no longer use the direct Protobuf version of the IPNS Record.
    Instead, we have a shiny new `ipns.Record` type that wraps all the required
    functionality to work the best as possible with IPNS v2 Records. Please
    check the [documentation](https://pkg.go.dev/github.com/ipfs/boxo/ipns) for
    more information, and follow
    [ipfs/specs#376](https://github.com/ipfs/specs/issues/376) for related
    IPIP.
  * There is no change to IPNS Records produced by `boxo/ipns`, it still
    produces both V1 and V2 signatures by default, it is still backward-compatible.

### Removed

- 🛠 `ipld/car`  has been removed. Please use [ipld/go-car](https://github.com/ipld/go-car) instead.
  More information regarding this decision can be found in [issue 218](https://github.com/ipfs/boxo/issues/218).

### Fixed

- Removed mentions of unused ARC algorithm ([#336](https://github.com/ipfs/boxo/issues/366#issuecomment-1597253540))
- Handle `_redirects` file when `If-None-Match` header is present ([#412](https://github.com/ipfs/boxo/pull/412))

### Security

## [0.10.3] - 2023-08-08

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
  - A routing system is now passed with the `provider.Online` option, by default the system run in offline mode (push stuff onto the queue).
  - When using `provider.Online` calling the `.Run` method is not required anymore, the background worker is implicitly started in the background by `provider.New`.
  - You do not have to pass a queue anymore, you pass a `datastore.Datastore` exclusively.
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
