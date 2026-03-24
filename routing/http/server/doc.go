// Package server implements an HTTP server for the [Delegated Routing V1] API
// (IPIP-337).
//
// The server handles requests for content providers, peer records, IPNS
// records, and DHT closest peers. It supports both JSON and streaming NDJSON
// response formats.
//
// # Basic Usage
//
//	router := ... // your DelegatedRouter implementation
//	handler := server.Handler(router)
//	http.ListenAndServe(":8080", handler)
//
// # Options
//
//   - [WithStreamingResultsDisabled]: Return only JSON, no NDJSON streaming
//   - [WithRecordsLimit]: Maximum records per non-streaming response (default: 20)
//   - [WithStreamingRecordsLimit]: Maximum records per streaming response (default: unlimited)
//   - [WithRoutingTimeout]: Timeout for routing operations (default: 30s)
//   - [WithPrometheusRegistry]: Enable Prometheus metrics
//
// # DelegatedRouter Interface
//
// Implement [DelegatedRouter] to provide the routing backend. The interface
// covers finding providers, finding peers, IPNS get/put, and DHT closest
// peers.
//
// [Delegated Routing V1]: https://specs.ipfs.tech/routing/http-routing-v1/
package server
