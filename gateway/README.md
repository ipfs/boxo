# IPFS Gateway

> A reference implementation of HTTP Gateway Specifications.

## Documentation

* Go Documentation: https://pkg.go.dev/github.com/ipfs/boxo/gateway
* Gateway Specification: https://github.com/ipfs/specs/tree/main/http-gateways#readme
* Types of HTTP Gateways: https://docs.ipfs.tech/how-to/address-ipfs-on-web/#http-gateways
## Example

```go
// Initialize your headers and apply the default headers.
headers := map[string][]string{}
gateway.AddAccessControlHeaders(headers)

conf := gateway.Config{
  Headers:  headers,
}

// Initialize a NodeAPI interface for both an online and offline versions.
// The offline version should not make any network request for missing content.
ipfs := ...

// Create http mux and setup path gateway handler.
mux := http.NewServeMux()
gwHandler := gateway.NewHandler(conf, ipfs)
mux.Handle("/ipfs/", gwHandler)
mux.Handle("/ipns/", gwHandler)

// Start the server on :8080 and voilá! You have a basic IPFS gateway running
// in http://localhost:8080.
_ = http.ListenAndServe(":8080", mux)
```
