## Updating Pinning Service Spec

Download the openapi-generator from https://github.com/OpenAPITools/openapi-generator and generate the code using:

Current code generated with: openapi-generator 5.0.0-beta

```
openapi-generator generate -g go-experimental -i https://raw.githubusercontent.com/ipfs/pinning-services-api-spec/master/ipfs-pinning-service.yaml -o openapi
rm openapi/go.mod openapi/go.sum
```

Notes:
Due to https://github.com/OpenAPITools/openapi-generator/issues/7473 the code generator the http error codes processing
may need some manual editing.

`go-experimental` is becoming mainstream and so in later versions will be replaced with `go`
