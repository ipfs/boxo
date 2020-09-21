# \PinsApi

All URIs are relative to *https://pinning-service.example.com*

Method | HTTP request | Description
------------- | ------------- | -------------
[**PinsGet**](PinsApi.md#PinsGet) | **Get** /pins | List pin objects
[**PinsPost**](PinsApi.md#PinsPost) | **Post** /pins | Add pin object
[**PinsRequestidDelete**](PinsApi.md#PinsRequestidDelete) | **Delete** /pins/{requestid} | Remove pin object
[**PinsRequestidGet**](PinsApi.md#PinsRequestidGet) | **Get** /pins/{requestid} | Get pin object
[**PinsRequestidPost**](PinsApi.md#PinsRequestidPost) | **Post** /pins/{requestid} | Replace pin object



## PinsGet

> PinResults PinsGet(ctx).Cid(cid).Name(name).Status(status).Before(before).After(after).Limit(limit).Meta(meta).Execute()

List pin objects



### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    openapiclient "./openapi"
)

func main() {
    cid := []string{"Inner_example"} // []string | Return pin objects responsible for pinning the specified CID(s); be aware that using longer hash functions introduces further constraints on the number of CIDs that will fit under the limit of 2000 characters per URL  in browser contexts (optional)
    name := "name_example" // string | Return pin objects with names that contain provided value (case-insensitive, partial or full match) (optional)
    status := []Status{openapiclient.Status{}} // []Status | Return pin objects for pins with the specified status (optional)
    before := Get-Date // time.Time | Return results created (queued) before provided timestamp (optional)
    after := Get-Date // time.Time | Return results created (queued) after provided timestamp (optional)
    limit := 987 // int32 | Max records to return (optional) (default to 10)
    meta := map[string]string{ "Key" = "Value" } // map[string]string | Return pin objects that match specified metadata (optional)

    configuration := openapiclient.NewConfiguration()
    api_client := openapiclient.NewAPIClient(configuration)
    resp, r, err := api_client.PinsApi.PinsGet(context.Background(), ).Cid(cid).Name(name).Status(status).Before(before).After(after).Limit(limit).Meta(meta).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PinsApi.PinsGet``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `PinsGet`: PinResults
    fmt.Fprintf(os.Stdout, "Response from `PinsApi.PinsGet`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiPinsGetRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **cid** | [**[]string**](string.md) | Return pin objects responsible for pinning the specified CID(s); be aware that using longer hash functions introduces further constraints on the number of CIDs that will fit under the limit of 2000 characters per URL  in browser contexts | 
 **name** | **string** | Return pin objects with names that contain provided value (case-insensitive, partial or full match) | 
 **status** | [**[]Status**](Status.md) | Return pin objects for pins with the specified status | 
 **before** | **time.Time** | Return results created (queued) before provided timestamp | 
 **after** | **time.Time** | Return results created (queued) after provided timestamp | 
 **limit** | **int32** | Max records to return | [default to 10]
 **meta** | [**map[string]string**](string.md) | Return pin objects that match specified metadata | 

### Return type

[**PinResults**](PinResults.md)

### Authorization

[accessToken](../README.md#accessToken)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## PinsPost

> PinStatus PinsPost(ctx).Pin(pin).Execute()

Add pin object



### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    openapiclient "./openapi"
)

func main() {
    pin := openapiclient.Pin{Cid: "Cid_example", Name: "Name_example", Origins: []string{"Origins_example"), Meta: map[string]string{ "Key" = "Value" }} // Pin | 

    configuration := openapiclient.NewConfiguration()
    api_client := openapiclient.NewAPIClient(configuration)
    resp, r, err := api_client.PinsApi.PinsPost(context.Background(), pin).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PinsApi.PinsPost``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `PinsPost`: PinStatus
    fmt.Fprintf(os.Stdout, "Response from `PinsApi.PinsPost`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiPinsPostRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **pin** | [**Pin**](Pin.md) |  | 

### Return type

[**PinStatus**](PinStatus.md)

### Authorization

[accessToken](../README.md#accessToken)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## PinsRequestidDelete

> PinsRequestidDelete(ctx, requestid).Execute()

Remove pin object



### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    openapiclient "./openapi"
)

func main() {
    requestid := "requestid_example" // string | 

    configuration := openapiclient.NewConfiguration()
    api_client := openapiclient.NewAPIClient(configuration)
    resp, r, err := api_client.PinsApi.PinsRequestidDelete(context.Background(), requestid).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PinsApi.PinsRequestidDelete``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**requestid** | **string** |  | 

### Other Parameters

Other parameters are passed through a pointer to a apiPinsRequestidDeleteRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

 (empty response body)

### Authorization

[accessToken](../README.md#accessToken)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## PinsRequestidGet

> PinStatus PinsRequestidGet(ctx, requestid).Execute()

Get pin object



### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    openapiclient "./openapi"
)

func main() {
    requestid := "requestid_example" // string | 

    configuration := openapiclient.NewConfiguration()
    api_client := openapiclient.NewAPIClient(configuration)
    resp, r, err := api_client.PinsApi.PinsRequestidGet(context.Background(), requestid).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PinsApi.PinsRequestidGet``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `PinsRequestidGet`: PinStatus
    fmt.Fprintf(os.Stdout, "Response from `PinsApi.PinsRequestidGet`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**requestid** | **string** |  | 

### Other Parameters

Other parameters are passed through a pointer to a apiPinsRequestidGetRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**PinStatus**](PinStatus.md)

### Authorization

[accessToken](../README.md#accessToken)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## PinsRequestidPost

> PinStatus PinsRequestidPost(ctx, requestid).Pin(pin).Execute()

Replace pin object



### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    openapiclient "./openapi"
)

func main() {
    requestid := "requestid_example" // string | 
    pin := openapiclient.Pin{Cid: "Cid_example", Name: "Name_example", Origins: []string{"Origins_example"), Meta: map[string]string{ "Key" = "Value" }} // Pin | 

    configuration := openapiclient.NewConfiguration()
    api_client := openapiclient.NewAPIClient(configuration)
    resp, r, err := api_client.PinsApi.PinsRequestidPost(context.Background(), requestid, pin).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PinsApi.PinsRequestidPost``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `PinsRequestidPost`: PinStatus
    fmt.Fprintf(os.Stdout, "Response from `PinsApi.PinsRequestidPost`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**requestid** | **string** |  | 

### Other Parameters

Other parameters are passed through a pointer to a apiPinsRequestidPostRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **pin** | [**Pin**](Pin.md) |  | 

### Return type

[**PinStatus**](PinStatus.md)

### Authorization

[accessToken](../README.md#accessToken)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

