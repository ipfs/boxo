# \PinsApi

All URIs are relative to *https://pinning-service.example.com*

Method | HTTP request | Description
------------- | ------------- | -------------
[**PinsGet**](PinsApi.md#PinsGet) | **Get** /pins | List pin objects
[**PinsIdDelete**](PinsApi.md#PinsIdDelete) | **Delete** /pins/{id} | Remove pin object
[**PinsIdGet**](PinsApi.md#PinsIdGet) | **Get** /pins/{id} | Get pin object
[**PinsIdPost**](PinsApi.md#PinsIdPost) | **Post** /pins/{id} | Modify pin object
[**PinsPost**](PinsApi.md#PinsPost) | **Post** /pins | Add pin object



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
    cid := []string{"Inner_example"} // []string | Return pin objects responsible for pinning the specified CID(s) (optional)
    name := "name_example" // string | Return pin objects with names that contain provided value (partial or full match) (optional)
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
 **cid** | [**[]string**](string.md) | Return pin objects responsible for pinning the specified CID(s) | 
 **name** | **string** | Return pin objects with names that contain provided value (partial or full match) | 
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


## PinsIdDelete

> PinsIdDelete(ctx, id).Execute()

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
    id := "id_example" // string | 

    configuration := openapiclient.NewConfiguration()
    api_client := openapiclient.NewAPIClient(configuration)
    resp, r, err := api_client.PinsApi.PinsIdDelete(context.Background(), id).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PinsApi.PinsIdDelete``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**id** | **string** |  | 

### Other Parameters

Other parameters are passed through a pointer to a apiPinsIdDeleteRequest struct via the builder pattern


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


## PinsIdGet

> PinStatus PinsIdGet(ctx, id).Execute()

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
    id := "id_example" // string | 

    configuration := openapiclient.NewConfiguration()
    api_client := openapiclient.NewAPIClient(configuration)
    resp, r, err := api_client.PinsApi.PinsIdGet(context.Background(), id).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PinsApi.PinsIdGet``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `PinsIdGet`: PinStatus
    fmt.Fprintf(os.Stdout, "Response from `PinsApi.PinsIdGet`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**id** | **string** |  | 

### Other Parameters

Other parameters are passed through a pointer to a apiPinsIdGetRequest struct via the builder pattern


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


## PinsIdPost

> PinStatus PinsIdPost(ctx, id).Pin(pin).Execute()

Modify pin object



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
    id := "id_example" // string | 
    pin := openapiclient.Pin{Cid: "Cid_example", Name: "Name_example", Origins: []string{"Origins_example"), Meta: map[string]string{ "Key" = "Value" }} // Pin | 

    configuration := openapiclient.NewConfiguration()
    api_client := openapiclient.NewAPIClient(configuration)
    resp, r, err := api_client.PinsApi.PinsIdPost(context.Background(), id, pin).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PinsApi.PinsIdPost``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `PinsIdPost`: PinStatus
    fmt.Fprintf(os.Stdout, "Response from `PinsApi.PinsIdPost`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**id** | **string** |  | 

### Other Parameters

Other parameters are passed through a pointer to a apiPinsIdPostRequest struct via the builder pattern


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

