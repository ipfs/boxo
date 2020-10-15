# Failure

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Error** | [**FailureError**](Failure_error.md) |  | 

## Methods

### NewFailure

`func NewFailure(error_ FailureError, ) *Failure`

NewFailure instantiates a new Failure object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewFailureWithDefaults

`func NewFailureWithDefaults() *Failure`

NewFailureWithDefaults instantiates a new Failure object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetError

`func (o *Failure) GetError() FailureError`

GetError returns the Error field if non-nil, zero value otherwise.

### GetErrorOk

`func (o *Failure) GetErrorOk() (*FailureError, bool)`

GetErrorOk returns a tuple with the Error field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetError

`func (o *Failure) SetError(v FailureError)`

SetError sets Error field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


