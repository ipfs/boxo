# FailureError

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Reason** | **string** | Mandatory string identifying the type of error | 
**Details** | Pointer to **string** | Optional, longer description of the error; may include UUID of transaction for support, links to documentation etc | [optional] 

## Methods

### NewFailureError

`func NewFailureError(reason string, ) *FailureError`

NewFailureError instantiates a new FailureError object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewFailureErrorWithDefaults

`func NewFailureErrorWithDefaults() *FailureError`

NewFailureErrorWithDefaults instantiates a new FailureError object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetReason

`func (o *FailureError) GetReason() string`

GetReason returns the Reason field if non-nil, zero value otherwise.

### GetReasonOk

`func (o *FailureError) GetReasonOk() (*string, bool)`

GetReasonOk returns a tuple with the Reason field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReason

`func (o *FailureError) SetReason(v string)`

SetReason sets Reason field to given value.


### GetDetails

`func (o *FailureError) GetDetails() string`

GetDetails returns the Details field if non-nil, zero value otherwise.

### GetDetailsOk

`func (o *FailureError) GetDetailsOk() (*string, bool)`

GetDetailsOk returns a tuple with the Details field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDetails

`func (o *FailureError) SetDetails(v string)`

SetDetails sets Details field to given value.

### HasDetails

`func (o *FailureError) HasDetails() bool`

HasDetails returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


