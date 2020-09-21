# PinStatus

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Requestid** | **string** | Globally unique identifier of the pin request; can be used to check the status of ongoing pinning, or pin removal | 
**Status** | [**Status**](Status.md) |  | 
**Created** | [**time.Time**](time.Time.md) | Immutable timestamp indicating when a pin request entered a pinning service; can be used for filtering results and pagination | 
**Pin** | [**Pin**](Pin.md) |  | 
**Delegates** | **[]string** | List of multiaddrs designated by pinning service for transferring any new data from external peers | 
**Info** | Pointer to **map[string]string** | Optional info for PinStatus response | [optional] 

## Methods

### NewPinStatus

`func NewPinStatus(requestid string, status Status, created time.Time, pin Pin, delegates []string, ) *PinStatus`

NewPinStatus instantiates a new PinStatus object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewPinStatusWithDefaults

`func NewPinStatusWithDefaults() *PinStatus`

NewPinStatusWithDefaults instantiates a new PinStatus object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetRequestid

`func (o *PinStatus) GetRequestid() string`

GetRequestid returns the Requestid field if non-nil, zero value otherwise.

### GetRequestidOk

`func (o *PinStatus) GetRequestidOk() (*string, bool)`

GetRequestidOk returns a tuple with the Requestid field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRequestid

`func (o *PinStatus) SetRequestid(v string)`

SetRequestid sets Requestid field to given value.


### GetStatus

`func (o *PinStatus) GetStatus() Status`

GetStatus returns the Status field if non-nil, zero value otherwise.

### GetStatusOk

`func (o *PinStatus) GetStatusOk() (*Status, bool)`

GetStatusOk returns a tuple with the Status field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetStatus

`func (o *PinStatus) SetStatus(v Status)`

SetStatus sets Status field to given value.


### GetCreated

`func (o *PinStatus) GetCreated() time.Time`

GetCreated returns the Created field if non-nil, zero value otherwise.

### GetCreatedOk

`func (o *PinStatus) GetCreatedOk() (*time.Time, bool)`

GetCreatedOk returns a tuple with the Created field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCreated

`func (o *PinStatus) SetCreated(v time.Time)`

SetCreated sets Created field to given value.


### GetPin

`func (o *PinStatus) GetPin() Pin`

GetPin returns the Pin field if non-nil, zero value otherwise.

### GetPinOk

`func (o *PinStatus) GetPinOk() (*Pin, bool)`

GetPinOk returns a tuple with the Pin field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPin

`func (o *PinStatus) SetPin(v Pin)`

SetPin sets Pin field to given value.


### GetDelegates

`func (o *PinStatus) GetDelegates() []string`

GetDelegates returns the Delegates field if non-nil, zero value otherwise.

### GetDelegatesOk

`func (o *PinStatus) GetDelegatesOk() (*[]string, bool)`

GetDelegatesOk returns a tuple with the Delegates field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDelegates

`func (o *PinStatus) SetDelegates(v []string)`

SetDelegates sets Delegates field to given value.


### GetInfo

`func (o *PinStatus) GetInfo() map[string]string`

GetInfo returns the Info field if non-nil, zero value otherwise.

### GetInfoOk

`func (o *PinStatus) GetInfoOk() (*map[string]string, bool)`

GetInfoOk returns a tuple with the Info field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetInfo

`func (o *PinStatus) SetInfo(v map[string]string)`

SetInfo sets Info field to given value.

### HasInfo

`func (o *PinStatus) HasInfo() bool`

HasInfo returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


