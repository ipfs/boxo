# Pin

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Cid** | **string** | Content Identifier (CID) to be pinned recursively | 
**Name** | Pointer to **string** | Optional name for pinned data; can be used for lookups later | [optional] 
**Origins** | Pointer to **[]string** | Optional list of multiaddrs known to provide the data | [optional] 
**Meta** | Pointer to **map[string]string** | Optional metadata for pin object | [optional] 

## Methods

### NewPin

`func NewPin(cid string, ) *Pin`

NewPin instantiates a new Pin object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewPinWithDefaults

`func NewPinWithDefaults() *Pin`

NewPinWithDefaults instantiates a new Pin object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetCid

`func (o *Pin) GetCid() string`

GetCid returns the Cid field if non-nil, zero value otherwise.

### GetCidOk

`func (o *Pin) GetCidOk() (*string, bool)`

GetCidOk returns a tuple with the Cid field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCid

`func (o *Pin) SetCid(v string)`

SetCid sets Cid field to given value.


### GetName

`func (o *Pin) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *Pin) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *Pin) SetName(v string)`

SetName sets Name field to given value.

### HasName

`func (o *Pin) HasName() bool`

HasName returns a boolean if a field has been set.

### GetOrigins

`func (o *Pin) GetOrigins() []string`

GetOrigins returns the Origins field if non-nil, zero value otherwise.

### GetOriginsOk

`func (o *Pin) GetOriginsOk() (*[]string, bool)`

GetOriginsOk returns a tuple with the Origins field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOrigins

`func (o *Pin) SetOrigins(v []string)`

SetOrigins sets Origins field to given value.

### HasOrigins

`func (o *Pin) HasOrigins() bool`

HasOrigins returns a boolean if a field has been set.

### GetMeta

`func (o *Pin) GetMeta() map[string]string`

GetMeta returns the Meta field if non-nil, zero value otherwise.

### GetMetaOk

`func (o *Pin) GetMetaOk() (*map[string]string, bool)`

GetMetaOk returns a tuple with the Meta field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMeta

`func (o *Pin) SetMeta(v map[string]string)`

SetMeta sets Meta field to given value.

### HasMeta

`func (o *Pin) HasMeta() bool`

HasMeta returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


