# PinResults

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Count** | **int32** | The total number of pin objects that exist for passed query filters | 
**Results** | [**[]PinStatus**](PinStatus.md) | An array of PinStatus results | 

## Methods

### NewPinResults

`func NewPinResults(count int32, results []PinStatus, ) *PinResults`

NewPinResults instantiates a new PinResults object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewPinResultsWithDefaults

`func NewPinResultsWithDefaults() *PinResults`

NewPinResultsWithDefaults instantiates a new PinResults object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetCount

`func (o *PinResults) GetCount() int32`

GetCount returns the Count field if non-nil, zero value otherwise.

### GetCountOk

`func (o *PinResults) GetCountOk() (*int32, bool)`

GetCountOk returns a tuple with the Count field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCount

`func (o *PinResults) SetCount(v int32)`

SetCount sets Count field to given value.


### GetResults

`func (o *PinResults) GetResults() []PinStatus`

GetResults returns the Results field if non-nil, zero value otherwise.

### GetResultsOk

`func (o *PinResults) GetResultsOk() (*[]PinStatus, bool)`

GetResultsOk returns a tuple with the Results field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetResults

`func (o *PinResults) SetResults(v []PinStatus)`

SetResults sets Results field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


