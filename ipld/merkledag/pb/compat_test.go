package merkledag_pb

// mirrored in JavaScript @ https://github.com/ipld/js-dag-pb/blob/master/test/test-compat.js

import (
	"encoding/hex"
	"encoding/json"
	"testing"
)

var dataZero []byte = make([]byte, 0)
var dataSome []byte = []byte{0, 1, 2, 3, 4}
var cidBytes []byte = []byte{1, 85, 0, 5, 0, 1, 2, 3, 4}
var zeroName string = ""
var someName string = "some name"
var zeroTsize uint64 = 0
var someTsize uint64 = 1010
var largeTsize uint64 = 9007199254740991 // JavaScript Number.MAX_SAFE_INTEGER

type testCase struct {
	name          string
	node          *PBNode
	expectedBytes string
	expectedForm  string
}

var testCases = []testCase{
	{
		name:          "empty",
		node:          &PBNode{},
		expectedBytes: "",
		expectedForm:  "{}",
	},
	{
		name:          "Data zero",
		node:          &PBNode{Data: dataZero},
		expectedBytes: "0a00",
		expectedForm: `{
	"Data": ""
}`,
	},
	{
		name:          "Data some",
		node:          &PBNode{Data: dataSome},
		expectedBytes: "0a050001020304",
		expectedForm: `{
	"Data": "0001020304"
}`,
	},
	{
		name:          "Links zero",
		node:          &PBNode{Links: make([]*PBLink, 0)},
		expectedBytes: "",
		expectedForm:  "{}",
	},
	{
		name:          "Data some Links zero",
		node:          &PBNode{Data: dataSome, Links: make([]*PBLink, 0)},
		expectedBytes: "0a050001020304",
		expectedForm: `{
	"Data": "0001020304"
}`,
	},
	{
		name:          "Links empty",
		node:          &PBNode{Links: []*PBLink{{}}},
		expectedBytes: "1200",
		expectedForm: `{
	"Links": [
		{}
	]
}`,
	},
	{
		name:          "Data some Links empty",
		node:          &PBNode{Data: dataSome, Links: []*PBLink{{}}},
		expectedBytes: "12000a050001020304",
		expectedForm: `{
	"Data": "0001020304",
	"Links": [
		{}
	]
}`,
	},
	{
		name:          "Links Hash zero",
		node:          &PBNode{Links: []*PBLink{{Hash: dataZero}}},
		expectedBytes: "12020a00",
		expectedForm: `{
	"Links": [
		{
			"Hash": ""
		}
	]
}`,
	},
	{
		name:          "Links Hash some",
		node:          &PBNode{Links: []*PBLink{{Hash: cidBytes}}},
		expectedBytes: "120b0a09015500050001020304",
		expectedForm: `{
	"Links": [
		{
			"Hash": "015500050001020304"
		}
	]
}`,
	},
	{
		name:          "Links Name zero",
		node:          &PBNode{Links: []*PBLink{{Name: &zeroName}}},
		expectedBytes: "12021200",
		expectedForm: `{
	"Links": [
		{
			"Name": ""
		}
	]
}`,
	},
	{
		name:          "Links Hash some Name zero",
		node:          &PBNode{Links: []*PBLink{{Hash: cidBytes, Name: &zeroName}}},
		expectedBytes: "120d0a090155000500010203041200",
		expectedForm: `{
	"Links": [
		{
			"Hash": "015500050001020304",
			"Name": ""
		}
	]
}`,
	},
	{
		name:          "Links Name some",
		node:          &PBNode{Links: []*PBLink{{Name: &someName}}},
		expectedBytes: "120b1209736f6d65206e616d65",
		expectedForm: `{
	"Links": [
		{
			"Name": "some name"
		}
	]
}`,
	},
	{
		name:          "Links Hash some Name some",
		node:          &PBNode{Links: []*PBLink{{Hash: cidBytes, Name: &someName}}},
		expectedBytes: "12160a090155000500010203041209736f6d65206e616d65",
		expectedForm: `{
	"Links": [
		{
			"Hash": "015500050001020304",
			"Name": "some name"
		}
	]
}`,
	},
	{
		name:          "Links Tsize zero",
		node:          &PBNode{Links: []*PBLink{{Tsize: &zeroTsize}}},
		expectedBytes: "12021800",
		expectedForm: `{
	"Links": [
		{
			"Tsize": 0
		}
	]
}`,
	},
	{
		name:          "Links Hash some Tsize zero",
		node:          &PBNode{Links: []*PBLink{{Hash: cidBytes, Tsize: &zeroTsize}}},
		expectedBytes: "120d0a090155000500010203041800",
		expectedForm: `{
	"Links": [
		{
			"Hash": "015500050001020304",
			"Tsize": 0
		}
	]
}`,
	},
	{
		name:          "Links Tsize some",
		node:          &PBNode{Links: []*PBLink{{Tsize: &someTsize}}},
		expectedBytes: "120318f207",
		expectedForm: `{
	"Links": [
		{
			"Tsize": 1010
		}
	]
}`,
	},
	{
		name:          "Links Hash some Tsize some",
		node:          &PBNode{Links: []*PBLink{{Hash: cidBytes, Tsize: &largeTsize}}},
		expectedBytes: "12140a0901550005000102030418ffffffffffffff0f",
		expectedForm: `{
	"Links": [
		{
			"Hash": "015500050001020304",
			"Tsize": 9007199254740991
		}
	]
}`,
	},
}

func TestCompat(t *testing.T) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			verifyRoundTrip(t, tc)
		})
	}
}

func verifyRoundTrip(t *testing.T, tc testCase) {
	actualBytes, actualForm, err := nodeRoundTripToString(t, tc.node)
	if err != nil {
		t.Fatal(err)
	}

	if actualBytes != tc.expectedBytes {
		t.Logf(
			"Expected bytes: [%v]\nGot: [%v]\n",
			tc.expectedBytes,
			actualBytes)
		t.Error("Did not match")
	}

	if actualForm != tc.expectedForm {
		t.Logf(
			"Expected form: [%v]\nGot: [%v]\n",
			tc.expectedForm,
			actualForm)
		t.Error("Did not match")
	}
}

func nodeRoundTripToString(t *testing.T, n *PBNode) (string, string, error) {
	bytes, err := n.Marshal()
	if err != nil {
		return "", "", err
	}
	t.Logf("[%v]\n", hex.EncodeToString(bytes))
	rt := new(PBNode)
	if err := rt.Unmarshal(bytes); err != nil {
		return "", "", err
	}
	str, err := json.MarshalIndent(cleanPBNode(t, rt), "", "\t")
	if err != nil {
		return "", "", err
	}
	return hex.EncodeToString(bytes), string(str), nil
}

// convert a PBLink into a map for clean JSON marshalling
func cleanPBLink(t *testing.T, link *PBLink) map[string]interface{} {
	if link == nil {
		return nil
	}
	// this would be a bad pb decode
	if link.XXX_unrecognized != nil {
		t.Fatal("Got unexpected XXX_unrecognized")
	}
	nl := make(map[string]interface{})
	if link.Hash != nil {
		nl["Hash"] = hex.EncodeToString(link.Hash)
	}
	if link.Name != nil {
		nl["Name"] = link.Name
	}
	if link.Tsize != nil {
		nl["Tsize"] = link.Tsize
	}
	return nl
}

// convert a PBNode into a map for clean JSON marshalling
func cleanPBNode(t *testing.T, node *PBNode) map[string]interface{} {
	// this would be a bad pb decode
	if node.XXX_unrecognized != nil {
		t.Fatal("Got unexpected XXX_unrecognized")
	}
	nn := make(map[string]interface{})
	if node.Data != nil {
		nn["Data"] = hex.EncodeToString(node.Data)
	}
	if node.Links != nil {
		links := make([]map[string]interface{}, len(node.Links))
		for i, l := range node.Links {
			links[i] = cleanPBLink(t, l)
		}
		nn["Links"] = links
	}
	return nn
}
