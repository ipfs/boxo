package parser

import (
	"encoding/base64"
)

type Envelope struct {
	Tag string `json:"tag"`
	Payload interface{} `json:"payload"`
}

type DJByte struct {
	Bytes string `json:"bytes"`
}

type DJSpecialBytes struct {
	Reserved DJByte `json:"/"`
}

func ToDJSpecialBytes(data []byte) DJSpecialBytes {
	return DJSpecialBytes{Reserved: DJByte{
		Bytes: base64.RawStdEncoding.EncodeToString(data),
	}}
}

func FromDJSpecialBytes(data DJSpecialBytes) ([]byte, error) {
	return base64.RawStdEncoding.DecodeString(data.Reserved.Bytes)
}
