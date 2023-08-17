// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: unixfs.proto

package unixfs_pb

import (
	fmt "fmt"
	math "math"

	proto "github.com/gogo/protobuf/proto"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type Data_DataType int32

const Data_Raw Data_DataType = 0
const Data_Directory Data_DataType = 1
const Data_File Data_DataType = 2
const Data_Metadata Data_DataType = 3
const Data_Symlink Data_DataType = 4
const Data_HAMTShard Data_DataType = 5

var Data_DataType_name = map[int32]string{
	0: "Raw",
	1: "Directory",
	2: "File",
	3: "Metadata",
	4: "Symlink",
	5: "HAMTShard",
}

var Data_DataType_value = map[string]int32{
	"Raw":       0,
	"Directory": 1,
	"File":      2,
	"Metadata":  3,
	"Symlink":   4,
	"HAMTShard": 5,
}

func (x Data_DataType) Enum() *Data_DataType {
	p := new(Data_DataType)
	*p = x
	return p
}

func (x Data_DataType) String() string {
	return proto.EnumName(Data_DataType_name, int32(x))
}

func (x *Data_DataType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(Data_DataType_value, data, "Data_DataType")
	if err != nil {
		return err
	}
	*x = Data_DataType(value)
	return nil
}

func (Data_DataType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_e2fd76cc44dfc7c3, []int{0, 0}
}

type Data struct {
	Type                 *Data_DataType `protobuf:"varint,1,req,name=Type,enum=unixfs.v1.pb.Data_DataType" json:"Type,omitempty"`
	Data                 []byte         `protobuf:"bytes,2,opt,name=Data" json:"Data,omitempty"`
	Filesize             *uint64        `protobuf:"varint,3,opt,name=filesize" json:"filesize,omitempty"`
	Blocksizes           []uint64       `protobuf:"varint,4,rep,name=blocksizes" json:"blocksizes,omitempty"`
	HashType             *uint64        `protobuf:"varint,5,opt,name=hashType" json:"hashType,omitempty"`
	Fanout               *uint64        `protobuf:"varint,6,opt,name=fanout" json:"fanout,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *Data) Reset()         { *m = Data{} }
func (m *Data) String() string { return proto.CompactTextString(m) }
func (*Data) ProtoMessage()    {}
func (*Data) Descriptor() ([]byte, []int) {
	return fileDescriptor_e2fd76cc44dfc7c3, []int{0}
}

func (m *Data) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Data.Unmarshal(m, b)
}

func (m *Data) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Data.Marshal(b, m, deterministic)
}

func (m *Data) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Data.Merge(m, src)
}

func (m *Data) XXX_Size() int {
	return xxx_messageInfo_Data.Size(m)
}

func (m *Data) XXX_DiscardUnknown() {
	xxx_messageInfo_Data.DiscardUnknown(m)
}

var xxx_messageInfo_Data proto.InternalMessageInfo

func (m *Data) GetType() Data_DataType {
	if m != nil && m.Type != nil {
		return *m.Type
	}
	return Data_Raw
}

func (m *Data) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *Data) GetFilesize() uint64 {
	if m != nil && m.Filesize != nil {
		return *m.Filesize
	}
	return 0
}

func (m *Data) GetBlocksizes() []uint64 {
	if m != nil {
		return m.Blocksizes
	}
	return nil
}

func (m *Data) GetHashType() uint64 {
	if m != nil && m.HashType != nil {
		return *m.HashType
	}
	return 0
}

func (m *Data) GetFanout() uint64 {
	if m != nil && m.Fanout != nil {
		return *m.Fanout
	}
	return 0
}

type Metadata struct {
	MimeType             *string  `protobuf:"bytes,1,opt,name=MimeType" json:"MimeType,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Metadata) Reset()         { *m = Metadata{} }
func (m *Metadata) String() string { return proto.CompactTextString(m) }
func (*Metadata) ProtoMessage()    {}
func (*Metadata) Descriptor() ([]byte, []int) {
	return fileDescriptor_e2fd76cc44dfc7c3, []int{1}
}

func (m *Metadata) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Metadata.Unmarshal(m, b)
}

func (m *Metadata) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Metadata.Marshal(b, m, deterministic)
}

func (m *Metadata) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Metadata.Merge(m, src)
}

func (m *Metadata) XXX_Size() int {
	return xxx_messageInfo_Metadata.Size(m)
}

func (m *Metadata) XXX_DiscardUnknown() {
	xxx_messageInfo_Metadata.DiscardUnknown(m)
}

var xxx_messageInfo_Metadata proto.InternalMessageInfo

func (m *Metadata) GetMimeType() string {
	if m != nil && m.MimeType != nil {
		return *m.MimeType
	}
	return ""
}

func init() {
	proto.RegisterEnum("unixfs.v1.pb.Data_DataType", Data_DataType_name, Data_DataType_value)
	proto.RegisterType((*Data)(nil), "unixfs.v1.pb.Data")
	proto.RegisterType((*Metadata)(nil), "unixfs.v1.pb.Metadata")
}

func init() { proto.RegisterFile("unixfs.proto", fileDescriptor_e2fd76cc44dfc7c3) }

var fileDescriptor_e2fd76cc44dfc7c3 = []byte{
	// 267 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x4c, 0x90, 0x41, 0x4f, 0x83, 0x30,
	0x18, 0x86, 0x05, 0xba, 0x0d, 0xbe, 0xa1, 0x69, 0xbe, 0x83, 0x21, 0x9a, 0x18, 0xc2, 0xc1, 0x70,
	0xc2, 0xe8, 0x3f, 0xd0, 0x2c, 0xc6, 0x0b, 0x97, 0x6e, 0xf1, 0xe0, 0xc5, 0x94, 0xad, 0x84, 0x66,
	0x8c, 0x12, 0xe8, 0x54, 0xfc, 0x1b, 0xfe, 0x61, 0x53, 0x18, 0xdb, 0x2e, 0x4d, 0x9e, 0xf6, 0x79,
	0x9b, 0x37, 0x2f, 0xf8, 0xfb, 0x4a, 0xfe, 0xe4, 0x6d, 0x52, 0x37, 0x4a, 0x2b, 0x1c, 0xe9, 0xeb,
	0x31, 0xa9, 0xb3, 0xe8, 0xcf, 0x06, 0xb2, 0xe0, 0x9a, 0xe3, 0x03, 0x90, 0x55, 0x57, 0x8b, 0xc0,
	0x0a, 0xed, 0xf8, 0xea, 0xe9, 0x36, 0x39, 0xb7, 0x12, 0x63, 0xf4, 0x87, 0x51, 0x58, 0x2f, 0x22,
	0x0e, 0xc1, 0xc0, 0x0e, 0xad, 0xd8, 0x67, 0xc3, 0x27, 0x37, 0xe0, 0xe6, 0xb2, 0x14, 0xad, 0xfc,
	0x15, 0x81, 0x13, 0x5a, 0x31, 0x61, 0x47, 0xc6, 0x3b, 0x80, 0xac, 0x54, 0xeb, 0xad, 0x81, 0x36,
	0x20, 0xa1, 0x13, 0x13, 0x76, 0x76, 0x63, 0xb2, 0x05, 0x6f, 0x8b, 0xbe, 0xc4, 0x64, 0xc8, 0x8e,
	0x8c, 0xd7, 0x30, 0xcd, 0x79, 0xa5, 0xf6, 0x3a, 0x98, 0xf6, 0x2f, 0x07, 0x8a, 0xde, 0xc1, 0x1d,
	0x5b, 0xe1, 0x0c, 0x1c, 0xc6, 0xbf, 0xe9, 0x05, 0x5e, 0x82, 0xb7, 0x90, 0x8d, 0x58, 0x6b, 0xd5,
	0x74, 0xd4, 0x42, 0x17, 0xc8, 0xab, 0x2c, 0x05, 0xb5, 0xd1, 0x07, 0x37, 0x15, 0x9a, 0x6f, 0xb8,
	0xe6, 0xd4, 0xc1, 0x39, 0xcc, 0x96, 0xdd, 0xae, 0x94, 0xd5, 0x96, 0x12, 0x93, 0x79, 0x7b, 0x4e,
	0x57, 0xcb, 0x82, 0x37, 0x1b, 0x3a, 0x89, 0xee, 0x4f, 0xa6, 0xe9, 0x95, 0xca, 0x9d, 0x38, 0x8c,
	0x63, 0xc5, 0x1e, 0x3b, 0xf2, 0xcb, 0xfc, 0xc3, 0x1b, 0x76, 0xfa, 0xac, 0xb3, 0xff, 0x00, 0x00,
	0x00, 0xff, 0xff, 0xbd, 0x16, 0xf8, 0x45, 0x67, 0x01, 0x00, 0x00,
}
