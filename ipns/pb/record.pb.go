// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.3
// 	protoc        v5.29.2
// source: github.com/ipfs/boxo/ipns/pb/record.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type IpnsRecord_ValidityType int32

const (
	IpnsRecord_EOL IpnsRecord_ValidityType = 0
)

// Enum value maps for IpnsRecord_ValidityType.
var (
	IpnsRecord_ValidityType_name = map[int32]string{
		0: "EOL",
	}
	IpnsRecord_ValidityType_value = map[string]int32{
		"EOL": 0,
	}
)

func (x IpnsRecord_ValidityType) Enum() *IpnsRecord_ValidityType {
	p := new(IpnsRecord_ValidityType)
	*p = x
	return p
}

func (x IpnsRecord_ValidityType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (IpnsRecord_ValidityType) Descriptor() protoreflect.EnumDescriptor {
	return file_github_com_ipfs_boxo_ipns_pb_record_proto_enumTypes[0].Descriptor()
}

func (IpnsRecord_ValidityType) Type() protoreflect.EnumType {
	return &file_github_com_ipfs_boxo_ipns_pb_record_proto_enumTypes[0]
}

func (x IpnsRecord_ValidityType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use IpnsRecord_ValidityType.Descriptor instead.
func (IpnsRecord_ValidityType) EnumDescriptor() ([]byte, []int) {
	return file_github_com_ipfs_boxo_ipns_pb_record_proto_rawDescGZIP(), []int{0, 0}
}

// https://specs.ipfs.tech/ipns/ipns-record/#record-serialization-format
type IpnsRecord struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// 1-6 are legacy fields used only in V1+V2 records
	Value        []byte                   `protobuf:"bytes,1,opt,name=value,proto3,oneof" json:"value,omitempty"`
	SignatureV1  []byte                   `protobuf:"bytes,2,opt,name=signatureV1,proto3,oneof" json:"signatureV1,omitempty"`
	ValidityType *IpnsRecord_ValidityType `protobuf:"varint,3,opt,name=validityType,proto3,enum=github.com.boxo.ipns.pb.IpnsRecord_ValidityType,oneof" json:"validityType,omitempty"`
	Validity     []byte                   `protobuf:"bytes,4,opt,name=validity,proto3,oneof" json:"validity,omitempty"`
	Sequence     *uint64                  `protobuf:"varint,5,opt,name=sequence,proto3,oneof" json:"sequence,omitempty"`
	Ttl          *uint64                  `protobuf:"varint,6,opt,name=ttl,proto3,oneof" json:"ttl,omitempty"`
	// 7-9 are V2 records
	PubKey        []byte `protobuf:"bytes,7,opt,name=pubKey,proto3,oneof" json:"pubKey,omitempty"`
	SignatureV2   []byte `protobuf:"bytes,8,opt,name=signatureV2,proto3,oneof" json:"signatureV2,omitempty"`
	Data          []byte `protobuf:"bytes,9,opt,name=data,proto3,oneof" json:"data,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *IpnsRecord) Reset() {
	*x = IpnsRecord{}
	mi := &file_github_com_ipfs_boxo_ipns_pb_record_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *IpnsRecord) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*IpnsRecord) ProtoMessage() {}

func (x *IpnsRecord) ProtoReflect() protoreflect.Message {
	mi := &file_github_com_ipfs_boxo_ipns_pb_record_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use IpnsRecord.ProtoReflect.Descriptor instead.
func (*IpnsRecord) Descriptor() ([]byte, []int) {
	return file_github_com_ipfs_boxo_ipns_pb_record_proto_rawDescGZIP(), []int{0}
}

func (x *IpnsRecord) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *IpnsRecord) GetSignatureV1() []byte {
	if x != nil {
		return x.SignatureV1
	}
	return nil
}

func (x *IpnsRecord) GetValidityType() IpnsRecord_ValidityType {
	if x != nil && x.ValidityType != nil {
		return *x.ValidityType
	}
	return IpnsRecord_EOL
}

func (x *IpnsRecord) GetValidity() []byte {
	if x != nil {
		return x.Validity
	}
	return nil
}

func (x *IpnsRecord) GetSequence() uint64 {
	if x != nil && x.Sequence != nil {
		return *x.Sequence
	}
	return 0
}

func (x *IpnsRecord) GetTtl() uint64 {
	if x != nil && x.Ttl != nil {
		return *x.Ttl
	}
	return 0
}

func (x *IpnsRecord) GetPubKey() []byte {
	if x != nil {
		return x.PubKey
	}
	return nil
}

func (x *IpnsRecord) GetSignatureV2() []byte {
	if x != nil {
		return x.SignatureV2
	}
	return nil
}

func (x *IpnsRecord) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

var File_github_com_ipfs_boxo_ipns_pb_record_proto protoreflect.FileDescriptor

var file_github_com_ipfs_boxo_ipns_pb_record_proto_rawDesc = []byte{
	0x0a, 0x29, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x69, 0x70, 0x66,
	0x73, 0x2f, 0x62, 0x6f, 0x78, 0x6f, 0x2f, 0x69, 0x70, 0x6e, 0x73, 0x2f, 0x70, 0x62, 0x2f, 0x72,
	0x65, 0x63, 0x6f, 0x72, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x17, 0x67, 0x69, 0x74,
	0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2e, 0x62, 0x6f, 0x78, 0x6f, 0x2e, 0x69, 0x70, 0x6e,
	0x73, 0x2e, 0x70, 0x62, 0x22, 0xe9, 0x03, 0x0a, 0x0a, 0x49, 0x70, 0x6e, 0x73, 0x52, 0x65, 0x63,
	0x6f, 0x72, 0x64, 0x12, 0x19, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0c, 0x48, 0x00, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x88, 0x01, 0x01, 0x12, 0x25,
	0x0a, 0x0b, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x56, 0x31, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0c, 0x48, 0x01, 0x52, 0x0b, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65,
	0x56, 0x31, 0x88, 0x01, 0x01, 0x12, 0x59, 0x0a, 0x0c, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x69, 0x74,
	0x79, 0x54, 0x79, 0x70, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x30, 0x2e, 0x67, 0x69,
	0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2e, 0x62, 0x6f, 0x78, 0x6f, 0x2e, 0x69, 0x70,
	0x6e, 0x73, 0x2e, 0x70, 0x62, 0x2e, 0x49, 0x70, 0x6e, 0x73, 0x52, 0x65, 0x63, 0x6f, 0x72, 0x64,
	0x2e, 0x56, 0x61, 0x6c, 0x69, 0x64, 0x69, 0x74, 0x79, 0x54, 0x79, 0x70, 0x65, 0x48, 0x02, 0x52,
	0x0c, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x69, 0x74, 0x79, 0x54, 0x79, 0x70, 0x65, 0x88, 0x01, 0x01,
	0x12, 0x1f, 0x0a, 0x08, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x69, 0x74, 0x79, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x0c, 0x48, 0x03, 0x52, 0x08, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x69, 0x74, 0x79, 0x88, 0x01,
	0x01, 0x12, 0x1f, 0x0a, 0x08, 0x73, 0x65, 0x71, 0x75, 0x65, 0x6e, 0x63, 0x65, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x04, 0x48, 0x04, 0x52, 0x08, 0x73, 0x65, 0x71, 0x75, 0x65, 0x6e, 0x63, 0x65, 0x88,
	0x01, 0x01, 0x12, 0x15, 0x0a, 0x03, 0x74, 0x74, 0x6c, 0x18, 0x06, 0x20, 0x01, 0x28, 0x04, 0x48,
	0x05, 0x52, 0x03, 0x74, 0x74, 0x6c, 0x88, 0x01, 0x01, 0x12, 0x1b, 0x0a, 0x06, 0x70, 0x75, 0x62,
	0x4b, 0x65, 0x79, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0c, 0x48, 0x06, 0x52, 0x06, 0x70, 0x75, 0x62,
	0x4b, 0x65, 0x79, 0x88, 0x01, 0x01, 0x12, 0x25, 0x0a, 0x0b, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74,
	0x75, 0x72, 0x65, 0x56, 0x32, 0x18, 0x08, 0x20, 0x01, 0x28, 0x0c, 0x48, 0x07, 0x52, 0x0b, 0x73,
	0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x56, 0x32, 0x88, 0x01, 0x01, 0x12, 0x17, 0x0a,
	0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x09, 0x20, 0x01, 0x28, 0x0c, 0x48, 0x08, 0x52, 0x04, 0x64,
	0x61, 0x74, 0x61, 0x88, 0x01, 0x01, 0x22, 0x17, 0x0a, 0x0c, 0x56, 0x61, 0x6c, 0x69, 0x64, 0x69,
	0x74, 0x79, 0x54, 0x79, 0x70, 0x65, 0x12, 0x07, 0x0a, 0x03, 0x45, 0x4f, 0x4c, 0x10, 0x00, 0x42,
	0x08, 0x0a, 0x06, 0x5f, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x42, 0x0e, 0x0a, 0x0c, 0x5f, 0x73, 0x69,
	0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x56, 0x31, 0x42, 0x0f, 0x0a, 0x0d, 0x5f, 0x76, 0x61,
	0x6c, 0x69, 0x64, 0x69, 0x74, 0x79, 0x54, 0x79, 0x70, 0x65, 0x42, 0x0b, 0x0a, 0x09, 0x5f, 0x76,
	0x61, 0x6c, 0x69, 0x64, 0x69, 0x74, 0x79, 0x42, 0x0b, 0x0a, 0x09, 0x5f, 0x73, 0x65, 0x71, 0x75,
	0x65, 0x6e, 0x63, 0x65, 0x42, 0x06, 0x0a, 0x04, 0x5f, 0x74, 0x74, 0x6c, 0x42, 0x09, 0x0a, 0x07,
	0x5f, 0x70, 0x75, 0x62, 0x4b, 0x65, 0x79, 0x42, 0x0e, 0x0a, 0x0c, 0x5f, 0x73, 0x69, 0x67, 0x6e,
	0x61, 0x74, 0x75, 0x72, 0x65, 0x56, 0x32, 0x42, 0x07, 0x0a, 0x05, 0x5f, 0x64, 0x61, 0x74, 0x61,
	0x42, 0x1e, 0x5a, 0x1c, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x69,
	0x70, 0x66, 0x73, 0x2f, 0x62, 0x6f, 0x78, 0x6f, 0x2f, 0x69, 0x70, 0x6e, 0x73, 0x2f, 0x70, 0x62,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_github_com_ipfs_boxo_ipns_pb_record_proto_rawDescOnce sync.Once
	file_github_com_ipfs_boxo_ipns_pb_record_proto_rawDescData = file_github_com_ipfs_boxo_ipns_pb_record_proto_rawDesc
)

func file_github_com_ipfs_boxo_ipns_pb_record_proto_rawDescGZIP() []byte {
	file_github_com_ipfs_boxo_ipns_pb_record_proto_rawDescOnce.Do(func() {
		file_github_com_ipfs_boxo_ipns_pb_record_proto_rawDescData = protoimpl.X.CompressGZIP(file_github_com_ipfs_boxo_ipns_pb_record_proto_rawDescData)
	})
	return file_github_com_ipfs_boxo_ipns_pb_record_proto_rawDescData
}

var file_github_com_ipfs_boxo_ipns_pb_record_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_github_com_ipfs_boxo_ipns_pb_record_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_github_com_ipfs_boxo_ipns_pb_record_proto_goTypes = []any{
	(IpnsRecord_ValidityType)(0), // 0: github.com.boxo.ipns.pb.IpnsRecord.ValidityType
	(*IpnsRecord)(nil),           // 1: github.com.boxo.ipns.pb.IpnsRecord
}
var file_github_com_ipfs_boxo_ipns_pb_record_proto_depIdxs = []int32{
	0, // 0: github.com.boxo.ipns.pb.IpnsRecord.validityType:type_name -> github.com.boxo.ipns.pb.IpnsRecord.ValidityType
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_github_com_ipfs_boxo_ipns_pb_record_proto_init() }
func file_github_com_ipfs_boxo_ipns_pb_record_proto_init() {
	if File_github_com_ipfs_boxo_ipns_pb_record_proto != nil {
		return
	}
	file_github_com_ipfs_boxo_ipns_pb_record_proto_msgTypes[0].OneofWrappers = []any{}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_github_com_ipfs_boxo_ipns_pb_record_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_github_com_ipfs_boxo_ipns_pb_record_proto_goTypes,
		DependencyIndexes: file_github_com_ipfs_boxo_ipns_pb_record_proto_depIdxs,
		EnumInfos:         file_github_com_ipfs_boxo_ipns_pb_record_proto_enumTypes,
		MessageInfos:      file_github_com_ipfs_boxo_ipns_pb_record_proto_msgTypes,
	}.Build()
	File_github_com_ipfs_boxo_ipns_pb_record_proto = out.File
	file_github_com_ipfs_boxo_ipns_pb_record_proto_rawDesc = nil
	file_github_com_ipfs_boxo_ipns_pb_record_proto_goTypes = nil
	file_github_com_ipfs_boxo_ipns_pb_record_proto_depIdxs = nil
}
