// Code generated by protoc-gen-go. DO NOT EDIT.
// source: eventdb/eventpb/subscription.proto

package eventpb

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type SubscriptionType int32

const (
	SubscriptionType_Volatile   SubscriptionType = 0
	SubscriptionType_CatchUp    SubscriptionType = 1
	SubscriptionType_Persistent SubscriptionType = 2
)

var SubscriptionType_name = map[int32]string{
	0: "Volatile",
	1: "CatchUp",
	2: "Persistent",
}

var SubscriptionType_value = map[string]int32{
	"Volatile":   0,
	"CatchUp":    1,
	"Persistent": 2,
}

func (x SubscriptionType) String() string {
	return proto.EnumName(SubscriptionType_name, int32(x))
}

func (SubscriptionType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_4cddf6503f7c80d8, []int{0}
}

// SubscriptionMetadata contains all the data about a specific subscription.
type SubscriptionMetadata struct {
	Key                  []byte            `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Group                string            `protobuf:"bytes,2,opt,name=group,proto3" json:"group,omitempty"`
	StreamID             string            `protobuf:"bytes,3,opt,name=StreamID,proto3" json:"StreamID,omitempty"`
	Type                 SubscriptionType  `protobuf:"varint,4,opt,name=type,proto3,enum=eventpb.SubscriptionType" json:"type,omitempty"`
	Position             uint64            `protobuf:"varint,5,opt,name=position,proto3" json:"position,omitempty"`
	CreationTime         int64             `protobuf:"varint,13,opt,name=creationTime,proto3" json:"creationTime,omitempty"`
	ModificationTime     int64             `protobuf:"varint,14,opt,name=modificationTime,proto3" json:"modificationTime,omitempty"`
	DeletionTime         int64             `protobuf:"varint,15,opt,name=deletionTime,proto3" json:"deletionTime,omitempty"`
	Extended             map[string]string `protobuf:"bytes,16,rep,name=extended,proto3" json:"extended,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *SubscriptionMetadata) Reset()         { *m = SubscriptionMetadata{} }
func (m *SubscriptionMetadata) String() string { return proto.CompactTextString(m) }
func (*SubscriptionMetadata) ProtoMessage()    {}
func (*SubscriptionMetadata) Descriptor() ([]byte, []int) {
	return fileDescriptor_4cddf6503f7c80d8, []int{0}
}

func (m *SubscriptionMetadata) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SubscriptionMetadata.Unmarshal(m, b)
}
func (m *SubscriptionMetadata) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SubscriptionMetadata.Marshal(b, m, deterministic)
}
func (m *SubscriptionMetadata) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SubscriptionMetadata.Merge(m, src)
}
func (m *SubscriptionMetadata) XXX_Size() int {
	return xxx_messageInfo_SubscriptionMetadata.Size(m)
}
func (m *SubscriptionMetadata) XXX_DiscardUnknown() {
	xxx_messageInfo_SubscriptionMetadata.DiscardUnknown(m)
}

var xxx_messageInfo_SubscriptionMetadata proto.InternalMessageInfo

func (m *SubscriptionMetadata) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *SubscriptionMetadata) GetGroup() string {
	if m != nil {
		return m.Group
	}
	return ""
}

func (m *SubscriptionMetadata) GetStreamID() string {
	if m != nil {
		return m.StreamID
	}
	return ""
}

func (m *SubscriptionMetadata) GetType() SubscriptionType {
	if m != nil {
		return m.Type
	}
	return SubscriptionType_Volatile
}

func (m *SubscriptionMetadata) GetPosition() uint64 {
	if m != nil {
		return m.Position
	}
	return 0
}

func (m *SubscriptionMetadata) GetCreationTime() int64 {
	if m != nil {
		return m.CreationTime
	}
	return 0
}

func (m *SubscriptionMetadata) GetModificationTime() int64 {
	if m != nil {
		return m.ModificationTime
	}
	return 0
}

func (m *SubscriptionMetadata) GetDeletionTime() int64 {
	if m != nil {
		return m.DeletionTime
	}
	return 0
}

func (m *SubscriptionMetadata) GetExtended() map[string]string {
	if m != nil {
		return m.Extended
	}
	return nil
}

func init() {
	proto.RegisterEnum("eventpb.SubscriptionType", SubscriptionType_name, SubscriptionType_value)
	proto.RegisterType((*SubscriptionMetadata)(nil), "eventpb.SubscriptionMetadata")
	proto.RegisterMapType((map[string]string)(nil), "eventpb.SubscriptionMetadata.ExtendedEntry")
}

func init() {
	proto.RegisterFile("eventdb/eventpb/subscription.proto", fileDescriptor_4cddf6503f7c80d8)
}

var fileDescriptor_4cddf6503f7c80d8 = []byte{
	// 355 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x6c, 0x52, 0x4d, 0x6f, 0xda, 0x40,
	0x10, 0xed, 0x62, 0x28, 0xb0, 0x7c, 0xd4, 0x5a, 0x71, 0x70, 0x39, 0x59, 0x9c, 0x5c, 0x50, 0x6d,
	0x89, 0x5e, 0xaa, 0x56, 0xbd, 0xb4, 0x45, 0x51, 0x0e, 0x91, 0x22, 0x93, 0xe4, 0x90, 0xdb, 0xda,
	0x9e, 0xc0, 0x2a, 0xb6, 0x77, 0xb5, 0x1e, 0x23, 0xfc, 0x6b, 0xf2, 0x57, 0x23, 0x1b, 0x70, 0x0c,
	0xc9, 0xc9, 0x7e, 0x1f, 0xf3, 0x46, 0xfb, 0x34, 0x74, 0x06, 0x3b, 0x48, 0x31, 0x0a, 0xbc, 0xea,
	0xab, 0x02, 0x2f, 0xcb, 0x83, 0x2c, 0xd4, 0x42, 0xa1, 0x90, 0xa9, 0xab, 0xb4, 0x44, 0xc9, 0xba,
	0x47, 0x6d, 0xf6, 0x62, 0xd0, 0xc9, 0xba, 0xa1, 0xdf, 0x00, 0xf2, 0x88, 0x23, 0x67, 0x26, 0x35,
	0x9e, 0xa1, 0xb0, 0x88, 0x4d, 0x9c, 0xa1, 0x5f, 0xfe, 0xb2, 0x09, 0xed, 0x6c, 0xb4, 0xcc, 0x95,
	0xd5, 0xb2, 0x89, 0xd3, 0xf7, 0x0f, 0x80, 0x4d, 0x69, 0x6f, 0x8d, 0x1a, 0x78, 0x72, 0xfd, 0xdf,
	0x32, 0x2a, 0xa1, 0xc6, 0xec, 0x3b, 0x6d, 0x63, 0xa1, 0xc0, 0x6a, 0xdb, 0xc4, 0x19, 0x2f, 0xbf,
	0xba, 0xc7, 0xa5, 0x6e, 0x73, 0xe1, 0x5d, 0xa1, 0xc0, 0xaf, 0x6c, 0x65, 0x94, 0x92, 0x99, 0x28,
	0x59, 0xab, 0x63, 0x13, 0xa7, 0xed, 0xd7, 0x98, 0xcd, 0xe8, 0x30, 0xd4, 0xc0, 0xab, 0x09, 0x91,
	0x80, 0x35, 0xb2, 0x89, 0x63, 0xf8, 0x67, 0x1c, 0x9b, 0x53, 0x33, 0x91, 0x91, 0x78, 0x12, 0xe1,
	0x9b, 0x6f, 0x5c, 0xf9, 0xde, 0xf1, 0x65, 0x5e, 0x04, 0x31, 0xd4, 0xbe, 0x2f, 0x87, 0xbc, 0x26,
	0xc7, 0xae, 0x68, 0x0f, 0xf6, 0x08, 0x69, 0x04, 0x91, 0x65, 0xda, 0x86, 0x33, 0x58, 0x2e, 0x3e,
	0x7c, 0xc2, 0xa9, 0x33, 0x77, 0x75, 0x74, 0xaf, 0x52, 0xd4, 0x85, 0x5f, 0x0f, 0x4f, 0x7f, 0xd3,
	0xd1, 0x99, 0xd4, 0x2c, 0xb7, 0x5f, 0x97, 0xbb, 0xe3, 0x71, 0x0e, 0xa7, 0x72, 0x2b, 0xf0, 0xab,
	0xf5, 0x93, 0xcc, 0xff, 0x50, 0xf3, 0xb2, 0x2f, 0x36, 0xa4, 0xbd, 0x07, 0x19, 0x73, 0x14, 0x31,
	0x98, 0x9f, 0xd8, 0x80, 0x76, 0xff, 0x71, 0x0c, 0xb7, 0xf7, 0xca, 0x24, 0x6c, 0x4c, 0xe9, 0x2d,
	0xe8, 0x4c, 0x64, 0x08, 0x29, 0x9a, 0xad, 0xbf, 0x8b, 0xc7, 0x6f, 0x1b, 0x81, 0xdb, 0x3c, 0x70,
	0x43, 0x99, 0x78, 0x11, 0x88, 0xbd, 0xc8, 0xbc, 0x0c, 0xa5, 0xe6, 0x1b, 0xf0, 0x2e, 0x2e, 0x25,
	0xf8, 0x5c, 0x5d, 0xc7, 0x8f, 0xd7, 0x00, 0x00, 0x00, 0xff, 0xff, 0x94, 0x52, 0x4e, 0xac, 0x43,
	0x02, 0x00, 0x00,
}
