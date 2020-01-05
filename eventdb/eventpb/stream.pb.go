// Code generated by protoc-gen-go. DO NOT EDIT.
// source: eventdb/eventpb/stream.proto

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
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// StreamMetadata contains all the data about a specific event.
type StreamMetadata struct {
	Key                  []byte            `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	ID                   string            `protobuf:"bytes,2,opt,name=ID,proto3" json:"ID,omitempty"`
	Version              uint64            `protobuf:"varint,3,opt,name=version,proto3" json:"version,omitempty"`
	CreationTime         int64             `protobuf:"varint,4,opt,name=creationTime,proto3" json:"creationTime,omitempty"`
	ModificationTime     int64             `protobuf:"varint,5,opt,name=modificationTime,proto3" json:"modificationTime,omitempty"`
	DeletionTime         int64             `protobuf:"varint,6,opt,name=deletionTime,proto3" json:"deletionTime,omitempty"`
	Extended             map[string]string `protobuf:"bytes,16,rep,name=extended,proto3" json:"extended,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *StreamMetadata) Reset()         { *m = StreamMetadata{} }
func (m *StreamMetadata) String() string { return proto.CompactTextString(m) }
func (*StreamMetadata) ProtoMessage()    {}
func (*StreamMetadata) Descriptor() ([]byte, []int) {
	return fileDescriptor_0da649564d2989ab, []int{0}
}

func (m *StreamMetadata) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_StreamMetadata.Unmarshal(m, b)
}
func (m *StreamMetadata) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_StreamMetadata.Marshal(b, m, deterministic)
}
func (m *StreamMetadata) XXX_Merge(src proto.Message) {
	xxx_messageInfo_StreamMetadata.Merge(m, src)
}
func (m *StreamMetadata) XXX_Size() int {
	return xxx_messageInfo_StreamMetadata.Size(m)
}
func (m *StreamMetadata) XXX_DiscardUnknown() {
	xxx_messageInfo_StreamMetadata.DiscardUnknown(m)
}

var xxx_messageInfo_StreamMetadata proto.InternalMessageInfo

func (m *StreamMetadata) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *StreamMetadata) GetID() string {
	if m != nil {
		return m.ID
	}
	return ""
}

func (m *StreamMetadata) GetVersion() uint64 {
	if m != nil {
		return m.Version
	}
	return 0
}

func (m *StreamMetadata) GetCreationTime() int64 {
	if m != nil {
		return m.CreationTime
	}
	return 0
}

func (m *StreamMetadata) GetModificationTime() int64 {
	if m != nil {
		return m.ModificationTime
	}
	return 0
}

func (m *StreamMetadata) GetDeletionTime() int64 {
	if m != nil {
		return m.DeletionTime
	}
	return 0
}

func (m *StreamMetadata) GetExtended() map[string]string {
	if m != nil {
		return m.Extended
	}
	return nil
}

func init() {
	proto.RegisterType((*StreamMetadata)(nil), "eventpb.StreamMetadata")
	proto.RegisterMapType((map[string]string)(nil), "eventpb.StreamMetadata.ExtendedEntry")
}

func init() { proto.RegisterFile("eventdb/eventpb/stream.proto", fileDescriptor_0da649564d2989ab) }

var fileDescriptor_0da649564d2989ab = []byte{
	// 278 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x64, 0x91, 0xcf, 0x4b, 0xc3, 0x30,
	0x14, 0xc7, 0x49, 0xbb, 0x1f, 0xee, 0x39, 0xc7, 0x08, 0x1e, 0x82, 0x78, 0x28, 0x03, 0xa1, 0x2a,
	0xb4, 0xa0, 0x17, 0xd1, 0x93, 0xb2, 0x1d, 0x76, 0xf0, 0x52, 0x3d, 0x79, 0x4b, 0x9b, 0xe7, 0x0c,
	0xae, 0xc9, 0x48, 0xdf, 0xca, 0xf6, 0xdf, 0xf9, 0xa7, 0xc9, 0xda, 0x3a, 0x57, 0x3d, 0x25, 0xef,
	0xfb, 0x3e, 0x79, 0xf0, 0x3e, 0x81, 0x73, 0x2c, 0xd1, 0x90, 0x4a, 0xe3, 0xea, 0x5c, 0xa5, 0x71,
	0x41, 0x0e, 0x65, 0x1e, 0xad, 0x9c, 0x25, 0xcb, 0xfb, 0x4d, 0x3a, 0xf9, 0xf2, 0x60, 0xf4, 0x52,
	0x75, 0x9e, 0x91, 0xa4, 0x92, 0x24, 0xf9, 0x18, 0xfc, 0x4f, 0xdc, 0x0a, 0x16, 0xb0, 0x70, 0x98,
	0xec, 0xae, 0x7c, 0x04, 0xde, 0x7c, 0x2a, 0xbc, 0x80, 0x85, 0x83, 0xc4, 0x9b, 0x4f, 0xb9, 0x80,
	0x7e, 0x89, 0xae, 0xd0, 0xd6, 0x08, 0x3f, 0x60, 0x61, 0x27, 0xf9, 0x29, 0xf9, 0x04, 0x86, 0x99,
	0x43, 0x49, 0xda, 0x9a, 0x57, 0x9d, 0xa3, 0xe8, 0x04, 0x2c, 0xf4, 0x93, 0x56, 0xc6, 0xaf, 0x60,
	0x9c, 0x5b, 0xa5, 0xdf, 0x75, 0xf6, 0xcb, 0x75, 0x2b, 0xee, 0x5f, 0xbe, 0x9b, 0xa7, 0x70, 0x89,
	0x7b, 0xae, 0x57, 0xcf, 0x3b, 0xcc, 0xf8, 0x23, 0x1c, 0xe1, 0x86, 0xd0, 0x28, 0x54, 0x62, 0x1c,
	0xf8, 0xe1, 0xf1, 0xcd, 0x45, 0xd4, 0xac, 0x17, 0xb5, 0x57, 0x8b, 0x66, 0x0d, 0x37, 0x33, 0xe4,
	0xb6, 0xc9, 0xfe, 0xd9, 0xd9, 0x03, 0x9c, 0xb4, 0x5a, 0x87, 0x0e, 0x06, 0xb5, 0x83, 0x53, 0xe8,
	0x96, 0x72, 0xb9, 0xc6, 0x46, 0x43, 0x5d, 0xdc, 0x7b, 0x77, 0xec, 0xe9, 0xfa, 0xed, 0x72, 0xa1,
	0xe9, 0x63, 0x9d, 0x46, 0x99, 0xcd, 0x63, 0x85, 0x7a, 0xa3, 0x8b, 0xb8, 0x20, 0xeb, 0xe4, 0x02,
	0xe3, 0x3f, 0xbf, 0x90, 0xf6, 0x2a, 0xff, 0xb7, 0xdf, 0x01, 0x00, 0x00, 0xff, 0xff, 0xb5, 0x4f,
	0x93, 0xcf, 0x9f, 0x01, 0x00, 0x00,
}
