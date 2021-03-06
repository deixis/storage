// Code generated by protoc-gen-go. DO NOT EDIT.
// source: eventdb/eventpb/event.proto

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

// RecordedEvent contains all the data about a specific event.
type RecordedEvent struct {
	ID     string `protobuf:"bytes,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Number uint64 `protobuf:"varint,2,opt,name=number,proto3" json:"number,omitempty"`
	// Type is the FQN name for this recorded event
	Name                 string   `protobuf:"bytes,3,opt,name=name,proto3" json:"name,omitempty"`
	Data                 []byte   `protobuf:"bytes,4,opt,name=data,proto3" json:"data,omitempty"`
	Meta                 []byte   `protobuf:"bytes,5,opt,name=meta,proto3" json:"meta,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RecordedEvent) Reset()         { *m = RecordedEvent{} }
func (m *RecordedEvent) String() string { return proto.CompactTextString(m) }
func (*RecordedEvent) ProtoMessage()    {}
func (*RecordedEvent) Descriptor() ([]byte, []int) {
	return fileDescriptor_3946c2c051896ffe, []int{0}
}

func (m *RecordedEvent) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RecordedEvent.Unmarshal(m, b)
}
func (m *RecordedEvent) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RecordedEvent.Marshal(b, m, deterministic)
}
func (m *RecordedEvent) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RecordedEvent.Merge(m, src)
}
func (m *RecordedEvent) XXX_Size() int {
	return xxx_messageInfo_RecordedEvent.Size(m)
}
func (m *RecordedEvent) XXX_DiscardUnknown() {
	xxx_messageInfo_RecordedEvent.DiscardUnknown(m)
}

var xxx_messageInfo_RecordedEvent proto.InternalMessageInfo

func (m *RecordedEvent) GetID() string {
	if m != nil {
		return m.ID
	}
	return ""
}

func (m *RecordedEvent) GetNumber() uint64 {
	if m != nil {
		return m.Number
	}
	return 0
}

func (m *RecordedEvent) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *RecordedEvent) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *RecordedEvent) GetMeta() []byte {
	if m != nil {
		return m.Meta
	}
	return nil
}

func init() {
	proto.RegisterType((*RecordedEvent)(nil), "eventpb.RecordedEvent")
}

func init() {
	proto.RegisterFile("eventdb/eventpb/event.proto", fileDescriptor_3946c2c051896ffe)
}

var fileDescriptor_3946c2c051896ffe = []byte{
	// 175 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x92, 0x4e, 0x2d, 0x4b, 0xcd,
	0x2b, 0x49, 0x49, 0xd2, 0x07, 0xd3, 0x05, 0x50, 0x5a, 0xaf, 0xa0, 0x28, 0xbf, 0x24, 0x5f, 0x88,
	0x1d, 0x2a, 0xa8, 0x54, 0xcc, 0xc5, 0x1b, 0x94, 0x9a, 0x9c, 0x5f, 0x94, 0x92, 0x9a, 0xe2, 0x0a,
	0x12, 0x12, 0xe2, 0xe3, 0x62, 0xf2, 0x74, 0x91, 0x60, 0x54, 0x60, 0xd4, 0xe0, 0x0c, 0x62, 0xf2,
	0x74, 0x11, 0x12, 0xe3, 0x62, 0xcb, 0x2b, 0xcd, 0x4d, 0x4a, 0x2d, 0x92, 0x60, 0x52, 0x60, 0xd4,
	0x60, 0x09, 0x82, 0xf2, 0x84, 0x84, 0xb8, 0x58, 0xf2, 0x12, 0x73, 0x53, 0x25, 0x98, 0xc1, 0x2a,
	0xc1, 0x6c, 0x90, 0x58, 0x4a, 0x62, 0x49, 0xa2, 0x04, 0x8b, 0x02, 0xa3, 0x06, 0x4f, 0x10, 0x98,
	0x0d, 0x12, 0xcb, 0x4d, 0x2d, 0x49, 0x94, 0x60, 0x85, 0x88, 0x81, 0xd8, 0x4e, 0xda, 0x51, 0x9a,
	0xe9, 0x99, 0x25, 0x19, 0xa5, 0x49, 0x7a, 0xc9, 0xf9, 0xb9, 0xfa, 0x29, 0xa9, 0x99, 0x15, 0x99,
	0xc5, 0xfa, 0xc5, 0x25, 0xf9, 0x45, 0x89, 0xe9, 0xa9, 0xfa, 0x68, 0xce, 0x4e, 0x62, 0x03, 0xbb,
	0xd8, 0x18, 0x10, 0x00, 0x00, 0xff, 0xff, 0xc6, 0x79, 0x46, 0x1c, 0xd0, 0x00, 0x00, 0x00,
}
