package kvdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
)

// ============================================================================
// Source from FoundationDB
// This implements a subset of FoundationDB tuple to avoid dependencies on
// FoundationDB for other drivers
// ============================================================================

// A TupleElement is one of the types that may be encoded in tuples.
// Although the Go compiler cannot enforce this, it is a programming
// error to use an unsupported types as a TupleElement (and will typically
// result in a runtime panic).
//
// The valid types for TupleElement are []byte (or fdb.KeyConvertible), string,
// int64 (or int), float, double, bool, UUID, Tuple, and nil.
type TupleElement interface{}

// Tuple is a slice of objects that can be encoded as tuples. If
// any of the TupleElements are of unsupported types, a runtime panic will occur
// when the Tuple is packed.
//
// Given a Tuple T containing objects only of these types, then T will be
// identical to the Tuple returned by unpacking the byte slice obtained by
// packing T (modulo type normalization to []byte and int64).
type Tuple []TupleElement

// Pack returns a new byte slice encoding the provided tuple. Pack will panic if
// the tuple contains an element of any type other than []byte,
// fdb.KeyConvertible, string, int64, int, uint64, uint, *big.Int, big.Int, float32,
// float64, bool, kvdb.UUID, nil, or a Tuple with elements of valid types. It will
// also panic if an integer is specified with a value outside the range
// [-2**2040+1, 2**2040-1]
//
// Tuple satisfies the fdb.KeyConvertible interface, so it is not necessary to
// call Pack when using a Tuple with a FoundationDB API function that requires a
// key.
func (t Tuple) Pack() []byte {
	p := packer{buf: make([]byte, 0, 64)}
	p.encodeTuple(t, false)
	return p.buf
}

// Unpack returns the tuple encoded by the provided byte slice, or an error if
// the key does not correctly encode a FoundationDB tuple.
func Unpack(b []byte) (Tuple, error) {
	t, _, err := decodeTuple(b, false)
	return t, err
}

// UUID wraps a basic byte array as a UUID. We do not provide any special
// methods for accessing or generating the UUID, but as Go does not provide
// a built-in UUID type, this simple wrapper allows for other libraries
// to write the output of their UUID type as a 16-byte array into
// an instance of this type.
type UUID [16]byte

// Type codes: These prefix the different elements in a packed Tuple
// to indicate what type they are.
const nilCode = 0x00
const bytesCode = 0x01
const stringCode = 0x02
const nestedCode = 0x05
const intZeroCode = 0x14
const posIntEnd = 0x1d
const negIntStart = 0x0b
const floatCode = 0x20
const doubleCode = 0x21
const falseCode = 0x26
const trueCode = 0x27
const uuidCode = 0x30

type packer struct {
	buf []byte
}

func (p *packer) putByte(b byte) {
	p.buf = append(p.buf, b)
}

func (p *packer) putBytes(b []byte) {
	p.buf = append(p.buf, b...)
}

func (p *packer) putBytesNil(b []byte, i int) {
	for i >= 0 {
		p.putBytes(b[:i+1])
		p.putByte(0xFF)
		b = b[i+1:]
		i = bytes.IndexByte(b, 0x00)
	}
	p.putBytes(b)
}

func (p *packer) encodeBytes(code byte, b []byte) {
	p.putByte(code)
	if i := bytes.IndexByte(b, 0x00); i >= 0 {
		p.putBytesNil(b, i)
	} else {
		p.putBytes(b)
	}
	p.putByte(0x00)
}

func (p *packer) encodeUint(i uint64) {
	if i == 0 {
		p.putByte(intZeroCode)
		return
	}

	n := bisectLeft(i)
	var scratch [8]byte

	p.putByte(byte(intZeroCode + n))
	binary.BigEndian.PutUint64(scratch[:], i)

	p.putBytes(scratch[8-n:])
}

func (p *packer) encodeInt(i int64) {
	if i >= 0 {
		p.encodeUint(uint64(i))
		return
	}

	n := bisectLeft(uint64(-i))
	var scratch [8]byte

	p.putByte(byte(intZeroCode - n))
	offsetEncoded := int64(sizeLimits[n]) + i
	binary.BigEndian.PutUint64(scratch[:], uint64(offsetEncoded))

	p.putBytes(scratch[8-n:])
}

func (p *packer) encodeBigInt(i *big.Int) {
	length := len(i.Bytes())
	if length > 0xff {
		panic(fmt.Sprintf("Integer magnitude is too large (more than 255 bytes)"))
	}

	if i.Sign() >= 0 {
		intBytes := i.Bytes()
		if length > 8 {
			p.putByte(byte(posIntEnd))
			p.putByte(byte(len(intBytes)))
		} else {
			p.putByte(byte(intZeroCode + length))
		}

		p.putBytes(intBytes)
	} else {
		add := new(big.Int).Lsh(big.NewInt(1), uint(length*8))
		add.Sub(add, big.NewInt(1))
		transformed := new(big.Int)
		transformed.Add(i, add)

		intBytes := transformed.Bytes()
		if length > 8 {
			p.putByte(byte(negIntStart))
			p.putByte(byte(length ^ 0xff))
		} else {
			p.putByte(byte(intZeroCode - length))
		}

		// For large negative numbers whose absolute value begins with 0xff bytes,
		// the transformed bytes may begin with 0x00 bytes. However, intBytes
		// will only contain the non-zero suffix, so this loop is needed to make
		// the value written be the correct length.
		for i := len(intBytes); i < length; i++ {
			p.putByte(0x00)
		}

		p.putBytes(intBytes)
	}
}

func (p *packer) encodeFloat(f float32) {
	var scratch [4]byte
	binary.BigEndian.PutUint32(scratch[:], math.Float32bits(f))
	adjustFloatBytes(scratch[:], true)

	p.putByte(floatCode)
	p.putBytes(scratch[:])
}

func (p *packer) encodeDouble(d float64) {
	var scratch [8]byte
	binary.BigEndian.PutUint64(scratch[:], math.Float64bits(d))
	adjustFloatBytes(scratch[:], true)

	p.putByte(doubleCode)
	p.putBytes(scratch[:])
}

func (p *packer) encodeUUID(u UUID) {
	p.putByte(uuidCode)
	p.putBytes(u[:])
}

func (p *packer) encodeTuple(t Tuple, nested bool) {
	if nested {
		p.putByte(nestedCode)
	}

	for i, e := range t {
		switch e := e.(type) {
		case Tuple:
			p.encodeTuple(e, true)
		case nil:
			p.putByte(nilCode)
			if nested {
				p.putByte(0xff)
			}
		case int:
			p.encodeInt(int64(e))
		case int64:
			p.encodeInt(e)
		case uint:
			p.encodeUint(uint64(e))
		case uint64:
			p.encodeUint(e)
		case *big.Int:
			p.encodeBigInt(e)
		case big.Int:
			p.encodeBigInt(&e)
		case []byte:
			p.encodeBytes(bytesCode, e)
		case string:
			p.encodeBytes(stringCode, []byte(e))
		case float32:
			p.encodeFloat(e)
		case float64:
			p.encodeDouble(e)
		case bool:
			if e {
				p.putByte(trueCode)
			} else {
				p.putByte(falseCode)
			}
		case UUID:
			p.encodeUUID(e)
		default:
			panic(fmt.Sprintf("unencodable element at index %d (%v, type %T)", i, t[i], t[i]))
		}
	}

	if nested {
		p.putByte(0x00)
	}
}

func decodeTuple(b []byte, nested bool) (Tuple, int, error) {
	var t Tuple

	var i int

	for i < len(b) {
		var el interface{}
		var off int

		switch {
		case b[i] == nilCode:
			if !nested {
				el = nil
				off = 1
			} else if i+1 < len(b) && b[i+1] == 0xff {
				el = nil
				off = 2
			} else {
				return t, i + 1, nil
			}
		case b[i] == bytesCode:
			el, off = decodeBytes(b[i:])
		case b[i] == stringCode:
			el, off = decodeString(b[i:])
		case negIntStart+1 < b[i] && b[i] < posIntEnd:
			el, off = decodeInt(b[i:])
		case negIntStart+1 == b[i] && (b[i+1]&0x80 != 0):
			el, off = decodeInt(b[i:])
		case negIntStart <= b[i] && b[i] <= posIntEnd:
			el, off = decodeBigInt(b[i:])
		case b[i] == floatCode:
			if i+5 > len(b) {
				return nil, i, fmt.Errorf("insufficient bytes to decode float starting at position %d of byte array for tuple", i)
			}
			el, off = decodeFloat(b[i:])
		case b[i] == doubleCode:
			if i+9 > len(b) {
				return nil, i, fmt.Errorf("insufficient bytes to decode double starting at position %d of byte array for tuple", i)
			}
			el, off = decodeDouble(b[i:])
		case b[i] == trueCode:
			el = true
			off = 1
		case b[i] == falseCode:
			el = false
			off = 1
		case b[i] == uuidCode:
			if i+17 > len(b) {
				return nil, i, fmt.Errorf("insufficient bytes to decode UUID starting at position %d of byte array for tuple", i)
			}
			el, off = decodeUUID(b[i:])
		case b[i] == nestedCode:
			var err error
			el, off, err = decodeTuple(b[i+1:], true)
			if err != nil {
				return nil, i, err
			}
			off++
		default:
			return nil, i, fmt.Errorf("unable to decode tuple element with unknown typecode %02x", b[i])
		}

		t = append(t, el)
		i += off
	}

	return t, i, nil
}

func findTerminator(b []byte) int {
	bp := b
	var length int

	for {
		idx := bytes.IndexByte(bp, 0x00)
		length += idx
		if idx+1 == len(bp) || bp[idx+1] != 0xFF {
			break
		}
		length += 2
		bp = bp[idx+2:]
	}

	return length
}

func decodeBytes(b []byte) ([]byte, int) {
	idx := findTerminator(b[1:])
	return bytes.Replace(b[1:idx+1], []byte{0x00, 0xFF}, []byte{0x00}, -1), idx + 2
}

func decodeString(b []byte) (string, int) {
	bp, idx := decodeBytes(b)
	return string(bp), idx
}

func decodeInt(b []byte) (interface{}, int) {
	if b[0] == intZeroCode {
		return int64(0), 1
	}

	var neg bool

	n := int(b[0]) - intZeroCode
	if n < 0 {
		n = -n
		neg = true
	}

	bp := make([]byte, 8)
	copy(bp[8-n:], b[1:n+1])

	var ret int64
	binary.Read(bytes.NewBuffer(bp), binary.BigEndian, &ret)

	if neg {
		return ret - int64(sizeLimits[n]), n + 1
	}

	if ret > 0 {
		return ret, n + 1
	}

	// The encoded value claimed to be positive yet when put in an int64
	// produced a negative value. This means that the number must be a positive
	// 64-bit value that uses the most significant bit. This can be fit in a
	// uint64, so return that. Note that this is the *only* time we return
	// a uint64.
	return uint64(ret), n + 1
}

func decodeBigInt(b []byte) (interface{}, int) {
	val := new(big.Int)
	offset := 1
	var length int

	if b[0] == negIntStart || b[0] == posIntEnd {
		length = int(b[1])
		if b[0] == negIntStart {
			length ^= 0xff
		}

		offset += 1
	} else {
		// Must be a negative 8 byte integer
		length = 8
	}

	val.SetBytes(b[offset : length+offset])

	if b[0] < intZeroCode {
		sub := new(big.Int).Lsh(big.NewInt(1), uint(length)*8)
		sub.Sub(sub, big.NewInt(1))
		val.Sub(val, sub)
	}

	// This is the only value that fits in an int64 or uint64 that is decoded with this function
	if val.Cmp(minInt64BigInt) == 0 {
		return val.Int64(), length + offset
	}

	return val, length + offset
}

func decodeFloat(b []byte) (float32, int) {
	bp := make([]byte, 4)
	copy(bp, b[1:])
	adjustFloatBytes(bp, false)
	var ret float32
	binary.Read(bytes.NewBuffer(bp), binary.BigEndian, &ret)
	return ret, 5
}

func decodeDouble(b []byte) (float64, int) {
	bp := make([]byte, 8)
	copy(bp, b[1:])
	adjustFloatBytes(bp, false)
	var ret float64
	binary.Read(bytes.NewBuffer(bp), binary.BigEndian, &ret)
	return ret, 9
}

func decodeUUID(b []byte) (UUID, int) {
	var u UUID
	copy(u[:], b[1:])
	return u, 17
}

func bisectLeft(u uint64) int {
	var n int
	for sizeLimits[n] < u {
		n++
	}
	return n
}

func adjustFloatBytes(b []byte, encode bool) {
	if (encode && b[0]&0x80 != 0x00) || (!encode && b[0]&0x80 == 0x00) {
		// Negative numbers: flip all of the bytes.
		for i := 0; i < len(b); i++ {
			b[i] = b[i] ^ 0xff
		}
	} else {
		// Positive number: flip just the sign bit.
		b[0] = b[0] ^ 0x80
	}
}

var sizeLimits = []uint64{
	1<<(0*8) - 1,
	1<<(1*8) - 1,
	1<<(2*8) - 1,
	1<<(3*8) - 1,
	1<<(4*8) - 1,
	1<<(5*8) - 1,
	1<<(6*8) - 1,
	1<<(7*8) - 1,
	1<<(8*8) - 1,
}

var minInt64BigInt = big.NewInt(math.MinInt64)
