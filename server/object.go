package server

import "encoding/binary"

type ObjectEncoding byte
type ObjectType byte

const (
	KeyType  ObjectType = 'k'
	ListType ObjectType = 'l'
	SetType  ObjectType = 's'
	ZsetType ObjectType = 'z'
	HashType ObjectType = 'h'
	TTLType  ObjectType = 't'

	EncodingRaw = ObjectEncoding(iota)
	EncodingInt
	EncodingHT
	EncodingZipmap
	EncodingLinkedlist
	EncodingZiplist
	EncodingIntset
	EncodingSkiplist
	EncodingEmbstr
	EncodingQuicklist
	EncodingStream
)

func (e ObjectEncoding) String() string {
	switch e {
	case EncodingRaw:
		return "raw"
	case EncodingInt:
		return "int"
	case EncodingHT:
		return "hashtable"
	case EncodingZipmap:
		return "zipmap"
	case EncodingLinkedlist:
		return "linkedlist"
	case EncodingZiplist:
		return "ziplist"
	case EncodingIntset:
		return "intset"
	case EncodingSkiplist:
		return "skiplist"
	case EncodingEmbstr:
		return "embstr"
	case EncodingQuicklist:
		return "quicklist"
	case EncodingStream:
		return "stream"
	default:
		return "unknown"
	}
}

type Object struct {
	Type      ObjectType
	TTL       int64
	Timestamp uint64
	Value     []byte
}

func ObjectEncode(o *Object) []byte {
	b := make([]byte, len(o.Value)+1+8+8)
	b[0] = byte(o.Type)
	binary.BigEndian.PutUint64(b[1:], uint64(o.TTL))
	binary.BigEndian.PutUint64(b[9:], o.Timestamp)
	copy(b[1+8+8:], o.Value)
	return b
}

func ObjectDecode(b []byte, o *Object) error {
	if len(b) < 1+8+8 {
		return ErrValueTooShort
	}
	o.Type = ObjectType(b[0])
	o.TTL = int64(binary.BigEndian.Uint64(b[1:9]))
	o.Timestamp = binary.BigEndian.Uint64(b[9:17])
	o.Value = b[1+8+8:]
	return nil
}
