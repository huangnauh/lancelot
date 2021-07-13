package server

import "encoding/binary"

func GetKeyBytes(otype ObjectType, key []byte) []byte {
	k := make([]byte, 0, len(key)+1)
	k = append(k, byte(otype))
	k = append(k, key...)
	return k
}

func EncodeTTLValue(timestamp uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, timestamp)
	return b
}

func DecodeTTLValue(b []byte) (uint64, error) {
	if len(b) < 8 {
		return 0, ErrValueTooShort
	}
	return binary.BigEndian.Uint64(b), nil
}

func GetTTLBytes(expire int64, otype ObjectType, key []byte) []byte {
	e := make([]byte, 8)
	binary.BigEndian.PutUint64(e, uint64(expire))
	k := make([]byte, 0, 1+8+1+len(key))
	k = append(k, byte(TTLType))
	k = append(k, e...)
	k = append(k, byte(otype))
	k = append(k, key...)
	return k
}
