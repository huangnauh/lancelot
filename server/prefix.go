package server

import "encoding/binary"

func GetKeyBytes(otype ObjectType, key []byte) []byte {
	k := make([]byte, len(key)+1)
	k[0] = byte(otype)
	copy(k[1:], key)
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
	k := make([]byte, 1+8+1+len(key))
	k[0] = byte(TTLType)
	binary.BigEndian.PutUint64(k[1:], uint64(expire))
	k[9] = byte(otype)
	copy(k[10:], key)
	return k
}
