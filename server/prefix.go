package server

import "encoding/binary"

var (
	StringPrefix = []byte("s")
	TTLPrefix    = []byte("t")
)

func GetKeyBytes(prefix, key []byte) []byte {
	k := make([]byte, 0, len(key)+len(prefix))
	k = append(k, prefix...)
	k = append(k, key...)
	return k
}

func SetStringValue(expire int64, value []byte) []byte {
	e := make([]byte, 8, 8+len(value))
	binary.BigEndian.PutUint64(e[:8], uint64(expire))
	e = append(e, value...)
	return e
}

func GetStringValue(value []byte) ([]byte, int64, error) {
	if len(value) < 8 {
		return nil, 0, ErrValueTooShort
	}
	expire := binary.BigEndian.Uint64(value[:8])
	return value[8:], int64(expire), nil
}

func GetTTLBytes(expire int64, prefix, key []byte) []byte {
	e := make([]byte, 8)
	binary.BigEndian.PutUint64(e, uint64(expire))
	k := make([]byte, 0, len(TTLPrefix)+8+len(prefix)+len(key))
	k = append(k, TTLPrefix...)
	k = append(k, e...)
	k = append(k, prefix...)
	k = append(k, key...)
	return k
}
