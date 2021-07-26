package command

import (
	"encoding/binary"

	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

func GetKeyBytes(userId uint16, db uint8, otype ObjectType, key []byte) []byte {
	k := make([]byte, 2+1+1+len(key))
	binary.BigEndian.PutUint16(k, userId)
	k[2] = byte(db)
	k[3] = byte(otype)
	copy(k[4:], key)
	return k
}

func GetKeyPrefix(userId uint16, db uint8, otype ObjectType) []byte {
	k := make([]byte, 2+1+1)
	binary.BigEndian.PutUint16(k, userId)
	k[2] = byte(db)
	k[3] = byte(otype)
	return k
}

func EncodeTTLValue(timestamp uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, timestamp)
	return b
}

func DecodeTTLValue(b []byte) (uint64, error) {
	if len(b) < 8 {
		return 0, xerror.ErrValueTooShort
	}
	return binary.BigEndian.Uint64(b), nil
}

func GetTTLPrefix(expire int64) []byte {
	k := make([]byte, 1+8)
	k[0] = byte(TTLType)
	binary.BigEndian.PutUint64(k[1:], uint64(expire))
	return k
}
