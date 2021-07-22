package command

import (
	"encoding/binary"

	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

func GetKeyBytes(otype ObjectType, key []byte) []byte {
	k := make([]byte, len(key)+1)
	k[0] = byte(otype)
	copy(k[1:], key)
	return k
}

func GetKeyPrefix(otype ObjectType) []byte {
	return []byte{byte(otype)}
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

func GetObjectFromTTL(ttl []byte) (*Object, error) {
	if len(ttl) <= 9 {
		return nil, xerror.ErrValueTooShort
	}
	if ttl[0] != byte(TTLType) {
		return nil, xerror.ErrNotTTL
	}

	o := &Object{
		TTL:  int64(binary.BigEndian.Uint64(ttl[1:9])),
		Type: ObjectType(ttl[9]),
	}

	if o.IsSimple() {
		o.Key = ttl[10:]
	} else {
		o.Value = ttl[10:]
	}
	return o, nil
}

func GetTTLPrefix(expire int64) []byte {
	k := make([]byte, 1+8)
	k[0] = byte(TTLType)
	binary.BigEndian.PutUint64(k[1:], uint64(expire))
	return k
}
