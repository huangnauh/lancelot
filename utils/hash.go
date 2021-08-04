package utils

import (
	"hash/fnv"
)

func Sum64(s []byte) uint64 {
	h := fnv.New64a()
	h.Write(s)
	return h.Sum64()
}

func IsPowerOfTwo16(number uint64) bool {
	return (number != 0) && (number&(number-1)) == 0 && number <= 1<<16
}

func GetShard(s []byte, shardMark uint64) uint16 {
	return uint16(Sum64(s) & shardMark)
}
