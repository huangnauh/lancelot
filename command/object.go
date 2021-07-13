package command

import (
	"encoding/binary"
	"strings"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

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

	ObjectHelpCommand = "OBJECT HELP"
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

var (
	objectHelpInfo = [][]byte{
		[]byte("OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
		[]byte("ENCODING <key>"),
		[]byte("    Return the kind of internal representation used in order to store the value"),
		[]byte("    associated with a <key>."),
		[]byte("FREQ <key>"),
		[]byte("    Return the access frequency index of the <key>. The returned integer is"),
		[]byte("    proportional to the logarithm of the recent access frequency of the key."),
		[]byte("IDLETIME <key>"),
		[]byte("    Return the idle time of the <key>, that is the approximated number of"),
		[]byte("    seconds elapsed since the last access to the key."),
		[]byte("REFCOUNT <key>"),
		[]byte("    Return the number of references of the value associated with the specified"),
		[]byte("    <key>."),
		[]byte("HELP"),
		[]byte("    Prints this help."),
	}
)

type Object struct {
	Type      ObjectType
	TTL       int64
	Timestamp uint64
	Value     []byte
}

func (o *Object) GetHashBytes(field []byte) []byte {
	k := make([]byte, 1+len(o.Value)+len(field))
	k[0] = byte(HashType)
	// binary.BigEndian.PutUint64(k[1:], uint64(o.Timestamp))
	copy(k[1:], o.Value)
	copy(k[1+len(o.Value):], field)
	return k
}

func (o *Object) ObjectEncoding() ObjectEncoding {
	switch o.Type {
	case KeyType:
		return EncodingEmbstr
	default:
		return EncodingRaw
	}
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
		return xerror.ErrValueTooShort
	}
	o.Type = ObjectType(b[0])
	o.TTL = int64(binary.BigEndian.Uint64(b[1:9]))
	o.Timestamp = binary.BigEndian.Uint64(b[9:17])
	o.Value = b[1+8+8:]
	return nil
}

// (generic) OBJECT subcommand [arguments [arguments ...]]
func ObjectHandle(txn *store.Txn, args [][]byte) store.RespFunc {
	if len(args) == 0 {
		return txn.LazyWriteWrongArgs(OBJECT_COMMAND)
	}
	subcommand := strings.ToLower(string(args[0]))
	if len(args) == 1 && subcommand == HELP_COMMAND {
		return txn.LazyWriteArrayBulk(objectHelpInfo)
	}

	if len(args) != 2 {
		return txn.LazyWriteWrongSubArgs(subcommand, ObjectHelpCommand)
	}

	switch subcommand {
	case ENCODING_COMMAND:
		key := GetKeyBytes(KeyType, args[0])
		object, err := getTxnObject(txn, key)
		if err != nil {
			return txn.LazyWriteNull()
		}
		return txn.LazyWriteString(object.ObjectEncoding().String())
	case IDLETIME_COMMAND:
		key := GetKeyBytes(KeyType, args[0])
		object, err := getTxnObject(txn, key)
		if err != nil {
			return txn.LazyWriteNull()
		}
		now := oracle.ExtractPhysical(txn.StartTS())
		timepstamp := oracle.ExtractPhysical(object.Timestamp)
		return txn.LazyWriteInt(int((now - timepstamp) / 1000))
	case REFCOUNT_COMMAND:
		return txn.LazyWriteInt(0)
	case FREQ_COMMAND:
		return txn.LazyWriteInt(0)
	default:
		return txn.LazyWriteWrongSubArgs(subcommand, ObjectHelpCommand)
	}
}
