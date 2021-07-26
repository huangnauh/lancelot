package command

import (
	"encoding/binary"
	"strings"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

type ObjectEncoding byte
type ObjectType byte
type TTL byte

const (
	KeyType   ObjectType = 'k'
	JsonType  ObjectType = 'j'
	ListType  ObjectType = 'l'
	SetType   ObjectType = 's'
	ZsetType  ObjectType = 'z'
	HashType  ObjectType = 'h'
	TTLType   ObjectType = 't'
	UserType  ObjectType = 'u'
	CountType ObjectType = 'c'

	KeyTTL   TTL = 'k'
	ValueTTL TTL = 'v'

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
	UserId    uint16
	Db        uint8
	Key       []byte
	Type      ObjectType
	TTL       int64
	Timestamp uint64
	Value     []byte
}

func NewObject(user uint16, db uint8, typo ObjectType, key []byte) *Object {
	return &Object{
		UserId: user,
		Db:     db,
		Key:    key,
		Type:   typo,
	}
}

func (o *Object) IsSimple() bool {
	return o.Type == KeyType || o.Type == JsonType
}

func (o *Object) getBytes(typo ObjectType, data ...[]byte) []byte {
	count := 0
	for _, v := range data {
		count += len(v)
	}
	k := make([]byte, 1+2+1+count)
	k[0] = DataPrefix
	binary.BigEndian.PutUint16(k[1:], o.UserId)
	k[3] = byte(o.Db)
	k[4] = byte(typo)
	start := 5
	for _, v := range data {
		copy(k[start:], v)
		start += len(v)
	}
	return k
}

func (o *Object) GetKeyBytes() []byte {
	return o.getBytes(KeyType, o.Key)
}

func (o *Object) GetObjectKeyBytes() []byte {
	return o.getBytes(o.Type, o.Key)
}

func (o *Object) GetKeyFieldBytes(field []byte) []byte {
	return o.getBytes(o.Type, o.Value, field)
}

func (o *Object) GetValueBytesPrefix() []byte {
	return o.getBytes(o.Type, o.Value)
}

func (o *Object) getTTLBytes(ttlType TTL, data []byte) []byte {
	k := make([]byte, 1+8+2+1+1+len(data))
	k[0] = TTLPrefix
	binary.BigEndian.PutUint64(k[1:], uint64(o.TTL))
	binary.BigEndian.PutUint16(k[9:], o.UserId)
	k[11] = byte(o.Db)
	k[12] = byte(ttlType)
	copy(k[13:], data)
	return k
}

func (o *Object) GetTTLKeyBytes() []byte {
	return o.getTTLBytes(KeyTTL, o.Key)
}

func (o *Object) GetTTLValueBytes() []byte {
	return o.getTTLBytes(ValueTTL, o.Value)
}

func GetObjectFromTTL(ttl []byte) (*Object, error) {
	if len(ttl) <= 9 {
		return nil, xerror.ErrValueTooShort
	}
	if ttl[0] != byte(TTLPrefix) {
		return nil, xerror.ErrNotTTL
	}

	o := &Object{
		TTL:    int64(binary.BigEndian.Uint64(ttl[1:9])),
		UserId: binary.BigEndian.Uint16(ttl[9:11]),
		Db:     ttl[11],
	}
	if ttl[12] == byte(KeyTTL) {
		o.Key = ttl[13:]
	} else if ttl[12] == byte(ValueTTL) {
		o.Value = ttl[13:]
	} else {
		return nil, xerror.ErrNotTTL
	}
	return o, nil
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
func ObjectHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) == 0 {
		return txn.SetWrongArgs(OBJECT_COMMAND)
	}
	subcommand := strings.ToLower(utils.B2S(args[0]))
	if len(args) == 1 && subcommand == HELP_COMMAND {
		return objectHelpInfo
	}

	if len(args) != 2 {
		return txn.SetWrongSubArgs(subcommand, ObjectHelpCommand)
	}

	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	switch subcommand {
	case ENCODING_COMMAND:
		err := getTxnObject(txn, key, object)
		if err != nil {
			return nil
		}
		return SimpleString(object.ObjectEncoding().String())
	case IDLETIME_COMMAND:
		err := getTxnObject(txn, key, object)
		if err != nil {
			return nil
		}
		now := oracle.ExtractPhysical(txn.StartTS())
		timepstamp := oracle.ExtractPhysical(object.Timestamp)
		return SimpleInt(int((now - timepstamp) / 1000))
	case REFCOUNT_COMMAND:
		return SimpleInt(0)
	case FREQ_COMMAND:
		return SimpleInt(0)
	default:
		return txn.SetWrongSubArgs(subcommand, ObjectHelpCommand)
	}
}
