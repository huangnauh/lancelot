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
	KeyType     ObjectType = 'k'
	JsonType    ObjectType = 'j'
	ListType    ObjectType = 'l'
	SetType     ObjectType = 's'
	ZsetType    ObjectType = 'z'
	HashType    ObjectType = 'h'
	TTLType     ObjectType = 't'
	UserType    ObjectType = 'u'
	CountType   ObjectType = 'c'
	UnknownType ObjectType = '?'
	GeneralType ObjectType = '*'
	PubType     ObjectType = 'p'
	MessageType ObjectType = 'm'

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
	DefaultHashMark   = 1<<4 - 1
)

var ObjectNameMap = map[string]ObjectType{
	"string": KeyType,
	"json":   JsonType,
	"list":   ListType,
	"hash":   HashType,
	"zset":   ZsetType,
	"set":    SetType,
}

func (o ObjectType) Type() string {
	switch o {
	case KeyType:
		return "string"
	case JsonType:
		return "json"
	case ListType:
		return "list"
	case HashType:
		return "hash"
	case ZsetType:
		return "zset"
	case SetType:
		return "set"
	default:
		return "unknown"
	}
}

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
	ValueTTL  int64
	Timestamp uint64
	Value     []byte
	Hash      uint16
}

func NewObject(user uint16, db uint8, typo ObjectType, key []byte) *Object {
	return &Object{
		UserId: user,
		Db:     db,
		Key:    key,
		Type:   typo,
		Hash:   DefaultHashMark,
	}
}

func GetUserPrefix(user uint16) []byte {
	k := make([]byte, 1+2)
	k[0] = DataPrefix
	binary.BigEndian.PutUint16(k[1:], user)
	return k
}

func GetUserDBPrefix(user uint16, db uint8) []byte {
	k := make([]byte, 1+2+1)
	k[0] = DataPrefix
	binary.BigEndian.PutUint16(k[1:], user)
	k[3] = byte(db)
	return k
}

func GetDataPrefix(user uint16, db uint8, typo ObjectType, data []byte) []byte {
	k := make([]byte, 1+2+1+1+len(data))
	k[0] = DataPrefix
	binary.BigEndian.PutUint16(k[1:], user)
	k[3] = byte(db)
	k[4] = byte(typo)
	copy(k[5:], data)
	return k
}

func (o *Object) IsSimple() bool {
	return o.Type == KeyType || o.Type == JsonType
}

func (o *Object) TTLType() TTL {
	if o.Type == PubType {
		return KeyTTL
	}
	return ValueTTL
}

func (o *Object) getKeyBytes(typo ObjectType, data ...[]byte) []byte {
	count := 0
	for _, v := range data {
		count += len(v)
	}
	k := make([]byte, 1+2+1+1+count)
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

func GetObjectFromKV(key, value []byte) (*Object, error) {
	if len(key) < 5 || len(value) < 1 {
		return nil, xerror.ErrValueTooShort
	}
	o := &Object{}
	o.Key = key[5:]
	err := ObjectDecode(value, o)
	if err != nil {
		return nil, err
	}
	return o, nil
}

func (o *Object) GetKeyBytes() []byte {
	return o.getKeyBytes(KeyType, o.Key)
}

// func (o *Object) GetObjectKeyBytes() []byte {
// 	return o.getKeyBytes(o.Type, o.Key)
// }

func (o *Object) GetKeyFieldBytes(field []byte) []byte {
	return o.getKeyBytes(o.Type, o.Value, field)
}

func (o *Object) GetValueBytesPrefix() []byte {
	return o.getKeyBytes(o.Type, o.Value)
}

func GetTTLPrefix(expire int64) []byte {
	k := make([]byte, 1+8)
	k[0] = byte(TTLPrefix)
	binary.BigEndian.PutUint64(k[1:], uint64(expire))
	return k
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
	b := make([]byte, len(o.Value)+1+8+8+2)
	b[0] = byte(o.Type)
	ttlType := o.TTLType()
	if ttlType == KeyTTL {
		binary.BigEndian.PutUint64(b[1:], uint64(o.TTL))
	} else {
		binary.BigEndian.PutUint64(b[1:], uint64(o.ValueTTL))
	}
	binary.BigEndian.PutUint64(b[9:], o.Timestamp)
	binary.BigEndian.PutUint16(b[17:], o.Hash)
	copy(b[1+8+8+2:], o.Value)
	return b
}

func (o *Object) CleanValue(typo ObjectType) {
	o.Type = typo
	o.TTL = 0
	o.Timestamp = 0
	o.Hash = DefaultHashMark
	o.ValueTTL = 0
	o.Value = nil
}

func ObjectDecode(b []byte, o *Object) error {
	if len(b) < 1+8+8+2 {
		return xerror.ErrValueTooShort
	}
	o.Type = ObjectType(b[0])
	ttlType := o.TTLType()
	if ttlType == KeyTTL {
		o.TTL = int64(binary.BigEndian.Uint64(b[1:9]))
	} else {
		o.ValueTTL = int64(binary.BigEndian.Uint64(b[1:9]))
	}
	o.Timestamp = binary.BigEndian.Uint64(b[9:17])
	o.Hash = binary.BigEndian.Uint16(b[17:19])
	o.Value = b[19:]
	return nil
}

func getTxnObject(txn *store.Txn, key []byte, object *Object, clear bool) error {
	getType := object.Type
	value, err := txn.Get(key)
	if err != nil {
		return err
	}
	err = ObjectDecode(value, object)
	if err != nil {
		return store.KeyNotFound
	}

	if object.TTL > 0 && object.TTL < txn.Now {
		if clear {
			err = DeleteKey(txn, key, object, object.TTL)
			if err != nil {
				return err
			}
		}
		object.CleanValue(getType)
		return store.KeyNotFound
	}

	if getType != object.Type {
		return xerror.WrongTypeError
	}
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
		err := getTxnObject(txn, key, object, false)
		if err != nil {
			return nil
		}
		return SimpleString(object.ObjectEncoding().String())
	case IDLETIME_COMMAND:
		err := getTxnObject(txn, key, object, false)
		if err != nil {
			return nil
		}
		timepstamp := oracle.ExtractPhysical(object.Timestamp)
		return SimpleInt(int((txn.Now - timepstamp) / 1000))
	case REFCOUNT_COMMAND:
		return SimpleInt(0)
	case FREQ_COMMAND:
		return SimpleInt(0)
	default:
		return txn.SetWrongSubArgs(subcommand, ObjectHelpCommand)
	}
}
