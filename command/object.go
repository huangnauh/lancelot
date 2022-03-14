package command

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/tikv/client-go/v2/oracle"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

type ObjectEncoding byte
type ObjectType byte
type TTL byte
type ChangeType byte

const (
	UnknownType ObjectType = '?'
	AListType   ObjectType = 'a'
	BListType   ObjectType = 'b'
	CountType   ObjectType = 'c'
	ServerType  ObjectType = 'e'
	GroupType   ObjectType = 'g'
	HashType    ObjectType = 'h'
	ClientType  ObjectType = 'i'
	JsonType    ObjectType = 'j'
	StringType  ObjectType = 'k'
	LListType   ObjectType = 'l'
	MessageType ObjectType = 'm'
	GeoType     ObjectType = 'o'
	StreamType  ObjectType = 'p'
	PubSubType  ObjectType = 'q'
	ScriptType  ObjectType = 'r'
	SetType     ObjectType = 's'
	TTLType     ObjectType = 't'
	UserType    ObjectType = 'u'
	ZsetType    ObjectType = 'z'
	GeneralType ObjectType = '*'

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
	DefaultHashMark   = 1<<10 - 1

	MinusCount    ChangeType = 0x01
	PlusCount     ChangeType = 0x02
	GCChange      ChangeType = 0x04
	DeleteKeyType ChangeType = 0x08
)

func (o ObjectType) Type() string {
	switch o {
	case StringType:
		return "string"
	case JsonType:
		return "json"
	case AListType:
		return "list"
	case BListType:
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

type GetKeyFunc func(o *Object, field []byte) []byte

func GetHashKey(o *Object, field []byte) []byte {
	return o.GetValueBytes(field)
}

func GetZsetKey(o *Object, field []byte) []byte {
	return o.GetValueBytes(EncodeMemberKey(field))
}

var (
	GetKeyFuncs = map[ObjectType]GetKeyFunc{
		HashType:  GetHashKey,
		SetType:   GetHashKey,
		AListType: GetHashKey,
		ZsetType:  GetZsetKey,
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
	Hash      uint16
	Extra     []byte
}

func (c *Command) NewObject(txn *store.Txn, typo ObjectType, key []byte) *Object {
	hash := txn.Config.Redis.ObjectHash
	if hash == 0 {
		hash = DefaultHashMark
	}
	return &Object{
		UserId: txn.UserId,
		Db:     txn.DBId,
		Key:    key,
		Type:   typo,
		Hash:   hash,
	}
}

func (o *Object) Info() string {
	if o.IsSimple() {
		return fmt.Sprintf("user:%d, db:%d, type: %s, ttl:%d, timestamp: %d, value: %s",
			o.UserId, o.Db, string(o.Type), o.TTL, o.Timestamp, o.Value)
	} else {
		id, _ := uuid.FromBytes(o.Value)
		return fmt.Sprintf("user:%d, db:%d, type: %s, ttl:%d, timestamp: %d, value: %s",
			o.UserId, o.Db, string(o.Type), o.TTL, o.Timestamp, id.String())
	}
}

func (o *Object) IsGeo() bool {
	return o.Type == GeoType
}

func GetUserPrefix(typo PrefixType, user uint16) []byte {
	k := make([]byte, 1+2)
	k[0] = byte(typo)
	binary.BigEndian.PutUint16(k[1:], user)
	return k
}

func GetUserDBPrefix(typo PrefixType, user uint16, db uint8) []byte {
	k := make([]byte, 1+2+1)
	k[0] = byte(typo)
	binary.BigEndian.PutUint16(k[1:], user)
	k[3] = byte(db)
	return k
}

func GetGeneralBytes(typo, keyPrefix PrefixType) []byte {
	k := make([]byte, 2)
	k[0] = byte(typo)
	k[1] = byte(keyPrefix)
	return k
}

func GetKeyBytes(typo PrefixType, user uint16, db uint8, keyPrefix PrefixType, data ...[]byte) []byte {
	count := 0
	for _, v := range data {
		count += len(v)
	}
	k := make([]byte, 1+2+1+1+count)
	k[0] = byte(typo)
	binary.BigEndian.PutUint16(k[1:], user)
	k[3] = byte(db)
	k[4] = byte(keyPrefix)
	start := 5
	for _, v := range data {
		if v == nil {
			continue
		}
		copy(k[start:], v)
		start += len(v)
	}
	return k
}

func (o *Object) IsSimple() bool {
	return o.Type == StringType || o.Type == JsonType
}

func (o *Object) DisableCount() bool {
	return o.Type == HashType || o.Type == AListType ||
		o.Type == ZsetType || o.Type == SetType || o.Type == GeoType
}

func (o *Object) IsCountable() bool {
	return o.Type != StringType && o.Type != JsonType && o.Type != BListType
}

func GetObjectFromKV(key, value []byte) (*Object, error) {
	if len(key) < 5 || len(value) < 1 {
		return nil, xerror.ErrValueTooShort
	}
	o := &Object{}
	o.UserId = binary.BigEndian.Uint16(key[1:3])
	o.Db = key[3]
	o.Key = key[5:]
	err := ObjectDecode(value, o)
	if err != nil {
		return nil, err
	}
	return o, nil
}

func (o *Object) GetKeyBytes() []byte {
	return GetKeyBytes(DataPrefix, o.UserId, o.Db, KeyPrefix, o.Key)
}

func (o *Object) GetValueBytes(data []byte) []byte {
	return GetKeyBytes(DataPrefix, o.UserId, o.Db, ValuePrefix, o.Value, data)
}

func GetTTLPrefix(expire int64) []byte {
	k := make([]byte, 1+8)
	k[0] = byte(TTLPrefix)
	binary.BigEndian.PutUint64(k[1:], uint64(expire))
	return k
}

func (o *Object) getTTLBytes(typo PrefixType, data []byte) []byte {
	if o.TTL <= 0 {
		return nil
	}
	ttl := make([]byte, 8)
	binary.BigEndian.PutUint64(ttl, uint64(o.TTL))
	return GetKeyBytes(TTLPrefix, o.UserId, o.Db, typo, ttl, data)
}

func (o *Object) GetTTLKeyBytes() []byte {
	return o.getTTLBytes(KeyPrefix, o.Key)
}

func (o *Object) GetTTLValueBytes() []byte {
	return o.getTTLBytes(ValuePrefix, o.Value)
}

func EncodeTTLValue(typo ObjectType, value []byte) []byte {
	k := make([]byte, 1+len(value))
	k[0] = byte(typo)
	copy(k[1:], value)
	return k
}

func DecodeTTLValue(value []byte) (ObjectType, []byte) {
	return ObjectType(value[0]), value[1:]
}

func GetObjectFromTTL(k, v []byte) (*Object, error) {
	if len(k) < 13 {
		return nil, xerror.ErrValueTooShort
	}
	if k[0] != byte(TTLPrefix) {
		return nil, xerror.ErrNotTTL
	}

	o := &Object{
		UserId: binary.BigEndian.Uint16(k[1:3]),
		Db:     k[3],
		TTL:    int64(binary.BigEndian.Uint64(k[5:13])),
	}
	if k[4] == byte(KeyPrefix) {
		o.Key = k[13:]
		if len(v) >= 17 {
			o.Type, o.Value = DecodeTTLValue(v)
		}
	} else if k[4] == byte(ValuePrefix) {
		o.Value = k[13:]
	} else {
		return nil, xerror.ErrNotTTL
	}
	return o, nil
}

func (o *Object) ObjectEncoding() ObjectEncoding {
	switch o.Type {
	case StringType:
		return EncodingEmbstr
	default:
		return EncodingRaw
	}
}

func ObjectEncode(o *Object) []byte {
	b := make([]byte, 1+8+8+2+len(o.Value)+len(o.Extra))
	b[0] = byte(o.Type)
	binary.BigEndian.PutUint64(b[1:], uint64(o.TTL))
	binary.BigEndian.PutUint64(b[9:], o.Timestamp)
	binary.BigEndian.PutUint16(b[17:], o.Hash)
	copy(b[1+8+8+2:], o.Value)
	if o.Extra != nil {
		copy(b[19+len(o.Value):], o.Extra)
	}
	return b
}

func (o *Object) CleanValue(hash uint16, typo ObjectType) {
	if hash == 0 {
		hash = DefaultHashMark
	}
	o.Type = typo
	o.TTL = 0
	o.Timestamp = 0
	o.Hash = hash
	o.Value = nil
	o.Extra = nil
}

func ObjectDecode(b []byte, o *Object) error {
	if len(b) < 1+8+8+2 {
		return xerror.ErrValueTooShort
	}
	o.Type = ObjectType(b[0])
	o.TTL = int64(binary.BigEndian.Uint64(b[1:9]))
	o.Timestamp = binary.BigEndian.Uint64(b[9:17])
	o.Hash = binary.BigEndian.Uint16(b[17:19])
	if o.IsSimple() {
		o.Value = b[19:]
	} else {
		if len(b) < 19+16 {
			return xerror.ErrValueTooShort
		}
		o.Value = b[19 : 19+16]
		o.Extra = b[19+16:]
	}
	return nil
}

func setTxnObject(txn *store.Txn, key []byte, o *Object, change ChangeType) error {
	delta := int64(0)
	var err error
	if change&DeleteKeyType != 0 {
		err = txn.Del(key)
		if change&MinusCount != 0 {
			delta = -1
		}
	} else {
		err = txn.Put(key, ObjectEncode(o))
		if change&PlusCount != 0 {
			delta = 1
		}
	}
	if err != nil {
		return err
	}

	if txn.Config.Redis.DbSizeHash != 0 && delta != 0 {
		var count *Count
		if change&GCChange == 0 {
			count, err = GetCount(txn, o.UserId, o.Db, txn.Config.Redis.DbSizeHash-1, CountGeneral, KEYSIZE, o.Key)
		} else {
			count, err = GetCount(txn, o.UserId, o.Db, txn.Config.Redis.DbSizeHash, CountGeneral, KEYSIZE, nil)
		}
		if err == store.KeyNotFound {
		} else if err != nil {
			return err
		}
		count.Value += delta
		err = SetCount(txn, count)
		if err != nil {
			return err
		}
	}
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

	if object.TTL > 0 && object.TTL <= txn.Now {
		if clear {
			err = DeleteKey(txn, key, object, object.TTL, MinusCount)
			if err != nil {
				return err
			}
		}
		object.CleanValue(txn.Config.Redis.ObjectHash, getType)
		return store.KeyNotFound
	}

	if getType != UnknownType && getType != object.Type {
		return xerror.WrongTypeErr
	}
	return nil
}

// (generic) OBJECT subcommand [arguments [arguments ...]]
func (c *Command) ObjectHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) == 0 {
		return txn.SetWrongArgs(OBJECT_COMMAND)
	}
	subcommand := strings.ToLower(utils.B2S(args[0]))
	if len(args) == 1 && subcommand == HELP_COMMAND {
		return objectHelpInfo
	}

	if len(args) < 2 {
		return txn.SetWrongSubArgs(subcommand, ObjectHelpCommand)
	}

	object := c.NewObject(txn, UnknownType, args[1])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err != nil {
		return nil
	}
	switch subcommand {
	case ENCODING_COMMAND:
		return SimpleString(object.ObjectEncoding().String())
	case IDLETIME_COMMAND:
		timepstamp := oracle.ExtractPhysical(object.Timestamp)
		return SimpleInt((txn.Now - timepstamp) / 1000)
	case REFCOUNT_COMMAND:
		return SimpleInt(0)
	case FREQ_COMMAND:
		return SimpleInt(0)
	case INFO_COMMAND:
		return SimpleString(object.Info())
	case SCAN_COMMAND:
		info := object.Info()
		rets := make([]string, 0)
		rets = append(rets, info)
		if !object.IsSimple() {
			num := 10
			if len(args) > 2 {
				num, err = utils.GetPositiveInt(args[2])
				if err != nil {
					return SimpleString("")
				}
			}
			start := object.GetValueBytes(nil)
			end := utils.PrefixNext(start)
			_ = txn.List(start, end, num, func(key, value []byte) bool {
				if len(key) < len(start) {
					return false
				}
				v := &Value{}
				DecodeValue(value, v)
				rets = append(rets, fmt.Sprintf("key: %s, value: %s", key[len(start):], v.Value))
				return true
			})
		}
		return rets
	default:
		return txn.SetWrongSubArgs(subcommand, ObjectHelpCommand)
	}
}

func PutOrDeleteKV(txn *store.Txn, object *Object, k, v []byte, delta int64) (int64, error) {
	var err error
	if v == nil {
		err = txn.Del(k)
	} else {
		err = txn.Put(k, v)
	}
	if err != nil {
		return 0, err
	}
	if delta == 0 {
		return 0, nil
	}

	if object.DisableCount() && txn.Config.Redis.DisableCount {
		return 0, nil
	}

	count, err := GetCount(txn, txn.UserId, txn.DBId, uint64(object.Hash), KeyPrefix, object.Value, v)
	if err == store.KeyNotFound {
	} else if err != nil {
		return 0, err
	}
	count.Value += delta
	err = SetCount(txn, count)
	return count.Value, err
}

func (c *Command) GetCountByKey(txn *store.Txn, arg []byte, typo ObjectType) (int64, error) {
	object := c.NewObject(txn, typo, arg)
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	return GetCountByObject(txn, object)
}

func GetCountByObject(txn *store.Txn, object *Object) (int64, error) {
	if txn.Config.Redis.DisableCount && object.DisableCount() {
		return ScanCount(txn, object)
	}
	utils.ZapLog.Debug("ListCount", zap.ByteString("key", object.Key))
	counts, err := ListCount(txn, txn.UserId, txn.DBId, KeyPrefix, object.Value)
	if err != nil {
		return 0, err
	}
	var sum int64
	for _, count := range counts {
		sum += count.Value
	}
	return sum, nil
}

type BFunc func(txn *store.Txn, args [][]byte) (interface{}, error)

func (c *Command) BlockHandle(txn *store.Txn, args [][]byte, bfunc BFunc) (interface{}, error) {
	start := txn.NowTime()
	second, err := strconv.ParseFloat(utils.B2S(args[len(args)-1]), 64)
	if err != nil {
		return nil, xerror.ErrNotFloat
	}
	if second < 0 {
		return nil, xerror.ErrTimeoutNegative
	}
	timeout := time.Duration(second * float64(time.Second))
	ret, err := bfunc(txn, args[0:len(args)-1])
	if err != nil {
		return nil, err
	}
	if ret != nil {
		return ret, nil
	}
	if txn.Multi {
		return nil, nil
	}

	now := time.Now()
	if timeout > 0 && now.Add(PullInternal).Sub(txn.NowTime()) >= timeout {
		return nil, nil
	}
	pullInternal := PullInternal
	if timeout == 0 || timeout > 50*PullInternal {
		pullInternal = 5 * PullInternal
	}
	txn.Rollback()

	txn.Blocked = true
	atomic.AddInt64(&c.Info.BlockClients, 1)
	defer func() {
		txn.Blocked = false
		atomic.AddInt64(&c.Info.BlockClients, -1)
	}()

	tick := time.NewTicker(pullInternal)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if txn.Conn.Closed() {
				return nil, xerror.ErrClientClosed
			}
			err = utils.ConnCheck(txn.NetConn())
			if err != nil {
				utils.ZapLog.Warn("conn check error", zap.Error(err),
					zap.String("remote", txn.RemoteAddr()))
				return nil, err
			}
			err = txn.Begin()
			if err != nil {
				return nil, err
			}
			ret, err := bfunc(txn, args[0:len(args)-1])
			if err != nil {
				return nil, err
			}
			if ret != nil {
				return ret, nil
			}
			txn.Rollback()
			now := time.Now()
			if timeout > 0 && now.Add(PullInternal).Sub(start) >= timeout {
				return nil, nil
			}
		case trigger, ok := <-txn.Conn.Trigger:
			if !ok {
				return nil, nil
			}
			switch trigger {
			case redcon.ErrorTrigger:
				return nil, xerror.ErrUnBlocked
			default:
				return nil, nil
			}
		}
	}
}
