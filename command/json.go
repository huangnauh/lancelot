package command

import (
	"encoding/json"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

func trimPath(path string) string {
	if len(path) == 0 {
		return path
	}

	if path[0] == '.' {
		if len(path) == 1 || path[1] != '.' {
			return path[1:]
		}
	}
	return path
}

// (json) JSON.DEL key path [path ...]
func (c *Command) JsonDelHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(JSONDEL_COMMAND)
	}

	startTs := txn.StartTS()
	now := oracle.ExtractPhysical(startTs)
	object := NewObject(txn.UserId, txn.DBId, JsonType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object)
	if err == store.KeyNotFound {
		return 0
	} else if err != nil {
		return txn.SetError(err)
	}

	if object.Value == nil {
		return c.DeleteKeyReturn(txn, key, object, now)
	}

	value := object.Value
	count := 0
	for i := 1; i < len(args); i++ {
		path := trimPath(utils.B2S(args[i]))
		if path == "" {
			return c.DeleteKeyReturn(txn, key, object, 0)
		}
		origin := len(value)
		value, err = sjson.DeleteBytes(value, path)
		if err != nil {
			return txn.SetError(err)
		}
		if origin != len(value) {
			count++
		}
	}
	if count == 0 {
		return 0
	}

	object.Timestamp = startTs
	object.Value = value
	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return txn.SetError(err)
	} else {
		return count
	}
}

// (json) JSON.SET <key> <path> <json> [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL] [NX|XX] [GET]
func (c *Command) JsonSetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(JSONSET_COMMAND)
	}

	jsonPath := utils.B2S(args[1])
	jsonValue := args[2]
	if !json.Valid(jsonValue) {
		return txn.SetError(xerror.InvalidJsonError)
	}
	oldObject, setOption, err := checkSetOption(txn, JSONSET_COMMAND, args[0], args[3:])
	if err != nil {
		return txn.SetError(err)
	}

	object := NewObject(txn.UserId, txn.DBId, JsonType, args[0])
	object.TTL = setOption.Expire
	object.Timestamp = setOption.StartTs

	var oldValue []byte
	if oldObject == nil || oldObject.Value == nil {
		oldValue = make([]byte, 0)
	} else {
		oldValue = oldObject.Value
	}

	var getRet interface{}
	jsonPath = trimPath(jsonPath)
	if jsonPath == "" {
		object.Value = jsonValue
		if setOption.Get {
			getRet = oldValue
		}
	} else {
		if setOption.Get && len(oldValue) > 0 {
			ret := gjson.GetBytes(oldValue, jsonPath)
			if ret.Exists() {
				getRet = ret.String()
			}
		}

		value, err := sjson.SetRawBytes(oldValue, jsonPath, jsonValue)
		if err != nil {
			return txn.SetError(err)
		}
		object.Value = value
	}

	if !setOption.KeepTTL && setOption.Expire > 0 {
		ttlKey := object.GetTTLKeyBytes()
		err := txn.Put(ttlKey, []byte{1})
		if err != nil {
			return txn.SetError(err)
		}
	}

	key := object.GetKeyBytes()
	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return txn.SetError(err)
	} else if setOption.Get {
		return getRet
	} else {
		return OK
	}
}

// (json) JSON.GET key [path [path ...]]
func (c *Command) JsonGetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(JSONGET_COMMAND)
	}

	object := NewObject(txn.UserId, txn.DBId, JsonType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}

	if len(args) == 1 {
		return object.Value
	}

	if len(args) == 2 {
		jsonPath := trimPath(utils.B2S(args[1]))
		if jsonPath == "" {
			return object.Value
		}
		ret := gjson.GetBytes(object.Value, jsonPath)
		if ret.Exists() {
			return ret.String()
		}
		return nil
	}

	paths := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		path := trimPath(utils.B2S(args[i]))
		if path == "" {
			return txn.SetError(xerror.InvalidJsonPathError)
		}
		paths[i-1] = path
	}

	var jsonResponse string
	results := gjson.GetManyBytes(object.Value, paths...)
	for i := 0; i < len(results); i++ {
		if !results[i].Exists() {
			return txn.SetError(xerror.NotExistKeyError(paths[i]))
		} else {
			jsonResponse, err = sjson.SetRaw(jsonResponse, paths[i], results[i].Raw)
			if err != nil {
				return txn.SetError(err)
			}
		}
	}
	return jsonResponse
}
