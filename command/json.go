package command

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/spyzhov/ajson"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

func (c *Command) setJsonValue(txn *store.Txn, key []byte, object *Object, root *ajson.Node) error {
	result, err := ajson.Marshal(root)
	if err != nil {
		utils.ZapLog.Error("json",
			zap.Any("root", root),
			zap.Error(err))
		return err
	}
	object.Value = result
	object.Timestamp = txn.Timestamp
	return setTxnObject(txn, key, object, 0)
}

// (json) JSON.DEL key [path]
func (c *Command) JsonDelHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 && len(args) != 2 {
		return txn.SetWrongArgs(JSONDEL_COMMAND)
	}
	object := NewObject(txn, StringType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}

	if object.Value == nil || len(args) == 1 {
		return DeleteKeyReturn(txn, key, object, 0, MinusCount)
	}

	jsonPath := utils.B2S(args[1])
	if jsonPath == "." || jsonPath == "$" {
		return DeleteKeyReturn(txn, key, object, 0, MinusCount)
	}
	jsonPath, _ = checkRootPath(jsonPath)
	root, err := ajson.Unmarshal(object.Value)
	if err != nil {
		utils.ZapLog.Error("json",
			zap.ByteString("key", args[0]),
			zap.ByteString("value", object.Value),
			zap.Error(err))
		return txn.SetError(err)
	}
	nodes, err := root.JSONPath(jsonPath)
	if err != nil {
		utils.ZapLog.Error("jsonpath",
			zap.String("jsonPath", jsonPath),
			zap.Error(err))
		return txn.SetError(err)
	}
	utils.ZapLog.Debug("json.del",
		zap.String("jsonPath", jsonPath),
		zap.Any("nodes", nodes))
	for _, node := range nodes {
		err = node.Delete()
		if err != nil {
			utils.ZapLog.Error("delete node",
				zap.Any("node", node),
				zap.Error(err))
			return txn.SetError(err)
		}
	}
	if len(nodes) > 0 {
		err = c.setJsonValue(txn, key, object, root)
		if err != nil {
			return txn.SetError(err)
		}
	}

	return redcon.SimpleInt(len(nodes))
}

func IsNormalElement(cmd string) bool {
	switch {
	case cmd == "$": // root element
		return false
	case cmd == "@": // current element
		return false
	case cmd == ".": // current element
		return false
	case cmd == "..": // recursive descent
		return false
	case cmd == "*": // wildcard
		return false
	case strings.Contains(cmd, ":"):
		return false
	case strings.Contains(cmd, "?"):
		return false
	case strings.Contains(cmd, "("):
		return false
	case strings.Contains(cmd, ")"):
		return false
	default:
		return true
	}
}

func checkParent(parent *ajson.Node, element string, value *ajson.Node) (bool, error) {
	if parent.IsArray() {
		index, err := strconv.Atoi(element)
		if err != nil {
			utils.ZapLog.Error("parent is not object",
				zap.Any("parent type", parent.Type()),
				zap.Any("parent", parent))
			return false, nil
		}
		size := parent.Size()
		if size == 0 {
			return false, xerror.ErrArrayOutOfRange
		}
		if index < 0 {
			index += size
		}
		if index < 0 {
			index = 0
		}
		if index >= parent.Size() {
			return false, xerror.ErrArrayOutOfRange
		}
		old, err := parent.GetIndex(index)
		if err != nil {
			return false, xerror.ErrArrayOutOfRange
		}
		err = old.SetNode(value)
		if err != nil {
			return false, xerror.InvalidJsonError
		}
	}
	if !parent.IsObject() {
		utils.ZapLog.Error("parent is not object",
			zap.Any("parent type", parent.Type()),
			zap.Any("parent", parent))
		return false, nil
	}
	err := parent.AppendObject(element, value)
	if err != nil {
		utils.ZapLog.Error("AppendObject",
			zap.String("element", element),
			zap.Any("value", value),
			zap.Error(err))
		return false, xerror.InvalidJsonError
	}
	return true, nil
}

// (json) JSON.SET <key> <path> <json> [NX|XX]
func (c *Command) JsonSetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 || len(args) > 4 {
		return txn.SetWrongArgs(JSONSET_COMMAND)
	}

	jsonPath := utils.B2S(args[1])
	jsonValue := args[2]

	var check CheckType
	if len(args) > 3 {
		str := strings.ToLower(utils.B2S(args[3]))
		switch str {
		case NX:
			check = CheckNotExist
		case XX:
			check = CheckExist
		default:
			return txn.SetError(xerror.ErrSyntax)
		}
	}

	object := NewObject(txn, StringType, args[0])
	key := object.GetKeyBytes()
	var create ChangeType
	var oldValue []byte
	err := getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		if CheckExist == check {
			return nil
		}
		create = PlusCount
		if jsonPath != "$" && jsonPath != "." {
			return txn.SetError(xerror.ErrMustCreateRoot)
		}
		ok := json.Valid(jsonValue)
		if !ok {
			utils.ZapLog.Error("invalid json",
				zap.ByteString("key", args[0]),
				zap.ByteString("value", jsonValue),
				zap.Error(err))
			return txn.SetError(xerror.InvalidJsonError)
		}
		object.Value = jsonValue
	} else if err != nil {
		return txn.SetError(err)
	} else if jsonPath == "$" || jsonPath == "." {
		if CheckNotExist == check {
			return nil
		}
		ok := json.Valid(jsonValue)
		if !ok {
			utils.ZapLog.Error("invalid json",
				zap.ByteString("key", args[0]),
				zap.ByteString("value", jsonValue),
				zap.Error(err))
			return txn.SetError(xerror.InvalidJsonError)
		}
		object.Value = jsonValue
	} else {
		jsonPath, _ = checkRootPath(jsonPath)
		oldValue = object.Value
		root, err := ajson.Unmarshal(oldValue)
		if err != nil {
			utils.ZapLog.Error("json",
				zap.ByteString("key", args[0]),
				zap.ByteString("value", oldValue),
				zap.Error(err))
			return txn.SetError(xerror.InvalidJsonError)
		}
		commands, err := ajson.ParseJSONPath(jsonPath)
		if err != nil {
			utils.ZapLog.Error("ParseJSONPath",
				zap.String("jsonPath", jsonPath),
				zap.Strings("commands", commands),
				zap.Error(err))
			return txn.SetError(xerror.ErrWrongStaticPath)
		}
		if len(commands) == 0 {
			utils.ZapLog.Error("ParseJSONPath",
				zap.String("jsonPath", jsonPath),
				zap.Strings("commands", commands))
			return txn.SetError(xerror.ErrWrongStaticPath)
		}
		utils.ZapLog.Debug("json.set", zap.Any("check", check),
			zap.Any("root", root),
			zap.Any("commands", commands))
		nodes, err := ajson.ApplyJSONPath(root, commands)
		if err != nil {
			utils.ZapLog.Error("jsonpath",
				zap.String("jsonPath", jsonPath),
				zap.Error(err))
			return txn.SetError(xerror.ErrWrongStaticPath)
		}
		value, err := ajson.Unmarshal(jsonValue)
		if err != nil {
			utils.ZapLog.Error("json",
				zap.ByteString("set-value", jsonValue),
				zap.Error(err))
			return txn.SetError(xerror.InvalidJsonError)
		}
		utils.ZapLog.Debug("json.set", zap.Any("check", check),
			zap.Any("nodes", nodes))
		if len(nodes) == 0 {
			lastElement, _ := ajson.Str(commands[len(commands)-1])
			if !IsNormalElement(lastElement) {
				return txn.SetError(xerror.ErrWrongStaticPath)
			}
			if CheckExist == check {
				return nil
			}
			parent := root
			if len(commands) > 1 {
				nodes, err := ajson.ApplyJSONPath(root, commands[:len(commands)-1])
				if err != nil {
					utils.ZapLog.Error("ApplyJSONPath",
						zap.String("jsonPath", jsonPath),
						zap.Strings("commands", commands[:len(commands)-1]),
						zap.Error(err))
					return txn.SetError(xerror.ErrWrongStaticPath)
				}
				if len(nodes) != 1 {
					utils.ZapLog.Error("ApplyJSONPath",
						zap.String("jsonPath", jsonPath),
						zap.Strings("commands", commands[:len(commands)-1]),
						zap.Any("nodes", nodes),
						zap.Error(err))
					return nil
				}
				parent = nodes[0]
			}
			ok, err := checkParent(parent, lastElement, value)
			if err != nil {
				return txn.SetError(err)
			}
			if !ok {
				return nil
			}
		} else {
			if CheckNotExist == check {
				return nil
			}
			for _, node := range nodes {
				err = node.SetNode(value)
				if err != nil {
					utils.ZapLog.Error("SetNode",
						zap.Any("set-node", value),
						zap.Error(err))
					return txn.SetError(err)
				}
			}
		}
		result, err := ajson.Marshal(root)
		if err != nil {
			utils.ZapLog.Error("json",
				zap.Any("root", root),
				zap.Error(err))
			return txn.SetError(err)
		}
		object.Value = result
	}
	object.Timestamp = txn.Timestamp
	err = setTxnObject(txn, key, object, create)
	if err != nil {
		return txn.SetError(err)
	}
	return OK
}

func checkRootPath(jsonPath string) (string, bool) {
	if strings.HasPrefix(jsonPath, "$") {
		return jsonPath, true
	}
	if strings.HasPrefix(jsonPath, ".") {
		return fmt.Sprintf("$%s", jsonPath), false
	}
	return fmt.Sprintf("$.%s", jsonPath), false
}

func (c *Command) jsonGet(txn *store.Txn, object *Object, root *ajson.Node, path []byte) (*ajson.Node, error) {
	jsonPath := utils.B2S(path)
	if jsonPath == "." {
		return root, nil
	}
	jsonPath, rootPrefix := checkRootPath(jsonPath)
	nodes, err := root.JSONPath(jsonPath)
	if err != nil {
		utils.ZapLog.Error("invalid jsonpath",
			zap.String("path", jsonPath), zap.Error(err))
		return nil, xerror.InvalidJsonPathError
	}
	var result *ajson.Node
	if !rootPrefix {
		if len(nodes) == 0 {
			return nil, xerror.ErrPathNotExist
		}
		result = nodes[0]
	} else {
		result = ajson.ArrayNode("", nodes)
	}
	return result, nil
}

type JsonCallback func(o *ajson.Node, args [][]byte) (interface{}, bool, error)

func getType(o *ajson.Node, args [][]byte) (interface{}, bool, error) {
	switch o.Type() {
	case ajson.Null:
		return "null", false, nil
	case ajson.String:
		return "string", false, nil
	case ajson.Numeric:
		return "integer", false, nil
	case ajson.Bool:
		return "boolean", false, nil
	case ajson.Array:
		return "array", false, nil
	case ajson.Object:
		return "object", false, nil
	default:
		return "", false, nil
	}
}

func getObjLen(o *ajson.Node, args [][]byte) (interface{}, bool, error) {
	if o.IsObject() {
		return redcon.SimpleInt(o.Size()), false, nil
	}
	return nil, false, xerror.ErrPathNotObject
}

func getObjKeys(o *ajson.Node, args [][]byte) (interface{}, bool, error) {
	if o.IsObject() {
		return o.Keys(), false, nil
	}
	return nil, false, xerror.ErrPathNotObject
}

func toggle(o *ajson.Node, args [][]byte) (interface{}, bool, error) {
	b, err := o.GetBool()
	if err != nil {
		return nil, false, xerror.ErrPathNotBool
	}
	err = o.SetBool(!b)
	if err != nil {
		return nil, false, xerror.ErrPathNotBool
	}
	if b {
		return redcon.SimpleInt(0), true, nil
	}
	return redcon.SimpleInt(1), true, nil
}

func getArrLen(o *ajson.Node, args [][]byte) (interface{}, bool, error) {
	if o.IsArray() {
		return redcon.SimpleInt(o.Size()), false, nil
	}
	return nil, false, xerror.ErrPathNotArray
}

func getArrIndex(o *ajson.Node, args [][]byte) (interface{}, bool, error) {
	value, err := ajson.Unmarshal(args[0])
	if err != nil {
		utils.ZapLog.Error("arrindex",
			zap.ByteString("value", args[0]),
			zap.Error(err))
		return redcon.SimpleInt(0), false, xerror.InvalidJsonError
	}
	// utils.ZapLog.Info("arrindex",
	// 	zap.ByteString("value", args[0]),
	// 	zap.Any("object", o))
	if !o.IsArray() {
		return redcon.SimpleInt(0), false, xerror.ErrPathNotArray
	}
	len := o.Size()
	for index := 0; index < len; index++ {
		av, err := o.GetIndex(index)
		if err != nil {
			utils.ZapLog.Error("arrindex",
				zap.Any("array", o),
				zap.Int("index", index),
				zap.Error(err))
			return redcon.SimpleInt(0), false, xerror.InvalidJsonError
		}
		ok, _ := value.Eq(av)
		if ok {
			return redcon.SimpleInt(index), false, nil
		}
	}
	return redcon.SimpleInt(-1), false, nil
}

func appendArr(o *ajson.Node, args [][]byte) (interface{}, bool, error) {
	change := false
	for _, v := range args {
		value, err := ajson.Unmarshal(v)
		if err != nil {
			utils.ZapLog.Error("arrindex",
				zap.ByteString("value", v),
				zap.Error(err))
			return redcon.SimpleInt(0), false, xerror.InvalidJsonError
		}
		err = o.AppendArray(value)
		if err != nil {
			utils.ZapLog.Error("arrindex",
				zap.ByteString("value", v),
				zap.Error(err))
			return nil, false, err
		}
		utils.ZapLog.Info("arrindex",
			zap.Any("array", o),
			zap.ByteString("value", v),
		)
		change = true
	}
	return redcon.SimpleInt(o.Size()), change, nil
}

func insertArr(o *ajson.Node, args [][]byte) (interface{}, bool, error) {
	index, err := strconv.Atoi(utils.B2S(args[0]))
	if err != nil {
		utils.ZapLog.Error("arrindex", zap.ByteString("index", args[0]), zap.Error(err))
		return redcon.SimpleInt(0), false, xerror.InvalidJsonError
	}
	vs := make([]*ajson.Node, 0, len(args)-1)
	for _, v := range args[1:] {
		value, err := ajson.Unmarshal(v)
		if err != nil {
			utils.ZapLog.Error("arrindex",
				zap.ByteString("value", v),
				zap.Error(err))
			return redcon.SimpleInt(0), false, xerror.InvalidJsonError
		}
		vs = append(vs, value)
	}
	if len(vs) == 0 {
		return redcon.SimpleInt(o.Size()), false, nil
	}
	err = o.InsertArray(index, vs...)
	if err != nil {
		utils.ZapLog.Error("arrindex",
			zap.Any("values", vs),
			zap.Error(err))
		return nil, false, err
	}
	return redcon.SimpleInt(o.Size()), true, nil
}

func trimArr(o *ajson.Node, args [][]byte) (interface{}, bool, error) {
	if !o.IsArray() {
		return nil, false, xerror.ErrPathNotArray
	}
	n := o.Size()
	if n == 0 {
		return redcon.SimpleInt(0), false, nil
	}
	start, err := strconv.Atoi(utils.B2S(args[0]))
	if err != nil {
		utils.ZapLog.Error("arrtrim",
			zap.ByteString("start", args[0]), zap.Error(err))
		return nil, false, xerror.ErrNotInteger
	}
	end, err := strconv.Atoi(utils.B2S(args[1]))
	if err != nil {
		utils.ZapLog.Error("arrtrim",
			zap.ByteString("end", args[1]), zap.Error(err))
		return nil, false, xerror.ErrNotInteger
	}
	change, err := o.TrimIndex(start, end)
	if err != nil {
		utils.ZapLog.Error("arrtrim",
			zap.Int("start", start),
			zap.Int("end", end),
			zap.Error(err))
		return nil, false, err
	}
	utils.ZapLog.Debug("arrtrim", zap.Int("start", start),
		zap.Int("end", end), zap.Any("object", o))
	return redcon.SimpleInt(o.Size()), change, nil
}

func popArr(o *ajson.Node, args [][]byte) (interface{}, bool, error) {
	if !o.IsArray() {
		return nil, false, xerror.ErrPathNotArray
	}
	n := o.Size()
	if n == 0 {
		return nil, false, nil
	}
	index := n - 1
	var err error
	if len(args) > 0 {
		index, err = strconv.Atoi(utils.B2S(args[0]))
		if err != nil {
			utils.ZapLog.Error("arrindex",
				zap.ByteString("index", args[0]), zap.Error(err))
			return nil, false, xerror.ErrInvalidIndex
		}
		if index >= n {
			index = n - 1
		}
		if index <= -n {
			index = 0
		}
	}
	v, err := o.PopIndex(index)
	if err != nil {
		utils.ZapLog.Error("arrindex",
			zap.Any("array", o),
			zap.Int("index", index),
			zap.Error(err))
		return nil, false, err
	}
	value, err := v.Value()
	if err != nil {
		utils.ZapLog.Error("arrindex",
			zap.Any("array", o),
			zap.Int("index", index),
			zap.Error(err))
		return nil, false, err
	}
	return value, true, nil
}

var JsonCallbackFucs = map[string]JsonCallback{
	JSONTYPE_COMMAND:      getType,
	JSONOBJLEN_COMMAND:    getObjLen,
	JSONOBJKEYS_COMMAND:   getObjKeys,
	JSONARRLEN_COMMAND:    getArrLen,
	JSONARRINDEX_COMMAND:  getArrIndex,
	JSONARRAPPEND_COMMAND: appendArr,
	JSONARRINSERT_COMMAND: insertArr,
	JSONARRTRIM_COMMAND:   trimArr,
	JSONARRPOP_COMMAND:    popArr,
	JSONTOGGLE_COMMAND:    toggle,
}

// JSON.ARRTRIM key path start stop
func (c *Command) JsonArrTrimHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 4 {
		return txn.SetWrongArgs(JSONARRTRIM_COMMAND)
	}
	return c.jsonCallbackHandle(txn, args[0], args[1], args[2:], true, JSONARRTRIM_COMMAND)
}

// JSON.ARRPOP key [ path [index]]
func (c *Command) JsonArrPopHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 || len(args) > 3 {
		return txn.SetWrongArgs(JSONARRPOP_COMMAND)
	}
	var path []byte
	if len(args) == 1 {
		path = []byte(".")
	} else {
		path = args[1]
	}
	var a [][]byte
	if len(args) == 3 {
		a = [][]byte{args[2]}
	}
	return c.jsonCallbackHandle(txn, args[0], path, a, true, JSONARRPOP_COMMAND)
}

// JSON.ARRINSERT key path index value [value ...]
func (c *Command) JsonArrInsertHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 4 {
		return txn.SetWrongArgs(JSONARRINSERT_COMMAND)
	}
	return c.jsonCallbackHandle(txn, args[0], args[1], args[2:], true, JSONARRINSERT_COMMAND)
}

// JSON.ARRAPPEND key path value [value ...]
func (c *Command) JsonArrAppendHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(JSONARRAPPEND_COMMAND)
	}
	return c.jsonCallbackHandle(txn, args[0], args[1], args[2:], true, JSONARRAPPEND_COMMAND)
}

// JSON.ARRINDEX key path value
func (c *Command) JsonArrIndexHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(JSONARRINDEX_COMMAND)
	}
	return c.jsonCallbackHandle(txn, args[0], args[1], args[2:], false, JSONARRINDEX_COMMAND)
}

// JSON.TOGGLE key [path]
func (c *Command) JsonToggleHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 && len(args) != 2 {
		return txn.SetWrongArgs(JSONTOGGLE_COMMAND)
	}
	var path []byte
	if len(args) == 1 {
		path = []byte(".")
	} else {
		path = args[1]
	}
	return c.jsonCallbackHandle(txn, args[0], path, nil, true, JSONTOGGLE_COMMAND)
}

// JSON.ARRLEN key [path]
func (c *Command) JsonArrLenHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 && len(args) != 2 {
		return txn.SetWrongArgs(JSONARRLEN_COMMAND)
	}
	var path []byte
	if len(args) == 1 {
		path = []byte(".")
	} else {
		path = args[1]
	}
	return c.jsonCallbackHandle(txn, args[0], path, nil, false, JSONARRLEN_COMMAND)
}

// JSON.OBJLEN key [path]
func (c *Command) JsonObjLenHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 && len(args) != 2 {
		return txn.SetWrongArgs(JSONOBJLEN_COMMAND)
	}
	var path []byte
	if len(args) == 1 {
		path = []byte(".")
	} else {
		path = args[1]
	}
	return c.jsonCallbackHandle(txn, args[0], path, nil, false, JSONOBJLEN_COMMAND)
}

// JSON.TYPE key [path]
func (c *Command) JsonTypeHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 && len(args) != 2 {
		return txn.SetWrongArgs(JSONTYPE_COMMAND)
	}
	var path []byte
	if len(args) == 1 {
		path = []byte(".")
	} else {
		path = args[1]
	}
	return c.jsonCallbackHandle(txn, args[0], path, nil, false, JSONTYPE_COMMAND)
}

// (json) JSON.OBJKEYS key [path]
func (c *Command) JsonObjKeysHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 && len(args) != 2 {
		return txn.SetWrongArgs(JSONOBJKEYS_COMMAND)
	}
	var path []byte
	if len(args) == 1 {
		path = []byte(".")
	} else {
		path = args[1]
	}
	return c.jsonCallbackHandle(txn, args[0], path, nil, false, JSONOBJKEYS_COMMAND)
}

func (c *Command) jsonCallbackHandle(txn *store.Txn, k, path []byte, args [][]byte, clear bool, cmd string) interface{} {
	object := NewObject(txn, StringType, k)
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, clear)
	if err != nil {
		return txn.SetError(err)
	}
	root, err := ajson.Unmarshal(object.Value)
	if err != nil {
		utils.ZapLog.Error("object value invalid json",
			zap.ByteString("value", object.Value), zap.Error(err))
		return txn.SetError(xerror.InvalidJsonError)
	}

	rootPrefix := false
	var nodes []*ajson.Node
	jsonPath := utils.B2S(path)
	if jsonPath == "." {
		nodes = []*ajson.Node{root}
	} else {
		jsonPath, rootPrefix = checkRootPath(jsonPath)
		nodes, err = root.JSONPath(jsonPath)
		if err != nil {
			utils.ZapLog.Error("invalid jsonpath",
				zap.String("path", jsonPath), zap.Error(err))
			return txn.SetError(xerror.InvalidJsonPathError)
		}
	}
	if !rootPrefix && len(nodes) == 0 {
		if cmd == JSONTYPE_COMMAND {
			return nil
		}
		return txn.SetError(xerror.ErrPathNotExist)
	}

	callback := JsonCallbackFucs[cmd]
	if callback == nil {
		return txn.SetError(xerror.UnsupportCmd)
	}

	change := false
	ret := make([]interface{}, 0)
	var reterr error
	for _, node := range nodes {
		c, isChange, err := callback(node, args)
		if err == nil {
			ret = append(ret, c)
		} else {
			reterr = err
			ret = append(ret, nil)
		}
		if isChange {
			change = true
		}
	}
	if change {
		utils.ZapLog.Info("json callback",
			zap.Any("root", root))
		err = c.setJsonValue(txn, key, object, root)
		if err != nil {
			return txn.SetError(err)
		}
	}

	if !rootPrefix {
		if reterr != nil {
			return txn.SetError(reterr)
		}
		return ret[0]
	}

	return ret
}

func (c *Command) jsonGetHandle(txn *store.Txn, k, path []byte, clear bool) (*ajson.Node, error) {
	object := NewObject(txn, StringType, k)
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, clear)
	if err == store.KeyNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	root, err := ajson.Unmarshal(object.Value)
	if err != nil {
		utils.ZapLog.Error("object value invalid json",
			zap.ByteString("value", object.Value), zap.Error(err))
		return nil, xerror.InvalidJsonError
	}
	result, err := c.jsonGet(txn, object, root, path)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// (json) JSON.MGET key [key ...] path
func (c *Command) JsonMgetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(JSONGET_COMMAND)
	}
	path := args[len(args)-1]
	ret := make([]interface{}, 0, len(args)-1)
	for _, k := range args[:len(args)-1] {
		result, err := c.jsonGetHandle(txn, k, path, false)
		if err == xerror.ErrPathNotExist {
			ret = append(ret, nil)
			continue
		}
		if err != nil {
			return txn.SetError(err)
		}
		if result == nil {
			ret = append(ret, nil)
			continue
		}

		data, err := ajson.Marshal(result)
		if err != nil {
			return txn.SetError(xerror.InvalidJsonError)
		}
		ret = append(ret, data)
	}
	return ret
}

// (json) JSON.GET key [path [path ...]]
func (c *Command) JsonGetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(JSONGET_COMMAND)
	}

	object := NewObject(txn, StringType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}

	if len(args) == 1 {
		return object.Value
	}

	root, err := ajson.Unmarshal(object.Value)
	if err != nil {
		utils.ZapLog.Error("object value invalid json",
			zap.ByteString("value", object.Value), zap.Error(err))
		return txn.SetError(xerror.InvalidJsonError)
	}
	if len(args) == 2 {
		result, err := c.jsonGet(txn, object, root, args[1])
		if err != nil {
			return txn.SetError(err)
		}
		data, err := ajson.Marshal(result)
		if err != nil {
			return txn.SetError(xerror.InvalidJsonError)
		}
		return data
	}

	m := make(map[string]*ajson.Node)
	result := ajson.ObjectNode("", m)
	for i := 1; i < len(args); i++ {
		jsonPath := utils.B2S(args[i])
		if _, ok := m[jsonPath]; ok {
			continue
		}
		ret, err := c.jsonGet(txn, object, root, args[i])
		if err != nil {
			return txn.SetError(err)
		}
		m[jsonPath] = ret
	}
	data, err := ajson.Marshal(result)
	if err != nil {
		return txn.SetError(err)
	}
	return data
}
