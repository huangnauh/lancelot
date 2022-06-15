package command

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/spyzhov/ajson"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

const (
	quotes byte = '"'
	quote  byte = '\''
)

// (json) JSON.DEL key [path]
func (c *Command) JsonDelHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 || len(args) != 2 {
		return txn.SetWrongArgs(JSONDEL_COMMAND)
	}
	object := NewObject(txn, StringType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		return 0
	} else if err != nil {
		return txn.SetError(err)
	}

	if object.Value == nil || len(args) == 1 {
		return DeleteKeyReturn(txn, key, object, 0, MinusCount)
	}

	jsonPath := utils.B2S(args[1])
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
		result, err := ajson.Marshal(root)
		if err != nil {
			utils.ZapLog.Error("json",
				zap.Any("root", root),
				zap.Error(err))
			return txn.SetError(err)
		}
		object.Value = result
		object.Timestamp = txn.Timestamp
		err = setTxnObject(txn, key, object, 0)
		if err != nil {
			return txn.SetError(err)
		}
	}

	return len(nodes)
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

func checkParent(parent *ajson.Node, element string) (bool, error) {
	if parent.IsArray() {
		index, err := strconv.Atoi(element)
		if err != nil {
			utils.ZapLog.Error("parent is not object",
				zap.Any("parent type", parent.Type()),
				zap.Any("parent", parent))
			return false, nil
		}
		if index >= parent.Size() {
			return false, xerror.ErrArrayOutOfRange
		}
	}
	if !parent.IsObject() {
		utils.ZapLog.Error("parent is not object",
			zap.Any("parent type", parent.Type()),
			zap.Any("parent", parent))
		return false, nil
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
		if strings.HasPrefix(jsonPath, ".") {
			jsonPath = "$" + jsonPath
		}
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
			if len(commands) == 1 {
				if root.IsArray() {
					index, err := strconv.Atoi(lastElement)
					if err != nil {
						utils.ZapLog.Error("parent is not object",
							zap.Any("parent type", root.Type()),
							zap.Any("parent", root))
						return nil
					}
					if index >= root.Size() {
						return txn.SetError(xerror.ErrArrayOutOfRange)
					}
				}
				ok, err := checkParent(root, lastElement)
				if err != nil {
					return txn.SetError(err)
				}
				if !ok {
					return nil
				}
			} else {
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
				parent := nodes[0]
				ok, err := checkParent(parent, lastElement)
				if err != nil {
					return txn.SetError(err)
				}
				if !ok {
					return nil
				}
				err = parent.AppendObject(lastElement, value)
				if err != nil {
					utils.ZapLog.Error("AppendObject",
						zap.String("element", lastElement),
						zap.Any("value", value),
						zap.Error(err))
					return txn.SetError(xerror.InvalidJsonError)
				}
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

	jsonPath := utils.B2S(args[1])
	if jsonPath == "." {
		return object.Value
	}

	root, err := ajson.Unmarshal(object.Value)
	if err != nil {
		return txn.SetError(err)
	}

	if len(args) == 2 {
		dotPrefix := strings.HasPrefix(jsonPath, ".")
		if dotPrefix {
			jsonPath = "$" + jsonPath
		}
		nodes, err := root.JSONPath(jsonPath)
		if err != nil {
			return txn.SetError(err)
		}
		var result *ajson.Node
		if dotPrefix {
			if len(nodes) == 0 {
				return txn.SetError(xerror.PathNotExistError(jsonPath))
			}
			result = nodes[0]
		} else {
			result = ajson.ArrayNode("", nodes)
		}
		data, err := ajson.Marshal(result)
		if err != nil {
			return txn.SetError(err)
		}
		return data
	}

	m := make(map[string]*ajson.Node)
	result := ajson.ObjectNode("", m)
	for i := 1; i < len(args); i++ {
		path := utils.B2S(args[i])
		if path == "" {
			return txn.SetError(xerror.InvalidJsonPathError)
		}
		if _, ok := m[path]; ok {
			continue
		}
		nodes, err := root.JSONPath(path)
		if err != nil {
			return txn.SetError(err)
		}
		a := ajson.ArrayNode("", nodes)
		m[path] = a
	}
	data, err := ajson.Marshal(result)
	if err != nil {
		return txn.SetError(err)
	}
	return data
}
