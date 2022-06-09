package command

import (
	"strings"

	"github.com/spyzhov/ajson"
	"gitlab.s.upyun.com/platform/lancelot/json"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
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
			return txn.SetError(xerror.ErrCheckFailed)
		}
		create = PlusCount
		if jsonPath != "$" && jsonPath != "." {
			return txn.SetError(xerror.ErrMustCreateRoot)
		}
		if !json.Valid(jsonValue) {
			return txn.SetError(xerror.InvalidJsonError)
		}
		object.Value = jsonValue
	} else if err != nil {
		return txn.SetError(err)
	} else if CheckNotExist == check {
		return txn.SetError(xerror.ErrCheckFailed)
	} else if jsonPath == "$" || jsonPath == "." {
		if !json.Valid(jsonValue) {
			return txn.SetError(xerror.InvalidJsonError)
		}
		object.Value = jsonValue
	} else {
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
		if len(commands) < 2 {
			utils.ZapLog.Error("ParseJSONPath",
				zap.String("jsonPath", jsonPath),
				zap.Strings("commands", commands))
			return txn.SetError(xerror.ErrWrongStaticPath)
		}
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
		if len(nodes) == 0 {
			lastElement := commands[len(commands)-1]
			if !IsNormalElement(lastElement) {
				return txn.SetError(xerror.ErrWrongStaticPath)
			}
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
					zap.Error(err))
				return txn.SetError(xerror.ErrWrongStaticPath)
			}
			nodes[0].AppendObject(lastElement, value)
		} else {
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

	root, err := ajson.Unmarshal(object.Value)
	if err != nil {
		return txn.SetError(err)
	}

	if len(args) == 2 {
		jsonPath := utils.B2S(args[1])
		nodes, err := root.JSONPath(jsonPath)
		if err != nil {
			return txn.SetError(err)
		}
		result := ajson.ArrayNode("", nodes)
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
