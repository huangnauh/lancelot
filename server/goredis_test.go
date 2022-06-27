package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	redis "github.com/go-redis/redis/v8"
	"github.com/nitishm/go-rejson/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var docs = map[string]interface{}{
	"simple": map[string]string{"foo": "bar"},
	"basic": map[string]interface{}{
		"string": "string value",
		"none":   nil,
		"bool":   true,
		"int":    42,
		"num":    4.2,
		"arr": []interface{}{42, nil, -1.2, false,
			[]string{"sub", "array"},
			map[string]bool{"subdict": true}},
		"dict": map[string]interface{}{"a": 1, "b": "2", "c": nil},
	},
	"scalars": map[string]interface{}{
		"unicode":  "string value",
		"NoneType": nil,
		"bool":     true,
		"int":      42,
		"float":    -1.2,
	},
	"values": map[string]interface{}{
		"str":      "string value",
		"NoneType": nil,
		"bool":     true,
		"int":      42,
		"float":    -1.2,
		"dict":     map[string]interface{}{"foo": "1", "bar": "2"},
		"list":     []interface{}{"foo", "bar"},
	},
	"types": map[string]interface{}{
		"null":    nil,
		"boolean": false,
		"integer": 42,
		"number":  1.2,
		"string":  "str",
		"object":  map[string]string{},
		"array":   []string{},
	},
}

var testInvalidJsonResult = []string{
	"{",
	"}",
	"[",
	"]",
	"{]",
	"[}",
	"\\",
	"\\\\",
	"",
	" ",
	"\\\"",
	"'",
	"\\[",
	"\u0000",
	"\n",
	"\f",
}

// https://github.com/RedisJSON/RedisJSON/blob/a31f2dabbc15c004d0cec7d1c25cc966b33a0a0b/tests/pytest/test.py#L119-L122
var testInvalidJsonPath = []string{
	// "",
	// " ",
	// "\u0000",
	// "\n",
	// "\f",
	".\"",
	// ".\u0000",
	// ".\n\f",
	".-foo",
	// ".43",
	// ".foo\n.bar",
}

var testRedisJsonValue = []interface{}{
	"string",
	1,
	-2,
	3.14,
	true,
	false,
	[]string{},
	map[string]string{},
}

func Bytes(reply interface{}, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	switch reply := reply.(type) {
	case []byte:
		return reply, nil
	case string:
		return []byte(reply), nil
	}
	return nil, fmt.Errorf("unexpected type for Bytes, got type %T", reply)
}

func TestRedisJsonValue(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	for _, tt := range testRedisJsonValue {
		_, err := c.Del(context.Background(), "jsontest").Result()
		assert.NoError(t, err)
		res, err := rh.JSONSet("jsontest", ".", tt)
		assert.NoError(t, err)
		assert.Equal(t, "OK", res)
		resBytes, err := Bytes(rh.JSONGet("jsontest", "."))
		assert.NoError(t, err)
		ttBytes, err := json.Marshal(tt)
		assert.NoError(t, err)
		assert.Equal(t, ttBytes, resBytes,
			"set %v expected %s, got %s", tt, string(ttBytes), string(resBytes))
	}
}

func TestJsonArrayCRUD(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	_, err := c.Del(context.Background(), "jsonarray").Result()
	assert.NoError(t, err)

	// Test creation of an empty array
	res, err := rh.JSONSet("jsonarray", ".", []string{})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONType("jsonarray", ".")
	assert.NoError(t, err)
	assert.Equal(t, "array", res)
	res, err = rh.JSONArrLen("jsonarray", ".")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), res)

	// Test failure of setting an element at different positons in an empty array
	_, err = rh.JSONSet("jsonarray", "[0]", 0)
	assert.Contains(t, err.Error(), "index out of range")
	_, err = rh.JSONSet("jsonarray", "[19]", 0)
	assert.Contains(t, err.Error(), "index out of range")
	_, err = rh.JSONSet("jsonarray", "[-1]", 0)
	assert.Contains(t, err.Error(), "index out of range")

	//  Test appending and inserting elements to the array
	res, err = rh.JSONArrAppend("jsonarray", ".", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
	res, err = rh.JSONArrLen("jsonarray", ".")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
	res, err = rh.JSONArrInsert("jsonarray", ".", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), res)
	res, err = rh.JSONArrLen("jsonarray", ".")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), res)
	res, err = Bytes(rh.JSONGet("jsonarray", "."))
	assert.NoError(t, err)
	assert.Equal(t, []byte("[-1,1]"), res)
	res, err = rh.JSONArrInsert("jsonarray", ".", -1, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), res)
	res, err = Bytes(rh.JSONGet("jsonarray", "."))
	assert.NoError(t, err)
	assert.Equal(t, []byte("[-1,0,1]"), res)
	res, err = rh.JSONArrInsert("jsonarray", ".", -3, -3, -2)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), res)
	res, err = Bytes(rh.JSONGet("jsonarray", "."))
	assert.NoError(t, err)
	assert.Equal(t, []byte("[-3,-2,-1,0,1]"), res)
	res, err = rh.JSONArrAppend("jsonarray", ".", 2, 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(7), res)
	res, err = Bytes(rh.JSONGet("jsonarray", "."))
	assert.NoError(t, err)
	assert.Equal(t, []byte("[-3,-2,-1,0,1,2,3]"), res)

	//Test replacing elements in the array
	res, err = rh.JSONSet("jsonarray", "[0]", "-inf")
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONSet("jsonarray", "[-1]", "+inf")
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONSet("jsonarray", "[3]", nil)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = Bytes(rh.JSONGet("jsonarray", "."))
	assert.NoError(t, err)
	assert.Equal(t, []byte(`["-inf",-2,-1,null,1,2,"+inf"]`), res)

	//Test deleting from the array
	res, err = rh.JSONDel("jsonarray", "[1]")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
	res, err = rh.JSONDel("jsonarray", "[-2]")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
	res, err = Bytes(rh.JSONGet("jsonarray", "."))
	assert.NoError(t, err)
	assert.Equal(t, []byte(`["-inf",-1,null,1,"+inf"]`), res)

	//Test trimming the array
	res, err = rh.JSONArrTrim("jsonarray", ".", 1, -1)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), res)
	res, err = Bytes(rh.JSONGet("jsonarray", "."))
	assert.NoError(t, err)
	assert.Equal(t, []byte(`[-1,null,1,"+inf"]`), res)
	res, err = rh.JSONArrTrim("jsonarray", ".", 0, -2)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), res)
	res, err = Bytes(rh.JSONGet("jsonarray", "."))
	assert.NoError(t, err)
	assert.Equal(t, []byte(`[-1,null,1]`), res)
	res, err = rh.JSONArrTrim("jsonarray", ".", 1, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
	res, err = Bytes(rh.JSONGet("jsonarray", "."))
	assert.NoError(t, err)
	assert.Equal(t, []byte(`[null]`), res)
}

func TestJsonToggleCommand(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	_, err := c.Del(context.Background(), "jsontoggle").Result()
	assert.NoError(t, err)
	res, err := rh.JSONSet("jsontoggle", ".", map[string]bool{"foo": true})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	ctx := context.Background()
	cmd := redis.NewIntCmd(ctx, "JSON.Toggle", "jsontoggle", ".foo")
	_ = c.Process(ctx, cmd)
	ret, err := cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, ret, int64(0))
	cmd = redis.NewIntCmd(ctx, "JSON.Toggle", "jsontoggle", ".foo")
	_ = c.Process(ctx, cmd)
	ret, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, ret, int64(1))
	res, err = rh.JSONSet("jsontoggle", ".", map[string]string{"foo": "bar"})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	cmd = redis.NewIntCmd(ctx, "JSON.Toggle", "jsontoggle", ".bar")
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.Contains(t, err.Error(), "not exist")
	cmd = redis.NewIntCmd(ctx, "JSON.Toggle", "jsontoggle", ".foo")
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.Contains(t, err.Error(), "not a bool")
}

func TestJsonDelCommand(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	_, err := c.Del(context.Background(), "jsondel").Result()
	assert.NoError(t, err)
	res, err := rh.JSONSet("jsondel", ".", map[string]string{})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONDel("jsondel", ".")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
	n, err := c.Exists(context.Background(), "jsondel").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), n)

	// Test deleting an empty object
	res, err = rh.JSONSet("jsondel", ".", map[string]string{"foo": "bar", "baz": "qux"})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONDel("jsondel", ".baz")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
	res, err = rh.JSONObjLen("jsondel", ".")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
	res, err = rh.JSONType("jsondel", ".baz")
	assert.NoError(t, err)
	assert.Equal(t, nil, res)
	res, err = rh.JSONDel("jsondel", ".foo")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
	res, err = rh.JSONObjLen("jsondel", ".")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), res)
	res, err = rh.JSONType("jsondel", ".foo")
	assert.NoError(t, err)
	assert.Equal(t, nil, res)
	res, err = rh.JSONType("jsondel", ".")
	assert.NoError(t, err)
	assert.Equal(t, "object", res)

	// Test deleting some keys from an object
	res, err = rh.JSONSet("jsondel", ".", map[string]string{})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONSet("jsondel", ".foo", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONSet("jsondel", ".baz", "qux")
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONDel("jsondel", ".baz")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
	res, err = rh.JSONObjLen("jsondel", ".")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
	res, err = rh.JSONType("jsondel", ".baz")
	assert.NoError(t, err)
	assert.Equal(t, nil, res)
	res, err = rh.JSONDel("jsondel", ".foo")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
	res, err = rh.JSONObjLen("jsondel", ".")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), res)
	res, err = rh.JSONType("jsondel", ".foo")
	assert.NoError(t, err)
	assert.Equal(t, nil, res)
	res, err = rh.JSONType("jsondel", ".")
	assert.NoError(t, err)
	assert.Equal(t, "object", res)

	//Test with an array
	res, err = rh.JSONSet("jsondel", ".foo", "bar")
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONSet("jsondel", ".baz", "qux")
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONSet("jsondel", ".arr", []interface{}{1.2, 1, 2})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONDel("jsondel", ".arr[1]")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
	res, err = rh.JSONObjLen("jsondel", ".")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), res)
	res, err = rh.JSONArrLen("jsondel", ".arr")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), res)
	res, err = rh.JSONType("jsondel", ".arr")
	assert.NoError(t, err)
	assert.Equal(t, "array", res)
	res, err = rh.JSONDel("jsondel", ".arr")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
	res, err = rh.JSONObjLen("jsondel", ".")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), res)
	res, err = rh.JSONDel("jsondel", ".")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
	_, err = rh.JSONGet("jsondel", ".")
	assert.Equal(t, redis.Nil, err)
}

func TestJsonMgetCommand(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	keys := make([]string, 5)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("mget:%d", i)
		keys[i] = key
		_, err := c.Del(context.Background(), key).Result()
		assert.NoError(t, err)
		res, err := rh.JSONSet(key, ".", docs["basic"])
		assert.NoError(t, err)
		assert.Equal(t, "OK", res)
	}
	basic, err := json.Marshal(docs["basic"])
	assert.NoError(t, err)
	res, err := rh.JSONMGet(".", keys...)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), len(res.([]interface{})))
	for _, v := range res.([]interface{}) {
		assert.JSONEq(t, string(basic), string(v.([]byte)))
	}
	// Test an MGET that fails for one key
	r, err := rh.JSONMGet("42isnotapath", "mget:0", "mget:1")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(r.([]interface{})))
	for _, v := range r.([]interface{}) {
		assert.Equal(t, nil, v)
	}
	_, err = c.Del(context.Background(), "mget:test").Result()
	assert.NoError(t, err)
	res, err = rh.JSONSet("mget:test", ".", `{"bull":4.2}`)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	r, err = rh.JSONMGet(".bool", "mget:0", "mget:test", "mget:1")
	assert.NoError(t, err)
	v := r.([]interface{})
	assert.Equal(t, 3, len(v))
	trueData, _ := json.Marshal(true)
	assert.Equal(t, trueData, v[0])
	assert.Equal(t, nil, v[1])
	assert.Equal(t, trueData, v[2])
}

func TestJsonGetPartsOfValuesDocumentOneByOne(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	_, err := c.Del(context.Background(), "getparts").Result()
	assert.NoError(t, err)

	res, err := rh.JSONSet("getparts", ".", docs["values"])
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	args := make([]interface{}, 0)
	args = append(args, "JSON.GET", "getparts")
	for k, v := range docs["values"].(map[string]interface{}) {
		resBytes, err := Bytes(rh.JSONGet("getparts", fmt.Sprintf(".%s", k)))
		assert.NoError(t, err)
		vBytes, err := json.Marshal(v)
		assert.NoError(t, err)
		assert.Equal(t, vBytes, resBytes)
		args = append(args, k)
	}
	ctx := context.Background()
	cmd := redis.NewStringCmd(ctx, args...)
	_ = c.Process(ctx, cmd)
	str, err := cmd.Result()
	assert.NoError(t, err)
	values, err := json.Marshal(docs["values"])
	assert.NoError(t, err)
	require.JSONEq(t, str, string(values))
}

func TestJsonGetNonExistantPathsFromBasicDocumentShouldFail(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	_, err := c.Del(context.Background(), "getnotexist").Result()
	assert.NoError(t, err)

	res, err := rh.JSONSet("getnotexist", ".", docs["scalars"])
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	// Paths that do not exist
	paths := []string{".foo", "boo", ".key1[0]", ".key2.bar", ".key5[99]", `.key5["moo"]`}
	for _, path := range paths {
		_, err = rh.JSONGet("getnotexist", path)
		assert.Contains(t, err.Error(), "does not exist")
	}
}

func TestJsonGetWithPathErrors(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	_, err := c.Del(context.Background(), "getwithpath").Result()
	assert.NoError(t, err)

	res, err := rh.JSONSet("getwithpath", ".", map[string]string{})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	_, err = Bytes(rh.JSONGet("getwithpath", "gar\x00\x00bage"))
	assert.Contains(t, err.Error(), "does not exist")
	_, err = Bytes(rh.JSONGet("getwithpath", "not\x0d\x0aallowed by protocol"))
	assert.Contains(t, err.Error(), "does not exist")
}

func TestJsonSetWithPathErrors(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	_, err := c.Del(context.Background(), "setwithpath").Result()
	assert.NoError(t, err)

	res, err := rh.JSONSet("setwithpath", ".", map[string]string{})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	// diff with RedisJson
	// https://github.com/RedisJSON/RedisJSON/blob/a31f2dabbc15c004d0cec7d1c25cc966b33a0a0b/tests/pytest/test.py#L237-L239
	res, err = rh.JSONSet("setwithpath", "$..f", 1)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	// diff RedisJson
	// https://github.com/RedisJSON/RedisJSON/blob/a31f2dabbc15c004d0cec7d1c25cc966b33a0a0b/tests/pytest/test.py#L241-L243
	res, err = rh.JSONSet("setwithpath", "$[0]", 1)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)

	res, err = Bytes(rh.JSONGet("setwithpath", "."))
	assert.NoError(t, err)
	assert.Equal(t, []byte(`{"f":1,"0":1}`), res)
}

func TestJsonGetWithBracketNotation(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	_, err := c.Del(context.Background(), "getwithbracket").Result()
	assert.NoError(t, err)
	res, err := rh.JSONSet("getwithbracket", ".", []interface{}{1, 2, 3})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = Bytes(rh.JSONGet("getwithbracket", ".[1]"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("2"), res)
	res, err = Bytes(rh.JSONGet("getwithbracket", "[1]"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("2"), res)
	res, err = Bytes(rh.JSONGet("getwithbracket", "$[1]"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("[2]"), res)
	res, err = Bytes(rh.JSONGet("getwithbracket", "$.[1]"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("[2]"), res)
}

func TestJsonSetWithBracketNotation(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	_, err := c.Del(context.Background(), "setwithbracket").Result()
	assert.NoError(t, err)
	res, err := rh.JSONSet("setwithbracket", "$", map[string]string{})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)

	res, err = rh.JSONSet("setwithbracket", `$.["f1"]`, map[string]string{})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONSet("setwithbracket", `.["f1"]`, map[string]string{})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONSet("setwithbracket", `$.["f1"].f2`, []int{0, 0, 0})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONSet("setwithbracket", `.["f1"].f2`, []int{0, 0, 0})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONSet("setwithbracket", `$.["f1"].f2[1]`, map[string]string{})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONSet("setwithbracket", `.["f1"].f2[1]`, map[string]string{})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	_, err = rh.JSONSet("setwithbracket", `$.["f1"].f2[1]["f.]$.f"`, map[string]string{})
	assert.EqualError(t, err, "ERR wrong static path")
	res, err = rh.JSONSet("setwithbracket", `$.["f3"].f2`, 1)
	assert.NoError(t, err)
	assert.Equal(t, nil, res)
	res, err = rh.JSONSet("setwithbracket", `.["f3"].f2`, 1)
	assert.NoError(t, err)
	assert.Equal(t, nil, res)

	res, err = Bytes(rh.JSONGet("setwithbracket", "."))
	assert.NoError(t, err)
	assert.Equal(t, []byte(`{"f1":{"f2":[0,{},0]}}`), res)
}

func TestJsonSetBehaviorModifyingSubcommands(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	_, err := c.Del(context.Background(), "setbehavior").Result()
	assert.NoError(t, err)

	// test against the root
	res, err := rh.JSONSet("setbehavior", "$", map[string]string{}, "xx")
	assert.NoError(t, err)
	assert.Equal(t, nil, res)
	res, err = rh.JSONSet("setbehavior", "$", map[string]string{}, "nx")
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONSet("setbehavior", "$", map[string]string{}, "nx")
	assert.NoError(t, err)
	assert.Equal(t, nil, res)
	res, err = rh.JSONSet("setbehavior", "$", map[string]string{}, "xx")
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)

	// test an object key
	res, err = rh.JSONSet("setbehavior", "$.foo", []string{}, "xx")
	assert.NoError(t, err)
	assert.Equal(t, nil, res)
	res, err = rh.JSONSet("setbehavior", "$.foo", []string{}, "nx")
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONSet("setbehavior", "$.foo", []string{}, "nx")
	assert.NoError(t, err)
	assert.Equal(t, nil, res)
	res, err = rh.JSONSet("setbehavior", "$.foo", []string{}, "xx")
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)

	// verify failure for arrays
	_, err = rh.JSONSet("setbehavior", "$.foo[1]", nil)
	assert.EqualError(t, err, "ERR array index out of range")

	// Wrong arguments
	ctx := context.Background()
	cmd := redis.NewStatusCmd(ctx, "JSON.SET", "setbehavior", "$.foo", "[]", "")
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.EqualError(t, err, "ERR syntax error")
	cmd = redis.NewStatusCmd(ctx, "JSON.SET", "setbehavior", "$.foo", "[]", "NN")
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.EqualError(t, err, "ERR syntax error")
	cmd = redis.NewStatusCmd(ctx, "JSON.SET", "setbehavior", "$.foo", "[]", "FORMAT", "TT")
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.EqualError(t, err, "ERR wrong number of arguments for 'json.set' command")
	cmd = redis.NewStatusCmd(ctx, "JSON.SET", "setbehavior", "$.foo", "[]", "XX", "FORMAT", "")
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.EqualError(t, err, "ERR wrong number of arguments for 'json.set' command")
	cmd = redis.NewStatusCmd(ctx, "JSON.SET", "setbehavior", "$.foo", "[]", "XX", "XN")
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.EqualError(t, err, "ERR wrong number of arguments for 'json.set' command")
	cmd = redis.NewStatusCmd(ctx, "JSON.SET", "setbehavior", "$.foo", "[]", "XX", "")
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.EqualError(t, err, "ERR wrong number of arguments for 'json.set' command")
}

func TestJsonSetGetWholeBasicDocumentShouldBeEqual(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	_, err := c.Del(context.Background(), "setgetwholebasic").Result()
	assert.NoError(t, err)
	res, err := rh.JSONSet("setgetwholebasic", ".", docs["basic"])
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	ctx := context.Background()
	cmd := redis.NewStringCmd(ctx, "JSON.GET", "setgetwholebasic")
	_ = c.Process(ctx, cmd)
	str, err := cmd.Result()
	assert.NoError(t, err)
	basic, err := json.Marshal(docs["basic"])
	assert.NoError(t, err)
	assert.Equal(t, []byte(str), basic)
}

func TestJsonSetReplaceRootShouldSucceed(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	_, err := c.Del(context.Background(), "setreplaceroot").Result()
	assert.NoError(t, err)
	res, err := rh.JSONSet("setreplaceroot", "$", docs)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONSet("setreplaceroot", ".", docs["basic"])
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONSet("setreplaceroot", ".", docs["simple"])
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = Bytes(rh.JSONGet("setreplaceroot", "."))
	assert.NoError(t, err)
	simple, err := json.Marshal(docs["simple"])
	assert.NoError(t, err)
	assert.Equal(t, simple, res)
	for _, value := range docs["values"].(map[string]interface{}) {
		res, err = rh.JSONSet("setreplaceroot", ".", value)
		assert.NoError(t, err)
		assert.Equal(t, "OK", res)
		res, err = Bytes(rh.JSONGet("setreplaceroot", "."))
		assert.NoError(t, err)
		valueBytes, err := json.Marshal(value)
		assert.NoError(t, err)
		assert.Equal(t, valueBytes, res)
	}
}

func TestJsonSetAddNewImmediateChild(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	_, err := c.Del(context.Background(), "addnewchild").Result()
	assert.NoError(t, err)
	res, err := rh.JSONSet("addnewchild", "$", docs)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	resBytes, err := Bytes(rh.JSONGet("addnewchild", "$.basic.dict.new_child_1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte(`[]`), resBytes)
	resBytes, err = Bytes(rh.JSONGet("addnewchild", "$.basic.dict.new_child_2"))
	assert.NoError(t, err)
	assert.Equal(t, []byte(`[]`), resBytes)
	resBytes, err = Bytes(rh.JSONGet("addnewchild", "$.basic.dict.new_child_3"))
	assert.NoError(t, err)
	assert.Equal(t, []byte(`[]`), resBytes)

	ctx := context.Background()
	cmd := redis.NewStatusCmd(ctx, "JSON.SET", "addnewchild", "$.basic.dict.new_child_1", `"new_child_1_val"`)
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.NoError(t, err)
	resBytes, err = Bytes(rh.JSONGet("addnewchild", "$.basic.dict.new_child_1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte(`["new_child_1_val"]`), resBytes)

	cmd = redis.NewStatusCmd(ctx, "JSON.SET", "addnewchild", "$.basic.dict.new_child_2", `"new_child_2_val"`, "nx")
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.NoError(t, err)
	resBytes, err = Bytes(rh.JSONGet("addnewchild", "$.basic.dict.new_child_2"))
	assert.NoError(t, err)
	assert.Equal(t, []byte(`["new_child_2_val"]`), resBytes)
	cmd = redis.NewStatusCmd(ctx, "JSON.SET", "addnewchild", "$.basic.dict.new_child_3", `"new_child_2_val"`, "xx")
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.Equal(t, err, redis.Nil)

	cmd = redis.NewStatusCmd(ctx, "JSON.SET", "addnewchild", "$.basic.dict.new_child_3.new_grandchild_1", `"new_child_3_val"`)
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.Equal(t, err, redis.Nil)

	resBytes, err = Bytes(rh.JSONGet("addnewchild", "$.basic.dict.new_child_3"))
	assert.NoError(t, err)
	assert.Equal(t, []byte(`[]`), resBytes)
	resBytes, err = Bytes(rh.JSONGet("addnewchild", "$.basic.dict.new_child_3.new_grandchild_1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte(`[]`), resBytes)
}

func TestJsonInvalidValue(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	for _, tt := range testInvalidJsonResult {
		ctx := context.Background()
		cmd := redis.NewStatusCmd(ctx, "JSON.SET", "invalidjsonresult", ".", tt)
		_ = c.Process(ctx, cmd)
		_, err := cmd.Result()
		assert.EqualError(t, err, "ERR invalid json")
	}
}

func TestJsonInvalidPath(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		err := c.Close()
		assert.NoError(t, err)
	}()
	for _, tt := range testInvalidJsonPath {
		ctx := context.Background()
		cmd := redis.NewStatusCmd(ctx, "JSON.SET", "invalidjsonpath", tt, "null")
		_ = c.Process(ctx, cmd)
		_, err := cmd.Result()
		assert.EqualError(t, err, "ERR new objects must be created at the root")
	}
	ctx := context.Background()
	cmd := redis.NewStatusCmd(ctx, "JSON.SET", "invalidjsonpath", "$", "{}")
	_ = c.Process(ctx, cmd)
	ret, err := cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", ret)
	for _, tt := range testInvalidJsonPath {
		ctx := context.Background()
		cmd := redis.NewStatusCmd(ctx, "JSON.SET", "invalidjsonpath", tt, "null")
		_ = c.Process(ctx, cmd)
		_, err := cmd.Result()
		assert.Error(t, err, fmt.Sprintf("path %s", tt))
	}
}

func TestRedis(t *testing.T) {
	t.Parallel()
	for i, tt := range testRedisString {
		tt := tt
		i := i
		t.Run("redis"+tt.Key, func(t *testing.T) {
			t.Parallel()
			c := redis.NewClient(&redis.Options{
				Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
				Password: cfg.Auth.Pass})
			defer func() {
				if err := c.Close(); err != nil {
					log.Fatalf("redis - failed to communicate to redis-server: %v", err)
				}
			}()

			ctx := context.Background()
			key := "redis" + tt.Key

			ok, err := c.SetXX(ctx, key, tt.Value+"xx", tt.Expire).Result()
			assert.NoError(t, err)
			assert.Equal(t, false, ok)

			if i%2 == 0 {
				set, err := c.Set(ctx, key, tt.Value, tt.Expire).Result()
				assert.NoError(t, err)
				assert.Equal(t, "OK", set)
			} else {
				ok, err := c.SetNX(ctx, key, tt.Value, tt.Expire).Result()
				assert.NoError(t, err)
				assert.Equal(t, true, ok)
			}

			ok, err = c.SetNX(ctx, key, tt.Value+"nx", tt.Expire).Result()
			assert.NoError(t, err)
			assert.Equal(t, false, ok)

			get, err := c.Get(ctx, key).Result()
			assert.NoError(t, err)
			assert.Equal(t, tt.Value, get)

			ok, err = c.SetXX(ctx, key, tt.Value+"xx", tt.Expire).Result()
			assert.NoError(t, err)
			assert.Equal(t, true, ok)
			get, err = c.Get(ctx, key).Result()
			assert.NoError(t, err)
			assert.Equal(t, tt.Value+"xx", get)

			if tt.KeepTTL {
				time.Sleep(tt.Expire / 2)
				set, err := c.Set(ctx, key, tt.Value+"keepttl", -1).Result()
				assert.NoError(t, err)
				assert.Equal(t, "OK", set)
				get, err := c.Get(ctx, key).Result()
				assert.NoError(t, err)
				assert.Equal(t, tt.Value+"keepttl", get)
				time.Sleep(tt.Expire / 2)
			} else {
				time.Sleep(tt.Expire)
			}
			time.Sleep(time.Millisecond)

			if tt.Expire <= 0 {
				del, err := c.Del(ctx, key).Result()
				assert.NoError(t, err)
				assert.Equal(t, int64(1), del)
			}

			expire, err := c.TTL(ctx, key).Result()
			assert.NoError(t, err)
			assert.Equal(t, time.Duration(-2), expire)

			value, err := c.Get(ctx, key).Result()
			assert.Equal(t, redis.Nil, err, "value %s", value)

			del, err := c.Del(ctx, key).Result()
			assert.NoError(t, err)
			assert.Equal(t, int64(0), del)
		})
	}
}
