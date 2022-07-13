package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
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

func TestJsonNumIncrCommand(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	key := "numincr"
	defer func() {
		_, err := c.Del(context.Background(), key).Result()
		assert.NoError(t, err)
		err = c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	res, err := rh.JSONSet(key, ".", map[string]interface{}{
		"foo": 0, "bar": "baz",
	})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONNumIncrBy(key, ".foo", 1)
	assert.NoError(t, err)
	resf, err := strconv.ParseFloat(string(res.([]byte)), 64)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), resf)
	res, err = rh.JSONGet(key, ".foo")
	assert.NoError(t, err)
	resf, err = strconv.ParseFloat(string(res.([]byte)), 64)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), resf)

	ctx := context.Background()
	cmd := redis.NewFloatCmd(ctx, "JSON.NumIncrby", key, ".foo", 2)
	err = c.Process(ctx, cmd)
	assert.NoError(t, err)
	f, err := cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(3), f)
	cmd = redis.NewFloatCmd(ctx, "JSON.NumIncrby", key, ".foo", .5)
	err = c.Process(ctx, cmd)
	assert.NoError(t, err)
	f, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(3.5), f)

	cmd = redis.NewFloatCmd(ctx, "JSON.NumIncrby", key, ".bar", 1)
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.Contains(t, err.Error(), "not a number")
	cmd = redis.NewFloatCmd(ctx, "JSON.NumIncrby", key, ".fuzz", 1)
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.Contains(t, err.Error(), "Path does not exist")

	res, err = rh.JSONSet(key, ".", 0)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	cmd = redis.NewFloatCmd(ctx, "JSON.NumIncrby", key, ".", 1)
	err = c.Process(ctx, cmd)
	assert.NoError(t, err)
	f, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(1), f)
	cmd = redis.NewFloatCmd(ctx, "JSON.NumIncrby", key, ".", 1.5)
	err = c.Process(ctx, cmd)
	assert.NoError(t, err)
	f, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(2.5), f)

	res, err = rh.JSONSet(key, ".", map[string]interface{}{
		"foo": 0, "bar": 42,
	})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	cmd = redis.NewFloatCmd(ctx, "JSON.NUMINCRBY", key, "foo", 1)
	err = c.Process(ctx, cmd)
	assert.NoError(t, err)
	f, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(1), f)
	cmd = redis.NewFloatCmd(ctx, "JSON.NUMMULTBY", key, "bar", 2)
	err = c.Process(ctx, cmd)
	assert.NoError(t, err)
	f, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(84), f)
	resBytes, err := Bytes(rh.JSONGet(key, "."))
	assert.NoError(t, err)
	assert.JSONEq(t, `{"foo": 1, "bar": 84}`, string(resBytes))
}

func TestJsonObjKeysCommand(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	key := "objkeys"
	defer func() {
		_, err := c.Del(context.Background(), key).Result()
		assert.NoError(t, err)
		err = c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	res, err := rh.JSONSet(key, ".", docs["types"])
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONObjKeys(key, ".")
	assert.NoError(t, err)
	types := docs["types"].(map[string]interface{})
	assert.Equal(t, len(types), len(res.([]string)))
	for _, k := range res.([]string) {
		_, ok := types[k]
		assert.Equal(t, ok, true)
	}
	_, err = rh.JSONObjKeys(key, ".null")
	assert.Contains(t, err.Error(), "not an object")
}

func TestJsonStrLenCommand(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	key := "strlen"
	defer func() {
		_, err := c.Del(context.Background(), key).Result()
		assert.NoError(t, err)
		err = c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	_, err := rh.JSONArrLen("notexists", ".bar")
	assert.Equal(t, redis.Nil, err)
	res, err := rh.JSONSet(key, ".", docs["basic"])
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONStrLen(key, ".string")
	assert.NoError(t, err)
	assert.Equal(t, int64(12), res)
	res, err = rh.JSONObjLen(key, ".dict")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), res)
	res, err = rh.JSONArrLen(key, ".arr")
	assert.NoError(t, err)
	assert.Equal(t, int64(6), res)

	_, err = rh.JSONArrLen(key, ".bool")
	assert.Contains(t, err.Error(), "not an array")
	_, err = rh.JSONStrLen(key, ".none")
	assert.Contains(t, err.Error(), "not a string")
	_, err = rh.JSONObjLen(key, ".int")
	assert.Contains(t, err.Error(), "not an object")
	_, err = rh.JSONStrLen(key, ".num")
	assert.Contains(t, err.Error(), "not a string")
	_, err = rh.JSONArrLen(key, ".foo")
	assert.Contains(t, err.Error(), "not exist")
	_, err = rh.JSONArrLen(key, ".arr[999]")
	assert.Contains(t, err.Error(), "not exist")
}

func TestJsonTypeCommand(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	key := "arrtype"
	defer func() {
		_, err := c.Del(context.Background(), key).Result()
		assert.NoError(t, err)
		err = c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	for k, v := range docs["types"].(map[string]interface{}) {
		res, err := rh.JSONSet(key, ".", v)
		assert.NoError(t, err)
		assert.Equal(t, "OK", res)
		res, err = rh.JSONType(key, ".")
		assert.NoError(t, err)
		assert.Equal(t, res, k)
	}

}

func TestJsonArrPopCommand(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		_, err := c.Del(context.Background(), "arrpop").Result()
		assert.NoError(t, err)
		err = c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	jv := `[1,2,3,4,5,6,7,8,9]`
	var v interface{}
	err := json.Unmarshal([]byte(jv), &v)
	assert.NoError(t, err)
	res, err := rh.JSONSet("arrpop", ".", v)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	ctx := context.Background()
	cmd := redis.NewFloatCmd(ctx, "JSON.ArrPop", "arrpop")
	_ = c.Process(ctx, cmd)
	i, err := cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(9), i)
	cmd = redis.NewFloatCmd(ctx, "JSON.ArrPop", "arrpop", ".")
	_ = c.Process(ctx, cmd)
	i, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(8), i)
	res, err = rh.JSONArrPop("arrpop", ".", -1)
	assert.NoError(t, err)
	resf, err := strconv.ParseFloat(string(res.([]byte)), 64)
	assert.NoError(t, err)
	assert.Equal(t, float64(7), resf)
	res, err = rh.JSONArrPop("arrpop", ".", -2)
	assert.NoError(t, err)
	resf, err = strconv.ParseFloat(string(res.([]byte)), 64)
	assert.NoError(t, err)
	assert.Equal(t, float64(5), resf)
	res, err = rh.JSONArrPop("arrpop", ".", 0)
	assert.NoError(t, err)
	resf, err = strconv.ParseFloat(string(res.([]byte)), 64)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), resf)
	res, err = rh.JSONArrPop("arrpop", ".", 2)
	assert.NoError(t, err)
	resf, err = strconv.ParseFloat(string(res.([]byte)), 64)
	assert.NoError(t, err)
	assert.Equal(t, float64(4), resf)
	res, err = rh.JSONArrPop("arrpop", ".", 99)
	assert.NoError(t, err)
	resf, err = strconv.ParseFloat(string(res.([]byte)), 64)
	assert.NoError(t, err)
	assert.Equal(t, float64(6), resf)
	res, err = rh.JSONArrPop("arrpop", ".", -99)
	assert.NoError(t, err)
	resf, err = strconv.ParseFloat(string(res.([]byte)), 64)
	assert.NoError(t, err)
	assert.Equal(t, float64(2), resf)
	cmd = redis.NewFloatCmd(ctx, "JSON.ArrPop", "arrpop")
	_ = c.Process(ctx, cmd)
	i, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(3), i)
	cmd = redis.NewFloatCmd(ctx, "JSON.ArrPop", "arrpop")
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.Equal(t, redis.Nil, err)
	cmd = redis.NewFloatCmd(ctx, "JSON.ArrPop", "arrpop", ".")
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.Equal(t, redis.Nil, err)
	cmd = redis.NewFloatCmd(ctx, "JSON.ArrPop", "arrpop", ".", 2)
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.Equal(t, redis.Nil, err)
	res, err = rh.JSONSet("arrpop", ".", 1)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	cmd = redis.NewFloatCmd(ctx, "JSON.ArrPop", "arrpop")
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.Contains(t, err.Error(), "not an array")
}

func TestJsonArrTrimCommand(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		_, err := c.Del(context.Background(), "arrtrim").Result()
		assert.NoError(t, err)
		err = c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	jv := `{ "arr": [0, 1, 2, 3, 2, 1, 0] }`
	var v interface{}
	err := json.Unmarshal([]byte(jv), &v)
	assert.NoError(t, err)
	res, err := rh.JSONSet("arrtrim", ".", v)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONArrTrim("arrtrim", ".arr", 1, -2)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), res)
	resBytes, err := Bytes(rh.JSONGet("arrtrim", ".arr"))
	assert.NoError(t, err)
	assert.JSONEq(t, `[1, 2, 3, 2, 1]`, string(resBytes))
	res, err = rh.JSONArrTrim("arrtrim", ".arr", 0, 99)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), res)
	resBytes, err = Bytes(rh.JSONGet("arrtrim", ".arr"))
	assert.NoError(t, err)
	assert.JSONEq(t, `[1, 2, 3, 2, 1]`, string(resBytes))
	res, err = rh.JSONArrTrim("arrtrim", ".arr", 0, 2)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), res)
	resBytes, err = Bytes(rh.JSONGet("arrtrim", ".arr"))
	assert.NoError(t, err)
	assert.JSONEq(t, `[1, 2, 3]`, string(resBytes))
	res, err = rh.JSONArrTrim("arrtrim", ".arr", 99, 2)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), res)
	resBytes, err = Bytes(rh.JSONGet("arrtrim", ".arr"))
	assert.NoError(t, err)
	assert.JSONEq(t, `[]`, string(resBytes))
	res, err = rh.JSONArrTrim("arrtrim", ".arr", -1, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), res)
	resBytes, err = Bytes(rh.JSONGet("arrtrim", ".arr"))
	assert.NoError(t, err)
	assert.JSONEq(t, `[]`, string(resBytes))
	res, err = rh.JSONSet("arrtrim", ".", v)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONArrTrim("arrtrim", ".arr", -1, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), res)
	resBytes, err = Bytes(rh.JSONGet("arrtrim", ".arr"))
	assert.NoError(t, err)
	assert.JSONEq(t, `[]`, string(resBytes))
	res, err = rh.JSONSet("arrtrim", ".", v)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONArrTrim("arrtrim", ".arr", -4, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), res)
	resBytes, err = Bytes(rh.JSONGet("arrtrim", ".arr"))
	assert.NoError(t, err)
	assert.JSONEq(t, `[]`, string(resBytes))
	res, err = rh.JSONSet("arrtrim", ".", 1)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	_, err = rh.JSONArrTrim("arrtrim", ".", 0, 1)
	assert.Contains(t, err.Error(), "not an array")
}

func TestJsonArrIndexMixCommand(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		_, err := c.Del(context.Background(), "arrindexmix").Result()
		assert.NoError(t, err)
		err = c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	jv := `{ "arr": [0, 1, 2, 3, 2, 1, 0, {"val": 4}, {"val": 9}, [3,4,8], ["a", "b", 8]] }`
	var v interface{}
	err := json.Unmarshal([]byte(jv), &v)
	assert.NoError(t, err)
	res, err := rh.JSONSet("arrindexmix", ".", v)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONArrIndex("arrindexmix", ".arr", 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), res)
	res, err = rh.JSONArrIndex("arrindexmix", ".arr", 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), res)
	res, err = rh.JSONArrIndex("arrindexmix", ".arr", 4)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), res)
	res, err = rh.JSONArrIndex("arrindexmix", ".arr", 0, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(6), res)
	res, err = rh.JSONArrIndex("arrindexmix", ".arr", 0, -5)
	assert.NoError(t, err)
	assert.Equal(t, int64(6), res)
	res, err = rh.JSONArrIndex("arrindexmix", ".arr", 0, 6)
	assert.NoError(t, err)
	assert.Equal(t, int64(6), res)
	res, err = rh.JSONArrIndex("arrindexmix", ".arr", 0, 4, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(6), res)
	res, err = rh.JSONArrIndex("arrindexmix", ".arr", 0, 5, -1)
	assert.NoError(t, err)
	assert.Equal(t, int64(6), res)
	res, err = rh.JSONArrIndex("arrindexmix", ".arr", 2, -2, 6)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), res)
	res, err = rh.JSONArrIndex("arrindexmix", ".arr", "foo")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), res)

	res, err = rh.JSONArrInsert("arrindexmix", ".arr", 4, []int{4})
	assert.NoError(t, err)
	assert.Equal(t, int64(12), res)
	res, err = rh.JSONArrIndex("arrindexmix", ".arr", 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), res)
	res, err = rh.JSONArrIndex("arrindexmix", ".arr", 2, 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), res)
	res, err = rh.JSONArrIndex("arrindexmix", ".arr", []int{4})
	assert.NoError(t, err)
	assert.Equal(t, int64(4), res)
	res, err = rh.JSONArrIndex("arrindexmix", ".arr", map[string]int{"val": 4})
	assert.NoError(t, err)
	assert.Equal(t, int64(8), res)
	res, err = rh.JSONArrIndex("arrindexmix", ".arr", []interface{}{"a", "b", 8})
	assert.NoError(t, err)
	assert.Equal(t, int64(11), res)
}

func TestJsonArrInsertCommand(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		_, err := c.Del(context.Background(), "jsonarrinsert").Result()
		assert.NoError(t, err)
		err = c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	jvs := [][2]string{
		{`{ "arr": [] }`, ".arr"},
		{`[]`, "."},
	}
	for _, jv := range jvs {
		var v interface{}
		err := json.Unmarshal([]byte(jv[0]), &v)
		assert.NoError(t, err)
		res, err := rh.JSONSet("jsonarrinsert", ".", v)
		assert.NoError(t, err)
		assert.Equal(t, "OK", res)
		res, err = rh.JSONArrInsert("jsonarrinsert", jv[1], 0, 1)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), res)
		res, err = rh.JSONArrInsert("jsonarrinsert", jv[1], -1, 2)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), res)
		res, err = rh.JSONArrInsert("jsonarrinsert", jv[1], -2, 3)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), res)
		res, err = rh.JSONArrInsert("jsonarrinsert", jv[1], 3, 4)
		assert.NoError(t, err)
		assert.Equal(t, int64(4), res)
		resBytes, err := Bytes(rh.JSONGet("jsonarrinsert", jv[1]))
		assert.NoError(t, err)
		assert.JSONEq(t, `[3,2,1,4]`, string(resBytes))
		res, err = rh.JSONArrInsert("jsonarrinsert", jv[1], 1, 5)
		assert.NoError(t, err)
		assert.Equal(t, int64(5), res)
		res, err = rh.JSONArrInsert("jsonarrinsert", jv[1], -2, 6)
		assert.NoError(t, err)
		assert.Equal(t, int64(6), res)
		resBytes, err = Bytes(rh.JSONGet("jsonarrinsert", jv[1]))
		assert.NoError(t, err)
		assert.JSONEq(t, `[3,5,2,6,1,4]`, string(resBytes))
		res, err = rh.JSONArrInsert("jsonarrinsert", jv[1], -3,
			7, map[string]string{"A": "Z"}, 9)
		assert.NoError(t, err)
		assert.Equal(t, int64(9), res)
		resBytes, err = Bytes(rh.JSONGet("jsonarrinsert", jv[1]))
		assert.NoError(t, err)
		assert.JSONEq(t, `[3,5,2,7,{"A":"Z"},9,6,1,4]`, string(resBytes))
		_, err = rh.JSONArrInsert("jsonarrinsert", jv[1], -10, 10)
		assert.Contains(t, err.Error(), "out of range")
		resBytes, err = Bytes(rh.JSONGet("jsonarrinsert", jv[1]))
		assert.NoError(t, err)
		assert.JSONEq(t, `[3,5,2,7,{"A":"Z"},9,6,1,4]`, string(resBytes))
		_, err = rh.JSONArrInsert("jsonarrinsert", jv[1], 10, 10)
		assert.Contains(t, err.Error(), "out of range")
		resBytes, err = Bytes(rh.JSONGet("jsonarrinsert", jv[1]))
		assert.NoError(t, err)
		assert.JSONEq(t, `[3,5,2,7,{"A":"Z"},9,6,1,4]`, string(resBytes))
	}
}

func TestJsonArrIndexCommand(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		_, err := c.Del(context.Background(), "jsonarrindex").Result()
		assert.NoError(t, err)
		err = c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)
	res, err := rh.JSONSet("jsonarrindex", ".", map[string]interface{}{
		"arr": []int{0, 1, 2, 3, 2, 1, 0}})
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	res, err = rh.JSONArrIndex("jsonarrindex", ".arr", 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), res)
	res, err = rh.JSONArrIndex("jsonarrindex", ".arr", 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), res)
	res, err = rh.JSONArrIndex("jsonarrindex", ".arr", 4)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), res)
	res, err = rh.JSONArrIndex("jsonarrindex", ".arr", 0, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(6), res)
	res, err = rh.JSONArrIndex("jsonarrindex", ".arr", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, int64(6), res)
	res, err = rh.JSONArrIndex("jsonarrindex", ".arr", 0, 6)
	assert.NoError(t, err)
	assert.Equal(t, int64(6), res)
	res, err = rh.JSONArrIndex("jsonarrindex", ".arr", 0, 4, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(6), res)
	res, err = rh.JSONArrIndex("jsonarrindex", ".arr", 0, -5, -1)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), res)
	res, err = rh.JSONArrIndex("jsonarrindex", ".arr", 0, 5, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(6), res)
	res, err = rh.JSONArrIndex("jsonarrindex", ".arr", 2, -2, 6)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), res)
	res, err = rh.JSONArrIndex("jsonarrindex", ".arr", "foo")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), res)

	res, err = rh.JSONArrInsert("jsonarrindex", ".arr", 4, []int{4})
	assert.NoError(t, err)
	assert.Equal(t, int64(8), res)
	res, err = rh.JSONArrIndex("jsonarrindex", ".arr", 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), res)
	res, err = rh.JSONArrIndex("jsonarrindex", ".arr", 2, 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), res)
	res, err = rh.JSONArrIndex("jsonarrindex", ".arr", []int{4})
	assert.NoError(t, err)
	assert.Equal(t, int64(4), res)
	res, err = rh.JSONArrIndex("jsonarrindex", ".arr", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
	res, err = rh.JSONArrIndex("jsonarrindex", "$.arr", 1)
	assert.NoError(t, err)
	resBytes, err := json.Marshal(res)
	assert.NoError(t, err)
	assert.Equal(t, `[1]`, string(resBytes))
	res, err = rh.JSONArrIndex("jsonarrindex", "$.arr", 2, 1, 4)
	assert.NoError(t, err)
	resBytes, err = json.Marshal(res)
	assert.NoError(t, err)
	assert.Equal(t, `[2]`, string(resBytes))
	res, err = rh.JSONArrIndex("jsonarrindex", "$.arr", 6)
	assert.NoError(t, err)
	resBytes, err = json.Marshal(res)
	assert.NoError(t, err)
	assert.Equal(t, `[-1]`, string(resBytes))
	res, err = rh.JSONArrIndex("jsonarrindex", "$.arr", 3, 0, 2)
	assert.NoError(t, err)
	resBytes, err = json.Marshal(res)
	assert.NoError(t, err)
	assert.Equal(t, `[-1]`, string(resBytes))
}

func TestJsonArrayCRUD(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
		Password: cfg.Auth.Pass})
	defer func() {
		_, err := c.Del(context.Background(), "jsonarray").Result()
		assert.NoError(t, err)
		err = c.Close()
		assert.NoError(t, err)
	}()
	rh := rejson.NewReJSONHandler()
	rh.SetGoRedisClient(c)

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

func TestJsonClearScalar(t *testing.T) {
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
	res, err := rh.JSONSet("jsonclearscalar", ".", docs["basic"])
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)

	// Clear numeric values
	ctx := context.Background()
	cmd := redis.NewIntCmd(ctx, "JSON.Clear", "jsonclearscalar", "$.int")
	_ = c.Process(ctx, cmd)
	del, err := cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, del, int64(1))
	resBytes, err := Bytes(rh.JSONGet("jsonclearscalar", "$.int"))
	assert.NoError(t, err)
	assert.JSONEq(t, "[0]", string(resBytes))

	cmd = redis.NewIntCmd(ctx, "JSON.Clear", "jsonclearscalar", "$.num")
	_ = c.Process(ctx, cmd)
	del, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, del, int64(1))
	resBytes, err = Bytes(rh.JSONGet("jsonclearscalar", "$.num"))
	assert.NoError(t, err)
	assert.JSONEq(t, "[0]", string(resBytes))

	cmd = redis.NewIntCmd(ctx, "JSON.Clear", "jsonclearscalar", "$..a")
	_ = c.Process(ctx, cmd)
	del, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, del, int64(1))
	resBytes, err = Bytes(rh.JSONGet("jsonclearscalar", "$..a"))
	assert.NoError(t, err)
	assert.JSONEq(t, "[0]", string(resBytes))

	res, err = rh.JSONSet("jsonclearscalar", ".", docs["scalars"])
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	cmd = redis.NewIntCmd(ctx, "JSON.Clear", "jsonclearscalar", "$.*")
	_ = c.Process(ctx, cmd)
	del, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, del, int64(2))
	resBytes, err = Bytes(rh.JSONGet("jsonclearscalar", "$.*"))
	assert.NoError(t, err)
	assert.JSONEq(t, `[null,true,0,0,"string value"]`, string(resBytes))
	// Do not clear already cleared values
	cmd = redis.NewIntCmd(ctx, "JSON.Clear", "jsonclearscalar", "$.*")
	_ = c.Process(ctx, cmd)
	del, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, del, int64(0))
	//  Do not clear null scalar
	cmd = redis.NewIntCmd(ctx, "JSON.Clear", "jsonclearscalar", "$.NoneType")
	_ = c.Process(ctx, cmd)
	del, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, del, int64(0))
}

func TestJsonClearCommand(t *testing.T) {
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
	jsonValue := map[string]interface{}{
		"n": 42, "s": "42",
		"arr": []interface{}{
			map[string]int{"n": 44},
			"s",
			map[string]interface{}{
				"n": map[string]interface{}{
					"a": 1,
					"b": 2,
				},
			},
			map[string]interface{}{
				"n2": map[string]interface{}{
					"x": 3.02,
					"n": []interface{}{"to", "be", "cleared", 4},
					"y": 4.91,
				},
			},
			nil,
		},
	}
	res, err := rh.JSONSet("jsonclear", ".", jsonValue)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)

	resBytes, err := Bytes(rh.JSONGet("jsonclear", "$..n"))
	assert.NoError(t, err)
	assert.Equal(t, `[42,44,{"a":1,"b":2},["to","be","cleared",4]]`, string(resBytes))

	// Make sure specific obj content exists before clear
	objContent := `[{"a":1,"b":2}]`
	objContentLegacy := `{"a":1,"b":2}`
	resBytes, err = Bytes(rh.JSONGet("jsonclear", "$.arr[2].n"))
	assert.NoError(t, err)
	assert.Equal(t, objContent, string(resBytes))
	resBytes, err = Bytes(rh.JSONGet("jsonclear", ".arr[2].n"))
	assert.NoError(t, err)
	assert.Equal(t, objContentLegacy, string(resBytes))
	//Make sure specific arr content exists before clear
	arrContent := `[["to","be","cleared",4]]`
	arrContentLegacy := `["to","be","cleared",4]`
	resBytes, err = Bytes(rh.JSONGet("jsonclear", "$.arr[3].n2.n"))
	assert.NoError(t, err)
	assert.Equal(t, arrContent, string(resBytes))
	resBytes, err = Bytes(rh.JSONGet("jsonclear", ".arr[3].n2.n"))
	assert.NoError(t, err)
	assert.Equal(t, arrContentLegacy, string(resBytes))

	// Clear obj and arr with specific paths
	ctx := context.Background()
	cmd := redis.NewIntCmd(ctx, "JSON.Clear", "jsonclear", "$.arr[2].n")
	_ = c.Process(ctx, cmd)
	del, err := cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, del, int64(1))
	cmd = redis.NewIntCmd(ctx, "JSON.Clear", "jsonclear", "$.arr[3].n2.n")
	_ = c.Process(ctx, cmd)
	del, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, del, int64(1))

	// No clear on inappropriate path (not null)
	cmd = redis.NewIntCmd(ctx, "JSON.Clear", "jsonclear", "$.arr[4]")
	_ = c.Process(ctx, cmd)
	del, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, del, int64(0))

	// Make sure specific obj content was cleared
	resBytes, err = Bytes(rh.JSONGet("jsonclear", "$.arr[2].n"))
	assert.NoError(t, err)
	assert.JSONEq(t, "[{}]", string(resBytes))
	resBytes, err = Bytes(rh.JSONGet("jsonclear", ".arr[2].n"))
	assert.NoError(t, err)
	assert.JSONEq(t, "{}", string(resBytes))
	resBytes, err = Bytes(rh.JSONGet("jsonclear", "$.arr[3].n2.n"))
	assert.NoError(t, err)
	assert.JSONEq(t, "[[]]", string(resBytes))
	resBytes, err = Bytes(rh.JSONGet("jsonclear", ".arr[3].n2.n"))
	assert.NoError(t, err)
	assert.JSONEq(t, "[]", string(resBytes))

	//Make sure only appropriate content (obj and arr) was cleared
	resBytes, err = Bytes(rh.JSONGet("jsonclear", "$..n"))
	assert.NoError(t, err)
	assert.JSONEq(t, `[42,44,{},[]]`, string(resBytes))

	// Clear dynamic path
	s := `{"n":42,"s":"42","arr":[{"n":44},"s",{"n":{"a":1,"b":2}},{"n2":{"x":3.02,"n":["to","be","cleared",4],"y":4.91}}]}`
	jv := make(map[string]interface{})
	err = json.Unmarshal([]byte(s), &jv)
	assert.NoError(t, err)
	res, err = rh.JSONSet("jsonclear", ".", jv)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	cmd = redis.NewIntCmd(ctx, "JSON.Clear", "jsonclear", "$.arr.*")
	_ = c.Process(ctx, cmd)
	del, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, del, int64(3))
	resBytes, err = Bytes(rh.JSONGet("jsonclear", "$"))
	assert.NoError(t, err)
	assert.JSONEq(t, `[{"n":42,"s":"42","arr":[{},"s",{},{}]}]`, string(resBytes))

	// Clear root
	cl := make(map[string]interface{})
	err = json.Unmarshal([]byte(objContentLegacy), &cl)
	assert.NoError(t, err)
	res, err = rh.JSONSet("jsonclear", "$", cl)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	cmd = redis.NewIntCmd(ctx, "JSON.Clear", "jsonclear")
	_ = c.Process(ctx, cmd)
	del, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, del, int64(1))
	resBytes, err = Bytes(rh.JSONGet("jsonclear", "$"))
	assert.NoError(t, err)
	assert.JSONEq(t, `[{}]`, string(resBytes))

	// Clear none existing path
	s = `{"a":[1,2], "b":{"c":"d"}}`
	jv = make(map[string]interface{})
	err = json.Unmarshal([]byte(s), &jv)
	assert.NoError(t, err)
	res, err = rh.JSONSet("jsonclear", ".", jv)
	assert.NoError(t, err)
	assert.Equal(t, "OK", res)
	cmd = redis.NewIntCmd(ctx, "JSON.Clear", "jsonclear", "$.c")
	_ = c.Process(ctx, cmd)
	del, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, del, int64(0))
	resBytes, err = Bytes(rh.JSONGet("jsonclear", "$"))
	assert.NoError(t, err)
	assert.JSONEq(t, `[{"a":[1,2], "b":{"c":"d"}}]`, string(resBytes))
	cmd = redis.NewIntCmd(ctx, "JSON.Clear", "jsonclear", "$.b..a")
	_ = c.Process(ctx, cmd)
	del, err = cmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, del, int64(0))
	resBytes, err = Bytes(rh.JSONGet("jsonclear", "$"))
	assert.NoError(t, err)
	assert.JSONEq(t, `[{"a":[1,2], "b":{"c":"d"}}]`, string(resBytes))

	//Key doesn't exist
	cmd = redis.NewIntCmd(ctx, "JSON.Clear", "notexists", "$.c")
	_ = c.Process(ctx, cmd)
	_, err = cmd.Result()
	assert.Contains(t, err.Error(), "not exist")
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
