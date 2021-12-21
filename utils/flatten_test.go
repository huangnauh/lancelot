package utils

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type flattenTest struct {
	Struct interface{}
	Map    map[string]interface{}
	Start  string
}

type FlattenA struct {
	A string        `json:"a, omitempty"`
	B uint8         `json:"b, omitempty"`
	C int16         `json:"c"`
	D float64       `json:"d"`
	E bool          `json:"e"`
	F time.Duration `json:"f, omitempty"`
	G time.Time     `json:"-"`
}

type FlattenB struct {
	A FlattenA            `json:"a, omitempty"`
	B map[string]FlattenA `json:"b, omitempty"`
}

type FlattenP struct {
	A *FlattenA            `json:"a, omitempty"`
	B map[string]*FlattenA `json:"b, omitempty"`
}

var flattentests = []flattenTest{
	{FlattenA{},
		map[string]interface{}{"c": int16(0), "d": float64(0), "e": false},
		""},
	{FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		map[string]interface{}{"a": "a", "b": uint8(1), "c": int16(2), "d": float64(3.0), "e": true, "f": "1s"},
		""},
	{FlattenA{A: "a", E: true},
		map[string]interface{}{"a": "a", "c": int16(0), "d": float64(0), "e": true},
		""},
	{FlattenB{}, map[string]interface{}{"a.c": int16(0), "a.d": float64(0), "a.e": false},
		""},
	{FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second}},
		map[string]interface{}{"a.a": "a", "a.b": uint8(1), "a.c": int16(2), "a.d": float64(3.0), "a.e": true, "a.f": "1s"},
		""},
	{FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"a.a": "a", "a.b": uint8(1), "a.c": int16(2), "a.d": float64(3.0), "a.e": true,
			"a.f": "1s", "b.c.a": "c", "b.c.b": uint8(1), "b.c.c": int16(2), "b.c.d": float64(3.0), "b.c.e": true, "b.c.f": "1s"},
		""},
	{FlattenP{}, map[string]interface{}{}, ""},
	{FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second}},
		map[string]interface{}{"a.a": "a", "a.b": uint8(1), "a.c": int16(2), "a.d": float64(3.0), "a.e": true, "a.f": "1s"},
		""},
	{FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]*FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"a.a": "a", "a.b": uint8(1), "a.c": int16(2), "a.d": float64(3.0), "a.e": true,
			"a.f": "1s", "b.c.a": "c", "b.c.b": uint8(1), "b.c.c": int16(2), "b.c.d": float64(3.0), "b.c.e": true, "b.c.f": "1s"},
		""},
	{&FlattenA{}, map[string]interface{}{"c": int16(0), "d": float64(0), "e": false}, ""},
	{&FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		map[string]interface{}{"a": "a", "b": uint8(1), "c": int16(2), "d": float64(3.0), "e": true, "f": "1s"},
		""},
	{&FlattenA{A: "a", E: true},
		map[string]interface{}{"a": "a", "c": int16(0), "d": float64(0), "e": true},
		""},
	{&FlattenB{}, map[string]interface{}{"a.c": int16(0), "a.d": float64(0), "a.e": false}, ""},
	{&FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second}},
		map[string]interface{}{"a.a": "a", "a.b": uint8(1), "a.c": int16(2), "a.d": float64(3.0), "a.e": true, "a.f": "1s"},
		""},
	{&FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"a.a": "a", "a.b": uint8(1), "a.c": int16(2), "a.d": float64(3.0), "a.e": true,
			"a.f": "1s", "b.c.a": "c", "b.c.b": uint8(1), "b.c.c": int16(2), "b.c.d": float64(3.0), "b.c.e": true, "b.c.f": "1s"},
		""},
	{&FlattenP{}, map[string]interface{}{}, ""},
	{&FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second}},
		map[string]interface{}{"a.a": "a", "a.b": uint8(1), "a.c": int16(2), "a.d": float64(3.0), "a.e": true, "a.f": "1s"},
		""},
	{&FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]*FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"a.a": "a", "a.b": uint8(1), "a.c": int16(2), "a.d": float64(3.0), "a.e": true,
			"a.f": "1s", "b.c.a": "c", "b.c.b": uint8(1), "b.c.c": int16(2), "b.c.d": float64(3.0), "b.c.e": true, "b.c.f": "1s"},
		""},
	{FlattenA{},
		map[string]interface{}{"c": int16(0)},
		"c"},
	{FlattenA{},
		map[string]interface{}{"e": false},
		"e"},
	{FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		map[string]interface{}{"f": "1s"},
		"f"},
	{FlattenA{A: "a", E: true},
		map[string]interface{}{"e": true},
		"e"},
	{FlattenB{}, map[string]interface{}{"a.c": int16(0), "a.d": float64(0), "a.e": false},
		"a"},
	{FlattenB{}, map[string]interface{}{"a.e": false},
		"a.e"},
	{FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second}},
		map[string]interface{}{"a.a": "a", "a.b": uint8(1), "a.c": int16(2), "a.d": float64(3.0), "a.e": true, "a.f": "1s"},
		"a"},
	{FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second}},
		map[string]interface{}{"a.d": float64(3.0)},
		"a.d"},
	{FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second}},
		map[string]interface{}{},
		"b"},
	{FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"a.a": "a", "a.b": uint8(1), "a.c": int16(2), "a.d": float64(3.0), "a.e": true, "a.f": "1s"},
		"a"},
	{FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"b.c.a": "c", "b.c.b": uint8(1), "b.c.c": int16(2), "b.c.d": float64(3.0), "b.c.e": true, "b.c.f": "1s"},
		"b"},
	{FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"a.f": "1s"},
		"a.f"},
	{FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"b.c.a": "c", "b.c.b": uint8(1), "b.c.c": int16(2), "b.c.d": float64(3.0), "b.c.e": true, "b.c.f": "1s"},
		"b.c"},
	{FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"b.c.f": "1s"},
		"b.c.f"},
	{FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{},
		"b.c.g"},
	{FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{},
		"c"},
	{FlattenP{}, map[string]interface{}{}, ""},
	{FlattenP{}, map[string]interface{}{}, "a"},
	{FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second}},
		map[string]interface{}{"a.a": "a", "a.b": uint8(1), "a.c": int16(2), "a.d": float64(3.0), "a.e": true, "a.f": "1s"},
		"a"},
	{FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second}},
		map[string]interface{}{"a.a": "a"},
		"a.a"},
	{FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]*FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"a.a": "a", "a.b": uint8(1), "a.c": int16(2), "a.d": float64(3.0), "a.e": true, "a.f": "1s"},
		"a"},
	{FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]*FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"b.c.a": "c", "b.c.b": uint8(1), "b.c.c": int16(2), "b.c.d": float64(3.0), "b.c.e": true, "b.c.f": "1s"},
		"b"},
	{FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]*FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"b.c.a": "c"},
		"b.c.a"},
	{FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]*FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"b.c.a": "c", "b.c.b": uint8(1), "b.c.c": int16(2), "b.c.d": float64(3.0), "b.c.e": true, "b.c.f": "1s"},
		"b.c"},
	{&FlattenA{}, map[string]interface{}{"c": int16(0)}, "c"},
	{&FlattenA{}, map[string]interface{}{}, "a"},
	{&FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		map[string]interface{}{"a": "a"},
		"a"},
	{&FlattenA{A: "a", E: true},
		map[string]interface{}{"d": float64(0)},
		"d"},
	{&FlattenA{A: "a", E: true},
		map[string]interface{}{},
		"d.d"},
	{&FlattenB{}, map[string]interface{}{"a.c": int16(0), "a.d": float64(0), "a.e": false}, "a"},
	{&FlattenB{}, map[string]interface{}{"a.c": int16(0)}, "a.c"},
	{&FlattenB{}, map[string]interface{}{}, "b"},
	{&FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second}},
		map[string]interface{}{"a.a": "a", "a.b": uint8(1), "a.c": int16(2), "a.d": float64(3.0), "a.e": true, "a.f": "1s"},
		"a"},
	{&FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second}},
		map[string]interface{}{"a.f": "1s"},
		"a.f"},
	{&FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second}},
		map[string]interface{}{},
		"a.g"},
	{&FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second}},
		map[string]interface{}{},
		"a.a.a"},
	{&FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"b.c.a": "c", "b.c.b": uint8(1), "b.c.c": int16(2), "b.c.d": float64(3.0), "b.c.e": true, "b.c.f": "1s"},
		"b"},
	{&FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"b.c.a": "c", "b.c.b": uint8(1), "b.c.c": int16(2), "b.c.d": float64(3.0), "b.c.e": true, "b.c.f": "1s"},
		"b.c"},
	{&FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"b.c.a": "c"},
		"b.c.a"},
	{&FlattenB{A: FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{},
		"b.c.c.a"},
	{&FlattenP{}, map[string]interface{}{}, ""},
	{&FlattenP{}, map[string]interface{}{}, "a"},
	{&FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second}},
		map[string]interface{}{"a.a": "a", "a.b": uint8(1), "a.c": int16(2), "a.d": float64(3.0), "a.e": true, "a.f": "1s"},
		"a"},
	{&FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second}},
		map[string]interface{}{"a.a": "a"},
		"a.a"},
	{&FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]*FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"b.c.a": "c", "b.c.b": uint8(1), "b.c.c": int16(2), "b.c.d": float64(3.0), "b.c.e": true, "b.c.f": "1s"},
		"b"},
	{&FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]*FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"b.c.a": "c", "b.c.b": uint8(1), "b.c.c": int16(2), "b.c.d": float64(3.0), "b.c.e": true, "b.c.f": "1s"},
		"b.c"},
	{&FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]*FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"b.c.a": "c", "b.c.b": uint8(1), "b.c.c": int16(2), "b.c.d": float64(3.0), "b.c.e": true, "b.c.f": "1s"},
		"b.c."},
	{&FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]*FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{"b.c.f": "1s"},
		"b.c.f"},
	{&FlattenP{A: &FlattenA{A: "a", B: 1, C: 2, D: 3.0, E: true, F: time.Second},
		B: map[string]*FlattenA{"c": {A: "c", B: 1, C: 2, D: 3.0, E: true, F: time.Second}}},
		map[string]interface{}{},
		"b.c.f.a"},
}

func TestFlatten(t *testing.T) {
	for _, tt := range flattentests {
		res, err := Flatten(tt.Struct, tt.Start)
		assert.NoError(t, err)
		for k, v := range tt.Map {
			assert.Equal(t, v, res[k], fmt.Sprintf("%s: %v", k, v))
		}
		for k, v := range res {
			assert.Equal(t, v, tt.Map[k], fmt.Sprintf("%s: %v", k, v))
		}
	}
}
