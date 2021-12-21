package utils

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var NotValidInputError = errors.New("Not a valid input")

func Flatten(nested interface{}, start string) (map[string]interface{}, error) {
	flatmap := make(map[string]interface{})

	err := flatten(true, flatmap, nested, "", start)
	if err != nil {
		return nil, err
	}

	return flatmap, nil
}

func getKind(val reflect.Value) reflect.Kind {
	kind := val.Kind()

	switch {
	case kind >= reflect.Int && kind <= reflect.Int64:
		return reflect.Int
	case kind >= reflect.Uint && kind <= reflect.Uint64:
		return reflect.Uint
	case kind >= reflect.Float32 && kind <= reflect.Float64:
		return reflect.Float32
	default:
		return kind
	}
}

func isEmptyValue(v reflect.Value) bool {
	switch getKind(v) {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

func flatten(top bool, flatMap map[string]interface{}, nested interface{}, prefix, start string) error {
	assign := func(newKey string, v interface{}, start string) error {
		if dv, ok := v.(time.Duration); ok {
			if start != "" {
				return nil
			}
			flatMap[newKey] = dv.String()
			return nil
		}

		val := reflect.ValueOf(v)
		kind := getKind(val)
		switch kind {
		case reflect.Struct, reflect.Map, reflect.Slice, reflect.Ptr:
			if err := flatten(false, flatMap, v, newKey, start); err != nil {
				return err
			}
		default:
			if start != "" {
				return nil
			}
			flatMap[newKey] = v
		}

		return nil
	}

	var nestedVal reflect.Value
	if nested != nil {
		nestedVal = reflect.ValueOf(nested)
		if nestedVal.Kind() == reflect.Ptr {
			if nestedVal.IsNil() {
				nested = nil
			} else {
				nestedVal = nestedVal.Elem()
			}
		}
	}

	if nested == nil {
		return NotValidInputError
	}
	if !nestedVal.IsValid() {
		return NotValidInputError
	}
	preStart, newStart := dekey(start)
	kind := getKind(nestedVal)
	switch kind {
	case reflect.Struct:
		typ := nestedVal.Type()
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			tagValue := f.Tag.Get("json")
			v := nestedVal.Field(i)
			keyName := f.Name
			if index := strings.Index(tagValue, ","); index != -1 {
				if tagValue[:index] == "-" {
					continue
				}
				if strings.Contains(tagValue[index+1:], "omitempty") && isEmptyValue(v) {
					continue
				}
				keyName = tagValue[:index]
			} else if len(tagValue) > 0 {
				if tagValue == "-" {
					continue
				}
				keyName = tagValue
			}
			if start != "" && preStart != keyName {
				continue
			}
			newKey := enkey(top, prefix, keyName)
			assign(newKey, v.Interface(), newStart)
		}
	case reflect.Map:
		for _, k := range nestedVal.MapKeys() {
			keyName := k.String()
			if start != "" && preStart != keyName {
				continue
			}
			v := nestedVal.MapIndex(k)
			newKey := enkey(top, prefix, keyName)
			assign(newKey, v.Interface(), newStart)
		}
	case reflect.Slice:
		if start != "" {
			break
		}
		for i := 0; i < nestedVal.Len(); i++ {
			v := nestedVal.Index(i)
			newKey := enkey(top, prefix, strconv.Itoa(i))
			assign(newKey, v.Interface(), "")
		}
	default:
		return NotValidInputError
	}

	return nil
}

func dekey(key string) (string, string) {
	if index := strings.Index(key, "."); index != -1 {
		return key[:index], key[index+1:]
	}
	return key, ""
}

func enkey(top bool, prefix, subkey string) string {
	key := prefix

	if top {
		key += subkey
	} else {
		key += "." + subkey
	}

	return key
}
