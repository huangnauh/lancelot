package utils

import (
	"reflect"
	"testing"
)

type SplitTest struct {
	s string
	a []string
}

var splittests = []SplitTest{
	{``, []string{}},
	{`.`, []string{}},
	{`a.`, []string{"a"}},
	{`.a`, []string{"a"}},
	{`a.a.a`, []string{"a", "a", "a"}},
	{`.a.b.c.`, []string{"a", "b", "c"}},
	{`a..a`, []string{"a", "a"}},
	{`a\.a`, []string{"a\\.a"}},
	{`a\a.a`, []string{"a\\a", "a"}},
	{`a\..a`, []string{"a\\.", "a"}},
	{`a\...a`, []string{"a\\.", "a"}},
	{`a\.bc..a`, []string{"a\\.bc", "a"}},
	{`a\\.bc..a`, []string{"a\\\\.bc", "a"}},
}

func TestSplit(t *testing.T) {
	for _, tt := range splittests {
		b := SplitDot(tt.s)
		if !reflect.DeepEqual(tt.a, b) {
			t.Errorf("SplitDot(%q) = %v; want %v", tt.s, b, tt.a)
		}
	}
}
