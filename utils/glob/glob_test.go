package glob

import "testing"

type globTest struct {
	pattern string
	name    string
	matched bool
}

var testGlob = []globTest{
	{"*", "", true},
	{"a", "b", false},
	{"a", "a", true},
	{"ab", "ab", true},
	{"ab", "cd", false},

	{"*.b", "a.b", true},
	{"*.b", "a.c", false},
	{"*.b", "ab", false},
	{"*.b", "ab.b", true},

	{"a.*", "a.", true},
	{"a.*", "a.b", true},
	{"a.*", "a", false},

	{"a.*.c", "a.b.d", false},
	{"a.*.c", "a.b.d", false},
	{"a.*.c", "a.b.c", true},
	{"a.*.c", "a.a.c", true},
	{"a.*.c", "a.bc.c", true},
	{"a.*.c", "a.b/c/d/e.c", true},

	{"a.*.c.*.e", "a.b.c.d.e", true},
	{"a.*.c.*.e", "a.b.c.d.d", false},
	{"a.*.c.*.e", "a.b.c.e", false},
	{"a.*.c.*.e", "aa.bb.cc.dd.ee", false},
	{"a.*.c.*.e", "a.bb.c.dd.e", true},
	{"a*c*e", "a.b.c.d.e", true},
	{"a***e", "abcde", true},

	{"a.**", "a.b.c", true},
	{"a.?.c", "a.b.c", true},
	{"a.?.?", "a.b.c", true},
	{"?at", "cat", true},
	{"*", "abc", true},
	{"*ä", "åä", true},
	{`\*`, "*", true},
	{"**", "a.b.c", true},

	{"?at", "at", false},
	{"*is", "this is a test", false},
	{"*no*", "this is a test", false},
	{"[^a]*", "this is a test", true},
	{"[^t]*", "this is a test", false},
	{"*test", "this is a test", true},
	{"this*", "this is a test", true},
	{"*is *", "this is a test", true},
	{"*is*a*", "this is a test", true},
	{"**test**", "this is a test", true},
	{"**is**a***test*", "this is a test", true},
	{"*abc", "abcabc", true},
	{"**abc", "abcabc", true},
	{"???", "abc", true},
	{"?*?", "abc", true},
	{"?*?", "ac", true},
	{"sta", "start", false},
	{"sta*", "start", true},
	{"sta?", "start", false},
	{"sta?n", "start", false},

	{"[a-z]at", "cat", true},
	{"[a-z]at", "bat", true},
	{"[a-z]at", "catz", false},
}

func TestGlob(t *testing.T) {
	for _, test := range testGlob {
		matched, err := Match(test.pattern, test.name)
		if err != nil {
			t.Errorf("%s: %s", test.pattern, err)
			continue
		}
		if matched != test.matched {
			t.Errorf("Glob(%q, %q) = %v, want %v", test.pattern, test.name, matched, test.matched)
		}
	}
}
