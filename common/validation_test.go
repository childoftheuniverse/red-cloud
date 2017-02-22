package common

import (
	"testing"
)

var validTableNames = []string{
	"foobar",
	"foo-bar",
	"foo_bar",
	"foo.bar",
	"foo.bar23",
	"b",
}
var invalidTableNames = []string{
	"foo^bar",
	"!foobar",
	"foobar:",
	"-xyz-",
	"",
}

func TestIsValidTableName(t *testing.T) {
	var name string

	for _, name = range validTableNames {
		if !IsValidTableName(name) {
			t.Errorf("Name \"%s\" should be a valid table name but is not",
				name)
		}
	}

	for _, name = range invalidTableNames {
		if IsValidTableName(name) {
			t.Errorf("Name \"%s\" should be an invalid table name but is not",
				name)
		}
	}
}

func BenchmarkIsValidTableNameTrue(b *testing.B) {
	var i int

	b.ResetTimer()
	for i = 0; i < b.N; i++ {
		IsValidTableName(validTableNames[i%len(validTableNames)])
	}
}

func BenchmarkIsValidTableNameFalse(b *testing.B) {
	var i int

	b.ResetTimer()
	for i = 0; i < b.N; i++ {
		IsValidTableName(invalidTableNames[i%len(invalidTableNames)])
	}
}
