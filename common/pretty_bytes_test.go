package common

import (
	"testing"
)

func TestFormatToString(t *testing.T) {
	var testdata = map[uint64]string{
		1:                "1B",
		999:              "999B",
		1020:             "1.00KB",
		1048576:          "1.00MB",
		190124234:        "181.32MB",
		4280540241242452: "3.80PB",
	}

	for k, v := range testdata {
		if PrettyBytes(k) != v {
			t.Errorf("Mismatched format (expected \"%s\", got \"%s\")",
				v, PrettyBytes(k))
		}
	}
}

func BenchmarkFormatToString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		PrettyBytes(uint64(i))
	}
}
