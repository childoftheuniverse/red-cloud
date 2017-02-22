package common

import (
	"testing"
)

type keyRangeHolder struct {
	kr  *KeyRange
	key []byte
}

type twoKeyRangeHolder struct {
	kr1, kr2 *KeyRange
}

var containedRanges = map[*KeyRange][]byte{
	NewKeyRange([]byte{4, 1, 2, 7}, []byte{4, 1, 2, 8}): []byte{4, 1, 2, 7, 2, 5},
	NewKeyRange([]byte{4, 1, 2, 7}, []byte{4, 1, 2, 8}): []byte{4, 1, 2, 7},
	NewKeyRange([]byte{4, 1, 2, 7}, []byte{4, 1, 2, 7}): []byte{4, 1, 2, 7},
	NewKeyRange([]byte{0xA0}, []byte{}):                 []byte{0xF0},
	NewKeyRange([]byte{}, []byte{4, 1, 2, 7}):           []byte{4},
	NewKeyRange([]byte{}, []byte{}):                     []byte{0xA7, 0xFD, 0x39, 0xD6},
	NewKeyRange([]byte{}, []byte{}):                     []byte{},
}

var overlapingRanges = map[*KeyRange]*KeyRange{
	NewKeyRange([]byte{}, []byte{}):         NewKeyRange([]byte{}, []byte{}),
	NewKeyRange([]byte{4, 1}, []byte{4, 2}): NewKeyRange([]byte{4, 1, 2}, []byte{4, 1, 3}),
	NewKeyRange([]byte{4, 1}, []byte{4, 3}): NewKeyRange([]byte{4, 2}, []byte{4, 4}),
	NewKeyRange([]byte{4, 2}, []byte{4, 4}): NewKeyRange([]byte{4, 1}, []byte{4, 3}),
	NewKeyRange([]byte{4, 2}, []byte{4, 4}): NewKeyRange([]byte{4}, []byte{5}),
}

var outsideRanges = map[*KeyRange][]byte{
	NewKeyRange([]byte{4, 1, 2, 7}, []byte{4, 1, 2, 8}): []byte{4, 1, 2, 6, 2, 5},
	NewKeyRange([]byte{4, 1, 2, 7}, []byte{4, 1, 2, 8}): []byte{4, 1, 2, 8, 2, 5},
	NewKeyRange([]byte{4, 1, 2, 7}, []byte{4, 1, 2, 8}): []byte{4, 1, 2, 8},
	NewKeyRange([]byte{4, 1, 2, 7}, []byte{4, 1, 2, 7}): []byte{4, 1, 2, 6},
	NewKeyRange([]byte{4, 1, 2, 7}, []byte{4, 1, 2, 7}): []byte{4, 1, 2, 8},
	NewKeyRange([]byte{0xA0}, []byte{}):                 []byte{0x1F},
}

var nonOverlapingRanges = map[*KeyRange]*KeyRange{
	NewKeyRange([]byte{0x90}, []byte{0xA0}): NewKeyRange([]byte{0xB0}, []byte{0xC0}),
	NewKeyRange([]byte{}, []byte{0xA0}):     NewKeyRange([]byte{0xA0}, []byte{}),
}

var fullyContainedRanges = map[*KeyRange]*KeyRange{
	NewKeyRange([]byte{}, []byte{}):                     NewKeyRange([]byte{}, []byte{}),
	NewKeyRange([]byte{}, []byte{}):                     NewKeyRange([]byte{0x20}, []byte{0x20, 0x22}),
	NewKeyRange([]byte{0xA0, 0x22}, []byte{0xB2, 0x3A}): NewKeyRange([]byte{0xA0, 0x22}, []byte{0xB2, 0x3A}),
	NewKeyRange([]byte{0xA0, 0x22}, []byte{0xB2, 0x3A}): NewKeyRange([]byte{0xA0, 0x22}, []byte{0xA0, 0x3A}),
	NewKeyRange([]byte{0xA0, 0x22}, []byte{0xB2, 0x3A}): NewKeyRange([]byte{0xB2, 0x22}, []byte{0xB2, 0x3A}),
	NewKeyRange([]byte{0xA0, 0x22}, []byte{0xB2, 0x3A}): NewKeyRange([]byte{0xB0}, []byte{0xB0, 0x3A}),
	NewKeyRange([]byte{0xA0}, []byte{0xB2}):             NewKeyRange([]byte{0xB0}, []byte{0xB0}),
	NewKeyRange([]byte{0xA0}, []byte{0xB2}):             NewKeyRange([]byte{0xA0}, []byte{0xA0}),
	NewKeyRange([]byte{0xA0}, []byte{0xB2}):             NewKeyRange([]byte{0xB2}, []byte{0xB2}),
	NewKeyRange([]byte{0xB2}, []byte{0xB2}):             NewKeyRange([]byte{0xB2}, []byte{0xB2}),
}

var notFullyContainedRanges = map[*KeyRange]*KeyRange{
	NewKeyRange([]byte{0xA0, 0x22}, []byte{0xB2, 0x3A}): NewKeyRange([]byte{0xB0}, []byte{0xB2, 0x3B}),
	NewKeyRange([]byte{0xA0, 0x22}, []byte{0xB2, 0x3A}): NewKeyRange([]byte{0xA0}, []byte{0xB2, 0x3A}),
	NewKeyRange([]byte{0xA0, 0x22}, []byte{0xB2, 0x3A}): NewKeyRange([]byte{0xA0, 0x22}, []byte{0xB2, 0x3A, 0x01}),
	NewKeyRange([]byte{0xA0, 0x22}, []byte{0xB2, 0x3A}): NewKeyRange([]byte{0xA0}, []byte{0xB3}),
}

func TestInvalidKeyRange(t *testing.T) {
	var kr = NewKeyRange([]byte{0xE0}, []byte{0xA0})
	if kr != nil {
		t.Errorf("Invalid key range was accepted as valid: %v", kr)
	}
}

func TestKeyRangeContains(t *testing.T) {
	var kr *KeyRange
	var key []byte

	for kr, key = range containedRanges {
		if !kr.Contains(key) {
			t.Errorf("Key %v is not in %v (but should be)", key, kr)
		}
	}

	for kr, key = range outsideRanges {
		if kr.Contains(key) {
			t.Errorf("Key %v is in %v (but shouldn't be)", key, kr)
		}
	}
}

func TestKeyRangeContainsRange(t *testing.T) {
	var kr1, kr2 *KeyRange

	for kr1, kr2 = range overlapingRanges {
		if !kr1.ContainsRange(kr2) {
			t.Errorf("Key range %v does not contain %v, but it should",
				kr1, kr2)
		}
	}

	for kr1, kr2 = range nonOverlapingRanges {
		if kr1.ContainsRange(kr2) {
			t.Errorf("Key range %v contains %v, but it shouldn't", kr1, kr2)
		}
	}
}

func TestKeyRangeContainsRangeFully(t *testing.T) {
	var kr1, kr2 *KeyRange

	for kr1, kr2 = range fullyContainedRanges {
		if !kr1.ContainsRangeFully(kr2) {
			t.Errorf("Key range %v dpes not contain %v fully, but it should",
				kr1, kr2)
		}
	}

	for kr1, kr2 = range notFullyContainedRanges {
		if kr1.ContainsRangeFully(kr2) {
			t.Errorf("Key range %v contains %v fully, but it shouldn't",
				kr1, kr2)
		}
	}
}

func TestKeyRangeEquals(t *testing.T) {
	var kr1, kr2 *KeyRange

	for kr1, kr2 = range fullyContainedRanges {
		if !kr1.Equals(NewKeyRange(kr1.StartKey, kr1.EndKey)) {
			t.Errorf("Key range %v is not equal to itself", kr1)
		}
		if !kr2.Equals(NewKeyRange(kr2.StartKey, kr2.EndKey)) {
			t.Errorf("Key range %v is not equal to itself", kr2)
		}
	}
}

func BenchmarkKeyRangeContainsTrue(b *testing.B) {
	var kr *KeyRange
	var key []byte
	var ranges []keyRangeHolder
	var i int

	for kr, key = range containedRanges {
		ranges = append(ranges, keyRangeHolder{
			kr:  kr,
			key: key,
		})
	}

	b.ResetTimer()
	for i = 0; i < b.N; i++ {
		ranges[i%len(ranges)].kr.Contains(ranges[i%len(ranges)].key)
	}
}

func BenchmarkKeyRangeContainsFalse(b *testing.B) {
	var kr *KeyRange
	var key []byte
	var ranges []keyRangeHolder
	var i int

	for kr, key = range outsideRanges {
		ranges = append(ranges, keyRangeHolder{
			kr:  kr,
			key: key,
		})
	}

	b.ResetTimer()
	for i = 0; i < b.N; i++ {
		ranges[i%len(ranges)].kr.Contains(ranges[i%len(ranges)].key)
	}
}

func BenchmarkKeyRangeContainsRangeTrue(b *testing.B) {
	var kr1, kr2 *KeyRange
	var ranges []twoKeyRangeHolder
	var i int

	for kr1, kr2 = range overlapingRanges {
		ranges = append(ranges, twoKeyRangeHolder{
			kr1: kr1,
			kr2: kr2,
		})
	}

	b.ResetTimer()
	for i = 0; i < b.N; i++ {
		ranges[i%len(ranges)].kr1.ContainsRange(ranges[i%len(ranges)].kr2)
	}
}

func BenchmarkKeyRangeContainsRangeFalse(b *testing.B) {
	var kr1, kr2 *KeyRange
	var ranges []twoKeyRangeHolder
	var i int

	for kr1, kr2 = range nonOverlapingRanges {
		ranges = append(ranges, twoKeyRangeHolder{
			kr1: kr1,
			kr2: kr2,
		})
	}

	b.ResetTimer()
	for i = 0; i < b.N; i++ {
		ranges[i%len(ranges)].kr1.ContainsRange(ranges[i%len(ranges)].kr2)
	}
}

func BenchmarkKeyRangeContainsRangeFullyTrue(b *testing.B) {
	var kr1, kr2 *KeyRange
	var ranges []twoKeyRangeHolder
	var i int

	for kr1, kr2 = range fullyContainedRanges {
		ranges = append(ranges, twoKeyRangeHolder{
			kr1: kr1,
			kr2: kr2,
		})
	}

	b.ResetTimer()
	for i = 0; i < b.N; i++ {
		ranges[i%len(ranges)].kr1.ContainsRangeFully(ranges[i%len(ranges)].kr2)
	}
}

func BenchmarkKeyRangeContainsRangeFullyFalse(b *testing.B) {
	var kr1, kr2 *KeyRange
	var ranges []twoKeyRangeHolder
	var i int

	for kr1, kr2 = range notFullyContainedRanges {
		ranges = append(ranges, twoKeyRangeHolder{
			kr1: kr1,
			kr2: kr2,
		})
	}

	b.ResetTimer()
	for i = 0; i < b.N; i++ {
		ranges[i%len(ranges)].kr1.ContainsRangeFully(ranges[i%len(ranges)].kr2)
	}
}

func BenchmarkKeyRangeEquals(b *testing.B) {
	var kr1, kr2 *KeyRange
	var ranges []twoKeyRangeHolder
	var i int

	for kr1, kr2 = range overlapingRanges {
		ranges = append(ranges, twoKeyRangeHolder{
			kr1: kr1,
			kr2: kr2,
		})
	}
	for kr1, kr2 = range nonOverlapingRanges {
		ranges = append(ranges, twoKeyRangeHolder{
			kr1: kr1,
			kr2: kr2,
		})
	}
	for kr1, kr2 = range fullyContainedRanges {
		ranges = append(ranges, twoKeyRangeHolder{
			kr1: kr1,
			kr2: kr2,
		})
	}
	for kr1, kr2 = range notFullyContainedRanges {
		ranges = append(ranges, twoKeyRangeHolder{
			kr1: kr1,
			kr2: kr2,
		})
	}

	b.ResetTimer()
	for i = 0; i < b.N; i++ {
		ranges[i%len(ranges)].kr1.Equals(ranges[i%len(ranges)].kr2)
	}
}
