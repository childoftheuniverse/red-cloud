package common

import (
	"bytes"
	"fmt"
)

/*
KeyRange is an object representing a range of keys going from start_key to
end_key, whereas start_key is part of the range and end_key is not.
If end_key is empty, it is considered to be the end of the key range.
*/
type KeyRange struct {
	StartKey, EndKey []byte
}

/*
NewKeyRange creates a new KeyRange object spanning the range from startKey to
endKey. Returns nil if startKey > endKey (with a non-empty endKey).

An empty startKey means the beginning of the key space, an empty endKey means
the end.
*/
func NewKeyRange(startKey, endKey []byte) *KeyRange {
	if len(endKey) > 0 && bytes.Compare(startKey, endKey) > 0 {
		return nil
	}

	return &KeyRange{
		StartKey: startKey,
		EndKey:   endKey,
	}
}

/*
Contains determines whether the specified key is part of this KeyRange.
*/
func (kr *KeyRange) Contains(key []byte) bool {
	return bytes.Compare(key, kr.StartKey) >= 0 && (len(kr.EndKey) == 0 ||
		bytes.Compare(key, kr.EndKey) < 0 ||
		(bytes.Compare(kr.StartKey, kr.EndKey) == 0 &&
			bytes.Compare(kr.StartKey, key) == 0))
}

/*
ContainsRange determines whether there is overlap between the two KeyRanges.
*/
func (kr *KeyRange) ContainsRange(other *KeyRange) bool {
	return ((len(kr.EndKey) == 0 || bytes.Compare(other.StartKey, kr.EndKey) < 0) &&
		(len(other.EndKey) == 0 || bytes.Compare(other.EndKey, kr.StartKey) > 0))
}

/*
ContainsRangeFully determines whether a given range is completely contained
in the other range.
*/
func (kr *KeyRange) ContainsRangeFully(other *KeyRange) bool {
	return (bytes.Compare(kr.StartKey, other.StartKey) <= 0 &&
		bytes.Compare(other.StartKey, other.EndKey) <= 0 &&
		(len(kr.EndKey) == 0 ||
			(len(other.EndKey) > 0 &&
				bytes.Compare(other.EndKey, kr.EndKey) <= 0)))
}

/*
Equals determines whether two key ranges are identical.
*/
func (kr *KeyRange) Equals(other *KeyRange) bool {
	return (bytes.Compare(kr.StartKey, other.StartKey) == 0 &&
		bytes.Compare(kr.EndKey, other.EndKey) == 0)
}

/*
String returns a string representation of the key range.
*/
func (kr *KeyRange) String() string {
	return fmt.Sprintf("KeyRange(%v, %v)", kr.StartKey, kr.EndKey)
}
