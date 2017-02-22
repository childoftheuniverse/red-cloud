package storage

import (
	"encoding/base64"
	"fmt"
	"time"
)

/*
SSTableLevel describes the level this sstable exists as. It is mostly used
to find the appropriate file name.
*/
type SSTableLevel int

const (
	/*
		SSTableLevelMAJOR indicates the sstable is a major sstable, holding
		all stable sstable data from the point of the last major compaction.
	*/
	SSTableLevelMAJOR = iota

	/*
		SSTableLevelMINOR indicates a minor sstable, holding all changes which
		have undergone logsorting since the last minor compaction.
	*/
	SSTableLevelMINOR

	/*
		SSTableLevelJOURNAL indicates a simple journal file, sorted by update
		time.
	*/
	SSTableLevelJOURNAL
)

/*
levelStringMap is used to map sstable level descriptions to their text
representations.
*/
var levelStringMap = map[SSTableLevel]string{
	SSTableLevelMAJOR:   "major",
	SSTableLevelMINOR:   "minor",
	SSTableLevelJOURNAL: "journal",
}

/*
SSTableLevel2String expresses the name of the SSTableLevel as text.
*/
func SSTableLevel2String(level SSTableLevel) string {
	var desc string
	var ok bool

	if desc, ok = levelStringMap[level]; ok {
		return desc
	}

	return "unknown"
}

/*
MakePathPrefix produces an appropriate path prefix for a file containing
data pertaining to the specified instance/table/column family/endkey tuple.
*/
func MakePathPrefix(instance, table, cf string, endkey []byte) string {
	return fmt.Sprintf("red-cloud/%s/%s.%s.%s",
		instance, table, cf, base64.URLEncoding.EncodeToString(endkey))
}

/*
MakePath produces the full path name for a file containing data pertaining
to the specified instance/table/column family/endkey/date/type tuple.
*/
func MakePath(instance, table, cf string, endkey []byte, when time.Time, level SSTableLevel) string {
	return fmt.Sprintf("%s.%s.%s", MakePathPrefix(instance, table, cf, endkey),
		when.Format("20060102-150405.0000000"), SSTableLevel2String(level))
}
