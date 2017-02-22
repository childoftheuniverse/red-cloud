package common

import (
	"fmt"
)

/*
Binary prefixes: KB (1024B), MB (1024KB), GB (1024MB), TB (1024GB), PB (1024TB)
and EB (1024PB).
*/
const (
	_         = iota // ignore first value by assigning to blank identifier
	KB uint64 = 1 << (10 * iota)
	MB
	GB
	TB
	PB
	EB
)

/*
PrettyBytes returns an approximated, human-readable representation of the
specified number of bytes.
*/
func PrettyBytes(b uint64) string {
	switch {
	case b >= 1000000000000000000:
		return fmt.Sprintf("%.2fEB", float64(b)/float64(EB))
	case b >= 1000000000000000:
		return fmt.Sprintf("%.2fPB", float64(b)/float64(PB))
	case b >= 1000000000000:
		return fmt.Sprintf("%.2fTB", float64(b)/float64(TB))
	case b >= 1000000000:
		return fmt.Sprintf("%.2fGB", float64(b)/float64(GB))
	case b >= 1000000:
		return fmt.Sprintf("%.2fMB", float64(b)/float64(MB))
	case b >= 1000:
		return fmt.Sprintf("%.2fKB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%dB", b)
	}
}
