package common

import (
	"time"
)

/*
TimeSince returns a string representation of the time which has passed
since a given point in time.
*/
func TimeSince(t time.Time) string {
	return time.Now().Truncate(time.Second).Sub(
		t.Truncate(time.Second)).String()
}
