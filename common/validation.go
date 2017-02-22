package common

import (
	"regexp"
)

var tablenameRe = regexp.MustCompile(`^[[:alnum:]][[:alnum:]_\-\.]*$`)

/*
IsValidTableName checks whether the given string is a valid table name.
It essentially just checks the name against a list of acceptable characters.
*/
func IsValidTableName(name string) bool {
	return tablenameRe.MatchString(name)
}
