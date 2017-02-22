package client

import (
	"errors"
	"net/url"
	"strings"
)

/*
SplitTablePath splits the instance and table name (if specified) out of a
red-cloud URL.
*/
func SplitTablePath(path string) (instance string, table string, err error) {
	var u *url.URL

	if u, err = url.Parse(path); err != nil {
		return
	}

	if u.Scheme != "red-cloud" {
		err = errors.New("Unsupported schema: " + u.Scheme)
		return
	}

	instance = u.Host

	if len(u.Path) > 1 {
		var tableParts = strings.Split(u.Path[1:], "/")
		table = tableParts[0]
	}

	return
}
