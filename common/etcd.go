package common

import (
	"fmt"
)

/*
EtcdTableConfigPath computes the absolute path of the etcd-based
configuration for the specified table.
*/
func EtcdTableConfigPath(instance, table string) string {
	return fmt.Sprintf("/red-cloud/%s/tables/%s", instance, table)
}

/*
EtcdMasterPrefix computes the prefix path of the etcd-based file containing
the host:port pair for the master of the specified red-cloud instance.
*/
func EtcdMasterPrefix(instance string) string {
	return fmt.Sprintf("/ns/service/red-cloud/%s/master/", instance)
}
