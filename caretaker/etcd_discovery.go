package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"context"
	"github.com/childoftheuniverse/etcd-discovery"
	etcd "go.etcd.io/etcd/clientv3"
)

/*
EtcdExportedNodeDiscoveryStrategy uses etcd to discover new and disappearing
data nodes. To use this, data nodes must use etcd exported ports.
*/
type EtcdExportedNodeDiscoveryStrategy struct {
	path       string
	dnr        *DataNodeRegistry
	etcdClient *etcd.Client
}

/*
NewEtcdExportedNodeDiscoveryStrategy creates a new
EtcdExportedNodeDiscoveryStrategy discovering nodes using the given etcd
client and feeding them into the specified DataNodeRegistry.
*/
func NewEtcdExportedNodeDiscoveryStrategy(dnr *DataNodeRegistry,
	etcdClient *etcd.Client, instance string) *EtcdExportedNodeDiscoveryStrategy {
	var strategy = &EtcdExportedNodeDiscoveryStrategy{
		path: fmt.Sprintf(
			"/ns/service/red-cloud/%s/data-node/", instance),
		dnr:        dnr,
		etcdClient: etcdClient,
	}
	discovery.MonitorExportedService(etcdClient, etcdClient, fmt.Sprintf(
		"/ns/service/red-cloud/%s/data-node/", instance), strategy)
	return strategy
}

/*
ReportChange indicates that a new data node has appeared or an old one has been
updated or disappeared.
*/
func (s *EtcdExportedNodeDiscoveryStrategy) ReportChange(
	notification *discovery.ExportedServiceUpdateNotification) {
	var err error

	if notification.Update == discovery.ExportedServiceUpdateNotification_NEW {
		var dest = net.JoinHostPort(
			notification.UpdatedData.Address,
			strconv.Itoa(int(notification.UpdatedData.Port)))
		var ctx context.Context
		ctx, _ = context.WithTimeout(context.Background(),
			20*time.Second)
		log.Print("Adding node ", notification.Path, " (", dest, ")")
		err = s.dnr.Add(ctx, notification.UpdatedData.Protocol, dest)
		if err != nil {
			log.Print("Error adding node ", dest, ": ", err)
		}
	} else if notification.Update == discovery.ExportedServiceUpdateNotification_DELETED {
		var dest = net.JoinHostPort(
			notification.UpdatedData.Address,
			strconv.Itoa(int(notification.UpdatedData.Port)))

		log.Print("Removing node ", notification.Path, " (", dest, ")")
		if err = s.dnr.Remove(dest); err != nil {
			log.Print("Error removing node ", dest, ": ", err)
		}
	}
}

/*
ReportError logs non-fatal errors to the console.
*/
func (s *EtcdExportedNodeDiscoveryStrategy) ReportError(err error) {
	log.Print("Error discovering node changes from etcd: ", err)
}

/*
ReportFatal logs fatal errors to the console and ends the program with an error
exit code.
*/
func (s *EtcdExportedNodeDiscoveryStrategy) ReportFatal(err error) {
	log.Fatal("Fatal error from etcd discovery: ", err)
}

/*
ReportCancelled reports that the watcher object has ceased to be active.
*/
func (s *EtcdExportedNodeDiscoveryStrategy) ReportCancelled() {
	/* TODO: probably do something here? */
}
