package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"context"
	"github.com/childoftheuniverse/etcd-discovery/exporter"
	"github.com/childoftheuniverse/red-cloud"
	etcd "go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

func main() {
	var exportedService *exporter.ServiceExporter
	var statusServer *StatusWebService
	var etcdServers, instanceName, thisHost, hostName string
	var nodeDiscoveryStrategy string
	var nodeRegistry *DataNodeRegistry
	var admin *AdminService
	var etcdClient *etcd.Client
	var srv *grpc.Server
	var maxMsgSize int
	var exportPort bool
	var startupWait time.Duration
	var startupDeadline time.Time
	var bootstrapCSSPath, bootstrapCSSHash string
	var l net.Listener
	var etcdTTL int64
	var port, statusPort int
	var err error

	thisHost, err = os.Hostname()
	if err != nil {
		log.Printf("Warning: cannot determine host name: %s", err)
	}

	flag.StringVar(&etcdServers, "etcd-servers", "",
		"etcd URL to get a server list from")
	flag.StringVar(&instanceName, "instance", "test",
		"Name of the red-cloud instance being run")
	flag.StringVar(&hostName, "host", thisHost,
		"Externally available name of this host")
	flag.IntVar(&port, "port", 0, "Specific port to bind to, if possible")
	flag.IntVar(&statusPort, "status-port", 0,
		"Port to use for exporting status information")
	flag.BoolVar(&exportPort, "export-port", false,
		"Export the RPC port to etcd as an exported service")
	flag.Int64Var(&etcdTTL, "etcd-ttl", 30,
		"Number of seconds the etcd exported service will stick around "+
			"without being renewed. Must be greater than or equal to 5")
	flag.IntVar(&maxMsgSize, "max-msg-size", 512*1048576,
		"Maximum size of RPC messages received")
	flag.StringVar(&nodeDiscoveryStrategy, "node-discovery-strategy",
		"etcd-exported",
		"Strategy to use for discovering new etcd nodes. Must be one of the "+
			"supported strategies (etcd-exported)")
	flag.StringVar(&bootstrapCSSPath, "bootstrap-css",
		"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css",
		"URL to import the bootstrap CSS from")
	flag.StringVar(&bootstrapCSSHash, "bootstrap-css-hash",
		"sha384-BVYiiSIFeK1dGmJRAkycuHAHRg32OmUcww7on3RYdg4Va+PmSTsz/K68vbdEjh4u",
		"Hash to verify the integrity of the bootstrap CSS hash")
	flag.DurationVar(&startupWait, "startup-deadline", 20*time.Second,
		"Number of seconds to wait on remote operations during startup")
	flag.Parse()

	startupDeadline = time.Now().Add(startupWait)

	// Connect to etcd.
	if etcdClient, err = etcd.NewFromURL(etcdServers); err != nil {
		log.Fatalf("Cannot connect to etcd %s: %s", etcdServers, err)
	}

	nodeRegistry = NewDataNodeRegistry(etcdClient, instanceName)
	admin = NewAdminService(etcdClient, nodeRegistry)
	statusServer = NewStatusWebService(
		bootstrapCSSPath, bootstrapCSSHash, nodeRegistry)
	http.Handle("/", statusServer)

	if exportPort {
		var ctx context.Context
		var listenTo, listenToStatus string

		if port > 0 {
			listenTo = net.JoinHostPort(hostName, strconv.Itoa(port))
		} else {
			listenTo = hostName
		}
		if statusPort > 0 {
			listenToStatus = net.JoinHostPort(
				hostName, strconv.Itoa(statusPort))
		} else {
			listenToStatus = hostName
		}

		ctx, _ = context.WithDeadline(context.Background(), startupDeadline)
		if exportedService, err = exporter.NewExporterFromClient(
			ctx, etcdClient, etcdClient, etcdTTL); err != nil {
			log.Fatal("Cannot create etcd port exporter: ", err)
		}

		go exportedService.ListenAndServeNamedHTTP(
			ctx, "red-cloud/"+instanceName+"/master-status",
			listenToStatus, nil)

		if l, err = exportedService.NewExportedPort(
			ctx, "tcp", listenTo,
			"red-cloud/"+instanceName+"/master"); err != nil {
			log.Fatal("Cannot establish exported TCP listener on ", hostName,
				":", port, ": ", err)
		}
	} else {
		go http.ListenAndServe(net.JoinHostPort(
			hostName, strconv.Itoa(statusPort)), nil)

		// Set up local RPC server to listen on.
		if l, err = net.Listen(
			"tcp", net.JoinHostPort(hostName, strconv.Itoa(port))); err != nil {
			log.Fatalf("Cannot establish TCP listener on %s:%d: %s", hostName,
				port, err)
		}
	}

	if nodeDiscoveryStrategy == "etcd-exported" {
		NewEtcdExportedNodeDiscoveryStrategy(
			nodeRegistry, etcdClient, instanceName)
	} else {
		log.Print("Unknown node discovery strategy ", nodeDiscoveryStrategy,
			", this will never find any data nodes")
	}

	srv = grpc.NewServer(grpc.MaxMsgSize(maxMsgSize))
	redcloud.RegisterAdminServiceServer(srv, admin)
	if err = srv.Serve(l); err != nil {
		log.Fatalf("Error listening to %s: %s", l.Addr(), err)
	}
}
