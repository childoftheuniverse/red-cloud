package main

import (
	"crypto/tls"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"context"
	"github.com/childoftheuniverse/etcd-discovery/exporter"
	"github.com/childoftheuniverse/filesystem"
	_ "github.com/childoftheuniverse/filesystem-file"
	rados "github.com/childoftheuniverse/filesystem-rados"
	"github.com/childoftheuniverse/red-cloud"
	"github.com/childoftheuniverse/tlsconfig"
	etcd "go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	var portExporter *exporter.ServiceExporter
	var etcdServers, instanceName, thisHost, hostName string
	var radosConfig, radosCluster, radosUser string
	var wantRados bool
	var ourAddr, statusAddr *net.TCPAddr
	var rangeRegistry *ServingRangeRegistry
	var statusServer *StatusWebService
	var dns *DataNodeService
	var ms *DataNodeMetadataService
	var etcdClient *etcd.Client
	var etcdTimeout time.Duration
	var etcdConfig etcd.Config
	var srv *grpc.Server
	var maxMsgSize int

	var privateKeyPath string
	var certificatePath string
	var caPath string
	var tlsConfig *tls.Config
	var grpcOptions []grpc.ServerOption

	var l, sl net.Listener
	var startupWait time.Duration
	var startupDeadline time.Time
	var bootstrapCSSPath, bootstrapCSSHash string
	var exportPort bool
	var etcdTTL int64
	var port, statusPort int
	var err error

	if thisHost, err = os.Hostname(); err != nil {
		log.Printf("Warning: cannot determine host name: %s", err)
	}

	flag.StringVar(&etcdServers, "etcd-servers", "",
		"List of etcd servers to connect to")
	flag.DurationVar(&etcdTimeout, "etcd-timeout", 30*time.Second,
		"Timeout for etcd connection")
	flag.StringVar(&instanceName, "instance", "test",
		"Name of the red-cloud instance being run")
	flag.StringVar(&hostName, "host", thisHost,
		"Externally available name of this host")
	flag.IntVar(&port, "port", 0, "Specific port to bind to, if possible")
	flag.IntVar(&statusPort, "status-port", 0,
		"Port to use for exporting status information")
	flag.IntVar(&maxMsgSize, "max-msg-size", 512*1048576,
		"Maximum size of RPC messages received")
	flag.BoolVar(&exportPort, "export-port", false,
		"Export the RPC port to etcd as an exported service")
	flag.Int64Var(&etcdTTL, "etcd-ttl", 30,
		"Number of seconds the etcd exported service will stick around "+
			"without being renewed. Must be greater than or equal to 5")
	flag.DurationVar(&startupWait, "startup-deadline", 20*time.Second,
		"Number of seconds to wait on remote operations during startup")
	flag.StringVar(&bootstrapCSSPath, "bootstrap-css",
		"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css",
		"URL to import the bootstrap CSS from")
	flag.StringVar(&bootstrapCSSHash, "bootstrap-css-hash",
		"sha384-BVYiiSIFeK1dGmJRAkycuHAHRg32OmUcww7on3RYdg4Va+PmSTsz/K68vbdEjh4u",
		"Hash to verify the integrity of the bootstrap CSS hash")

	// TLS authentication settings.
	flag.StringVar(&privateKeyPath, "private-key", "",
		"Path to the TLS private key file (PEM format). Empty disables TLS.")
	flag.StringVar(&certificatePath, "server-certificate", "",
		"Path to the TLS server certificate file (PEM format). Empty disables TLS.")
	flag.StringVar(&caPath, "ca", "",
		"Path to the TLS CA certificate file (PEM format). "+
			"Empty disables client authentication.")

	// Rados specific configuration flags.
	flag.StringVar(&radosConfig, "rados-config", "",
		"Path to a Rados client configuration file. If unset, the default file "+
			"will be read")
	flag.StringVar(&radosCluster, "rados-cluster", "",
		"Name of the Rados cluster in the configuration file to use")
	flag.StringVar(&radosUser, "rados-user", "",
		"Name of the user to use for accessing Rados")
	flag.BoolVar(&wantRados, "want-rados", false,
		"Fail startup if Rados cannot be configured")
	flag.Parse()

	startupDeadline = time.Now().Add(startupWait)

	if certificatePath != "" && privateKeyPath != "" && caPath != "" {
		if tlsConfig, err = tlsconfig.TLSConfigWithRootAndClientCAAndCert(
			caPath, caPath, certificatePath, privateKeyPath); err != nil {
			log.Fatal("Unable to initialize TLS context: ", err)
		}
	}

	etcdConfig.Endpoints = strings.Split(etcdServers, ",")
	etcdConfig.DialTimeout = etcdTimeout

	if tlsConfig != nil {
		etcdConfig.TLS = tlsConfig
	}

	// Connect to etcd.
	if etcdClient, err = etcd.New(etcdConfig); err != nil {
		log.Fatalf("Cannot connect to etcd %s: %s", etcdServers, err)
	}
	defer etcdClient.Close()

	// Check for a request to set up Rados in a non-default way.
	if radosConfig != "" {
		if radosUser != "" {
			if radosCluster != "" {
				err = rados.RegisterRadosConfigWithClusterAndUser(
					radosConfig, radosCluster, radosUser)
			} else {
				err = rados.RegisterRadosConfigWithUser(radosConfig, radosUser)
			}
		} else {
			err = rados.RegisterRadosConfig(radosConfig)
		}

		if err != nil {
			log.Print("Error initializing Rados client: ", err)
		}
	}

	if wantRados && !filesystem.HasImplementation("rados") {
		log.Fatal("Rados requested but Rados initialization failed")
	}

	// Set up local RPC server to listen on.
	if exportPort {
		var ctx context.Context
		var listenTo string

		ctx, _ = context.WithDeadline(context.Background(), startupDeadline)
		if portExporter, err = exporter.NewExporterFromClient(
			ctx, etcdClient, etcdClient, etcdTTL); err != nil {
			log.Fatal("Cannot create etcd port exporter: ", err)
		}

		if statusPort > 0 {
			listenTo = net.JoinHostPort(
				hostName, strconv.Itoa(statusPort))
		} else {
			listenTo = hostName
		}

		if sl, err = portExporter.NewExportedPort(ctx, "tcp", listenTo,
			"red-cloud/"+instanceName+"/data-node-status"); err != nil {
			log.Fatalf("Cannot establish exported TCP listener on %s:%d: %s",
				hostName, statusPort, err)
		}
		statusAddr = sl.Addr().(*net.TCPAddr)

		if port > 0 {
			listenTo = net.JoinHostPort(hostName, strconv.Itoa(port))
		} else {
			listenTo = hostName
		}

		if l, err = portExporter.NewExportedPort(ctx, "tcp", listenTo,
			"red-cloud/"+instanceName+"/data-node"); err != nil {
			log.Fatalf("Cannot establish exported TCP listener on %s:%d: %s",
				hostName, port, err)
		}
		ourAddr = l.Addr().(*net.TCPAddr)
	} else {
		if statusAddr, err = net.ResolveTCPAddr(
			"tcp",
			net.JoinHostPort(hostName, strconv.Itoa(statusPort))); err != nil {
			log.Fatalf("Cannot determine what to do with %s:%d: %s",
				hostName, statusPort, err)
		}
		if sl, err = net.ListenTCP("tcp", statusAddr); err != nil {
			log.Fatalf("Cannot establish TCP listener on %s:%d: %s", hostName,
				statusPort, err)
		}
		statusAddr = sl.Addr().(*net.TCPAddr)

		if ourAddr, err = net.ResolveTCPAddr(
			"tcp", net.JoinHostPort(hostName, strconv.Itoa(port))); err != nil {
			log.Fatalf("Cannot determine what to do with %s:%d: %s",
				hostName, port, err)
		}

		if l, err = net.ListenTCP("tcp", ourAddr); err != nil {
			log.Fatalf("Cannot establish TCP listener on %s:%d: %s", hostName,
				port, err)
		}
		ourAddr = l.Addr().(*net.TCPAddr)
	}

	rangeRegistry = NewServingRangeRegistry(
		instanceName, ourAddr.IP.String(), uint16(ourAddr.Port), etcdClient)
	statusServer = NewStatusWebService(
		bootstrapCSSPath, bootstrapCSSHash, rangeRegistry)
	ms = NewDataNodeMetadataService(rangeRegistry, uint16(statusAddr.Port))
	dns = NewDataNodeService(rangeRegistry)

	http.Handle("/", statusServer)
	go http.Serve(sl, nil)

	grpcOptions = append(grpcOptions, grpc.MaxMsgSize(maxMsgSize))

	if tlsConfig != nil {
		grpcOptions = append(grpcOptions, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	srv = grpc.NewServer(grpcOptions...)
	redcloud.RegisterDataNodeMetadataServiceServer(srv, ms)
	redcloud.RegisterDataNodeServiceServer(srv, dns)
	if err = srv.Serve(l); err != nil {
		log.Fatalf("Error listening to %s: %s", l.Addr(), err)
	}
}
