package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"context"
	"contrib.go.opencensus.io/exporter/zipkin"
	"github.com/childoftheuniverse/etcd-discovery/exporter"
	"github.com/childoftheuniverse/red-cloud"
	"github.com/childoftheuniverse/tlsconfig"
	openzipkin "github.com/openzipkin/zipkin-go"
	openzipkinModel "github.com/openzipkin/zipkin-go/model"
	zipkinReporter "github.com/openzipkin/zipkin-go/reporter"
	zipkinHTTP "github.com/openzipkin/zipkin-go/reporter/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	etcd "go.etcd.io/etcd/clientv3"
	"go.opencensus.io/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	var exportedService *exporter.ServiceExporter
	var statusServer *StatusWebService
	var etcdServers, instanceName, thisHost, hostName string
	var nodeDiscoveryStrategy string
	var nodeRegistry *DataNodeRegistry
	var admin *AdminService
	var etcdClient *etcd.Client
	var etcdTimeout time.Duration
	var etcdConfig etcd.Config
	var etcdTLSConfig *tls.Config
	var etcdCA string
	var srv *grpc.Server
	var maxMsgSize int
	var exportPort bool
	var startupWait time.Duration
	var startupDeadline time.Time
	var bootstrapCSSPath, bootstrapCSSHash string

	var privateKeyPath string
	var certificatePath string
	var caPath string
	var tlsConfig *tls.Config
	var grpcOptions []grpc.ServerOption

	var l net.Listener
	var etcdTTL int64
	var port, statusPort int
	var zipkinEndpoint string
	var err error

	thisHost, err = os.Hostname()
	if err != nil {
		log.Printf("Warning: cannot determine host name: %s", err)
	}

	flag.StringVar(&etcdServers, "etcd-servers", "",
		"Comma separated list of etcd server URLs to connect to")
	flag.DurationVar(&etcdTimeout, "etcd-timeout", 30*time.Second,
		"Timeout for etcd connection")
	flag.StringVar(&zipkinEndpoint, "zipkin-endpoint",
		fmt.Sprintf("%s:9411", thisHost),
		"host:port pair to send Zipkin traces to")
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
	flag.StringVar(&etcdCA, "etcd-ca", "",
		"Use a separate CA for etcd only. If unset, use the value from --ca")
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

	// TLS authentication settings.
	flag.StringVar(&privateKeyPath, "private-key", "",
		"Path to the TLS private key file (PEM format). Empty disables TLS.")
	flag.StringVar(&certificatePath, "server-certificate", "",
		"Path to the TLS server certificate file (PEM format). Empty disables TLS.")
	flag.StringVar(&caPath, "ca", "",
		"Path to the TLS CA certificate file (PEM format). "+
			"Empty disables client authentication.")
	flag.Parse()

	startupDeadline = time.Now().Add(startupWait)

	if certificatePath != "" && privateKeyPath != "" && caPath != "" {
		if tlsConfig, err = tlsconfig.TLSConfigWithRootAndClientCAAndCert(
			caPath, caPath, certificatePath, privateKeyPath); err != nil {
			log.Fatal("Unable to initialize TLS context: ", err)
		}
	}

	if etcdCA != "" {
		if etcdTLSConfig, err = tlsconfig.TLSConfigWithRootCAAndCert(
			etcdCA, certificatePath, privateKeyPath); err != nil {
			log.Fatal("Unable to initialize TLS context for etcd: ", err)
		}
	} else if tlsConfig != nil {
		etcdTLSConfig = tlsConfig
	}

	etcdConfig.Endpoints = strings.Split(etcdServers, ",")
	etcdConfig.DialTimeout = etcdTimeout

	if etcdTLSConfig != nil {
		etcdConfig.TLS = etcdTLSConfig
	}

	// Connect to etcd.
	if etcdClient, err = etcd.New(etcdConfig); err != nil {
		log.Fatalf("Cannot connect to etcd %s: %s", etcdServers, err)
	}
	defer etcdClient.Close()

	nodeRegistry = NewDataNodeRegistry(etcdClient, tlsConfig, instanceName)
	admin = NewAdminService(etcdClient, nodeRegistry)
	statusServer = NewStatusWebService(
		bootstrapCSSPath, bootstrapCSSHash, nodeRegistry)
	http.Handle("/", statusServer)
	http.Handle("/metrics", promhttp.Handler())

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

	grpcOptions = append(grpcOptions, grpc.MaxMsgSize(maxMsgSize))

	if tlsConfig != nil {
		grpcOptions = append(grpcOptions, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	if zipkinEndpoint != "" {
		var localEndpoint *openzipkinModel.Endpoint
		var reporter zipkinReporter.Reporter
		var zipkinExporter trace.Exporter

		localEndpoint, err = openzipkin.NewEndpoint(
			fmt.Sprintf("red-cloud-caretaker-%s", instanceName),
			net.JoinHostPort(hostName, strconv.Itoa(statusPort)))
		if err != nil {
			log.Fatalf("Failed to create the local zipkin endpoint: %s", err)
		}
		reporter = zipkinHTTP.NewReporter(fmt.Sprintf("http://%s/api/v2/spans",
			zipkinEndpoint))
		zipkinExporter = zipkin.NewExporter(reporter, localEndpoint)
		trace.RegisterExporter(zipkinExporter)

		/* All caretaker traces should be reported. */
		trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	}

	srv = grpc.NewServer(grpcOptions...)
	redcloud.RegisterAdminServiceServer(srv, admin)
	if err = srv.Serve(l); err != nil {
		log.Fatalf("Error listening to %s: %s", l.Addr(), err)
	}
}
