/*
wc_cli is a command line client for red-cloud. It implements RPCs for most
red-cloud functions.
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/childoftheuniverse/red-cloud"
	etcd "github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

/*
RedCloudCLI provides all CLI functions as an object, in order to share some
properties between the different commands.
*/
type RedCloudCLI struct {
	etcdClient *etcd.Client
}

func usage() {
	fmt.Println("Usage: ", os.Args[0], " [--flag=value] command [arg [arg...]]")
	fmt.Println()
	fmt.Println("Supported flags:")
	flag.PrintDefaults()
	fmt.Println()
	fmt.Println("Important commands supported by this binary:")
	fmt.Println()
	fmt.Println("    createtable <table-path> <TableMetadata>")
	fmt.Println("        Create a new table under the specified path (URL) and")
	fmt.Println("        with the given metadata (as a text protocol buffer)")
	fmt.Println("    updatetable <table-path> <TableMetadata>")
	fmt.Println("        Update a new table under the specified path (URL)")
	fmt.Println("        with the given metadata (as a text protocol buffer)")
	fmt.Println("    gettable <table-path>")
	fmt.Println("        Get metadata of the table with the specified path (URL)")
	fmt.Println("    deletetable <table-path>")
	fmt.Println("        Delete the table with the specified path (URL)")
	fmt.Println("    compacttable <table-path>")
	fmt.Println("        Force a manual major compaction on all tablets of the")
	fmt.Println("        specified table")
	fmt.Println()
	fmt.Println("    get <table-path> <column-family> <column> <key>")
	fmt.Println("        Get the specified column from the given table/cf/key")
	fmt.Println("        and output it as text to stdout")
	fmt.Println("    getrange <table-path> <column-family> \\")
	fmt.Println("             <comma-separated-cols> <startkey> <endkey>")
	fmt.Println("        Get all data in the given column family between start")
	fmt.Println("        and end key in the specified columns")
	os.Exit(0)
}

func main() {
	var cli *RedCloudCLI
	var etcdServers string
	var etcdClient *etcd.Client
	var timeout time.Duration
	var ctx context.Context
	var verbose bool
	var flags []string
	var cmd string
	var err error

	flag.StringVar(&etcdServers, "etcd-servers", "",
		"etcd URL to get a server list from")
	flag.DurationVar(&timeout, "timeout", 30*time.Second,
		"Maximum time to allow for operations to finish")
	flag.BoolVar(&verbose, "verbose", false,
		"Print additional information about the operation progress")
	flag.Parse()
	flags = flag.Args()

	if len(flags) <= 0 {
		usage()
	}

	cmd = flags[0]
	flags = flags[1:]

	// Connect to etcd.
	if etcdClient, err = etcd.NewFromURL(etcdServers); err != nil {
		log.Fatalf("Cannot connect to etcd %s: %s", etcdServers, err)
	}

	cli = &RedCloudCLI{
		etcdClient: etcdClient,
	}

	if timeout.Nanoseconds() == 0 {
		ctx = context.Background()
	} else {
		ctx, _ = context.WithTimeout(context.Background(), timeout)
	}

	switch cmd {
	case "createtable":
		var md = new(redcloud.TableMetadata)
		if len(flags) != 2 {
			usage()
		}
		if err = proto.UnmarshalText(flags[1], md); err != nil {
			log.Fatal("Error decoding table metadata: ", err)
		}
		cli.CreateTable(ctx, flags[0], md)
	case "updatetable":
		var md = new(redcloud.TableMetadata)
		if len(flags) != 2 {
			usage()
		}
		if err = proto.UnmarshalText(flags[1], md); err != nil {
			log.Fatal("Error decoding table metadata: ", err)
		}
		cli.UpdateTable(ctx, flags[0], md)
	case "gettable":
		if len(flags) != 1 {
			usage()
		}
		cli.GetTable(ctx, flags[0])
	case "deletetable":
		if len(flags) != 1 {
			usage()
		}
		cli.DeleteTable(ctx, flags[0])
	case "get":
		if len(flags) != 4 {
			usage()
		}
		cli.Get(ctx, flags[0], flags[1], flags[2], flags[3])
	case "getrange":
		if len(flags) != 5 {
			usage()
		}
		cli.GetRange(ctx, flags[0], flags[1], strings.Split(flags[2], ","),
			flags[3], flags[4])
	case "insert":
		if len(flags) != 5 {
			usage()
		}
		cli.Insert(ctx, flags[0], flags[1], flags[2], flags[3], flags[4])
	case "help":
		usage()
	default:
		usage()
	}
}
