package main

import (
	"log"
	"os"

	"context"
	"github.com/childoftheuniverse/red-cloud"
	"github.com/childoftheuniverse/red-cloud/client"
	"github.com/childoftheuniverse/red-cloud/common"
	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

/*
CreateTable runs a CreateTable RPC against the master for the specified table.
*/
func (c *RedCloudCLI) CreateTable(
	ctx context.Context, path string, md *redcloud.TableMetadata) {
	var table string
	var adminClient redcloud.AdminServiceClient
	var conn *grpc.ClientConn
	var err error

	if conn, err = client.GetMasterConnection(
		ctx, c.etcdClient, path); err != nil {
		log.Fatal("Error connecting to instance master for ", path, ": ", err)
	}

	adminClient = redcloud.NewAdminServiceClient(conn)

	_, table, err = client.SplitTablePath(path)
	if err != nil && len(table) > 0 {
		md.Name = table
	}

	if _, err = adminClient.CreateTable(ctx, md); err != nil {
		log.Fatal("Error creating table ", md.Name, ": ", err)
	}
}

/*
UpdateTable runs an UpdateTable RPC against the master for the specified
table.
*/
func (c *RedCloudCLI) UpdateTable(
	ctx context.Context, path string, md *redcloud.TableMetadata) {
	var table string
	var adminClient redcloud.AdminServiceClient
	var conn *grpc.ClientConn
	var err error

	if conn, err = client.GetMasterConnection(
		ctx, c.etcdClient, path); err != nil {
		log.Fatal("Error connecting to instance master for ", path, ": ", err)
	}

	adminClient = redcloud.NewAdminServiceClient(conn)

	_, table, err = client.SplitTablePath(path)
	if err != nil && len(table) > 0 {
		md.Name = table
	}

	if _, err = adminClient.UpdateTable(ctx, md); err != nil {
		log.Fatal("Error updating table metadate for ", md.Name, ": ", err)
	}
}

/*
GetTable fetches and displays the metadata of the specified table.
*/
func (c *RedCloudCLI) GetTable(ctx context.Context, path string) {
	var md = new(redcloud.ServerTableMetadata)
	var kv *mvccpb.KeyValue
	var resp *etcd.GetResponse
	var instance, table string
	var etcdPath string
	var err error

	if instance, table, err = client.SplitTablePath(path); err != nil {
		log.Fatal("Error decoding table path ", path, ": ", err)
	}

	etcdPath = common.EtcdTableConfigPath(instance, table)
	if resp, err = c.etcdClient.Get(ctx, etcdPath); err != nil {
		log.Fatal("Error fetching metadata for ", path, " from etcd: ", err)
	}

	for _, kv = range resp.Kvs {
		if err = proto.Unmarshal(kv.Value, md); err != nil {
			log.Fatal("Error decoding table metadata ", etcdPath, ": ", err)
		}

		if err = proto.MarshalText(os.Stdout, md); err != nil {
			log.Fatal("Error encoding table metadata to text: ", err)
		}
	}
}

/*
DeleteTable runs a DeleteTable RPC against the master for the specified table.
*/
func (c *RedCloudCLI) DeleteTable(ctx context.Context, path string) {
	var table string
	var err error
	var adminClient redcloud.AdminServiceClient
	var tname *redcloud.TableName
	var conn *grpc.ClientConn

	if conn, err = client.GetMasterConnection(
		ctx, c.etcdClient, path); err != nil {
		log.Fatal("Error connecting to instance master for ", path, ": ", err)
	}

	adminClient = redcloud.NewAdminServiceClient(conn)

	if _, table, err = client.SplitTablePath(path); err != nil {
		log.Fatal("Error parsing table path ", path, ": ", err)
	}

	tname = &redcloud.TableName{
		Name: table,
	}

	if _, err = adminClient.DeleteTable(ctx, tname); err != nil {
		log.Fatal("Error creating table ", table, ": ", err)
	}
}
