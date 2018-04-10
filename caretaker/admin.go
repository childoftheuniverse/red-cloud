package main

import (
	"fmt"

	"context"
	"github.com/childoftheuniverse/red-cloud"
	"github.com/childoftheuniverse/red-cloud/common"
	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

/*
AdminService contains methods for managing table metadata or perform other
administrative operations.
*/
type AdminService struct {
	etcdClient *etcd.Client
	reg        *DataNodeRegistry
}

/*
NewAdminService instantiates a new AdminService object to export. It will
use the speified etcd client and data node registry for operation.
*/
func NewAdminService(
	etcdClient *etcd.Client, reg *DataNodeRegistry) *AdminService {
	return &AdminService{
		etcdClient: etcdClient,
		reg:        reg,
	}
}

/*
CreateTable creates a new table based on the metadata record.
The metadata will be added to etcd and a data node will be found to host
the tablet.
*/
func (adm *AdminService) CreateTable(
	ctx context.Context, table *redcloud.TableMetadata) (
	*redcloud.Empty, error) {
	var md = &redcloud.ServerTableMetadata{
		Name:    table.Name,
		TableMd: table,
		Tablet: []*redcloud.ServerTabletMetadata{
			&redcloud.ServerTabletMetadata{
				StartKey: []byte{},
				EndKey:   []byte{},
			},
		},
	}
	var etcdPath = common.EtcdTableConfigPath(adm.reg.Instance(), table.Name)
	var rsr = &redcloud.RangeServingRequest{
		Table:    table.Name,
		StartKey: []byte{},
		EndKey:   []byte{},
	}
	var ts redcloud.DataNodeMetadataServiceClient
	var client *grpc.ClientConn
	var encData []byte
	var i int
	var err error

	if md.TableMd.SplitSize <= 0 {
		md.TableMd.SplitSize = 128 * 1048576
	}

	if encData, err = proto.Marshal(md); err != nil {
		return &redcloud.Empty{}, fmt.Errorf(
			"Unable to encode new table metadata: %s", err)
	}

	// TODO: make sure we don't overwite anything.
	if _, err = adm.etcdClient.Put(ctx, etcdPath, string(encData)); err != nil {
		return &redcloud.Empty{}, err
	}

	for {
		// Check if our retries have exceeded our deadline.
		if ctx.Err() != nil {
			return &redcloud.Empty{}, ctx.Err()
		}

		// Find a free data node to talk to.
		if client, err = adm.reg.GetNextFreeDataNode(i); err != nil {
			return &redcloud.Empty{}, err
		}
		ts = redcloud.NewDataNodeMetadataServiceClient(client)

		if _, err = ts.ServeRange(ctx, rsr, grpc.FailFast(true)); err == nil {
			return &redcloud.Empty{}, nil
		}
		if grpc.Code(err) != codes.DeadlineExceeded &&
			grpc.Code(err) != codes.Unavailable &&
			grpc.Code(err) != codes.Canceled {
			return &redcloud.Empty{}, err
		}

		i++
	}
}

/*
UpdateTable updates an existing table based on the metadata record.
The metadata will be replaced in etcd. The tablets will not be relocated
to a different data node.
*/
func (adm *AdminService) UpdateTable(
	ctx context.Context, table *redcloud.TableMetadata) (
	*redcloud.Empty, error) {
	var presp *etcd.TxnResponse
	var resp *etcd.GetResponse
	var ev *mvccpb.KeyValue
	var md = new(redcloud.ServerTableMetadata)
	var etcdPath = common.EtcdTableConfigPath(adm.reg.Instance(), table.Name)
	var encData []byte
	var modrev int64
	var version int64
	var err error

	if resp, err = adm.etcdClient.Get(ctx, etcdPath); err != nil {
		return &redcloud.Empty{}, err
	}

	for _, ev = range resp.Kvs {
		modrev = ev.ModRevision
		version = ev.Version
		if err = proto.Unmarshal(ev.Value, md); err != nil {
			return &redcloud.Empty{}, fmt.Errorf(
				"Unable to parse old metadata: %s", err)
		}
	}

	if table.SplitSize <= 0 {
		table.SplitSize = 128 * 1048576
	}

	md.TableMd = table

	if encData, err = proto.Marshal(md); err != nil {
		return &redcloud.Empty{}, fmt.Errorf(
			"Unable to encode new table metadata: %s", err)
	}

	// TODO: we should probably repeat the command until it succeeds.
	presp, err = adm.etcdClient.Txn(ctx).If(
		etcd.Compare(etcd.ModRevision(etcdPath), "=", modrev),
		etcd.Compare(etcd.Version(etcdPath), "=", version)).Then(
		etcd.OpPut(etcdPath, string(encData))).Commit()
	if err != nil {
		return &redcloud.Empty{}, err
	}

	if !presp.Succeeded {
		return &redcloud.Empty{}, fmt.Errorf(
			"Error updating %s: concurrent update", etcdPath)
	}

	return &redcloud.Empty{}, nil
}

/*
DeleteTable deletes an existing table after telling all involved data nodes
to forget about it.
*/
func (adm *AdminService) DeleteTable(
	ctx context.Context, name *redcloud.TableName) (
	*redcloud.Empty, error) {
	var etcdPath = common.EtcdTableConfigPath(adm.reg.Instance(), name.Name)
	var err error

	// TODO: unload the range from all data nodes.
	_, err = adm.etcdClient.Delete(ctx, etcdPath)
	return &redcloud.Empty{}, err
}
