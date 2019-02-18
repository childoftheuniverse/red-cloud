package main

import (
	"context"
	"fmt"

	"github.com/childoftheuniverse/red-cloud"
	"github.com/childoftheuniverse/red-cloud/common"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	etcd "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.opencensus.io/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var numRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "admin_service",
	Name:      "num_requests",
	Help:      "Number of RPCs received by the AdminService interface",
}, []string{"method"})
var numErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "admin_service",
	Name:      "num_errors",
	Help:      "Number of RPCs received by the AdminService interface",
}, []string{"method", "error_class"})

func init() {
	prometheus.MustRegister(numRequests)
	prometheus.MustRegister(numErrors)
}

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
	parentCtx context.Context, table *redcloud.TableMetadata) (
	*redcloud.Empty, error) {
	var ctx context.Context
	var span *trace.Span
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

	ctx, span = trace.StartSpan(parentCtx, "red-cloud.AdminService/CreateTable")
	defer span.End()
	numRequests.With(prometheus.Labels{"method": "CreateTable"}).Inc()

	span.AddAttributes(
		trace.StringAttribute("table-name", table.Name),
		trace.Int64Attribute("split-size", md.TableMd.SplitSize))

	if md.TableMd.SplitSize <= 0 {
		md.TableMd.SplitSize = 128 * 1048576
	}

	if md.TableMd.DataUsage == redcloud.DataUsage_UNKNOWN {
		numErrors.With(prometheus.Labels{
			"method":      "CreateTable",
			"error_class": "unknown_data_usage",
		}).Inc()
		span.Annotate(nil, "Unspecified data usage")
		return &redcloud.Empty{}, grpc.Errorf(codes.InvalidArgument,
			"Created table is missing data_usage declaration, please add")
	}

	if encData, err = proto.Marshal(md); err != nil {
		numErrors.With(prometheus.Labels{
			"method":      "CreateTable",
			"error_class": "metadata_marshal_error",
		}).Inc()
		span.Annotate([]trace.Attribute{
			trace.StringAttribute("error", err.Error()),
		}, "Metadata marshalling error")
		return &redcloud.Empty{}, fmt.Errorf(
			"Unable to encode new table metadata: %s", err)
	}

	// TODO: make sure we don't overwite anything.
	span.Annotate(nil, "Syncing table metadata to etcd")
	if _, err = adm.etcdClient.Put(ctx, etcdPath, string(encData)); err != nil {
		numErrors.With(prometheus.Labels{
			"method":      "CreateTable",
			"error_class": "etcd_communication_errors",
		}).Inc()
		span.Annotate([]trace.Attribute{
			trace.StringAttribute("error", err.Error()),
		}, "Error writing to etcd")
		return &redcloud.Empty{}, err
	}

	for {
		// Check if our retries have exceeded our deadline.
		if ctx.Err() != nil {
			numErrors.With(prometheus.Labels{
				"method":      "CreateTable",
				"error_class": "context_expired",
			}).Inc()
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", ctx.Err().Error()),
				trace.Int64Attribute("attempt", int64(i)),
			}, "Context expired")
			return &redcloud.Empty{}, ctx.Err()
		}

		// Find a free data node to talk to.
		if client, err = adm.reg.GetNextFreeDataNode(ctx, i); err != nil {
			numErrors.With(prometheus.Labels{
				"method":      "CreateTable",
				"error_class": "no_free_data_nodes",
			}).Inc()
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
			}, "Unable to find available data node")
			return &redcloud.Empty{}, err
		}
		ts = redcloud.NewDataNodeMetadataServiceClient(client)

		span.Annotate(nil, "Sending ServeRange RPC to tablet server")
		if _, err = ts.ServeRange(ctx, rsr, grpc.FailFast(true)); err == nil {
			return &redcloud.Empty{}, nil
		}
		if grpc.Code(err) != codes.DeadlineExceeded &&
			grpc.Code(err) != codes.Unavailable &&
			grpc.Code(err) != codes.Canceled {
			numErrors.With(prometheus.Labels{
				"method":      "CreateTable",
				"error_class": "server_refused",
			}).Inc()
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
			}, "Tablet server reports error")
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
	parentCtx context.Context, table *redcloud.TableMetadata) (
	*redcloud.Empty, error) {
	var ctx context.Context
	var span *trace.Span
	var presp *etcd.TxnResponse
	var resp *etcd.GetResponse
	var ev *mvccpb.KeyValue
	var md = new(redcloud.ServerTableMetadata)
	var etcdPath = common.EtcdTableConfigPath(adm.reg.Instance(), table.Name)
	var encData []byte
	var modrev int64
	var version int64
	var err error

	ctx, span = trace.StartSpan(parentCtx, "red-cloud.AdminService/UpdateTable")
	defer span.End()
	numRequests.With(prometheus.Labels{"method": "UpdateTable"}).Inc()

	span.AddAttributes(
		trace.StringAttribute("table", table.Name))

	if resp, err = adm.etcdClient.Get(ctx, etcdPath); err != nil {
		numErrors.With(prometheus.Labels{
			"method":      "UpdateTable",
			"error_space": "etcd_communication_errors",
		}).Inc()
		span.Annotate([]trace.Attribute{
			trace.StringAttribute("error", err.Error()),
		}, "Error communicating with etcd")
		return &redcloud.Empty{}, err
	}

	for _, ev = range resp.Kvs {
		modrev = ev.ModRevision
		version = ev.Version
		if err = proto.Unmarshal(ev.Value, md); err != nil {
			numErrors.With(prometheus.Labels{
				"method":      "UpdateTable",
				"error_space": "tablet_metadata_corruption",
			}).Inc()
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
			}, "Error parsing original table metadata")
			return &redcloud.Empty{}, fmt.Errorf(
				"Unable to parse old metadata: %s", err)
		}
	}

	if table.SplitSize <= 0 {
		table.SplitSize = 128 * 1048576
	}

	md.TableMd = table

	if encData, err = proto.Marshal(md); err != nil {
		numErrors.With(prometheus.Labels{
			"method":      "UpdateTable",
			"error_space": "metadata_marshal_error",
		}).Inc()
		span.Annotate([]trace.Attribute{
			trace.StringAttribute("error", err.Error()),
		}, "Metadata marshalling error")
		return &redcloud.Empty{}, fmt.Errorf(
			"Unable to encode new table metadata: %s", err)
	}

	// TODO: we should probably repeat the command until it succeeds.
	presp, err = adm.etcdClient.Txn(ctx).If(
		etcd.Compare(etcd.ModRevision(etcdPath), "=", modrev),
		etcd.Compare(etcd.Version(etcdPath), "=", version)).Then(
		etcd.OpPut(etcdPath, string(encData))).Commit()
	if err != nil {
		numErrors.With(prometheus.Labels{
			"method":      "UpdateTable",
			"error_space": "etcd_communication_errors",
		}).Inc()
		span.Annotate([]trace.Attribute{
			trace.StringAttribute("error", err.Error()),
		}, "etcd communication failed (update transaction)")
		return &redcloud.Empty{}, err
	}

	if !presp.Succeeded {
		numErrors.With(prometheus.Labels{
			"method":      "UpdateTable",
			"error_space": "etcd_txn_failed",
		}).Inc()
		span.Annotate([]trace.Attribute{
			trace.StringAttribute("error", err.Error()),
		}, "etcd metadata update transaction failed")
		return &redcloud.Empty{}, fmt.Errorf(
			"Error updating %s: concurrent update", etcdPath)
	}

	span.Annotate(nil, "Table metadata updated")
	return &redcloud.Empty{}, nil
}

/*
DeleteTable deletes an existing table after telling all involved data nodes
to forget about it.
*/
func (adm *AdminService) DeleteTable(
	parentCtx context.Context, name *redcloud.TableName) (
	*redcloud.Empty, error) {
	var ctx context.Context
	var span *trace.Span
	var etcdPath = common.EtcdTableConfigPath(adm.reg.Instance(), name.Name)
	var err error

	ctx, span = trace.StartSpan(parentCtx, "red-cloud.AdminService/DeleteTable")
	defer span.End()
	numRequests.With(prometheus.Labels{"method": "DeleteTable"}).Inc()

	span.AddAttributes(
		trace.StringAttribute("table", name.Name))

	// TODO: unload the range from all data nodes.
	_, err = adm.etcdClient.Delete(ctx, etcdPath)
	if err == nil {
		span.Annotate(nil, "Table metadata deleted")
	} else {
		numErrors.With(prometheus.Labels{
			"method":      "DeleteTable",
			"error_space": "etcd_communication_errors",
		}).Inc()
		span.Annotate([]trace.Attribute{
			trace.StringAttribute("error", err.Error()),
		}, "Error communicating with etcd")
	}
	return &redcloud.Empty{}, err
}
