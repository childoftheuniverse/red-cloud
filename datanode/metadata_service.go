package main

import (
	"context"
	"runtime"

	"github.com/childoftheuniverse/red-cloud"
	"go.opencensus.io/trace"
)

/*
DataNodeMetadataService is an RPC service for RPCs from red-cloud
caretaker to the data node. It is used to instruct nodes to load or unload
tablets, or to determine node health.
*/
type DataNodeMetadataService struct {
	registry   *ServingRangeRegistry
	statusPort uint16
}

/*
NewDataNodeMetadataService creates a new DataNodeMetadataService object
feeding from the specified ServingRangeRegistry. Any ranges the registry
knows about will be served.
*/
func NewDataNodeMetadataService(
	reg *ServingRangeRegistry, statusPort uint16) *DataNodeMetadataService {
	return &DataNodeMetadataService{registry: reg, statusPort: statusPort}
}

/*
ServeRange expresses the request of the caretaker for the data node to
start serving the described range off the specified sstable files.
*/
func (ds *DataNodeMetadataService) ServeRange(parentCtx context.Context,
	req *redcloud.RangeServingRequest) (*redcloud.Empty, error) {
	var ctx context.Context
	var span *trace.Span
	ctx, span = trace.StartSpan(
		parentCtx, "red-cloud.DataNodeMetadataService/ServeRange")
	defer span.End()
	return new(redcloud.Empty), ds.registry.LoadRange(ctx, req)
}

/*
ReleaseRange instructs the data node to stop serving the described range and
return details about the current sstables backing the range so they can be
passed on to a different host.
*/
func (ds *DataNodeMetadataService) ReleaseRange(parentCtx context.Context,
	req *redcloud.RangeReleaseRequest) (
	*redcloud.RangeReleaseResponse, error) {
	var ctx context.Context
	var span *trace.Span
	ctx, span = trace.StartSpan(
		parentCtx, "red-cloud.DataNodeMetadataService/ReleaseRange")
	defer span.End()
	return ds.registry.UnloadRange(ctx, req)
}

/*
GetServerStatus determines the server status and returns it to the caller.
*/
func (ds *DataNodeMetadataService) GetServerStatus(
	parentCtx context.Context, e *redcloud.Empty) (
	*redcloud.ServerStatus, error) {
	var span *trace.Span
	var stats runtime.MemStats
	var rv *redcloud.ServerStatus

	_, span = trace.StartSpan(
		parentCtx, "red-cloud.DataNodeMetadataService/GetServerStatus")
	defer span.End()

	runtime.ReadMemStats(&stats)
	rv = &redcloud.ServerStatus{
		HeapSize:   stats.HeapAlloc,
		StatusPort: uint32(ds.statusPort),
	}

	return rv, nil
}

/*
Ping provides a simple RPC interface to determine whether or not the node is
still alive. Ping respects deadlines and cancellations and returns
successfully otherwise.
*/
func (*DataNodeMetadataService) Ping(
	parentCtx context.Context, e *redcloud.Empty) (*redcloud.Empty, error) {
	var ctx context.Context
	var span *trace.Span
	ctx, span = trace.StartSpan(
		parentCtx, "red-cloud.DataNodeMetadataService/Ping")
	defer span.End()

	return new(redcloud.Empty), ctx.Err()
}
