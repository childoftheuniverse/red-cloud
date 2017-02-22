package main

import (
	"runtime"

	"github.com/childoftheuniverse/red-cloud"
	"golang.org/x/net/context"
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
func (ds *DataNodeMetadataService) ServeRange(ctx context.Context,
	req *redcloud.RangeServingRequest) (*redcloud.Empty, error) {
	return new(redcloud.Empty), ds.registry.LoadRange(ctx, req)
}

/*
ReleaseRange instructs the data node to stop serving the described range and
return details about the current sstables backing the range so they can be
passed on to a different host.
*/
func (ds *DataNodeMetadataService) ReleaseRange(ctx context.Context,
	req *redcloud.RangeReleaseRequest) (
	*redcloud.RangeReleaseResponse, error) {
	return ds.registry.UnloadRange(ctx, req)
}

/*
GetServerStatus determines the server status and returns it to the caller.
*/
func (ds *DataNodeMetadataService) GetServerStatus(
	ctx context.Context, e *redcloud.Empty) (
	*redcloud.ServerStatus, error) {
	var stats runtime.MemStats
	var rv *redcloud.ServerStatus

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
func (*DataNodeMetadataService) Ping(ctx context.Context, e *redcloud.Empty) (
	*redcloud.Empty, error) {
	return new(redcloud.Empty), ctx.Err()
}
