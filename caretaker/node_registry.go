package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"context"
	"github.com/childoftheuniverse/red-cloud"
	"github.com/childoftheuniverse/red-cloud/common"
	"github.com/golang/protobuf/proto"
	etcd "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

/*
DataNode represents an individual data node known to the caretaker.
It will be health checked and its load monitored regularly.
*/
type DataNode struct {
	addr       string
	statusAddr string
	conn       *grpc.ClientConn
	lastCheck  time.Time
	heapUsage  uint64
	isHealthy  bool
}

/*
NewDataNode instantiates a new DataNode object.
*/
func NewDataNode(
	addr string, conn *grpc.ClientConn, statusAddr string,
	heapUsage uint64, healthy bool) *DataNode {
	return &DataNode{
		addr:       addr,
		statusAddr: statusAddr,
		conn:       conn,
		lastCheck:  time.Now(),
		heapUsage:  heapUsage,
		isHealthy:  healthy,
	}
}

/*
Address returns the known address of the client node. It can be used to
identify nodes, since we shouldn't have multiple connections to the same
backend.
*/
func (ts *DataNode) Address() string {
	return ts.addr
}

/*
StatusAddress returns the known address of the HTTP status port of the
data node. It can be used in dashboards etc. to provide useful pointers
to information contained on the data node.
*/
func (ts *DataNode) StatusAddress() string {
	return ts.statusAddr
}

/*
Connection returns a GRPC client connection to the specified data node.
*/
func (ts *DataNode) Connection() *grpc.ClientConn {
	return ts.conn
}

/*
HeapUsage returns the last recorded heap usage stats from the data node.
*/
func (ts *DataNode) HeapUsage() uint64 {
	return ts.heapUsage
}

/*
IsHealthy returns wether the node is currently considered healthy.
*/
func (ts *DataNode) IsHealthy() bool {
	return ts.isHealthy
}

/*
LastCheck returns the time of the last successful check performed on the
data node.
*/
func (ts *DataNode) LastCheck() time.Time {
	return ts.lastCheck
}

/*
SetHeapUsage is used by the registry to update our idea of the heap usage.
*/
func (ts *DataNode) SetHeapUsage(heapUsage uint64) {
	ts.heapUsage = heapUsage
	ts.lastCheck = time.Now()
}

/*
SetHealthy is used to set the node to healthy or not.
*/
func (ts *DataNode) SetHealthy(healthy bool) {
	ts.isHealthy = healthy
}

/*
Release should be called once it is decided the data node object is no
longer useful.
*/
func (ts *DataNode) Release() {
	ts.conn.Close()
}

/*
NodeSlice represents a slice of DataNode objects which can be sorted by
their reported heap size.
*/
type NodeSlice []*DataNode

/*
Len returns the number of DataNodes in the slice.
*/
func (n NodeSlice) Len() int {
	return len(n)
}

/*
Less determines whether the i-th element in NodeSlice is less than the j-th.
*/
func (n NodeSlice) Less(i, j int) bool {
	return n[i].HeapUsage() < n[j].HeapUsage()
}

/*
Swap swaps the two slice entries with the numbers i and j.
*/
func (n NodeSlice) Swap(i, j int) {
	var dn = n[i]
	n[i] = n[j]
	n[j] = dn
}

/*
DataNodeRegistry manages the connections to all registered data nodes.
The node health will be health checked and associated tablets will be
unloaded and pushed off to other data nodes in case of overload.
*/
type DataNodeRegistry struct {
	knownNodes map[string]*DataNode
	tlsConfig  *tls.Config
	etcdClient *etcd.Client
	alive      NodeSlice
	missing    NodeSlice
	lock       sync.RWMutex
	instance   string
}

/*
NewDataNodeRegistry creates a new instance of a DataNodeRegistry
and returns it to the caller.
*/
func NewDataNodeRegistry(
	etcdClient *etcd.Client,
	tlsConfig *tls.Config,
	instance string) *DataNodeRegistry {
	var rv = &DataNodeRegistry{
		etcdClient: etcdClient,
		tlsConfig:  tlsConfig,
		instance:   instance,
		knownNodes: make(map[string]*DataNode),
	}
	go rv.registryMaintenance()
	return rv
}

/*
Add adds the specified data node to the registry, if a connection can be
established.
*/
func (r *DataNodeRegistry) Add(ctx context.Context, proto, tgt string) error {
	var ts *DataNode
	var tsc redcloud.DataNodeMetadataServiceClient
	var dialOpts []grpc.DialOption
	var status *redcloud.ServerStatus
	var client *grpc.ClientConn
	var statusAddr string
	var host string
	var err error

	// If we already know the node, we don't need to add it.
	if ts = r.GetNodeByAddress(tgt); ts != nil {
		return nil
	}

	if r.tlsConfig == nil {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	} else {
		var tlsCreds credentials.TransportCredentials = credentials.NewTLS(
			r.tlsConfig)
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(tlsCreds))
	}
	if client, err = grpc.Dial(tgt, dialOpts...); err != nil {
		return err
	}
	tsc = redcloud.NewDataNodeMetadataServiceClient(client)

	if status, err = tsc.GetServerStatus(ctx, &redcloud.Empty{}); err != nil {
		return err
	}

	if host, _, err = net.SplitHostPort(tgt); err != nil {
		host = tgt // Wrong, but whatever
	}
	statusAddr = net.JoinHostPort(host, strconv.Itoa(int(status.StatusPort)))

	ts = NewDataNode(tgt, client, statusAddr, status.HeapSize, true)

	r.lock.Lock()
	defer r.lock.Unlock()
	r.knownNodes[tgt] = ts
	r.alive = append(r.alive, ts)

	return nil
}

/*
Remove removes the specified node from the registry after reassigning
any data assigned to it.
*/
func (r *DataNodeRegistry) Remove(target string) error {
	var node *DataNode
	var ok bool
	var i int

	// TODO: unregister all tablets held by this data node.

	r.lock.Lock()
	defer r.lock.Unlock()

	for i, node = range r.alive {
		if node.Address() == target {
			r.alive = append(r.alive[:i], r.alive[i+1:]...)
		}
	}
	for i, node = range r.missing {
		if node.Address() == target {
			r.missing = append(r.missing[:i], r.missing[i+1:]...)
		}
	}

	if _, ok = r.knownNodes[target]; ok {
		r.knownNodes[target].Release()
		delete(r.knownNodes, target)
	}
	return nil
}

/*
GetNodeByAddress finds a known node matching the given address and returns
it. If there is no such node known to the registry, returns nil.
*/
func (r *DataNodeRegistry) GetNodeByAddress(address string) *DataNode {
	var rv *DataNode
	var ok bool

	r.lock.RLock()
	defer r.lock.RUnlock()

	if rv, ok = r.knownNodes[address]; ok {
		return rv
	}

	return nil
}

/*
GetTablets returns all the tablets the specified table is comprised of.
*/
func (r *DataNodeRegistry) GetTablets(table string) (
	[]*redcloud.ServerTabletMetadata, error) {
	var etcdContext context.Context
	var foundTable = new(redcloud.ServerTableMetadata)
	var resp *etcd.GetResponse
	var kv *mvccpb.KeyValue
	var err error

	etcdContext, _ = context.WithTimeout(context.Background(), 10*time.Second)

	if resp, err = r.etcdClient.Get(
		etcdContext,
		common.EtcdTableConfigPath(r.instance, table)); err != nil {
		return nil, err
	}

	for _, kv = range resp.Kvs {
		if err = proto.Unmarshal(kv.Value, foundTable); err != nil {
			return nil, fmt.Errorf("%s: table metadata looks corrupted",
				string(kv.Key))
		}
	}

	return foundTable.Tablet, nil
}

/*
GetNodeTablets returns all tablets the specified node is currently supposed
to hold.
*/
func (r *DataNodeRegistry) GetNodeTablets(
	address string) (map[string][]*common.KeyRange, error) {
	var rv = make(map[string][]*common.KeyRange)
	var mds map[string]*redcloud.ServerTableMetadata
	var md *redcloud.ServerTableMetadata
	var err error

	if mds, err = r.GetTableList(); err != nil {
		return nil, err
	}

	for _, md = range mds {
		var tablet *redcloud.ServerTabletMetadata

		for _, tablet = range md.Tablet {
			var holder = net.JoinHostPort(
				tablet.Host, strconv.Itoa(int(tablet.Port)))

			if holder == address {
				var ok bool

				_, ok = rv[md.Name]
				if !ok {
					rv[md.Name] = make([]*common.KeyRange, 0)
				}

				rv[md.Name] = append(rv[md.Name],
					common.NewKeyRange(tablet.StartKey, tablet.EndKey))
			}
		}
	}

	return rv, nil
}

/*
GetNextFreeDataNode returns one of the least loaded data nodes known
to the caretaker. The attempt number will simply be used as an offset into
the list of data nodes so a different one is returned, at least probably.
*/
func (r *DataNodeRegistry) GetNextFreeDataNode(attempt int) (
	*grpc.ClientConn, error) {
	var ts *DataNode
	r.lock.RLock()
	defer r.lock.RUnlock()
	if len(r.alive) > 0 {
		ts = r.alive[attempt%len(r.alive)]
		return ts.Connection(), nil
	}
	return nil, grpc.Errorf(codes.Unavailable, "No data nodes alive")
}

/*
Instance returns the name of the red-cloud instance this registry serves.
*/
func (r *DataNodeRegistry) Instance() string {
	return r.instance
}

/*
GetNodeLists returns the list of alive and dead nodes known to the
registry for status and debugging information. Do not use this to talk to
the nodes; use GetNodeByAddress() and GetNextFreeDataNode() instead.
*/
func (r *DataNodeRegistry) GetNodeLists() ([]*DataNode, []*DataNode) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.alive[:], r.missing[:]
}

/*
GetTableList returns the list of all tables currently known to the
caretaker. Reads it from etcd.
*/
func (r *DataNodeRegistry) GetTableList() (
	map[string]*redcloud.ServerTableMetadata, error) {
	var etcdContext context.Context
	var foundTables = make(map[string]*redcloud.ServerTableMetadata)
	var resp *etcd.GetResponse
	var kv *mvccpb.KeyValue
	var err error

	etcdContext, _ = context.WithTimeout(context.Background(), 10*time.Second)

	if resp, err = r.etcdClient.Get(
		etcdContext, common.EtcdTableConfigPath(r.instance, ""),
		etcd.WithPrefix()); err != nil {
		return nil, err
	}

	for _, kv = range resp.Kvs {
		var md = new(redcloud.ServerTableMetadata)
		if err = proto.Unmarshal(kv.Value, md); err != nil {
			return nil, fmt.Errorf("%s: table metadata looks corrupted",
				string(kv.Key))
		}

		foundTables[md.Name] = md
	}

	return foundTables, nil
}

/*
updateNodeList pings all known nodes (alive or not) and pulls the node
status. Nodes are then sorted by their load (currently represented by
their memory usage). Node lists are updated last to keep the time the
write lock is held short.
*/
func (r *DataNodeRegistry) updateNodeList() {
	var newlyUnhealthy []*DataNode
	var newlyHealthy []*DataNode
	var node *DataNode

	r.lock.RLock()

	/*
		Check the heap size on all known-alive nodes. Mark nodes as missing
		if they don't reply.
	*/
	for _, node = range r.alive {
		var ctx context.Context
		var status *redcloud.ServerStatus
		var tsc = redcloud.NewDataNodeMetadataServiceClient(
			node.Connection())
		var err error

		ctx, _ = context.WithTimeout(context.Background(), time.Second)

		if status, err = tsc.GetServerStatus(
			ctx, &redcloud.Empty{}); err == nil {
			node.SetHeapUsage(status.HeapSize)
		} else {
			log.Print("Node ", node.Address(), " is now unhealthy: ", err)
			newlyUnhealthy = append(newlyUnhealthy, node)
		}
	}

	// Check all missing nodes of whether they have come back.
	for _, node = range r.missing {
		var ctx context.Context
		var status *redcloud.ServerStatus
		var tsc = redcloud.NewDataNodeMetadataServiceClient(
			node.Connection())
		var err error

		ctx, _ = context.WithTimeout(context.Background(), time.Second)

		if status, err = tsc.GetServerStatus(
			ctx, &redcloud.Empty{}); err == nil {
			node.SetHeapUsage(status.HeapSize)
			newlyHealthy = append(newlyHealthy, node)
		}
	}
	r.lock.RUnlock()

	// Time to take the write lock.
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, node = range newlyUnhealthy {
		var n *DataNode
		var i int

		node.SetHealthy(false)
		r.missing = append(r.missing, node)

		for i, n = range r.alive {
			if n.Address() == node.Address() {
				r.alive = append(r.alive[:i], r.alive[i+1:]...)
			}
		}
	}

	for _, node = range newlyHealthy {
		var n *DataNode
		var i int

		node.SetHealthy(true)
		r.alive = append(r.alive, node)

		for i, n = range r.missing {
			if n.Address() == node.Address() {
				r.missing = append(r.missing[:i], r.missing[i+1:]...)
			}
		}
	}

	// Now sort the live nodes by the amount of memory used.
	sort.Sort(r.alive)
}

/*
updateTabletList fetches the current list of tables and tablets from
etcd, determines problems (such as uncovered ranges) and fixes them.
*/
func (r *DataNodeRegistry) updateTabletList() {
	var tables map[string]*redcloud.ServerTableMetadata
	var table *redcloud.ServerTableMetadata
	var err error

	// None of this makes any sense if we have no nodes to load things on.
	if len(r.alive) == 0 {
		return
	}

	if tables, err = r.GetTableList(); err != nil {
		log.Print("Error fetching table list: ", err)
		return
	}

	for _, table = range tables {
		var tablet *redcloud.ServerTabletMetadata

		for _, tablet = range table.Tablet {
			var addr = net.JoinHostPort(
				tablet.Host, strconv.Itoa(int(tablet.Port)))
			var node *DataNode

			node = r.GetNodeByAddress(addr)
			if node == nil || !node.IsHealthy() {
				var rsr = &redcloud.RangeServingRequest{
					Table:    table.Name,
					StartKey: tablet.StartKey,
					EndKey:   tablet.EndKey,
				}
				var ctx context.Context
				var i int

				ctx, _ = context.WithTimeout(context.Background(),
					time.Minute)

				// We have no clue what the node is, so we'll need a new one.
				for {
					var ts redcloud.DataNodeMetadataServiceClient
					var client *grpc.ClientConn

					// Check if our retries have exceeded our deadline.
					if ctx.Err() != nil {
						log.Print("Could not find a node to load ",
							table.Name, ": ", tablet.EndKey)
						break
					}

					// Find a free data node to talk to.
					if client, err = r.GetNextFreeDataNode(i); err != nil {
						log.Print("No free data nodes: ", err)
						break
					}
					ts = redcloud.NewDataNodeMetadataServiceClient(client)

					if _, err = ts.ServeRange(
						ctx, rsr, grpc.FailFast(true)); err == nil {
						break
					}
					if grpc.Code(err) != codes.DeadlineExceeded &&
						grpc.Code(err) != codes.Unavailable &&
						grpc.Code(err) != codes.Canceled {
						log.Print("Could not load table ", table.Name, ": ",
							tablet.EndKey, ": ", err)
					}

					i++
				}
			}
		}
	}
}

/*
registryMaintenance runs regularly to perform various maintenance
operations on the known tables and nodes, such as updating the list of
known nodes and discovering and reassigning tablets which aren't covered
by anybody.
*/
func (r *DataNodeRegistry) registryMaintenance() {
	for {
		var startTime = time.Now()
		var runTime time.Duration

		r.updateNodeList()
		r.updateTabletList()

		// TODO: export maintenance latency stats through ExpVar.
		runTime = time.Now().Sub(startTime)
		log.Print("Maintenance run took ", runTime)

		// We want to leave at least 30 seconds between two runs.
		time.Sleep(30 * time.Second)
	}
}
