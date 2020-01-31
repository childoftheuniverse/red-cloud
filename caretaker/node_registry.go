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
	"github.com/prometheus/client_golang/prometheus"
	etcd "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.opencensus.io/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

var numNodesAdded = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "caretaker_node_registry",
	Name:      "num_nodes_added",
	Help:      "Number of nodes which were added successfully",
})
var numNodesAlreadyAdded = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "caretaker_node_registry",
	Name:      "num_nodes_already_added",
	Help:      "Number of nodes which were already added when registered",
})
var numNodesAdditionErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "caretaker_node_registry",
	Name:      "num_node_add_errors",
	Help:      "Number of errors while attempting to add nodes",
}, []string{"error_class"})
var numNodesRemoved = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "caretaker_node_registry",
	Name:      "num_nodes_removed",
	Help:      "Number of nodes which were removed successfully",
})
var metadataCorruptionDetected = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "caretaker_node_registry",
	Name:      "tablet_metadata_corruption",
	Help:      "Errors caused by metadata corruption",
})
var etcdCommunicationErrors = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "caretaker_node_registry",
	Name:      "etcd_communication_errors",
	Help:      "Errors communicating with etcd",
})
var numNodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "red_cloud",
	Subsystem: "caretaker_node_registry",
	Name:      "num_nodes",
	Help:      "Number of known nodes by status",
}, []string{"status"})
var nodeStateChanges = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "caretaker_node_registry",
	Name:      "num_node_state_changes",
	Help:      "Number of nodes changing state",
}, []string{"new_status"})
var numRelocations = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "caretaker_node_registry",
	Name:      "num_tablet_relocations",
	Help:      "Number of times a tablet was moved to another data node",
})
var numRelocationErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "caretaker_node_registry",
	Name:      "num_tablet_relocation_errors",
	Help:      "Number of times a tablet failed to be moved to another data node",
}, []string{"error_class"})
var numMaintenanceRuns = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "caretaker_node_registry",
	Name:      "num_maintenance_runs",
	Help:      "Number of times caretaker has run maintenance",
})
var maintenanceLatencyTotal = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "red_cloud",
	Subsystem: "caretaker_node_registry",
	Name:      "maintenance_latency_sec_total",
	Help:      "Sum of the number of seconds spent in caretaker maintenance",
})

func init() {
	prometheus.MustRegister(numNodesAdded)
	prometheus.MustRegister(numNodesAlreadyAdded)
	prometheus.MustRegister(numNodesAdditionErrors)
	prometheus.MustRegister(numNodesRemoved)
	prometheus.MustRegister(metadataCorruptionDetected)
	prometheus.MustRegister(etcdCommunicationErrors)
	prometheus.MustRegister(numNodes)
	prometheus.MustRegister(nodeStateChanges)
	prometheus.MustRegister(numRelocations)
	prometheus.MustRegister(numRelocationErrors)
	prometheus.MustRegister(numMaintenanceRuns)
	prometheus.MustRegister(maintenanceLatencyTotal)
}

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
	knownNodes         map[string]*DataNode
	tlsConfig          *tls.Config
	expectedServerName string
	etcdClient         *etcd.Client
	alive              NodeSlice
	missing            NodeSlice
	lock               sync.RWMutex
	instance           string
}

/*
NewDataNodeRegistry creates a new instance of a DataNodeRegistry
and returns it to the caller.
*/
func NewDataNodeRegistry(
	etcdClient *etcd.Client,
	tlsConfig *tls.Config,
	instance string,
	expectedServerName string) *DataNodeRegistry {
	var rv = &DataNodeRegistry{
		etcdClient:         etcdClient,
		tlsConfig:          tlsConfig,
		instance:           instance,
		knownNodes:         make(map[string]*DataNode),
		expectedServerName: expectedServerName,
	}
	go rv.registryMaintenance()
	return rv
}

/*
Add adds the specified data node to the registry, if a connection can be
established.
*/
func (r *DataNodeRegistry) Add(
	parentCtx context.Context, proto, tgt string) error {
	var ctx = parentCtx
	var ts *DataNode
	var tsc redcloud.DataNodeMetadataServiceClient
	var dialOpts []grpc.DialOption
	var status *redcloud.ServerStatus
	var client *grpc.ClientConn
	var statusAddr string
	var span *trace.Span
	var host string
	var err error

	if span = trace.FromContext(parentCtx); span == nil {
		ctx, span = trace.StartSpan(
			parentCtx, "red-cloud.DataNodeRegistry/Add")
	}
	defer span.End()

	span.AddAttributes(
		trace.StringAttribute("target-address", tgt))

	// If we already know the node, we don't need to add it.
	if ts = r.GetNodeByAddress(ctx, tgt); ts != nil {
		span.Annotate(nil, "Node already registered")
		numNodesAlreadyAdded.Inc()
		return nil
	}

	if r.tlsConfig == nil {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	} else {
		var tlsCreds credentials.TransportCredentials = credentials.NewTLS(
			r.tlsConfig)
		if r.expectedServerName != "" {
			tlsCreds.OverrideServerName(r.expectedServerName)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(tlsCreds))
	}
	if client, err = grpc.Dial(tgt, dialOpts...); err != nil {
		span.Annotate(
			[]trace.Attribute{trace.StringAttribute("error", err.Error())},
			"Node cannot be reached")
		numNodesAdditionErrors.With(
			prometheus.Labels{"error_class": "unreachable"}).Inc()
		return err
	}
	tsc = redcloud.NewDataNodeMetadataServiceClient(client)

	if status, err = tsc.GetServerStatus(ctx, &redcloud.Empty{}); err != nil {
		span.Annotate(
			[]trace.Attribute{trace.StringAttribute("error", err.Error())},
			"Node cannot respond to GetServerStatus")
		numNodesAdditionErrors.With(
			prometheus.Labels{"error_class": "rpc_failed"}).Inc()
		return err
	}

	if host, _, err = net.SplitHostPort(tgt); err != nil {
		host = tgt // Wrong, but whatever
		numNodesAdditionErrors.With(
			prometheus.Labels{"error_class": "wrong_host_port_format"}).Inc()
	}
	statusAddr = net.JoinHostPort(host, strconv.Itoa(int(status.StatusPort)))

	ts = NewDataNode(tgt, client, statusAddr, status.HeapSize, true)

	span.Annotate(nil, "Acquiring registry lock")
	r.lock.Lock()
	defer r.lock.Unlock()
	span.Annotate(nil, "Registry lock acquired")
	r.knownNodes[tgt] = ts
	r.alive = append(r.alive, ts)

	span.Annotate(nil, "Node registered")
	numNodesAdded.Inc()
	return nil
}

/*
Remove removes the specified node from the registry after reassigning
any data assigned to it.
*/
func (r *DataNodeRegistry) Remove(
	parentCtx context.Context, target string) error {
	var span *trace.Span
	var node *DataNode
	var ok bool
	var i int

	if span = trace.FromContext(parentCtx); span == nil {
		_, span = trace.StartSpan(
			parentCtx, "red-cloud.DataNodeRegistry/Remove")
	}
	defer span.End()

	span.AddAttributes(
		trace.StringAttribute("target-address", target))

	// TODO: unregister all tablets held by this data node.

	span.Annotate(nil, "Acquiring registry lock")
	r.lock.Lock()
	defer r.lock.Unlock()
	span.Annotate(nil, "Registry lock acquired")

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
	span.Annotate(nil, "Node removed")
	numNodesRemoved.Inc()
	return nil
}

/*
GetNodeByAddress finds a known node matching the given address and returns
it. If there is no such node known to the registry, returns nil.
*/
func (r *DataNodeRegistry) GetNodeByAddress(
	parentCtx context.Context, address string) *DataNode {
	var span *trace.Span
	var rv *DataNode
	var ok bool

	if span = trace.FromContext(parentCtx); span == nil {
		_, span = trace.StartSpan(
			parentCtx, "red-cloud.DataNodeRegistry/GetNodeByAddress")
	}
	defer span.End()

	span.AddAttributes(
		trace.StringAttribute("target-address", address))

	span.Annotate(nil, "Acquiring registry lock")
	r.lock.RLock()
	defer r.lock.RUnlock()
	span.Annotate(nil, "Registry lock acquired")

	if rv, ok = r.knownNodes[address]; ok {
		span.Annotate(nil, "Node found")
		return rv
	}

	span.Annotate(nil, "Node unknown")
	return nil
}

/*
GetTablets returns all the tablets the specified table is comprised of.
*/
func (r *DataNodeRegistry) GetTablets(parentCtx context.Context, table string) (
	[]*redcloud.ServerTabletMetadata, error) {
	var ctx = parentCtx
	var etcdContext context.Context
	var foundTable = new(redcloud.ServerTableMetadata)
	var resp *etcd.GetResponse
	var kv *mvccpb.KeyValue
	var span *trace.Span
	var err error

	if span = trace.FromContext(parentCtx); span == nil {
		ctx, span = trace.StartSpan(
			parentCtx, "red-cloud.DataNodeRegistry/GetNodeByAddress")
	}
	defer span.End()

	span.AddAttributes(
		trace.StringAttribute("table", table))

	etcdContext, _ = context.WithTimeout(ctx, 10*time.Second)

	if resp, err = r.etcdClient.Get(
		etcdContext,
		common.EtcdTableConfigPath(r.instance, table)); err != nil {
		span.Annotate(
			[]trace.Attribute{trace.StringAttribute("error", err.Error())},
			"etcd communication error")
		etcdCommunicationErrors.Inc()
		return nil, err
	}

	for _, kv = range resp.Kvs {
		if err = proto.Unmarshal(kv.Value, foundTable); err != nil {
			span.Annotate(
				[]trace.Attribute{trace.StringAttribute("md-key", string(kv.Key))},
				"metadata corruption")
			metadataCorruptionDetected.Inc()
			return nil, fmt.Errorf("%s: table metadata looks corrupted",
				string(kv.Key))
		}
	}

	span.Annotate(nil, "Tablet found")
	return foundTable.Tablet, nil
}

/*
GetNodeTablets returns all tablets the specified node is currently supposed
to hold.
*/
func (r *DataNodeRegistry) GetNodeTablets(
	parentCtx context.Context, address string) (
	map[string][]*common.KeyRange, error) {
	var ctx = parentCtx
	var span *trace.Span
	var rv = make(map[string][]*common.KeyRange)
	var mds map[string]*redcloud.ServerTableMetadata
	var md *redcloud.ServerTableMetadata
	var tables int64
	var tablets int64
	var matches int64
	var err error

	if span = trace.FromContext(parentCtx); span == nil {
		ctx, span = trace.StartSpan(
			parentCtx, "red-cloud.DataNodeRegistry/GetNodeTablets")
	}
	defer span.End()

	span.AddAttributes(trace.StringAttribute("target-address", address))

	if mds, err = r.GetTableList(ctx); err != nil {
		span.Annotate(
			[]trace.Attribute{trace.StringAttribute("error", err.Error())},
			"Error fetching table list")
		return nil, err
	}

	for _, md = range mds {
		var tablet *redcloud.ServerTabletMetadata

		tables++

		for _, tablet = range md.Tablet {
			var holder = net.JoinHostPort(
				tablet.Host, strconv.Itoa(int(tablet.Port)))

			tablets++

			if holder == address {
				var ok bool
				matches++

				_, ok = rv[md.Name]
				if !ok {
					rv[md.Name] = make([]*common.KeyRange, 0)
				}

				rv[md.Name] = append(rv[md.Name],
					common.NewKeyRange(tablet.StartKey, tablet.EndKey))
			}
		}
	}

	span.Annotate(
		[]trace.Attribute{
			trace.Int64Attribute("tables-scanned", tables),
			trace.Int64Attribute("tablets-scanned", tablets),
			trace.Int64Attribute("matches-found", matches),
		}, "Tablet scan finished")

	return rv, nil
}

/*
GetNextFreeDataNode returns one of the least loaded data nodes known
to the caretaker. The attempt number will simply be used as an offset into
the list of data nodes so a different one is returned, at least probably.
*/
func (r *DataNodeRegistry) GetNextFreeDataNode(
	parentCtx context.Context, attempt int) (*grpc.ClientConn, error) {
	var span *trace.Span
	var ts *DataNode

	if span = trace.FromContext(parentCtx); span == nil {
		_, span = trace.StartSpan(
			parentCtx, "red-cloud.DataNodeRegistry/GetNextFreeDataNode")
	}

	span.Annotate(nil, "Acquiring registry lock")
	r.lock.RLock()
	defer r.lock.RUnlock()
	span.Annotate(nil, "Registry lock acquired")
	if len(r.alive) > 0 {
		ts = r.alive[attempt%len(r.alive)]
		return ts.Connection(), nil
	}
	span.Annotate(nil, "No data nodes found alive")
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
func (r *DataNodeRegistry) GetNodeLists(
	parentCtx context.Context) ([]*DataNode, []*DataNode) {
	var span *trace.Span

	if span = trace.FromContext(parentCtx); span == nil {
		_, span = trace.StartSpan(
			parentCtx, "tbd.childoftheuniverse.github.com/GetNodeLists")
	}
	defer span.End()

	span.Annotate(nil, "Attempting to acquire read lock")
	r.lock.RLock()
	defer r.lock.RUnlock()
	span.Annotate(nil, "Read lock acquired, returning data")
	return r.alive[:], r.missing[:]
}

/*
GetTableList returns the list of all tables currently known to the
caretaker. Reads it from etcd.
*/
func (r *DataNodeRegistry) GetTableList(parentCtx context.Context) (
	map[string]*redcloud.ServerTableMetadata, error) {
	var ctx = parentCtx
	var span *trace.Span
	var etcdContext context.Context
	var foundTables = make(map[string]*redcloud.ServerTableMetadata)
	var resp *etcd.GetResponse
	var kv *mvccpb.KeyValue
	var err error

	if span = trace.FromContext(parentCtx); span == nil {
		ctx, span = trace.StartSpan(
			parentCtx, "red-cloud.DataNodeRegistry/GetTableList")
	}
	defer span.End()

	etcdContext, _ = context.WithTimeout(ctx, 10*time.Second)

	if resp, err = r.etcdClient.Get(
		etcdContext, common.EtcdTableConfigPath(r.instance, ""),
		etcd.WithPrefix()); err != nil {
		span.Annotate(
			[]trace.Attribute{trace.StringAttribute("error", err.Error())},
			"Error communicating with etcd")
		etcdCommunicationErrors.Inc()
		return nil, err
	}

	for _, kv = range resp.Kvs {
		var md = new(redcloud.ServerTableMetadata)
		if err = proto.Unmarshal(kv.Value, md); err != nil {
			span.Annotate(
				[]trace.Attribute{trace.StringAttribute("md-key", string(kv.Key))},
				"metadata corruption")
			metadataCorruptionDetected.Inc()
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
func (r *DataNodeRegistry) updateNodeList(parentCtx context.Context) {
	var ctx context.Context
	var span *trace.Span
	var newlyUnhealthy []*DataNode
	var newlyHealthy []*DataNode
	var node *DataNode

	/* Unconditionally start a new span. */
	ctx, span = trace.StartSpan(
		parentCtx, "red-cloud.DataNodeRegistry/updateNodeList")

	span.Annotate(nil, "Attempting to require registry read lock")
	r.lock.RLock()
	span.Annotate(nil, "Registry read lock acquired")

	/*
		Check the heap size on all known-alive nodes. Mark nodes as missing
		if they don't reply.
	*/
	for _, node = range r.alive {
		var clientCtx context.Context
		var status *redcloud.ServerStatus
		var tsc = redcloud.NewDataNodeMetadataServiceClient(
			node.Connection())
		var cancel context.CancelFunc
		var err error

		clientCtx, cancel = context.WithTimeout(ctx, time.Second)
		defer cancel()

		if status, err = tsc.GetServerStatus(
			clientCtx, &redcloud.Empty{}); err == nil {
			node.SetHeapUsage(status.HeapSize)
		} else {
			span.Annotate(
				[]trace.Attribute{
					trace.StringAttribute("target-address", node.Address()),
					trace.StringAttribute("error", err.Error()),
				}, "Reporting node "+node.Address()+" unhealthy")
			log.Print("Node ", node.Address(), " is now unhealthy: ", err)
			newlyUnhealthy = append(newlyUnhealthy, node)
			nodeStateChanges.With(prometheus.Labels{"new_status": "missing"}).Inc()
		}
	}

	// Check all missing nodes of whether they have come back.
	for _, node = range r.missing {
		var clientCtx context.Context
		var status *redcloud.ServerStatus
		var tsc = redcloud.NewDataNodeMetadataServiceClient(
			node.Connection())
		var cancel context.CancelFunc
		var err error

		clientCtx, cancel = context.WithTimeout(ctx, time.Second)
		defer cancel()

		if status, err = tsc.GetServerStatus(
			clientCtx, &redcloud.Empty{}); err == nil {
			span.Annotate(
				[]trace.Attribute{
					trace.StringAttribute("target-address", node.Address()),
				}, "Reporting node "+node.Address()+" unhealthy")
			node.SetHeapUsage(status.HeapSize)
			newlyHealthy = append(newlyHealthy, node)
			nodeStateChanges.With(prometheus.Labels{"new_status": "alive"}).Inc()
		}
	}
	r.lock.RUnlock()

	// Time to take the write lock.
	span.Annotate(nil, "Attempting to require registry write lock")
	r.lock.Lock()
	defer r.lock.Unlock()
	span.Annotate(nil, "Registry write lock acquired")
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

	numNodes.With(prometheus.Labels{"status": "alive"}).Set(
		float64(len(r.alive)))
	numNodes.With(prometheus.Labels{"status": "missing"}).Set(
		float64(len(r.missing)))

	// Now sort the live nodes by the amount of memory used.
	span.Annotate(nil, "Sorting nodes by memory consumption")
	sort.Sort(r.alive)
	span.Annotate(nil, "Done")
}

/*
updateTabletList fetches the current list of tables and tablets from
etcd, determines problems (such as uncovered ranges) and fixes them.
*/
func (r *DataNodeRegistry) updateTabletList(parentCtx context.Context) {
	var ctx context.Context
	var span *trace.Span
	var tables map[string]*redcloud.ServerTableMetadata
	var table *redcloud.ServerTableMetadata
	var numRelocated int64
	var err error

	/* Unconditionally start a new span. */
	ctx, span = trace.StartSpan(
		parentCtx, "red-cloud.DataNodeRegistry/updateTabletList")

	// None of this makes any sense if we have no nodes to load things on.
	if len(r.alive) == 0 {
		span.Annotate(nil, "No nodes reported alive")
		return
	}

	if tables, err = r.GetTableList(ctx); err != nil {
		log.Print("Error fetching table list: ", err)
		return
	}

	for _, table = range tables {
		var tablet *redcloud.ServerTabletMetadata

		for _, tablet = range table.Tablet {
			var addr = net.JoinHostPort(
				tablet.Host, strconv.Itoa(int(tablet.Port)))
			var node *DataNode

			node = r.GetNodeByAddress(ctx, addr)
			if node == nil || !node.IsHealthy() {
				var cancel context.CancelFunc
				var rsr = &redcloud.RangeServingRequest{
					Table:    table.Name,
					StartKey: tablet.StartKey,
					EndKey:   tablet.EndKey,
				}
				var clientCtx context.Context
				var i int

				span.Annotate(
					[]trace.Attribute{
						trace.StringAttribute("table", table.Name),
						/* TODO: escape this somehow */
						trace.StringAttribute("end-key", string(tablet.EndKey)),
					}, "Tablet needs a new server")

				clientCtx, cancel = context.WithTimeout(ctx, time.Minute)
				defer cancel()

				// We have no clue what the node is, so we'll need a new one.
				for {
					var ts redcloud.DataNodeMetadataServiceClient
					var client *grpc.ClientConn

					// Check if our retries have exceeded our deadline.
					if clientCtx.Err() != nil {
						span.Annotate(nil, "Context expired while moving tablet")
						log.Print("Could not find a node to load ",
							table.Name, ": ", tablet.EndKey)
						numRelocationErrors.With(
							prometheus.Labels{"error_class": "context_expired"}).Inc()
						break
					}

					// Find a free data node to talk to.
					if client, err = r.GetNextFreeDataNode(ctx, i); err != nil {
						span.Annotate(
							[]trace.Attribute{trace.StringAttribute("error", err.Error())},
							"No free data nodes available")
						log.Print("No free data nodes: ", err)
						numRelocationErrors.With(
							prometheus.Labels{"error_class": "no_free_data_nodes"}).Inc()
						break
					}
					ts = redcloud.NewDataNodeMetadataServiceClient(client)

					if _, err = ts.ServeRange(
						clientCtx, rsr, grpc.FailFast(true)); err == nil {
						numRelocated++
						numRelocations.Inc()
						break
					}
					if grpc.Code(err) != codes.DeadlineExceeded &&
						grpc.Code(err) != codes.Unavailable &&
						grpc.Code(err) != codes.Canceled {
						span.Annotate(
							[]trace.Attribute{
								trace.StringAttribute("error", err.Error()),
								trace.StringAttribute("table", table.Name),
								trace.StringAttribute("end-key", string(tablet.EndKey)),
							}, "Tablet load failed")
						log.Print("Could not load table ", table.Name, ": ",
							tablet.EndKey, ": ", err)
						numRelocationErrors.With(
							prometheus.Labels{"error_class": "server_refused"}).Inc()
					}

					i++
				}
			}
		}
	}

	span.AddAttributes(
		trace.Int64Attribute("tablets-relocated", numRelocated))
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
		var ctx = context.Background()

		r.updateNodeList(ctx)
		r.updateTabletList(ctx)

		runTime = time.Now().Sub(startTime)
		numMaintenanceRuns.Inc()
		maintenanceLatencyTotal.Add(runTime.Seconds())
		log.Print("Maintenance run took ", runTime)

		// We want to leave at least 30 seconds between two runs.
		time.Sleep(30 * time.Second)
	}
}
