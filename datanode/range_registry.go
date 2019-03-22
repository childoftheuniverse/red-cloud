package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"context"
	"github.com/childoftheuniverse/fancylocking"
	"github.com/childoftheuniverse/filesystem"
	"github.com/childoftheuniverse/filesystem-internal"
	"github.com/childoftheuniverse/recordio"
	"github.com/childoftheuniverse/red-cloud"
	"github.com/childoftheuniverse/red-cloud/common"
	"github.com/childoftheuniverse/red-cloud/storage"
	"github.com/childoftheuniverse/sstable"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	etcd "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.opencensus.io/trace"
)

var logSortsInProgress = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "red_cloud",
	Subsystem: "range_registry",
	Name:      "log_sorts_in_progress",
	Help:      "Number of log sorting operations currently in progress.",
})
var logSortsDone = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "range_registry",
	Name:      "num_log_sorts_done",
	Help:      "Cumulative number of log sorts which have been done.",
})
var logSortsFailed = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "range_registry",
	Name:      "num_log_sorts_failed",
	Help:      "Cumulative number of log sorts which have failed.",
})
var logSortLatency = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "red_cloud",
	Subsystem: "range_registry",
	Name:      "log_sort_latency",
	Help:      "Latency of log sorting operations (in seconds).",
})
var minorCompactionsInProgress = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "red_cloud",
	Subsystem: "range_registry",
	Name:      "minor_compactions_in_progress",
	Help:      "Number of minor compactions currently in progress.",
})
var minorCompactionsDone = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "range_registry",
	Name:      "num_minor_compactions_done",
	Help:      "Cumulative number of minor compactions which have been done.",
})
var minorCompactionsFailed = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "range_registry",
	Name:      "num_minor_compactions_failed",
	Help:      "Cumulative number of log sorts which have failed.",
})
var minorCompactionsLatency = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "red_cloud",
	Subsystem: "range_registry",
	Name:      "minor_compaction_latency",
	Help:      "Latency of minor compaction operations (in seconds).",
})
var majorCompactionsInProgress = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "red_cloud",
	Subsystem: "range_registry",
	Name:      "major_compactions_in_progress",
	Help:      "Number of major compactions currently in progress.",
})
var majorCompactionsDone = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "range_registry",
	Name:      "num_major_compactions_done",
	Help:      "Cumulative number of major compactions which have been done.",
})
var majorCompactionsFailed = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "range_registry",
	Name:      "num_minor_compactions_failed",
	Help:      "Cumulative number of log sorts which have failed.",
})
var majorCompactionsLatency = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "red_cloud",
	Subsystem: "range_registry",
	Name:      "major_compaction_latency",
	Help:      "Latency of major compaction operations (in seconds).",
})
var journalBytesWritten = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "range_registry",
	Name:      "journal_bytes_written",
	Help:      "Number of bytes written to the journal",
})

func init() {
	prometheus.MustRegister(logSortsInProgress)
	prometheus.MustRegister(logSortsDone)
	prometheus.MustRegister(logSortsFailed)
	prometheus.MustRegister(logSortLatency)
	prometheus.MustRegister(minorCompactionsInProgress)
	prometheus.MustRegister(minorCompactionsDone)
	prometheus.MustRegister(minorCompactionsFailed)
	prometheus.MustRegister(minorCompactionsLatency)
	prometheus.MustRegister(majorCompactionsInProgress)
	prometheus.MustRegister(majorCompactionsDone)
	prometheus.MustRegister(majorCompactionsFailed)
	prometheus.MustRegister(majorCompactionsLatency)
	prometheus.MustRegister(journalBytesWritten)
}

/*
sstableInfo contains all relevant data about a specific sstable, i.e. a
data record specified by the table name, end key and column family.
*/
type sstableInfo struct {
	// Pointers to the relevant table metadata.
	Descriptor *redcloud.SSTablePathDescription

	// Open writer to the journal.
	Journal *recordio.RecordWriter

	// Number of times the journal has been written to.
	JournalNumUses uint64

	/*
		Size of all data we think we have written to the journal, minor and
		major sstables.
	*/
	JournalSize      int64
	MinorSstableSize int64
	MajorSstableSize int64

	// Lock to ensure serialized access to the sstable journal.
	JournalLock fancylocking.MutexWithDeadline

	// Timestamp of when the most recent journal was created.
	JournalCreateTime time.Time

	// Lock ensuring that we don't logsort this twice at the same time.
	LogsortLock sync.Mutex

	/*
		Lock ensuring that there aren't multiple compactions running in
		parallel.
	*/
	CompactionLock sync.Mutex
}

/*
ServingRangeRegistry contains a list of all loaded tables on the server with
their corresponding column families.
*/
type ServingRangeRegistry struct {
	/*
		coveredRanges contains a list of all ranges covered by this tablet
		server, per table. It only serves to mark covered ranges.
	*/
	coveredRanges map[string][]*common.KeyRange

	/*
		columnFamilies maps table names to key ranges to column families to
		their ServerTabletMetadata.
		table -> end key -> column family -> metadata
	*/
	columnFamilies map[string]map[string]map[string]*sstableInfo

	/*
		columnFamilyMetadata points to the metadata for the specified column
		family in the specified table. table -> column family -> metadata.
	*/
	columnFamilyMetadata map[string]map[string]*redcloud.ColumnFamilyMetadata

	/*
		Path prefixes for each table as seen on the last reload of the
		metadata.
	*/
	prefixes map[string]string

	/*
		registryAccessLock controls read/write access to the registry to
		prevent trying to access key ranges while they are being written.
	*/
	registryAccessLock sync.RWMutex

	/*
		instance is the name of the red-cloud instance this server belongs
		to.
	*/
	instance string

	/*
		host is the externally available name of the host the data node is
		running on.
	*/
	host string

	// port is the externally available port the data node is running on.
	port uint16

	// etcd client to use for updating the table description.
	etcdClient *etcd.Client
}

/*
NewServingRangeRegistry instantiates a ServingRangeRegistry for this given
red-cloud instance on the specified host:port (which should point to the
server this is running on) and the given etcd client.
*/
func NewServingRangeRegistry(instance, host string, port uint16,
	etcdClient *etcd.Client) *ServingRangeRegistry {
	var rv = &ServingRangeRegistry{
		coveredRanges:        make(map[string][]*common.KeyRange),
		columnFamilies:       make(map[string]map[string]map[string]*sstableInfo),
		columnFamilyMetadata: make(map[string]map[string]*redcloud.ColumnFamilyMetadata),
		prefixes:             make(map[string]string),
		instance:             instance,
		host:                 host,
		port:                 port,
		etcdClient:           etcdClient,
	}
	go rv.findSortableLogs()
	return rv
}

/*
Instance returns the name of the red-cloud instance the node is serving.
*/
func (reg *ServingRangeRegistry) Instance() string {
	return reg.instance
}

/*
GetRanges gets a list of tables, start and end keys to display in debugging
information and dashboards.
*/
func (reg *ServingRangeRegistry) GetRanges() map[string][]*common.KeyRange {
	var rv = make(map[string][]*common.KeyRange)
	var k string
	var v []*common.KeyRange

	reg.registryAccessLock.RLock()
	defer reg.registryAccessLock.RUnlock()

	// Create a copy of the covered ranges and return it.
	for k, v = range reg.coveredRanges {
		rv[k] = v[:]
	}

	return rv
}

/*
LoadRange instructs the data node to start serving the given range.
*/
func (reg *ServingRangeRegistry) LoadRange(parentCtx context.Context,
	ranges *redcloud.RangeServingRequest) error {
	var ctx context.Context
	var span *trace.Span
	var kr = common.NewKeyRange(ranges.StartKey, ranges.EndKey)
	var knownRange *common.KeyRange
	var table = ranges.Table
	var endkey = string(ranges.EndKey)
	var pathdescs map[string]*redcloud.SSTablePathDescription
	var pathdesc *redcloud.SSTablePathDescription
	var ok bool

	ctx, span = trace.StartSpan(
		parentCtx, "red-cloud.ServingRangeRegistry/LoadRange")
	defer span.End()

	span.AddAttributes(
		trace.StringAttribute("table", table))

	if kr == nil {
		span.Annotate(nil, "Invalid Key Range specified")
		return fmt.Errorf("Invalid key range: %v to %v", ranges.StartKey,
			ranges.EndKey)
	}

	/*
		Any loading of data should be done before taking this lock in order to
		keep latency low.
	*/
	reg.registryAccessLock.Lock()
	defer reg.registryAccessLock.Unlock()

	// Attempt to updated table metadata in etcd.
	for {
		var etcdPath = common.EtcdTableConfigPath(reg.instance, table)
		var md redcloud.ServerTableMetadata
		var tabletMd *redcloud.ServerTabletMetadata
		var cfmd *redcloud.ColumnFamilyMetadata
		var gresp *etcd.GetResponse
		var presp *etcd.TxnResponse
		var ev *mvccpb.KeyValue
		var modrev int64
		var version int64
		var encData []byte
		var found bool
		var err error

		// Check whether the deadline is up or the RPC has been cancelled.
		if ctx.Err() != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", ctx.Err().Error()),
			}, "Context expired")
			return ctx.Err()
		}

		if gresp, err = reg.etcdClient.Get(
			ctx, etcdPath, etcd.WithLimit(1)); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
			}, "etcd communication error")
			return err
		}

		for _, ev = range gresp.Kvs {
			if err = proto.Unmarshal(ev.Value, &md); err != nil {
				/*
					TODO: undo the changes made in the previous part of this
					function.
				*/
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("error", err.Error()),
				}, "Metadata reading error")
				return fmt.Errorf(
					"Unable to parse %s as ServerTableMetadata protobuf at version %d",
					etcdPath, ev.Version)
			}

			modrev = ev.ModRevision
			version = ev.Version
		}

		/*
			While loading the tablet, take note of the configured path prefix.
			If a change has occurred, it will be applied gradually as tablets
			get rewritten.
		*/
		reg.prefixes[table] = md.TableMd.PathPrefix

		// Find an existing tablet matching the specified range.
		for _, tabletMd = range md.Tablet {
			// TODO: maybe care about overlap somehow?
			if kr.Contains(tabletMd.StartKey) && (kr.Contains(tabletMd.EndKey) ||
				bytes.Equal(tabletMd.EndKey, kr.EndKey)) {
				tabletMd.Host = reg.host
				tabletMd.Port = int32(reg.port)
				found = true
			}
		}

		/*
			Create a new metadata entry for the range which is apparently
			not covered at this time.
		*/
		if !found {
			tabletMd = new(redcloud.ServerTabletMetadata)
			tabletMd.StartKey = kr.StartKey
			tabletMd.EndKey = kr.EndKey
			tabletMd.Host = reg.host
			tabletMd.Port = int32(reg.port)
			md.Tablet = append(md.Tablet, tabletMd)
		}

		pathdescs = make(map[string]*redcloud.SSTablePathDescription)
		for _, pathdesc = range tabletMd.SstablePath {
			pathdescs[pathdesc.ColumnFamily] = pathdesc
		}

		// Add all missing column families to the metadata descriptor.
		for _, cfmd = range md.TableMd.ColumnFamily {
			if _, ok = pathdescs[cfmd.Name]; !ok {
				tabletMd.SstablePath = append(
					tabletMd.SstablePath,
					&redcloud.SSTablePathDescription{
						ColumnFamily: cfmd.Name,
					})
			}
		}

		if encData, err = proto.Marshal(&md); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
			}, "Metadata marshalling error")
			return fmt.Errorf("Unable to encode updated metadata: %s", err)
		}

		if presp, err = reg.etcdClient.Txn(ctx).If(
			etcd.Compare(etcd.ModRevision(etcdPath), "=", modrev),
			etcd.Compare(etcd.Version(etcdPath), "=", version)).Then(
			etcd.OpPut(etcdPath, string(encData))).Commit(); err != nil {
			log.Printf("Error committing update to %s: %s", etcdPath, err)
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
			}, "Error committing metadata update")
			continue
		}

		// Successful update -> we're out.
		if presp.Succeeded {
			break
		}
	}

	// Make sure our data structures are complete.
	if _, ok = reg.coveredRanges[table]; !ok {
		reg.coveredRanges[table] = make([]*common.KeyRange, 0)
	}

	for _, knownRange = range reg.coveredRanges[table] {
		if knownRange.ContainsRangeFully(kr) {
			// We already have the range loaded.
			return nil
		}
	}

	reg.coveredRanges[table] = append(reg.coveredRanges[table], kr)

	if _, ok = reg.columnFamilies[table]; !ok {
		reg.columnFamilies[table] =
			make(map[string]map[string]*sstableInfo)
	}
	if _, ok = reg.columnFamilies[table][endkey]; !ok {
		reg.columnFamilies[table][endkey] = make(map[string]*sstableInfo)
	}

	for _, pathdesc = range pathdescs {
		// Load the tablet for the specified end key.
		reg.columnFamilies[table][endkey][pathdesc.ColumnFamily] = &sstableInfo{
			Descriptor:        pathdesc,
			Journal:           nil,
			JournalCreateTime: time.Now(),
			JournalLock:       fancylocking.NewMutexWithDeadline(),
		}
	}

	return nil
}

/*
UnloadRange instructs the tablet server to stop serving the specified tablets
in the specified table.
*/
func (reg *ServingRangeRegistry) UnloadRange(parentCtx context.Context,
	req *redcloud.RangeReleaseRequest) (
	resp *redcloud.RangeReleaseResponse, err error) {
	var ctx context.Context
	var span *trace.Span
	var ncr []*common.KeyRange
	var kr *common.KeyRange
	var table = req.Table
	var endkey = string(req.EndKey)
	var cfs map[string]*sstableInfo
	var sstp *sstableInfo
	var ok bool

	ctx, span = trace.StartSpan(
		parentCtx, "red-cloud.ServingRangeRegistry/UnloadRange")
	defer span.End()

	span.AddAttributes(
		trace.StringAttribute("table", table))

	resp = new(redcloud.RangeReleaseResponse)
	if kr = common.NewKeyRange(req.StartKey, req.EndKey); kr != nil {
		span.Annotate(nil, "Invalid Key Range specified")
		return resp, fmt.Errorf("Invalid key range: %v to %v", req.StartKey,
			req.EndKey)
	}

	reg.registryAccessLock.Lock()

	// First, let's check the RPC wasn't cancelled in the meantime.
	if ctx.Err() != nil {
		reg.registryAccessLock.Unlock()
		span.Annotate([]trace.Attribute{
			trace.StringAttribute("error", ctx.Err().Error()),
		}, "Context expired")
		return resp, ctx.Err()
	}

	if _, ok = reg.columnFamilies[table]; !ok {
		reg.registryAccessLock.Unlock()
		span.Annotate(nil, "Table not covered by server")
		return resp, fmt.Errorf("Table %s not covered", table)
	}
	if _, ok = reg.columnFamilies[table][endkey]; !ok {
		reg.registryAccessLock.Unlock()
		span.Annotate(nil, "End key not covered by server")
		return resp, fmt.Errorf("End key %s in table %s not covered",
			table, endkey)
	}

	cfs = reg.columnFamilies[table][endkey]
	delete(reg.columnFamilies[table], endkey)

	for _, kr = range reg.coveredRanges[table] {
		if bytes.Compare(kr.EndKey, req.StartKey) <= 0 ||
			bytes.Compare(kr.StartKey, req.EndKey) >= 0 {
			ncr = append(ncr, kr)
		}
	}
	reg.coveredRanges[table] = ncr
	reg.registryAccessLock.Unlock()

	for _, sstp = range cfs {
		resp.Paths = append(resp.Paths, sstp.Descriptor)
	}

	return resp, nil
}

/*
ServesRange determines whether this server is serving the specified range.
*/
func (reg *ServingRangeRegistry) ServesRange(tableName string,
	startKey, endKey []byte) bool {
	var kr = common.NewKeyRange(startKey, endKey)
	var tableRanges []*common.KeyRange
	var other *common.KeyRange
	var ok bool

	reg.registryAccessLock.RLock()
	defer reg.registryAccessLock.RUnlock()

	// Determine whether we know about the table at all.
	if tableRanges, ok = reg.coveredRanges[tableName]; !ok {
		return false
	}

	/*
		If we find a range which overlaps with the requested range in some
		way, we cover the range and should handle requests for it.
	*/
	for _, other = range tableRanges {
		if kr.ContainsRange(other) {
			return true
		}
	}

	return false
}

/*
Lock locks the entire range registry for reading, i.e. no modifications can
be made until the lock is released. (Other reads may happen however.)
*/
func (reg *ServingRangeRegistry) Lock() {
	reg.registryAccessLock.RLock()
}

/*
Unlock releases the previously obtained read lock on the range registry,
potentially allowing future modifications to interfere with further
processing. (This will not affect other read locks being held.)
*/
func (reg *ServingRangeRegistry) Unlock() {
	reg.registryAccessLock.RUnlock()
}

/*
getInfoDescriptor looks up the info descriptor of the one specific key.
*/
func (reg *ServingRangeRegistry) getInfoDescriptor(
	ctx context.Context, table, cf string, key []byte) (*sstableInfo, error) {
	var kr = common.NewKeyRange(key, key)
	var infos []*sstableInfo
	var err error

	if infos, err = reg.getInfoDescriptors(ctx, table, cf, kr); err != nil {
		return nil, err
	}

	if len(infos) != 1 {
		return nil, fmt.Errorf("Found %d info records for %v, expected 1",
			len(infos), key)
	}

	return infos[0], nil
}

/*
getInfoDescriptors looks up the info descriptors for the specified table.
*/
func (reg *ServingRangeRegistry) getInfoDescriptors(
	ctx context.Context, table, cf string, keyRange *common.KeyRange) (
	[]*sstableInfo, error) {
	var infos []*sstableInfo
	var info *sstableInfo
	var families map[string]*sstableInfo
	var krs []*common.KeyRange
	var kr *common.KeyRange
	var endKeys []string
	var endKey string
	var ok, found bool

	if krs, ok = reg.coveredRanges[table]; !ok {
		// We don't know the table.
		return []*sstableInfo{}, common.ErrTabletNotLoaded
	}

	// We cover at least part of the table.
	for _, kr = range krs {
		if kr.ContainsRange(keyRange) {
			endKeys = append(endKeys, string(kr.EndKey))
			found = true
			break
		}
	}
	if !found {
		return []*sstableInfo{}, common.ErrTabletNotLoaded
	}

	for _, endKey = range endKeys {
		/*
			Just for safety, we should check that we know about some of the
			specified endKeys.
			table -> end key -> column family -> metadata
		*/
		if families, ok = reg.columnFamilies[table][endKey]; ok {
			// Check that we also know about the specified column family.
			if info, ok = families[cf]; ok {
				infos = append(infos, info)
			}
		}
	}
	if len(infos) == 0 {
		return []*sstableInfo{}, common.ErrColumnFamilyNotConfigured
	}

	return infos, nil
}

/*
GetSSTablePathDescription looks up the SSTablePathDescriptions associated
with the given table, column family and key range. If the triplet cannot be
found and/or is not handled by this server, nil is returned.
*/
func (reg *ServingRangeRegistry) GetSSTablePathDescription(
	ctx context.Context, table, cf string, keyRange *common.KeyRange) (
	[]*redcloud.SSTablePathDescription, error) {
	var descs []*redcloud.SSTablePathDescription
	var infos []*sstableInfo
	var info *sstableInfo
	var err error

	/*
		The caller probably holds a read lock but we should probably protect
		our access below for cleanliness.
	*/
	reg.registryAccessLock.RLock()
	defer reg.registryAccessLock.RUnlock()

	if infos, err = reg.getInfoDescriptors(
		ctx, table, cf, keyRange); err != nil {
		return nil, err
	}

	for _, info = range infos {
		descs = append(descs, info.Descriptor)
	}

	return descs, nil
}

/*
internalCreateJournalWriter does the work of creating a new journal writer
for the specified table, cf and key tuple and registers it. It assumes the
corresponding write locks are already held.
*/
func (reg *ServingRangeRegistry) internalCreateJournalWriter(
	ctx context.Context, table, cf string, key []byte, info *sstableInfo) (
	*recordio.RecordWriter, error) {
	var journalPath string
	var journalFile filesystem.WriteCloser
	var now = time.Now()
	var u *url.URL
	var err error

	journalPath = fmt.Sprintf("%s/%s", reg.prefixes[table], storage.MakePath(
		reg.instance, table, cf, key, now, storage.SSTableLevelJOURNAL))

	if u, err = url.Parse(journalPath); err != nil {
		return nil, err
	}

	if journalFile, err = filesystem.OpenWriter(ctx, u); err != nil {
		return nil, err
	}

	// Attempt to updated table metadata in etcd.
	for {
		var etcdPath = common.EtcdTableConfigPath(reg.instance, table)
		var md redcloud.ServerTableMetadata
		var tabletMd *redcloud.ServerTabletMetadata
		var pathDescription *redcloud.SSTablePathDescription
		var gresp *etcd.GetResponse
		var presp *etcd.TxnResponse
		var ev *mvccpb.KeyValue
		var modrev int64
		var version int64
		var encData []byte
		var found bool
		var err error

		// Check whether the deadline is up or the RPC has been cancelled.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if gresp, err = reg.etcdClient.Get(
			ctx, etcdPath, etcd.WithLimit(1)); err != nil {
			return nil, err
		}

		for _, ev = range gresp.Kvs {
			if err = proto.Unmarshal(ev.Value, &md); err != nil {
				/*
					TODO: undo the changes made in the previous part of this
					function.
				*/
				return nil, fmt.Errorf(
					"Unable to parse %s as ServerTableMetadata protobuf at version %d",
					etcdPath, ev.Version)
			}

			modrev = ev.ModRevision
			version = ev.Version
		}

		/*
			While loading the tablet, take note of the configured path prefix.
			If a change has occurred, it will be applied gradually as tablets
			get rewritten.
		*/
		reg.prefixes[table] = md.TableMd.PathPrefix

		// Find an existing tablet containing the specified key.
		for _, tabletMd = range md.Tablet {
			var kr = common.NewKeyRange(tabletMd.StartKey, tabletMd.EndKey)
			if kr.Contains(key) {
				for _, pathDescription = range tabletMd.SstablePath {
					if pathDescription.ColumnFamily == cf {
						pathDescription.RelevantJournalPaths = append(
							pathDescription.RelevantJournalPaths, journalPath)
						found = true
					}
				}
			}
		}

		/*
			We could not find the record in the authoritative etcd mapping so
			we could not register the new journal. So, we cannot use it.
		*/
		if !found {
			filesystem.Remove(ctx, u)
			return nil, common.ErrTabletNotLoaded
		}

		if encData, err = proto.Marshal(&md); err != nil {
			return nil, fmt.Errorf("Unable to encode updated metadata: %s", err)
		}

		if presp, err = reg.etcdClient.Txn(ctx).If(
			etcd.Compare(etcd.ModRevision(etcdPath), "=", modrev),
			etcd.Compare(etcd.Version(etcdPath), "=", version)).Then(
			etcd.OpPut(etcdPath, string(encData))).Commit(); err != nil {
			log.Printf("Error committing update to %s: %s", etcdPath, err)
			continue
		}

		// Successful update -> we're out.
		if presp.Succeeded {
			break
		}
	}

	info.Descriptor.RelevantJournalPaths = append(
		info.Descriptor.RelevantJournalPaths, journalPath)

	return recordio.NewRecordWriter(journalFile), nil
}

/*
CreateJournalWriter creates a new journal writer for the specified sstable
range. The writer will write to a newly created journal file. The new journal
file will be registered with red-cloud.
*/
func (reg *ServingRangeRegistry) CreateJournalWriter(
	ctx context.Context, table, cf string, key []byte) (
	*recordio.RecordWriter, error) {
	var writer *recordio.RecordWriter
	var info *sstableInfo
	var err error

	if info, err = reg.getInfoDescriptor(ctx, table, cf, key); err != nil {
		return nil, err
	}

	if !info.JournalLock.LockWithContext(ctx) {
		return nil, ctx.Err()
	}
	defer info.JournalLock.Unlock()

	if writer, err = reg.internalCreateJournalWriter(
		ctx, table, cf, key, info); err != nil {
		return nil, err
	}

	info.Journal = writer
	info.JournalNumUses = 0
	info.JournalCreateTime = time.Now()
	return writer, nil
}

/*
GetJournalWriter determines a currently open writer to the journal file of
the specified sstable range and returns it. If no writer is currently open,
a new journal file will be created and registered and a writer to that file
will be returned. Therefore, it is expected that a write lock on the
registry be held.
*/
func (reg *ServingRangeRegistry) GetJournalWriter(
	ctx context.Context, table, cf string, key []byte) (
	*recordio.RecordWriter, error) {
	var writer *recordio.RecordWriter
	var info *sstableInfo
	var err error

	if info, err = reg.getInfoDescriptor(ctx, table, cf, key); err != nil {
		return nil, err
	}

	if !info.JournalLock.LockWithContext(ctx) {
		return nil, ctx.Err()
	}
	defer info.JournalLock.Unlock()

	if info.Journal != nil {
		info.JournalNumUses++
		return info.Journal, nil
	}

	if writer, err = reg.internalCreateJournalWriter(
		ctx, table, cf, key, info); err != nil {
		return nil, err
	}

	info.Journal = writer
	info.JournalNumUses = 1
	info.JournalCreateTime = time.Now()
	return writer, nil
}

/*
ReportJournalUsage increments the journal usage counter by the specified
number of bytes to get some rough idea of how much data was written to the
journal.
*/
func (reg *ServingRangeRegistry) ReportJournalUsage(
	ctx context.Context, table, cf string, key []byte, size int64) {
	var info *sstableInfo
	var err error

	if info, err = reg.getInfoDescriptor(ctx, table, cf, key); err != nil {
		return
	}

	if info != nil {
		info.JournalSize += size
	}
}

/*
ColumnFamilySlice is a list of ColumnFamily protocol buffers which should
be sorted by key and timestamp.
*/
type ColumnFamilySlice []*redcloud.ColumnFamily

/*
Len returns the number of entries currently in the column family slice.
*/
func (c ColumnFamilySlice) Len() int {
	return len(c)
}

/*
Less determines whether the key, column name or time stamp of the i-th
element in the ColumnFamilySlice is considered to be less than that of
the j-th element.
*/
func (c ColumnFamilySlice) Less(i, j int) bool {
	return bytes.Compare(c[i].Key, c[j].Key) < 0
}

/*
Swap exchanges the i-th and j-th element of the ColumnFamilySlice.
*/
func (c ColumnFamilySlice) Swap(i, j int) {
	var cf = c[j]
	c[j] = c[i]
	c[i] = cf
}

/*
sortLogs runs a log sorting process on the non-sorted journal files in the
given sstableInfo. The wait group wg is invoked once processing has
finished in order to restricit the number of log sorting processes which
can run in parallel.
*/
func (reg *ServingRangeRegistry) sortLogs(
	parentCtx context.Context, table string, endKey []byte, cf string,
	info *sstableInfo, wg *sync.WaitGroup) {
	var paths []string
	var childCtx context.Context
	var ctx context.Context
	var span *trace.Span
	var path string
	var started = time.Now()
	var startedMs = int64(started.Nanosecond() / 1000000)
	var err error

	ctx, span = trace.StartSpan(
		parentCtx, "red-cloud.ServingRangeRegistry/sortLogs")
	defer span.End()

	/*
		Ensure that there's nobody else logsorting this tablet. Not because
		that would be a problem, but because that would be pointless.
	*/
	info.LogsortLock.Lock()
	defer info.LogsortLock.Unlock()

	logSortsInProgress.Inc()
	defer logSortsInProgress.Add(-1)
	defer wg.Done()

	span.AddAttributes(
		trace.Int64Attribute(
			"relevant-jornal-files",
			int64(len(info.Descriptor.RelevantJournalPaths))))

	// Only logsort if there are logs available for sorting.
	if len(info.Descriptor.RelevantJournalPaths) == 0 {
		span.Annotate(nil, "No logs available for sorting")
		return
	}

	// Collect a list of all logs which have not been sorted yet.
	for _, path = range info.Descriptor.RelevantJournalPaths[:len(info.Descriptor.RelevantJournalPaths)] {
		if !strings.HasSuffix(path, ".sorted") {
			paths = append(paths, path)
		}
	}

	span.AddAttributes(
		trace.Int64Attribute(
			"unsorted-jornal-files", int64(len(paths))))

	if len(paths) == 0 {
		// All logs are already sorted.
		span.Annotate(nil, "All logs already sorted")
		return
	}

	if info.JournalNumUses > 0 {
		var cancel context.CancelFunc
		// Create a new journal for all future writes.
		childCtx, cancel = context.WithTimeout(ctx, time.Minute)
		if _, err = reg.CreateJournalWriter(
			childCtx, table, cf, endKey); err != nil {
			span.Annotate(nil, "All logs already sorted")
			log.Print("Unable to create a new journal writer for ", table,
				":", endKey, ":", cf, ": ", err)
			logSortsFailed.Inc()
			cancel()
			return
		}
		cancel()
	} else {
		if paths = paths[:len(paths)-1]; len(paths) == 0 {
			// Only unsorted log has 0 length.
			span.Annotate(nil, "Unsorted log has length 0")
			return
		}
	}

	for _, path = range paths {
		var inurl, outurl *url.URL
		var input filesystem.ReadCloser
		var output filesystem.WriteCloser
		var journalWriter *recordio.RecordWriter
		var cfs ColumnFamilySlice
		var columnFamily = new(redcloud.ColumnFamily)
		var reader *recordio.RecordReader
		var cancel context.CancelFunc
		var i int

		if inurl, err = url.Parse(path); err != nil {
			span.Annotate(nil, "All logs already sorted")
			log.Print("Unable to parse log path ", path, ": ", err)
			logSortsFailed.Inc()
			return
		}

		childCtx, cancel = context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()
		if input, err = filesystem.OpenReader(childCtx, inurl); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("path", path),
				trace.StringAttribute("error", err.Error()),
			}, "Unable to open log for sorting")
			log.Print("Unable to open ", path, ": ", err)
			logSortsFailed.Inc()
			return
		}
		reader = recordio.NewRecordReader(input)

		for {
			if err = reader.ReadMessage(ctx, columnFamily); err == io.EOF {
				break
			} else if err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("path", path),
					trace.StringAttribute("error", err.Error()),
				}, "Unable to open log for sorting")
				log.Print("Unable to read record from ", path, ": ", err)
				logSortsFailed.Inc()
				return
			}
			cfs = append(cfs, columnFamily)
		}

		// Sort all the read records.
		sort.Stable(cfs)

		// Merge all records touching the same key.
		for i < len(cfs) {
			var j int

			// Eliminate all entries whose TTL has expired.
			for j < len(cfs[i].ColumnSet) {
				var cs = cfs[i].ColumnSet[j]
				var k int

				for k < len(cs.Column) {
					var col = cs.Column[k]
					if col.Ttl > 0 && col.Timestamp+col.Ttl < startedMs {
						cs.Column = append(cs.Column[:k], cs.Column[k+1:]...)
					} else {
						k++
					}
				}

				if len(cs.Column) == 0 {
					cfs[i].ColumnSet = append(cfs[i].ColumnSet[:j],
						cfs[i].ColumnSet[j+1:]...)
				} else {
					j++
				}
			}

			if len(cfs[i].ColumnSet) == 0 {
				// Remove now-empty write.
				cfs = append(cfs[:i], cfs[i+1:]...)
				continue
			}

			/*
				We cannot merge the 0th record into the previous one, so
				skip the rest here.
			*/
			if i == 0 {
				i++
				continue
			}

			if bytes.Equal(cfs[i].Key, cfs[i-1].Key) {
				// Merge columns for the same row. Shorten list by 1.
				cfs[i-1].ColumnSet = append(cfs[i-1].ColumnSet,
					cfs[i].ColumnSet...)
				cfs = append(cfs[:i], cfs[i+1:]...)
			} else {
				// Go to next record.
				i++
			}
		}

		outurl = inurl.ResolveReference(
			&url.URL{Path: inurl.Path + ".sorted"})
		if output, err = filesystem.OpenWriter(ctx, outurl); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("path", outurl.String()),
				trace.StringAttribute("error", err.Error()),
			}, "Unable to open sorted log output file")
			log.Print("Unable to create output file ", outurl, ": ", err)
			logSortsFailed.Inc()
			return
		}

		journalWriter = recordio.NewRecordWriter(output)

		for _, columnFamily = range cfs {
			if err = journalWriter.WriteMessage(ctx, columnFamily); err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("path", outurl.String()),
					trace.StringAttribute("error", err.Error()),
				}, "Unable to open sorted log output file")
				log.Print("Error writing rewritten column family to ", outurl,
					": ", err)
				filesystem.Remove(ctx, outurl)
				logSortsFailed.Inc()
				return
			}
		}

		if err = output.Close(ctx); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("path", outurl.String()),
				trace.StringAttribute("error", err.Error()),
			}, "Unable to close sorted log output file")
			log.Print("Cannot close sorted log output ", outurl, ": ", err)
			filesystem.Remove(ctx, outurl)
			logSortsFailed.Inc()
			return
		}

		if !info.JournalLock.LockWithContext(ctx) {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("path", outurl.String()),
				trace.StringAttribute("error", ctx.Err().Error()),
			}, "Context expired")
			log.Print("Context expired while sorting logs")
			return
		}

		// Update etcd with the updated journal list.
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Minute)
		for {
			var etcdPath = common.EtcdTableConfigPath(reg.instance, table)
			var md redcloud.ServerTableMetadata
			var tabletMd *redcloud.ServerTabletMetadata
			var pathDescription *redcloud.SSTablePathDescription
			var gresp *etcd.GetResponse
			var presp *etcd.TxnResponse
			var ev *mvccpb.KeyValue
			var modrev int64
			var version int64
			var encData []byte

			if ctx.Err() != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("path", etcdPath),
					trace.StringAttribute("error", ctx.Err().Error()),
				}, "Context expired while updating table metadata")
				log.Print("Error writing back updated table ", table, " to ",
					etcdPath, ": ", err)
				info.JournalLock.Unlock()
				filesystem.Remove(ctx, outurl)
				logSortsFailed.Inc()
				return
			}

			if gresp, err = reg.etcdClient.Get(
				ctx, etcdPath, etcd.WithLimit(1)); err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("path", etcdPath),
					trace.StringAttribute("error", err.Error()),
				}, "Error fetching table metadata from etcd")
				log.Print("Error fetching metadata ", etcdPath,
					" from etcd: ", err)
				info.JournalLock.Unlock()
				filesystem.Remove(ctx, outurl)
				logSortsFailed.Inc()
				return
			}

			for _, ev = range gresp.Kvs {
				if err = proto.Unmarshal(ev.Value, &md); err != nil {
					span.Annotate([]trace.Attribute{
						trace.StringAttribute("path", etcdPath),
						trace.StringAttribute("error", err.Error()),
					}, "Table metadata corrupted")
					log.Print("Error parsing table metadata in ", etcdPath,
						": ", err)
					info.JournalLock.Unlock()
					filesystem.Remove(ctx, outurl)
					logSortsFailed.Add(1)
					return
				}

				modrev = ev.ModRevision
				version = ev.Version
			}

			/*
				While loading the tablet, take note of the configured path
				prefix. If a change has occurred, it will be applied gradually
				as tablets get rewritten.
			*/
			reg.prefixes[table] = md.TableMd.PathPrefix

			// Find an existing tablet containing the specified key.
			for _, tabletMd = range md.Tablet {
				var kr = common.NewKeyRange(tabletMd.StartKey, tabletMd.EndKey)
				if kr.Contains(endKey) || bytes.Equal(endKey, tabletMd.EndKey) {
					for _, pathDescription = range tabletMd.SstablePath {
						if pathDescription.ColumnFamily == cf {
							var idx int
							var cpath string
							for idx, cpath = range pathDescription.RelevantJournalPaths {
								if cpath == path {
									pathDescription.RelevantJournalPaths[idx] = outurl.String()
								}
							}
						}
					}
				}
			}

			if encData, err = proto.Marshal(&md); err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("path", etcdPath),
					trace.StringAttribute("error", err.Error()),
				}, "Error encoding table metadata")
				log.Print("Error encoding updated metadata for ", table,
					": ", err)
				info.JournalLock.Unlock()
				filesystem.Remove(ctx, outurl)
				logSortsFailed.Add(1)
				return
			}

			if presp, err = reg.etcdClient.Txn(ctx).If(
				etcd.Compare(etcd.ModRevision(etcdPath), "=", modrev),
				etcd.Compare(etcd.Version(etcdPath), "=", version)).Then(
				etcd.OpPut(etcdPath, string(encData))).Commit(); err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("path", etcdPath),
					trace.StringAttribute("error", err.Error()),
				}, "Unable to update table metadata")
				log.Printf("Error committing update to %s: %s", etcdPath, err)
				continue
			}

			// Successful update -> we're out.
			if presp.Succeeded {
				break
			}
		}

		// Update our internal account of the journal list.
		for i = range info.Descriptor.RelevantJournalPaths {
			if info.Descriptor.RelevantJournalPaths[i] == path {
				info.Descriptor.RelevantJournalPaths[i] = outurl.String()
			}
		}

		info.JournalLock.Unlock()

		// Overwrite path of sorted log.
		if err = filesystem.Remove(ctx, inurl); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("path", inurl.String()),
				trace.StringAttribute("error", err.Error()),
			}, "")
			log.Print("Unable to delete original unsorted input sstable ",
				inurl, ": ", err)
		}
	}

	span.Annotate(nil, "Log sorting done")
	logSortsDone.Inc()
	logSortLatency.Add(time.Now().Sub(started).Seconds())
}

/*
mergeSstables does the actual merging of two sstable readers a and b
into a new sstable out.

TODO: delete data which has expired (TTL) or is explicitly deleted.
*/
func (reg *ServingRangeRegistry) mergeSstables(
	ctx context.Context, a, b *sstable.Reader, out *sstable.Writer) (
	int64, error) {
	var aHasData, bHasData bool
	var aKey, bKey string
	var aData, bData redcloud.ColumnFamily
	var size int64
	var err error

	// Fill in initial data.
	if aKey, err = a.ReadNextProto(ctx, &aData); err != nil && err != io.EOF {
		return 0, err
	} else if err != io.EOF {
		aHasData = true
	}

	if bKey, err = b.ReadNextProto(ctx, &bData); err != nil && err != io.EOF {
		return 0, err
	} else if err != io.EOF {
		bHasData = true
	}

	for aHasData || bHasData {
		if aHasData && bHasData && bytes.Equal(aData.Key, bData.Key) {
			// If both sstables have data for the same key, merge it.
			var columnSets = make(map[string]*redcloud.ColumnSet)
			var cset *redcloud.ColumnSet

			for _, cset = range append(aData.ColumnSet, bData.ColumnSet...) {
				var ok bool

				if _, ok = columnSets[cset.Name]; ok {
					columnSets[cset.Name].Column = append(
						columnSets[cset.Name].Column, cset.Column...)
				} else {
					columnSets[cset.Name] = cset
				}
			}

			// Treat data in a as original, fetch next result from b.
			bData.Reset()
			if bKey, err = b.ReadNextProto(ctx, &bData); err == io.EOF {
				bHasData = false
			} else if err != nil {
				return 0, err
			}
		} else if !bHasData || (aHasData && bytes.Compare(aData.Key, bData.Key) < 0) {
			// Data in a is next.
			if err = out.WriteProto(ctx, aKey, &aData); err != nil {
				return 0, err
			}
			size += int64(proto.Size(&aData))

			aData.Reset()
			if aKey, err = a.ReadNextProto(ctx, &aData); err == io.EOF {
				aHasData = false
			} else if err != nil {
				return 0, err
			}
		} else if bHasData {
			// Data in b is next.
			if err = out.WriteProto(ctx, bKey, &bData); err != nil {
				return 0, err
			}
			size += int64(proto.Size(&bData))

			bData.Reset()
			if bKey, err = b.ReadNextProto(ctx, &bData); err == io.EOF {
				bHasData = false
			} else if err != nil {
				return 0, err
			}
		}
	}

	return size, nil
}

/*
mergeLogsToSstable gets input from a sorted journal log and merges it with
the data in the given sstable into a new sstable out.

TODO: delete data which has expired (TTL) or is explicitly deleted.
*/
func (reg *ServingRangeRegistry) mergeLogsToSstable(
	ctx context.Context, a *sstable.Reader, b *recordio.RecordReader,
	out *sstable.Writer) (int64, error) {
	var aHasData, bHasData bool
	var aData, bData redcloud.ColumnFamily
	var size int64
	var err error

	// Fill in initial data.
	if _, err = a.ReadNextProto(ctx, &aData); err != nil && err != io.EOF {
		return 0, err
	} else if err != io.EOF {
		aHasData = true
	}

	if err = b.ReadMessage(ctx, &bData); err != nil && err != io.EOF {
		return 0, err
	} else if err != io.EOF {
		bHasData = true
	}

	for aHasData || bHasData {
		if aHasData && bHasData && bytes.Equal(aData.Key, bData.Key) {
			// If both sstables have data for the same key, merge it.
			var columnSets = make(map[string]*redcloud.ColumnSet)
			var cset *redcloud.ColumnSet

			for _, cset = range append(aData.ColumnSet, bData.ColumnSet...) {
				var ok bool

				if _, ok = columnSets[cset.Name]; ok {
					columnSets[cset.Name].Column = append(
						columnSets[cset.Name].Column, cset.Column...)
				} else {
					columnSets[cset.Name] = cset
				}
			}

			// Treat data in a as original, fetch next result from b.
			bData.Reset()
			if err = b.ReadMessage(ctx, &bData); err == io.EOF {
				bHasData = false
			} else if err != nil {
				return 0, err
			}
		} else if !bHasData || (aHasData && bytes.Compare(aData.Key, bData.Key) < 0) {
			// Data in a is next.
			if err = out.WriteProto(
				ctx, string(aData.Key), &aData); err != nil {
				return 0, err
			}
			size += int64(proto.Size(&aData))

			aData.Reset()
			if _, err = a.ReadNextProto(ctx, &aData); err == io.EOF {
				aHasData = false
			} else if err != nil {
				return 0, err
			}
		} else if bHasData {
			// Data in b is next.
			err = out.WriteProto(ctx, string(bData.Key), &bData)
			if err != nil {
				return 0, err
			}
			size += int64(proto.Size(&bData))

			bData.Reset()
			if err = b.ReadMessage(ctx, &bData); err == io.EOF {
				bHasData = false
			} else if err != nil {
				return 0, err
			}
		}
	}

	return size, nil
}

/*
majorCompaction runs a major compaction on two sorted recordio files to
produce a third one which will be registered as the official major sstable.
*/
func (reg *ServingRangeRegistry) majorCompaction(
	parentCtx context.Context, table string, endKey []byte, cf string,
	info *sstableInfo, wg *sync.WaitGroup) {
	var ctx context.Context
	var span *trace.Span
	var started = time.Now()
	var sstPath string
	var a, b *sstable.Reader
	var usst, uidx *url.URL
	var out *sstable.Writer
	var outsst, outidx filesystem.WriteCloser
	var size int64
	var err error

	ctx, span = trace.StartSpan(
		parentCtx, "red-cloud.ServingRangeRegistry/majorCompaction")
	defer span.End()

	span.AddAttributes(
		trace.StringAttribute("table", table),
		trace.StringAttribute("column-family", cf))

	/*
		Ensure that there's nobody else compacting this tablet. Not because
		that would be a problem, but because that would be pointless.
	*/
	info.CompactionLock.Lock()
	defer info.CompactionLock.Unlock()

	majorCompactionsInProgress.Inc()
	defer majorCompactionsInProgress.Add(-1)
	sstPath = fmt.Sprintf("%s/%s", reg.prefixes[table], storage.MakePath(
		reg.instance, table, cf, endKey, started, storage.SSTableLevelMAJOR))
	defer wg.Done()

	if len(info.Descriptor.MinorSstablePath) > 0 {
		var ssta, sstb filesystem.ReadCloser
		var origMinorSst, origMinorIdx, origMajorSst, origMajorIdx *url.URL

		// Create target sstable.
		if usst, err = url.Parse(sstPath + ".sst"); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
				trace.StringAttribute("path", sstPath),
			}, "Error generating target sstable URL")
			log.Printf("Error parsing sstable destination URL %s: %s", sstPath, err)
			majorCompactionsFailed.Inc()
			return
		}

		if outsst, err = filesystem.OpenWriter(ctx, usst); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
				trace.StringAttribute("path", usst.String()),
			}, "Error creating target sstable writer")
			log.Printf("Error opening destination sstable %s: %s", sstPath, err)
			majorCompactionsFailed.Inc()
			return
		}

		if uidx, err = url.Parse(sstPath + ".idx"); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
				trace.StringAttribute("path", sstPath),
			}, "Error generating target sstable index URL")
			log.Printf("Error parsing index destination URL %s: %s", sstPath, err)
			majorCompactionsFailed.Inc()
			filesystem.Remove(ctx, usst)
			return
		}

		if outidx, err = filesystem.OpenWriter(ctx, uidx); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
				trace.StringAttribute("path", uidx.String()),
			}, "Error creating target sstable index writer")
			log.Printf("Error opening destination index %s: %s", sstPath, err)
			majorCompactionsFailed.Inc()
			filesystem.Remove(ctx, usst)
			return
		}

		out = sstable.NewIndexedWriter(ctx, outsst, outidx,
			sstable.IndexType_EVERY_N, 32)

		if origMinorSst, err = url.Parse(
			info.Descriptor.MinorSstablePath + ".sst"); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
				trace.StringAttribute("path", info.Descriptor.MinorSstablePath),
			}, "Error generating original minor sstable URL")
			log.Printf("Error creating URL for minor sstable %s: %s",
				info.Descriptor.MinorSstablePath, err)
			majorCompactionsFailed.Inc()
			filesystem.Remove(ctx, usst)
			filesystem.Remove(ctx, uidx)
			return
		}
		if origMinorIdx, err = url.Parse(
			info.Descriptor.MinorSstablePath + ".idx"); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
				trace.StringAttribute("path", info.Descriptor.MinorSstablePath),
			}, "Error generating original minor sstable index URL")
			log.Printf("Error creating URL for minor sstable index %s: %s",
				info.Descriptor.MinorSstablePath, err)
		}
		if ssta, err = filesystem.OpenReader(ctx, origMinorSst); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
				trace.StringAttribute("path", uidx.String()),
			}, "Error creating original minor sstable reader")
			log.Printf("Error opening minor sstable %s: %s",
				info.Descriptor.MinorSstablePath, err)
			majorCompactionsFailed.Inc()
			filesystem.Remove(ctx, usst)
			filesystem.Remove(ctx, uidx)
			return
		}
		defer ssta.Close(ctx)

		a = sstable.NewReader(ssta)

		if len(info.Descriptor.MajorSstablePath) > 0 {
			if origMajorSst, err = url.Parse(
				info.Descriptor.MajorSstablePath + ".sst"); err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("error", err.Error()),
					trace.StringAttribute("path", info.Descriptor.MajorSstablePath),
				}, "Error generating original minor sstable URL")
				log.Printf("Error creating URL for major sstable %s: %s",
					info.Descriptor.MajorSstablePath, err)
				majorCompactionsFailed.Inc()
				filesystem.Remove(ctx, usst)
				filesystem.Remove(ctx, uidx)
				return
			}
			if origMajorIdx, err = url.Parse(
				info.Descriptor.MajorSstablePath + ".idx"); err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("error", err.Error()),
					trace.StringAttribute("path", info.Descriptor.MajorSstablePath),
				}, "Error generating original minor sstable index URL")
				log.Printf("Error creating URL for major sstable index %s: %s",
					info.Descriptor.MajorSstablePath, err)
			}
			if sstb, err = filesystem.OpenReader(
				ctx, origMajorSst); err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("error", err.Error()),
					trace.StringAttribute("path", uidx.String()),
				}, "Error creating original major sstable reader")
				log.Printf("Error opening major sstable %s: %s",
					info.Descriptor.MajorSstablePath, err)
				majorCompactionsFailed.Inc()
				filesystem.Remove(ctx, usst)
				filesystem.Remove(ctx, uidx)
				return
			}
			defer sstb.Close(ctx)

			b = sstable.NewReader(sstb)
		} else {
			b = sstable.NewReader(internal.NewAnonymousFile())
		}

		if size, err = reg.mergeSstables(ctx, a, b, out); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
			}, "Error merging minor and major sstables")
			log.Printf("Error sorting sstable %s: %s", sstPath, err)
			majorCompactionsFailed.Inc()
			filesystem.Remove(ctx, usst)
			filesystem.Remove(ctx, uidx)
			return
		}

		if !info.JournalLock.LockWithContext(ctx) {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("path", usst.String()),
				trace.StringAttribute("error", ctx.Err().Error()),
			}, "Context expired")
			log.Print("Context expired while merging sstable")
			return
		}

		// Update etcd with the updated journal list.
		ctx, _ = context.WithTimeout(ctx, 10*time.Minute)
		for {
			var etcdPath = common.EtcdTableConfigPath(reg.instance, table)
			var md redcloud.ServerTableMetadata
			var tabletMd *redcloud.ServerTabletMetadata
			var pathDescription *redcloud.SSTablePathDescription
			var gresp *etcd.GetResponse
			var presp *etcd.TxnResponse
			var ev *mvccpb.KeyValue
			var modrev int64
			var version int64
			var encData []byte

			if ctx.Err() != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("error", ctx.Err().Error()),
				}, "Context expired")
				log.Print("Error writing back updated table ", table, " to ",
					etcdPath, ": ", err)
				info.JournalLock.Unlock()
				filesystem.Remove(ctx, usst)
				filesystem.Remove(ctx, uidx)
				minorCompactionsFailed.Inc()
				return
			}

			if gresp, err = reg.etcdClient.Get(
				ctx, etcdPath, etcd.WithLimit(1)); err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("error", err.Error()),
					trace.StringAttribute("path", etcdPath),
				}, "etcd communication error")
				log.Print("Error fetching metadata ", etcdPath,
					" from etcd: ", err)
				info.JournalLock.Unlock()
				filesystem.Remove(ctx, usst)
				filesystem.Remove(ctx, uidx)
				minorCompactionsFailed.Inc()
				return
			}

			for _, ev = range gresp.Kvs {
				if err = proto.Unmarshal(ev.Value, &md); err != nil {
					span.Annotate([]trace.Attribute{
						trace.StringAttribute("error", err.Error()),
						trace.StringAttribute("path", etcdPath),
					}, "Table metadata corrupted")
					log.Print("Error parsing table metadata in ", etcdPath,
						": ", err)
					info.JournalLock.Unlock()
					filesystem.Remove(ctx, usst)
					filesystem.Remove(ctx, uidx)
					minorCompactionsFailed.Inc()
					return
				}

				modrev = ev.ModRevision
				version = ev.Version
			}

			/*
				While loading the tablet, take note of the configured path
				prefix. If a change has occurred, it will be applied gradually
				as tablets get rewritten.
			*/
			reg.prefixes[table] = md.TableMd.PathPrefix

			// Find an existing tablet containing the specified key.
			for _, tabletMd = range md.Tablet {
				var kr = common.NewKeyRange(tabletMd.StartKey, tabletMd.EndKey)
				if kr.Contains(endKey) || bytes.Equal(endKey, tabletMd.EndKey) {
					for _, pathDescription = range tabletMd.SstablePath {
						if pathDescription.ColumnFamily == cf {
							pathDescription.MajorSstablePath = sstPath
							pathDescription.MinorSstablePath = ""
							pathDescription.MajorSstableSize = size
							pathDescription.MinorSstableSize = 0
						}
					}
				}
			}

			if encData, err = proto.Marshal(&md); err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("error", err.Error()),
				}, "Error marshalling tablet metadata")
				log.Print("Error encoding updated metadata for ", table,
					": ", err)
				info.JournalLock.Unlock()
				filesystem.Remove(ctx, usst)
				filesystem.Remove(ctx, uidx)
				minorCompactionsFailed.Inc()
				return
			}

			if presp, err = reg.etcdClient.Txn(ctx).If(
				etcd.Compare(etcd.ModRevision(etcdPath), "=", modrev),
				etcd.Compare(etcd.Version(etcdPath), "=", version)).Then(
				etcd.OpPut(etcdPath, string(encData))).Commit(); err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("error", err.Error()),
					trace.StringAttribute("path", etcdPath),
				}, "Error updating table metadata")
				log.Printf("Error committing update to %s: %s", etcdPath, err)
				continue
			}

			// Successful update -> we're out.
			if presp.Succeeded {
				break
			}
		}

		// Update our internal account of the sstable paths.
		info.Descriptor.MajorSstablePath = sstPath
		info.Descriptor.MinorSstablePath = ""
		info.MajorSstableSize = size
		info.MinorSstableSize = 0

		info.JournalLock.Unlock()

		if origMajorSst != nil {
			filesystem.Remove(ctx, origMajorSst)
		}
		if origMajorIdx != nil {
			filesystem.Remove(ctx, origMajorIdx)
		}
		if origMinorSst != nil {
			filesystem.Remove(ctx, origMinorSst)
		}
		if origMinorIdx != nil {
			filesystem.Remove(ctx, origMinorIdx)
		}
	}
}

/*
minorCompaction runs a major compaction on two sorted recordio files to
produce a third one which will be registered as the official minor sstable.
*/
func (reg *ServingRangeRegistry) minorCompaction(
	parentCtx context.Context, table string, endKey []byte, cf string,
	info *sstableInfo, wg *sync.WaitGroup) {
	var ctx context.Context
	var span *trace.Span
	var started = time.Now()
	var sstPath string
	var a *sstable.Reader
	var b *recordio.RecordReader
	var usst, uidx *url.URL
	var out *sstable.Writer
	var outsst, outidx filesystem.WriteCloser
	var size int64
	var err error
	var p string
	var ssta, sstb filesystem.ReadCloser
	var origMinorSst, origMinorIdx *url.URL
	var sortedLogPaths []string
	var sortedLogPath string
	var sortedLogU *url.URL

	ctx, span = trace.StartSpan(
		parentCtx, "red-cloud.ServingRangeRegistry/minorCompaction")
	defer span.End()

	span.AddAttributes(
		trace.StringAttribute("table", table),
		trace.StringAttribute("column-family", cf))

	/*
		Ensure that there's nobody else compacting this tablet. Not because
		that would be a problem, but because that would be pointless.
	*/
	info.CompactionLock.Lock()
	defer info.CompactionLock.Unlock()

	minorCompactionsInProgress.Inc()
	defer minorCompactionsInProgress.Add(-1)
	defer wg.Done()

	for _, p = range info.Descriptor.RelevantJournalPaths {
		if strings.HasSuffix(p, ".sorted") {
			sortedLogPaths = append(sortedLogPaths, p)
		}
	}

	// Wait for logsorting to give us some input.
	if len(sortedLogPaths) == 0 {
		span.Annotate(nil, "No sortable logs")
		return
	}

	span.AddAttributes(
		trace.Int64Attribute("num-sorted-logs-input", int64(len(sortedLogPath))))

	for _, sortedLogPath = range sortedLogPaths {
		var i int
		var p string

		if len(info.Descriptor.MinorSstablePath) > 0 {
			if origMinorSst, err = url.Parse(
				info.Descriptor.MinorSstablePath + ".sst"); err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("error", err.Error()),
					trace.StringAttribute("path", info.Descriptor.MinorSstablePath),
				}, "Error generating original minor sstable URL")
				log.Printf("Error creating URL for minor sstable %s: %s",
					info.Descriptor.MinorSstablePath, err)
				minorCompactionsFailed.Inc()
				filesystem.Remove(ctx, usst)
				filesystem.Remove(ctx, uidx)
				return
			}
			if origMinorIdx, err = url.Parse(
				info.Descriptor.MinorSstablePath + ".idx"); err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("error", err.Error()),
					trace.StringAttribute("path", info.Descriptor.MinorSstablePath),
				}, "Error generating original minor sstable index URL")
				log.Printf("Error creating URL for minor sstable index %s: %s",
					info.Descriptor.MinorSstablePath, err)
			}
			if ssta, err = filesystem.OpenReader(
				ctx, origMinorSst); err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("error", err.Error()),
					trace.StringAttribute("path", origMinorSst.String()),
				}, "Error opening original minor sstable for reading")
				log.Printf("Error opening minor sstable %s: %s",
					info.Descriptor.MinorSstablePath, err)
				minorCompactionsFailed.Inc()
				filesystem.Remove(ctx, usst)
				filesystem.Remove(ctx, uidx)
				return
			}
			defer ssta.Close(ctx)

			a = sstable.NewReader(ssta)
		} else {
			a = sstable.NewReader(internal.NewAnonymousFile())
		}

		if sortedLogU, err = url.Parse(sortedLogPath); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
				trace.StringAttribute("path", sortedLogPath),
			}, "Error generating sorted lag URL")
			log.Printf("Error creating URL for log %s: %s", sortedLogPath, err)
			minorCompactionsFailed.Inc()
			filesystem.Remove(ctx, usst)
			filesystem.Remove(ctx, uidx)
			return
		}
		if sstb, err = filesystem.OpenReader(ctx, sortedLogU); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
				trace.StringAttribute("path", sortedLogU.String()),
			}, "Error generating sorted lag URL")
			log.Printf("Error opening log %s: %s", sortedLogPath, err)
			minorCompactionsFailed.Inc()
			filesystem.Remove(ctx, usst)
			filesystem.Remove(ctx, uidx)
			return
		}
		defer sstb.Close(ctx)

		b = recordio.NewRecordReader(sstb)

		sstPath = fmt.Sprintf("%s/%s", reg.prefixes[table], storage.MakePath(
			reg.instance, table, cf, endKey, time.Now(),
			storage.SSTableLevelMINOR))

		// Create target sstable.
		if usst, err = url.Parse(sstPath + ".sst"); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
				trace.StringAttribute("path", sortedLogU.String()),
			}, "Error generating sorted log URL")
			log.Printf("Error parsing sstable destination URL %s: %s", sstPath, err)
			majorCompactionsFailed.Inc()
			return
		}

		if outsst, err = filesystem.OpenWriter(ctx, usst); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
				trace.StringAttribute("path", usst.String()),
			}, "Error opening output sstable writer")
			log.Printf("Error opening destination sstable %s: %s", sstPath, err)
			majorCompactionsFailed.Inc()
			return
		}

		if uidx, err = url.Parse(sstPath + ".idx"); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
				trace.StringAttribute("path", usst.String()),
			}, "Error generating output sstable index URL")
			log.Printf("Error parsing index destination URL %s: %s", sstPath, err)
			majorCompactionsFailed.Inc()
			filesystem.Remove(ctx, usst)
			return
		}

		if outidx, err = filesystem.OpenWriter(ctx, uidx); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
				trace.StringAttribute("path", usst.String()),
			}, "Error opening output sstable index writer")
			log.Printf("Error opening destination index %s: %s", sstPath, err)
			majorCompactionsFailed.Inc()
			filesystem.Remove(ctx, usst)
			return
		}

		out = sstable.NewIndexedWriter(ctx, outsst, outidx,
			sstable.IndexType_EVERY_N, 32)

		if size, err = reg.mergeLogsToSstable(ctx, a, b, out); err != nil {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
			}, "Error merging log to sstable")
			log.Printf("Error sorting sstable %s: %s", sstPath, err)
			minorCompactionsFailed.Inc()
			filesystem.Remove(ctx, usst)
			filesystem.Remove(ctx, uidx)
			return
		}

		if !info.JournalLock.LockWithContext(ctx) {
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("path", usst.String()),
				trace.StringAttribute("error", ctx.Err().Error()),
			}, "Context expired")
			log.Print("Context expired while merging sstable")
			return
		}

		// Update etcd with the updated journal list.
		ctx, _ = context.WithTimeout(ctx, 10*time.Minute)
		for {
			var etcdPath = common.EtcdTableConfigPath(reg.instance, table)
			var md redcloud.ServerTableMetadata
			var tabletMd *redcloud.ServerTabletMetadata
			var pathDescription *redcloud.SSTablePathDescription
			var gresp *etcd.GetResponse
			var presp *etcd.TxnResponse
			var ev *mvccpb.KeyValue
			var modrev int64
			var version int64
			var encData []byte

			if ctx.Err() != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("error", ctx.Err().Error()),
				}, "Context expired")
				log.Print("Error writing back updated table ", table, " to ",
					etcdPath, ": ", err)
				info.JournalLock.Unlock()
				minorCompactionsFailed.Inc()
				filesystem.Remove(ctx, usst)
				filesystem.Remove(ctx, uidx)
				return
			}

			if gresp, err = reg.etcdClient.Get(
				ctx, etcdPath, etcd.WithLimit(1)); err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("error", err.Error()),
				}, "Error communicating with etcd")
				log.Print("Error fetching metadata ", etcdPath,
					" from etcd: ", err)
				info.JournalLock.Unlock()
				minorCompactionsFailed.Inc()
				filesystem.Remove(ctx, usst)
				filesystem.Remove(ctx, uidx)
				return
			}

			for _, ev = range gresp.Kvs {
				if err = proto.Unmarshal(ev.Value, &md); err != nil {
					span.Annotate([]trace.Attribute{
						trace.StringAttribute("error", err.Error()),
					}, "Table metadata corrupted")
					log.Print("Error parsing table metadata in ", etcdPath,
						": ", err)
					info.JournalLock.Unlock()
					minorCompactionsFailed.Inc()
					filesystem.Remove(ctx, usst)
					filesystem.Remove(ctx, uidx)
					return
				}

				modrev = ev.ModRevision
				version = ev.Version
			}

			/*
				While loading the tablet, take note of the configured path
				prefix. If a change has occurred, it will be applied gradually
				as tablets get rewritten.
			*/
			reg.prefixes[table] = md.TableMd.PathPrefix

			// Find an existing tablet containing the specified key.
			for _, tabletMd = range md.Tablet {
				var kr = common.NewKeyRange(tabletMd.StartKey, tabletMd.EndKey)
				if kr.Contains(endKey) || bytes.Equal(endKey, tabletMd.EndKey) {
					for _, pathDescription = range tabletMd.SstablePath {
						if pathDescription.ColumnFamily == cf {
							var idx int
							var cpath string
							pathDescription.MinorSstablePath = sstPath
							pathDescription.MinorSstableSize = size
							for idx, cpath = range pathDescription.RelevantJournalPaths {
								if cpath == sortedLogPath {
									pathDescription.RelevantJournalPaths = append(
										pathDescription.RelevantJournalPaths[:idx],
										pathDescription.RelevantJournalPaths[idx+1:]...)
								}
							}
						}
					}
				}
			}

			if encData, err = proto.Marshal(&md); err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("error", err.Error()),
				}, "Error marshalling table metadata")
				log.Print("Error encoding updated metadata for ", table,
					": ", err)
				info.JournalLock.Unlock()
				minorCompactionsFailed.Inc()
				filesystem.Remove(ctx, usst)
				filesystem.Remove(ctx, uidx)
				return
			}

			if presp, err = reg.etcdClient.Txn(ctx).If(
				etcd.Compare(etcd.ModRevision(etcdPath), "=", modrev),
				etcd.Compare(etcd.Version(etcdPath), "=", version)).Then(
				etcd.OpPut(etcdPath, string(encData))).Commit(); err != nil {
				span.Annotate([]trace.Attribute{
					trace.StringAttribute("error", err.Error()),
				}, "Error updating table metadata (temporary)")
				log.Printf("Error committing update to %s: %s", etcdPath, err)
				continue
			}

			// Successful update -> we're out.
			if presp.Succeeded {
				break
			}
		}

		// Update our internal account of the journal list.
		info.Descriptor.MinorSstablePath = sstPath
		info.MinorSstableSize = size
		for i, p = range info.Descriptor.RelevantJournalPaths {
			if p == sortedLogPath {
				info.Descriptor.RelevantJournalPaths = append(
					info.Descriptor.RelevantJournalPaths[:i],
					info.Descriptor.RelevantJournalPaths[i+1:]...)
			}
		}

		info.JournalLock.Unlock()

		if origMinorSst != nil {
			filesystem.Remove(ctx, origMinorSst)
		}
		if origMinorIdx != nil {
			filesystem.Remove(ctx, origMinorIdx)
		}
		if sortedLogU != nil {
			filesystem.Remove(ctx, sortedLogU)
		}
	}

	minorCompactionsDone.Inc()
	minorCompactionsLatency.Add(time.Now().Sub(started).Seconds())
}

/*
findSortableLogs finds tablets which require log sorting and fires off
up to 5 sortLogs threads at a time. Once they all finished, more logs
in need of sorting are sought.
*/
func (reg *ServingRangeRegistry) findSortableLogs() {
	// Wait a little for things to settle before starting the first round.
	time.Sleep(30 * time.Second)
	for {
		// We don't run until at least an hour has passed since the last run.
		var nextSortRun = time.Now().Add(time.Hour)
		var table string
		var tablets map[string]map[string]*sstableInfo
		var wg sync.WaitGroup
		var weThinkAreRunning int
		var ctx context.Context
		var span *trace.Span

		ctx, span = trace.StartSpan(
			context.Background(), "red-cloud.ServingRangeRegistry/findSortableLogs")

		for table, tablets = range reg.columnFamilies {
			var cfs map[string]*sstableInfo
			var endKey string

			for endKey, cfs = range tablets {
				var cf string
				var sst *sstableInfo

				for cf, sst = range cfs {
					wg.Add(1)
					weThinkAreRunning++
					go reg.sortLogs(ctx, table, []byte(endKey), cf, sst, &wg)

					if weThinkAreRunning == 5 {
						wg.Wait()
						weThinkAreRunning = 0
					}
				}
			}
		}

		// Run minor compactions.
		for table, tablets = range reg.columnFamilies {
			var cfs map[string]*sstableInfo
			var endKey string

			for endKey, cfs = range tablets {
				var cf string
				var sst *sstableInfo

				for cf, sst = range cfs {
					wg.Add(1)
					weThinkAreRunning++
					go reg.minorCompaction(ctx, table, []byte(endKey), cf, sst, &wg)

					if weThinkAreRunning == 5 {
						wg.Wait()
						weThinkAreRunning = 0
					}
				}
			}
		}

		/*
			Run major compactions.
			TODO: only run major compactions on tablets which haven't been
			compacted in a while.
		*/
		for table, tablets = range reg.columnFamilies {
			var cfs map[string]*sstableInfo
			var endKey string

			for endKey, cfs = range tablets {
				var cf string
				var sst *sstableInfo

				for cf, sst = range cfs {
					wg.Add(1)
					weThinkAreRunning++
					go reg.majorCompaction(ctx, table, []byte(endKey), cf, sst, &wg)

					if weThinkAreRunning == 5 {
						wg.Wait()
						weThinkAreRunning = 0
					}
				}
			}
		}

		span.AddAttributes(trace.Int64Attribute(
			"num-logsorting-ops", int64(weThinkAreRunning)))

		if weThinkAreRunning > 0 {
			wg.Wait()
		}
		span.End()

		// Wait until we should run for the next time.
		if time.Now().Before(nextSortRun) {
			time.Sleep(nextSortRun.Sub(time.Now()))
		}
	}
}
