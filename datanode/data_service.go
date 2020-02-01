package main

import (
	"context"
	"strings"

	"github.com/childoftheuniverse/recordio"
	"github.com/childoftheuniverse/red-cloud"
	"github.com/childoftheuniverse/red-cloud/common"
	"github.com/childoftheuniverse/red-cloud/storage"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"go.opencensus.io/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var numRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "datanode",
	Name:      "num_requests",
	Help:      "Number of RPCs received by the data node",
}, []string{"service", "method"})
var numErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "datanode",
	Name:      "num_errors",
	Help:      "Number of RPCs received to the data node",
}, []string{"service", "method", "error_class"})

func init() {
	prometheus.MustRegister(numRequests)
	prometheus.MustRegister(numErrors)
}

/*
DataNodeService implements the DataNodeService API object which offers actual
access to data contained inside the database.
*/
type DataNodeService struct {
	rangeRegistry *ServingRangeRegistry
}

/*
NewDataNodeService instantiates a data node service around the specified
range registry.
*/
func NewDataNodeService(rangeRegistry *ServingRangeRegistry) *DataNodeService {
	return &DataNodeService{
		rangeRegistry: rangeRegistry,
	}
}

/*
Get fetches an individual data cell from the involved sstables.
*/
func (dns *DataNodeService) Get(
	parentCtx context.Context, req *redcloud.GetRequest) (
	*redcloud.Column, error) {
	var ctx context.Context
	var span *trace.Span
	var results = make(chan *redcloud.ColumnFamily)
	var errors = make(chan error)
	var doners = make(chan struct{})
	var allErrors []string
	var result *redcloud.Column
	var sstPaths []*redcloud.SSTablePathDescription
	var sstPath *redcloud.SSTablePathDescription
	var path string
	var numRequired int
	var numDone int
	var err error

	ctx, span = trace.StartSpan(parentCtx, "red-cloud.DataNodeService/Get")
	defer span.End()

	span.AddAttributes(
		trace.StringAttribute("table", req.Table),
		trace.StringAttribute("column-family", req.ColumnFamily))

	numRequests.With(prometheus.Labels{
		"service": "DataNodeService",
		"method":  "Get",
	}).Inc()

	dns.rangeRegistry.Lock()
	defer dns.rangeRegistry.Unlock()

	/*
		Try to figure out where the sstable files for the given path are
		stored.
	*/
	sstPaths, err = dns.rangeRegistry.GetSSTablePathDescription(
		ctx, req.Table, req.ColumnFamily, common.NewKeyRange(req.Key, req.Key))
	if err != nil {
		numErrors.With(prometheus.Labels{
			"service":     "DataNodeService",
			"method":      "Get",
			"error_class": "get_sstable_path_description",
		}).Inc()
		span.AddAttributes(trace.StringAttribute("error", err.Error()))
		span.Annotate(nil, "Error fetching sstable path")
		return nil, err
	}

	for _, sstPath = range sstPaths {
		if len(sstPath.MajorSstablePath) > 0 {
			numRequired++
			go storage.LookupInSstable(ctx, sstPath.MajorSstablePath,
				[]string{req.Column}, common.NewKeyRange(req.Key, req.Key),
				results, errors, doners)
		}

		if len(sstPath.MinorSstablePath) > 0 {
			numRequired++
			go storage.LookupInSstable(ctx, sstPath.MinorSstablePath,
				[]string{req.Column}, common.NewKeyRange(req.Key, req.Key),
				results, errors, doners)
		}

		numRequired += len(sstPath.RelevantJournalPaths)
		for _, path = range sstPath.RelevantJournalPaths {
			go storage.LookupInJournal(ctx, path, []string{req.Column},
				common.NewKeyRange(req.Key, req.Key), results, errors, doners)
		}
	}

	span.AddAttributes(
		trace.Int64Attribute("files-touched", int64(numRequired)))

	if numRequired == 0 {
		numErrors.With(prometheus.Labels{
			"service":     "DataNodeService",
			"method":      "Get",
			"error_class": "no_data_sources",
		}).Inc()
		span.Annotate(nil, "No data sources found")
		// No sources means no data.
		return nil, grpc.Errorf(codes.NotFound,
			"No data sources in column family %s for key: empty",
			req.ColumnFamily)
	}

	/*
		Collect matching data from all sources (as above) and determine the
		latest result. The result will be sent back to the client.
	*/
	for {
		var cf *redcloud.ColumnFamily
		var cs *redcloud.ColumnSet
		var col *redcloud.Column
		var found bool
		var err error

		select {
		case err = <-errors:
			allErrors = append(allErrors, err.Error())
		case <-doners:
			numDone++
			if numDone == numRequired {
				found = true
				break
			}
		case cf = <-results:
			for _, cs = range cf.ColumnSet {
				for _, col = range cs.Column {
					if result == nil {
						result = col
					} else if result.Timestamp < col.Timestamp {
						result = col
					}
				}
			}
		}

		if found {
			break
		}
	}

	if len(allErrors) > 0 {
		numErrors.With(prometheus.Labels{
			"service":     "DataNodeService",
			"method":      "Get",
			"error_class": "lookup_errors",
		}).Inc()
		span.AddAttributes(
			trace.Int64Attribute("num-errors", int64(len(allErrors))))
		span.Annotate(nil, "Read errors encountered")
		return result, grpc.Errorf(codes.Internal,
			strings.Join(allErrors, "; "))
	}

	if result == nil {
		span.Annotate(nil, "Result empty")
		return nil, grpc.Errorf(codes.NotFound,
			"No column matched requirements")
	}

	return result, nil
}

/*
GetRange fetches all data cells from the involved sstables which are
matching the specified criteria.
*/
func (dns *DataNodeService) GetRange(
	req *redcloud.GetRangeRequest, resp redcloud.DataNodeService_GetRangeServer) error {
	var parentCtx context.Context
	var ctx context.Context
	var span *trace.Span
	var cancel context.CancelFunc
	var results = make(chan *redcloud.ColumnFamily)
	var errors = make(chan error)
	var doners = make(chan struct{})
	var allErrors []string
	var sstPaths []*redcloud.SSTablePathDescription
	var sstPath *redcloud.SSTablePathDescription
	var path string
	var numRequired int
	var numDone int
	var numFound int64
	var err error

	parentCtx, cancel = context.WithCancel(resp.Context())
	ctx, span = trace.StartSpan(parentCtx, "red-cloud.DataNodeService/GetRange")
	defer span.End()

	span.AddAttributes(
		trace.StringAttribute("table", req.Table),
		trace.StringAttribute("column-family", req.ColumnFamily))

	numRequests.With(prometheus.Labels{
		"service": "DataNodeService",
		"method":  "GetRange",
	}).Inc()

	dns.rangeRegistry.Lock()
	defer dns.rangeRegistry.Unlock()

	/*
		Try to figure out where the sstable files for the given path are
		stored.
	*/
	if sstPaths, err = dns.rangeRegistry.GetSSTablePathDescription(
		ctx, req.Table, req.ColumnFamily,
		common.NewKeyRange(req.StartKey, req.EndKey)); err != nil {
		numErrors.With(prometheus.Labels{
			"service":     "DataNodeService",
			"method":      "GetRange",
			"error_class": "get_sstable_path_description",
		}).Inc()
		span.AddAttributes(trace.StringAttribute("error", err.Error()))
		span.Annotate(nil, "Error fetching sstable path")
		return err
	}

	for _, sstPath = range sstPaths {
		if len(sstPath.MajorSstablePath) > 0 {
			numRequired++
			go storage.LookupInSstable(ctx, sstPath.MajorSstablePath,
				req.Column, common.NewKeyRange(req.StartKey, req.EndKey),
				results, errors, doners)
		}

		if len(sstPath.MinorSstablePath) > 0 {
			numRequired++
			go storage.LookupInSstable(ctx, sstPath.MinorSstablePath,
				req.Column, common.NewKeyRange(req.StartKey, req.EndKey),
				results, errors, doners)
		}

		numRequired += len(sstPath.RelevantJournalPaths)
		for _, path = range sstPath.RelevantJournalPaths {
			go storage.LookupInJournal(ctx, path,
				req.Column, common.NewKeyRange(req.StartKey, req.EndKey),
				results, errors, doners)
		}
	}

	span.AddAttributes(
		trace.Int64Attribute("files-touched", int64(numRequired)))

	if numRequired == 0 {
		numErrors.With(prometheus.Labels{
			"service":     "DataNodeService",
			"method":      "GetRange",
			"error_class": "no_data_sources",
		}).Inc()
		span.Annotate(nil, "No data sources found")
		// No sources means no data.
		return grpc.Errorf(codes.NotFound,
			"No data sources in column family %s for key: empty",
			req.ColumnFamily)
	}

	/*
		Determine all matching columns from all selected sources (see above)
		and stream them back to the client.
	*/
	for {
		var cf *redcloud.ColumnFamily
		var cs *redcloud.ColumnSet
		var col *redcloud.Column
		var complete bool
		var err error

		select {
		case err = <-errors:
			allErrors = append(allErrors, err.Error())
		case <-doners:
			numDone++
			if numDone == numRequired {
				complete = true
				break
			}
		case cf = <-results:
			for _, cs = range cf.ColumnSet {
				var rcs = new(redcloud.ColumnSet)
				rcs.Name = cs.Name
				for _, col = range cs.Column {
					if (req.MinTimestamp == 0 || col.Timestamp >= req.MinTimestamp) &&
						(req.MaxTimestamp == 0 || col.Timestamp <= req.MaxTimestamp) {
						rcs.Column = append(rcs.Column, col)
						numFound++
					}
				}

				if len(rcs.Column) > 0 {
					resp.Send(rcs)
				}
			}

			if req.MaxResults > 0 && numFound >= req.MaxResults {
				complete = true
				cancel()
			}
		}

		if complete {
			break
		}
	}

	if len(allErrors) > 0 {
		numErrors.With(prometheus.Labels{
			"service":     "DataNodeService",
			"method":      "GetRange",
			"error_class": "lookup_errors",
		}).Inc()
		span.AddAttributes(
			trace.Int64Attribute("num-errors", int64(len(allErrors))))
		span.Annotate(nil, "Read errors encountered")
		return grpc.Errorf(codes.Internal,
			strings.Join(allErrors, "; "))
	}

	return nil
}

/*
Insert is used to set a very specific data cell to the specified value.
*/
func (dns *DataNodeService) Insert(
	parentCtx context.Context, req *redcloud.InsertRequest) (
	*redcloud.Empty, error) {
	var ctx context.Context
	var span *trace.Span
	var cf redcloud.ColumnFamily
	var cs *redcloud.ColumnSet
	var writer *recordio.RecordWriter
	var err error

	ctx, span = trace.StartSpan(parentCtx, "red-cloud.DataNodeService/Insert")
	defer span.End()

	span.AddAttributes(
		trace.StringAttribute("table", req.Table),
		trace.StringAttribute("column-family", req.ColumnFamily))

	numRequests.With(prometheus.Labels{
		"service": "DataNodeService",
		"method":  "Insert",
	}).Inc()

	cs = new(redcloud.ColumnSet)
	cs.Name = req.ColumnName
	cs.Column = append(cs.Column, req.Column)

	cf.Key = req.Key
	cf.ColumnSet = append(cf.ColumnSet, cs)

	// Get the journal writer for the corresponding data set.
	dns.rangeRegistry.Lock()
	defer dns.rangeRegistry.Unlock()

	if writer, err = dns.rangeRegistry.GetJournalWriter(
		ctx, req.Table, req.ColumnFamily, req.Key); err != nil {
		numErrors.With(prometheus.Labels{
			"service":     "DataNodeService",
			"method":      "Insert",
			"error_class": "get_journal_writer_failed",
		}).Inc()
		span.AddAttributes(trace.StringAttribute("error", err.Error()))
		span.Annotate(nil, "Error setting up journal writer")
		return &redcloud.Empty{}, err
	}

	if err = writer.WriteMessage(ctx, &cf); err != nil {
		numErrors.With(prometheus.Labels{
			"service":     "DataNodeService",
			"method":      "Insert",
			"error_class": "write_message_failed",
		}).Inc()
		span.AddAttributes(trace.StringAttribute("error", err.Error()))
		span.Annotate(nil, "Error writing inserted data to journal")
	} else {
		dns.rangeRegistry.ReportJournalUsage(
			ctx, req.Table, req.ColumnFamily, req.Key, int64(proto.Size(&cf)))
	}
	return &redcloud.Empty{}, err
}
