package storage

import (
	"context"
	"io"
	"net/url"
	"sort"

	"github.com/childoftheuniverse/filesystem"
	"github.com/childoftheuniverse/red-cloud"
	"github.com/childoftheuniverse/red-cloud/common"
	"github.com/childoftheuniverse/sstable"
	"github.com/prometheus/client_golang/prometheus"
	"go.opencensus.io/trace"
)

var sstableLookups = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "storage",
	Name:      "num_sstable_lookups",
	Help:      "Number of lookups of data in sstable files",
})
var sstableLookupNumResults = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "storage",
	Name:      "num_sstable_lookup_results",
	Help:      "Number of results from lookups of data in sstable files",
})
var sstableLookupErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "storage",
	Name:      "num_sstable_lookup_errors",
	Help:      "Number of errors when looking up data in sstable files",
}, []string{"error_class"})

func init() {
	prometheus.MustRegister(sstableLookups)
	prometheus.MustRegister(sstableLookupNumResults)
	prometheus.MustRegister(sstableLookupErrors)
}

/*
LookupInSstable finds all records in the specified key range in the specified
file matching the selected columns. Deadlines and cancellations from the
context "ctx" are taken into account. Any results will be reported through
the channel "results", errors will be reported through "errors", and "done"
will be marked as soon as processing the file has completed.

columns is expected to be sorted (see sort.Strings).
*/
func LookupInSstable(parentCtx context.Context, path string, columns []string,
	kr *common.KeyRange, results chan *redcloud.ColumnFamily,
	errors chan error, done chan struct{}) {
	var ctx context.Context
	var span *trace.Span
	var cf *redcloud.ColumnFamily
	var reader *sstable.Reader
	var sst, idx filesystem.ReadCloser
	var sstu, idxu *url.URL
	var key string
	var err error

	ctx, span = trace.StartSpan(parentCtx, "red-cloud.storage/LookupInSstable")
	defer span.End()
	sstableLookups.Inc()

	span.AddAttributes(trace.StringAttribute("path", path))

	/*
		As soon as this function exits, successul or not, there's no point in
		waiting for the result any longer.
	*/
	defer markDone(done)

	if sstu, err = url.Parse(path + ".sst"); err != nil {
		span.AddAttributes(trace.StringAttribute("error", err.Error()))
		span.Annotate(nil, "Invalid sstable path URL")
		sstableLookupErrors.With(
			prometheus.Labels{"error_class": "parse_file_name"}).Inc()
		errors <- err
		return
	}
	if sst, err = filesystem.OpenReader(ctx, sstu); err != nil {
		span.AddAttributes(trace.StringAttribute("error", err.Error()))
		span.Annotate(nil, "Unable to open sstable for reading")
		sstableLookupErrors.With(
			prometheus.Labels{"error_class": "open_sstable_readonly"}).Inc()
		errors <- err
		return
	}
	defer sst.Close(ctx)

	if idxu, err = url.Parse(path + ".idx"); err != nil {
		span.AddAttributes(trace.StringAttribute("error", err.Error()))
		span.Annotate(nil, "Invalid sstable index path URL")
		sstableLookupErrors.With(
			prometheus.Labels{"error_class": "parse_file_name"}).Inc()
		errors <- err
		return
	}
	if idx, err = filesystem.OpenReader(ctx, idxu); err != nil {
		span.AddAttributes(trace.StringAttribute("error", err.Error()))
		span.Annotate(nil, "Unable to open sstable index for reading")
		sstableLookupErrors.With(
			prometheus.Labels{"error_class": "open_sstable_readonly"}).Inc()
		errors <- err
		return
	}
	defer idx.Close(ctx)

	if reader, err = sstable.NewReaderWithIdx(
		ctx, sst, idx, false); err != nil {
		errors <- err
		return
	}

	cf = new(redcloud.ColumnFamily)

	if key, err = reader.ReadSubsequentProto(
		ctx, string(kr.StartKey), cf); err == io.EOF {
		return
	} else if err != nil {
		errors <- err
		return
	}
	for {
		var rcf *redcloud.ColumnFamily
		var cs *redcloud.ColumnSet

		if !kr.Contains([]byte(key)) {
			// The key is no longer in our range, so we don't care about it.
			return
		}

		/*
			Check if our result is still interesting to the caller.
			There's no need to report the error as the caller will know about
			it from the context as well.
		*/
		if ctx.Err() != nil {
			return
		}

		for _, cs = range cf.ColumnSet {
			var off int
			off = sort.SearchStrings(columns, cs.Name)
			if off >= 0 && off < len(columns) && columns[off] == cs.Name {
				/*
					Collect all matching columns in a special return
					ColumnFamily.
				*/
				if rcf == nil {
					rcf = new(redcloud.ColumnFamily)
					rcf.Key = cf.Key
				}

				rcf.ColumnSet = append(rcf.ColumnSet, cs)
			}
		}

		if rcf != nil {
			sstableLookupNumResults.Inc()
			results <- rcf
		}

		if key, err = reader.ReadNextProto(ctx, cf); err == io.EOF {
			return
		} else if err != nil {
			errors <- err
			return
		}
	}
}
