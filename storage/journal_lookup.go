package storage

import (
	"context"
	"io"
	"net/url"
	"sort"

	"github.com/childoftheuniverse/filesystem"
	"github.com/childoftheuniverse/recordio"
	"github.com/childoftheuniverse/red-cloud"
	"github.com/childoftheuniverse/red-cloud/common"
	"github.com/prometheus/client_golang/prometheus"
	"go.opencensus.io/trace"
)

func markDone(done chan struct{}) {
	done <- struct{}{}
}

var journalLookups = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "storage",
	Name:      "num_journal_lookups",
	Help:      "Number of lookups of data in journal files",
})
var journalLookupNumResults = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "storage",
	Name:      "num_journal_lookup_results",
	Help:      "Number of results from lookups of data in journal files",
})
var journalLookupErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "red_cloud",
	Subsystem: "storage",
	Name:      "num_journal_lookup_errors",
	Help:      "Number of errors when looking up data in journal files",
}, []string{"error_class"})

func init() {
	prometheus.MustRegister(journalLookups)
	prometheus.MustRegister(journalLookupNumResults)
	prometheus.MustRegister(journalLookupErrors)
}

/*
LookupInJournal finds all records in the specified key range in the specified
file matching the selected columns. Deadlines and cancellations from the
context "ctx" are taken into account. Any results will be reported through
the channel "results", errors will be reported through "errors", and "done"
will be marked as soon as processing the file has completed.

columns is expected to be sorted (see sort.Strings).
*/
func LookupInJournal(parentCtx context.Context, path string, columns []string,
	kr *common.KeyRange, results chan *redcloud.ColumnFamily,
	errors chan error, done chan struct{}) {
	var ctx context.Context
	var span *trace.Span
	var reader *recordio.RecordReader
	var cf *redcloud.ColumnFamily
	var u *url.URL
	var f filesystem.ReadCloser
	var err error

	ctx, span = trace.StartSpan(parentCtx, "red-cloud.storage/LookupInJournal")
	defer span.End()
	journalLookups.Inc()

	/*
		As soon as this function exits, successul or not, there's no point in
		waiting for the result any longer.
	*/
	defer markDone(done)

	span.AddAttributes(trace.StringAttribute("path", path))

	if u, err = url.Parse(path); err != nil {
		span.AddAttributes(trace.StringAttribute("error", err.Error()))
		span.Annotate(nil, "Invalid journal path URL")
		journalLookupErrors.With(
			prometheus.Labels{"error_class": "parse_file_name"}).Inc()
		errors <- err
		return
	}

	if f, err = filesystem.OpenReader(ctx, u); err != nil {
		span.AddAttributes(trace.StringAttribute("error", err.Error()))
		span.Annotate(nil, "Unable to open journal for reading")
		journalLookupErrors.With(
			prometheus.Labels{"error_class": "open_journal_readonly"}).Inc()
		errors <- err
		return
	}
	defer f.Close(ctx)

	reader = recordio.NewRecordReader(f)

	for {
		var rcf *redcloud.ColumnFamily
		var cs *redcloud.ColumnSet

		/*
			Check if our result is still interesting to the caller.
			There's no need to report the error as the caller will know about
			it from the context as well.
		*/
		if ctx.Err() != nil {
			span.AddAttributes(
				trace.StringAttribute("error", ctx.Err().Error()))
			span.Annotate(nil, "Context expired")
			journalLookupErrors.With(
				prometheus.Labels{"error_class": "context_expired"}).Inc()
			return
		}

		cf = new(redcloud.ColumnFamily)
		if err = reader.ReadMessage(ctx, cf); err == io.EOF {
			span.Annotate(nil, "End of File reached before data")
			return
		} else if err != nil {
			span.AddAttributes(trace.StringAttribute("error", err.Error()))
			span.Annotate(nil, "Error reading message from journal")
			journalLookupErrors.With(
				prometheus.Labels{"error_class": "read_error"}).Inc()
			errors <- err
		}

		// Check if the key of the record is in the range of interest.
		if !kr.Contains(cf.Key) {
			continue
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

		// Report a result if applicable.
		if rcf != nil {
			journalLookupNumResults.Inc()
			results <- rcf
		}
	}

	span.Annotate(nil, "All results sent")
}
