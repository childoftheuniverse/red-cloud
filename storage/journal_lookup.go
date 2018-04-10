package storage

import (
	"io"
	"net/url"
	"sort"

	"context"
	"github.com/childoftheuniverse/filesystem"
	"github.com/childoftheuniverse/recordio"
	"github.com/childoftheuniverse/red-cloud"
	"github.com/childoftheuniverse/red-cloud/common"
)

func markDone(done chan bool) {
	done <- true
}

/*
LookupInJournal finds all records in the specified key range in the specified
file matching the selected columns. Deadlines and cancellations from the
context "ctx" are taken into account. Any results will be reported through
the channel "results", errors will be reported through "errors", and "done"
will be marked as soon as processing the file has completed.

columns is expected to be sorted (see sort.Strings).
*/
func LookupInJournal(ctx context.Context, path string, columns []string,
	kr *common.KeyRange, results chan *redcloud.ColumnFamily,
	errors chan error, done chan bool) {
	var reader *recordio.RecordReader
	var cf *redcloud.ColumnFamily
	var u *url.URL
	var f filesystem.ReadCloser
	var err error

	/*
		As soon as this function exits, successul or not, there's no point in
		waiting for the result any longer.
	*/
	defer markDone(done)

	if u, err = url.Parse(path); err != nil {
		errors <- err
		return
	}

	if f, err = filesystem.OpenReader(ctx, u); err != nil {
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
			return
		}

		cf = new(redcloud.ColumnFamily)
		if err = reader.ReadMessage(ctx, cf); err == io.EOF {
			return
		} else if err != nil {
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
			results <- rcf
		}
	}
}
