package storage

import (
	"io"
	"net/url"
	"sort"

	"github.com/childoftheuniverse/filesystem"
	"github.com/childoftheuniverse/red-cloud"
	"github.com/childoftheuniverse/red-cloud/common"
	"github.com/childoftheuniverse/sstable"
	"golang.org/x/net/context"
)

/*
LookupInSstable finds all records in the specified key range in the specified
file matching the selected columns. Deadlines and cancellations from the
context "ctx" are taken into account. Any results will be reported through
the channel "results", errors will be reported through "errors", and "done"
will be marked as soon as processing the file has completed.

columns is expected to be sorted (see sort.Strings).
*/
func LookupInSstable(ctx context.Context, path string, columns []string,
	kr *common.KeyRange, results chan *redcloud.ColumnFamily,
	errors chan error, done chan bool) {
	var cf *redcloud.ColumnFamily
	var reader *sstable.Reader
	var sst, idx filesystem.ReadCloser
	var sstu, idxu *url.URL
	var key string
	var err error

	/*
		As soon as this function exits, successul or not, there's no point in
		waiting for the result any longer.
	*/
	defer markDone(done)

	if sstu, err = url.Parse(path + ".sst"); err != nil {
		errors <- err
		return
	}
	if sst, err = filesystem.OpenReader(ctx, sstu); err != nil {
		errors <- err
		return
	}
	defer sst.Close(ctx)

	if idxu, err = url.Parse(path + ".idx"); err != nil {
		errors <- err
		return
	}
	if idx, err = filesystem.OpenReader(ctx, idxu); err != nil {
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
