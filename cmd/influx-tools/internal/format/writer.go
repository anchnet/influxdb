package format

import (
	"github.com/influxdata/influxdb/cmd/influx-tools/internal/storage"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

var (
	// Discard is a Writer where all writes calls succeed and do nothing.
	Discard Writer = &devNull{}
)

type Writer interface {
	NewBucket(start, end int64) (BucketWriter, error)
	Close() error
}

type BucketWriter interface {
	Err() error
	WriteSeries(name, field []byte, tags models.Tags)
	WriteCursor(cur tsdb.Cursor)
	Close() error
}

func WriteBucket(w Writer, start, end int64, rs *storage.ResultSet) error {
	bw, err := w.NewBucket(start, end)
	if err != nil {
		return err
	}
	defer bw.Close()

	for rs.Next() {
		cur := rs.Cursor()
		if cur == nil {
			// no data for series key + field combination
			continue
		}

		bw.WriteSeries(rs.Name(), rs.Field(), rs.Tags())
		bw.WriteCursor(cur)
		cur.Close()

		if bw.Err() != nil {
			return bw.Err()
		}
	}
	return nil
}

type devNull struct{}

func (*devNull) Err() error                                         { return nil }
func (*devNull) WriteSeries(name, field []byte, tags models.Tags)   {}
func (*devNull) WriteCursor(cur tsdb.Cursor)                        {}
func (w *devNull) NewBucket(start, end int64) (BucketWriter, error) { return w, nil }
func (*devNull) Close() error                                       { return nil }
