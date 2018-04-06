package storage

//go:generate tmpl -data=@batch_cursor.gen.go.tmpldata batch_cursor.gen.go.tmpl

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
)

type MetaClient interface {
	ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
}

// Request message for Storage.Read.
type ReadRequest struct {
	Database    string
	RP          string
	Shards      []*tsdb.Shard
	Start       int64 // start time
	End         int64 // end time
	PointsLimit uint64
}

type Store struct {
	TSDBStore *tsdb.Store
}

// Read creates a ResultSet that reads all points with a timestamp ts, such that start ≤ ts < end.
func (s *Store) Read(ctx context.Context, req *ReadRequest) (*ResultSet, error) {
	var cur seriesCursor
	if ic, err := newIndexSeriesCursor(ctx, req.Shards); err != nil {
		return nil, err
	} else if ic == nil {
		return nil, nil
	} else {
		cur = ic
	}

	return newResultSet(ctx, req, cur), nil
}
