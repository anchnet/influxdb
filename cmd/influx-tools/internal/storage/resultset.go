package storage

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

type readRequest struct {
	ctx        context.Context
	start, end int64
	asc        bool
	limit      uint64
}

type ResultSet struct {
	req     readRequest
	cur     seriesCursor
	row     seriesRow
	cursors struct {
		cur tsdb.Cursor
		req tsdb.CursorRequest
		i   integerMultiShardBatchCursor
		f   floatMultiShardBatchCursor
		u   unsignedMultiShardBatchCursor
		s   stringMultiShardBatchCursor
		b   booleanMultiShardBatchCursor
	}
}

func newResultSet(ctx context.Context, req *ReadRequest, cur seriesCursor) *ResultSet {
	r := &ResultSet{
		req: readRequest{
			ctx:   ctx,
			start: req.Start,
			end:   req.End,
			asc:   true,
			limit: req.PointsLimit,
		},
		cur: cur,
	}

	r.cursors.i = *newIntegerMultiShardBatchCursor(ctx, nil, &r.req, &r.cursors.req, nil)
	r.cursors.f = *newFloatMultiShardBatchCursor(ctx, nil, &r.req, &r.cursors.req, nil)
	r.cursors.u = *newUnsignedMultiShardBatchCursor(ctx, nil, &r.req, &r.cursors.req, nil)
	r.cursors.s = *newStringMultiShardBatchCursor(ctx, nil, &r.req, &r.cursors.req, nil)
	r.cursors.b = *newBooleanMultiShardBatchCursor(ctx, nil, &r.req, &r.cursors.req, nil)

	r.cursors.req.Ascending = true
	r.cursors.req.StartTime = req.Start
	r.cursors.req.EndTime = req.End

	return r
}

func (r *ResultSet) Close() {
	r.row.query = nil
	r.cur.Close()
}

func (r *ResultSet) Next() bool {
	row := r.cur.Next()
	if row == nil {
		return false
	}

	r.row = *row

	return true
}

func (r *ResultSet) Cursor() tsdb.Cursor { return r.nextCursor() }
func (r *ResultSet) Name() []byte        { return r.row.name }
func (r *ResultSet) Tags() models.Tags   { return r.row.tags }
func (r *ResultSet) Field() []byte       { return []byte(r.row.field) }

func (r *ResultSet) nextCursor() tsdb.Cursor {
	r.cursors.req.Name = r.row.name
	r.cursors.req.Tags = r.row.tags
	r.cursors.req.Field = r.row.field

	itrs := r.row.query

	var shard tsdb.CursorIterator
	var cur tsdb.Cursor
	for cur == nil && len(itrs) > 0 {
		shard, itrs = itrs[0], itrs[1:]
		cur, _ = shard.Next(r.req.ctx, &r.cursors.req)
	}

	if cur == nil {
		return nil
	}

	switch c := cur.(type) {
	case tsdb.IntegerBatchCursor:
		cc := &r.cursors.i
		cc.IntegerBatchCursor = c
		cc.itrs = itrs
		cc.count = 0
		cc.err = nil
		r.cursors.cur = cc
	case tsdb.FloatBatchCursor:
		cc := &r.cursors.f
		cc.FloatBatchCursor = c
		cc.itrs = itrs
		cc.count = 0
		cc.err = nil
		r.cursors.cur = cc
	case tsdb.UnsignedBatchCursor:
		cc := &r.cursors.u
		cc.UnsignedBatchCursor = c
		cc.itrs = itrs
		cc.count = 0
		cc.err = nil
		r.cursors.cur = cc
	case tsdb.StringBatchCursor:
		cc := &r.cursors.s
		cc.StringBatchCursor = c
		cc.itrs = itrs
		cc.count = 0
		cc.err = nil
		r.cursors.cur = cc
	case tsdb.BooleanBatchCursor:
		cc := &r.cursors.b
		cc.BooleanBatchCursor = c
		cc.itrs = itrs
		cc.count = 0
		cc.err = nil
		r.cursors.cur = cc
	default:
		panic(fmt.Sprintf("unreachable: %T", cur))
	}

	return r.cursors.cur
}
