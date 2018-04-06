package binary

import (
	"bufio"
	"fmt"
	"io"
	"time"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format"
	"github.com/influxdata/influxdb/cmd/influx-tools/internal/tlv"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

type Writer struct {
	w           *bufio.Writer
	buf         []byte
	db, rp      string
	duration    time.Duration
	err         error
	curr        *bucketWriter
	wroteHeader bool

	msg struct {
		bucketHeader BucketHeader
		bucketFooter BucketFooter
		seriesHeader SeriesHeader
		seriesFooter SeriesFooter
	}

	stats struct {
		series int
		counts [8]struct {
			series, values int
		}
	}
}

func NewWriter(w io.Writer, database, rp string, duration time.Duration) *Writer {
	var wr *bufio.Writer
	if wr, _ = w.(*bufio.Writer); wr == nil {
		wr = bufio.NewWriter(w)
	}
	return &Writer{w: wr, db: database, rp: rp, duration: duration}
}

func (w *Writer) WriteStats(o io.Writer) {
	fmt.Fprintf(o, "total series: %d\n", w.stats.series)

	for i := 0; i < 5; i++ {
		ft := FieldType(i)
		fmt.Fprintf(o, "%s unique series: %d\n", ft, w.stats.counts[i].series)
		fmt.Fprintf(o, "%s total values : %d\n", ft, w.stats.counts[i].values)
	}
}

func (w *Writer) NewBucket(start, end int64) (format.BucketWriter, error) {
	if !w.wroteHeader {
		w.writeHeader()
	}

	if w.err != nil {
		return nil, w.err
	}

	if w.curr != nil && !w.curr.closed {
		w.curr.Close()
	}

	w.curr = &bucketWriter{w: w, start: start, end: end}
	w.writeBucketHeader(start, end)

	return w.curr, w.err
}

func (w *Writer) Close() error {
	if w.err == ErrWriteAfterClose {
		return nil
	}
	if w.err != nil {
		return w.err
	}

	w.err = ErrWriteAfterClose

	return nil
}

func (w *Writer) writeHeader() {
	w.wroteHeader = true

	w.write(Magic[:])

	h := Header{
		Version:         Version0,
		Database:        w.db,
		RetentionPolicy: w.rp,
		ShardDuration:   w.duration,
	}
	w.writeTypeMessage(HeaderType, &h)
}

func (w *Writer) writeBucketHeader(start, end int64) {
	w.msg.bucketHeader.Start = start
	w.msg.bucketHeader.End = end
	w.writeTypeMessage(BucketHeaderType, &w.msg.bucketHeader)
}

func (w *Writer) writeBucketFooter() {
	w.writeTypeMessage(BucketFooterType, &w.msg.bucketFooter)
}

func (w *Writer) writeSeriesHeader(key, field []byte, ft FieldType) {
	w.stats.series++
	w.stats.counts[ft&7].series++

	w.msg.seriesHeader.SeriesKey = key
	w.msg.seriesHeader.Field = field
	w.msg.seriesHeader.FieldType = ft
	w.writeTypeMessage(SeriesHeaderType, &w.msg.seriesHeader)
}

func (w *Writer) writeSeriesFooter(ft FieldType, count int) {
	w.stats.counts[ft&7].values += count
	w.writeTypeMessage(SeriesFooterType, &w.msg.seriesFooter)
}

func (w *Writer) write(p []byte) {
	if w.err != nil {
		return
	}
	_, w.err = w.w.Write(p)
}

func (w *Writer) writeTypeMessage(typ MessageType, msg message) {
	if w.err != nil {
		return
	}

	// ensure size
	n := msg.Size()
	if n > cap(w.buf) {
		w.buf = make([]byte, n)
	} else {
		w.buf = w.buf[:n]
	}

	_, w.err = msg.MarshalTo(w.buf)
	w.writeTypeBytes(typ, w.buf)
}

func (w *Writer) writeTypeBytes(typ MessageType, b []byte) {
	if w.err != nil {
		return
	}
	w.err = tlv.WriteTLV(w.w, byte(typ), w.buf)
}

type bucketWriter struct {
	w          *Writer
	err        error
	start, end int64
	key        []byte
	field      []byte
	closed     bool
}

func (bw *bucketWriter) Err() error {
	if bw.w.err != nil {
		return bw.w.err
	}
	return bw.err
}

func (bw *bucketWriter) hasErr() bool {
	return bw.w.err != nil || bw.err != nil
}

func (bw *bucketWriter) WriteSeries(name, field []byte, tags models.Tags) {
	if bw.hasErr() {
		return
	}

	bw.key = models.AppendMakeKey(bw.key[:0], name, tags)
	bw.field = field
}

func (bw *bucketWriter) WriteCursor(cur tsdb.Cursor) {
	if bw.hasErr() {
		return
	}

	switch c := cur.(type) {
	case tsdb.IntegerBatchCursor:
		bw.writeIntegerPoints(c)
	case tsdb.FloatBatchCursor:
		bw.writeFloatPoints(c)
	case tsdb.UnsignedBatchCursor:
		bw.writeUnsignedPoints(c)
	case tsdb.BooleanBatchCursor:
		bw.writeBooleanPoints(c)
	case tsdb.StringBatchCursor:
		bw.writeStringPoints(c)
	default:
		panic(fmt.Sprintf("unreachable: %T", c))
	}
}

func (bw *bucketWriter) writeIntegerPoints(cur tsdb.IntegerBatchCursor) {
	bw.w.writeSeriesHeader(bw.key, bw.field, IntegerFieldType)

	var (
		msg IntegerPoints
		c   int
	)
	for {
		ts, vs := cur.Next()
		if len(ts) == 0 {
			break
		}

		c += len(ts)
		msg.Timestamps = ts
		msg.Values = vs
		bw.w.writeTypeMessage(IntegerPointsType, &msg)
	}
	bw.w.writeSeriesFooter(IntegerFieldType, c)
}

func (bw *bucketWriter) writeFloatPoints(cur tsdb.FloatBatchCursor) {
	bw.w.writeSeriesHeader(bw.key, bw.field, FloatFieldType)

	var (
		msg FloatPoints
		c   int
	)
	for {
		ts, vs := cur.Next()
		if len(ts) == 0 {
			break
		}

		c += len(ts)
		msg.Timestamps = ts
		msg.Values = vs
		bw.w.writeTypeMessage(FloatPointsType, &msg)
	}
	bw.w.writeSeriesFooter(FloatFieldType, c)
}

func (bw *bucketWriter) writeUnsignedPoints(cur tsdb.UnsignedBatchCursor) {
	bw.w.writeSeriesHeader(bw.key, bw.field, UnsignedFieldType)

	var (
		msg UnsignedPoints
		c   int
	)
	for {
		ts, vs := cur.Next()
		if len(ts) == 0 {
			break
		}

		c += len(ts)
		msg.Timestamps = ts
		msg.Values = vs
		bw.w.writeTypeMessage(UnsignedPointsType, &msg)
	}
	bw.w.writeSeriesFooter(UnsignedFieldType, c)
}

func (bw *bucketWriter) writeBooleanPoints(cur tsdb.BooleanBatchCursor) {
	bw.w.writeSeriesHeader(bw.key, bw.field, BooleanFieldType)

	var (
		msg BooleanPoints
		c   int
	)
	for {
		ts, vs := cur.Next()
		if len(ts) == 0 {
			break
		}

		c += len(ts)
		msg.Timestamps = ts
		msg.Values = vs
		bw.w.writeTypeMessage(BooleanPointsType, &msg)
	}
	bw.w.writeSeriesFooter(BooleanFieldType, c)
}

func (bw *bucketWriter) writeStringPoints(cur tsdb.StringBatchCursor) {
	bw.w.writeSeriesHeader(bw.key, bw.field, StringFieldType)

	var (
		msg StringPoints
		c   int
	)
	for {
		ts, vs := cur.Next()
		if len(ts) == 0 {
			break
		}

		c += len(ts)
		msg.Timestamps = ts
		msg.Values = vs
		bw.w.writeTypeMessage(StringPointsType, &msg)
	}
	bw.w.writeSeriesFooter(StringFieldType, c)
}

func (bw *bucketWriter) Close() error {
	if bw.closed {
		return nil
	}

	bw.closed = true

	if bw.hasErr() {
		return bw.Err()
	}

	bw.w.writeBucketFooter()
	bw.err = ErrWriteBucketAfterClose

	return bw.w.w.Flush()
}
