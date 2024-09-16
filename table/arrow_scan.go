// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"context"
	"errors"
	"io"
	"iter"
	"runtime"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/parquet"
	"github.com/substrait-io/substrait-go/expr"
	"golang.org/x/sync/errgroup"
)

func readDeletes(fs iceio.IO, dataFile iceberg.DataFile) (map[string]*arrow.Chunked, error) {
	src := parquet.FileSource{Fs: fs, File: dataFile}
	rdr, err := src.GetReader(true)
	if err != nil {
		return nil, err
	}
	defer rdr.ParquetReader().Close()

	ctx := context.Background()
	tbl, err := rdr.ReadTable(ctx)
	if err != nil {
		return nil, err
	}
	defer tbl.Release()

	tbl, err = array.UnifyTableDicts(memory.DefaultAllocator, tbl)
	if err != nil {
		return nil, err
	}
	defer tbl.Release()

	filePathCol := tbl.Column(tbl.Schema().FieldIndices("file_path")[0]).Data()
	posCol := tbl.Column(tbl.Schema().FieldIndices("pos")[0]).Data()
	dict := filePathCol.Chunk(0).(*array.Dictionary).Dictionary().(*array.String)

	results := make(map[string]*arrow.Chunked)
	for i := 0; i < dict.Len(); i++ {
		v := dict.Value(i)

		mask, err := compute.CallFunction(ctx, "equal", nil,
			compute.NewDatumWithoutOwning(filePathCol), compute.NewDatum(v))
		if err != nil {
			return nil, err
		}
		defer mask.Release()

		filtered, err := compute.Filter(ctx, compute.NewDatumWithoutOwning(posCol),
			mask, *compute.DefaultFilterOptions())
		if err != nil {
			return nil, err
		}

		results[v] = filtered.(*compute.ChunkedDatum).Value
	}
	return results, nil
}

func readAllDeleteFiles(fs iceio.IO, tasks []FileScanTask) (map[string][]*arrow.Chunked, error) {
	deletesPerFile := make(map[string][]*arrow.Chunked)
	uniqueDeletes := map[string]iceberg.DataFile{}
	for _, t := range tasks {
		for _, d := range t.DeleteFiles {
			if _, ok := uniqueDeletes[d.FilePath()]; !ok {
				uniqueDeletes[d.FilePath()] = d
			}
		}
	}

	if len(uniqueDeletes) == 0 {
		return deletesPerFile, nil
	}

	ctx := context.Background()
	g, _ := errgroup.WithContext(ctx)
	var err error
	g.SetLimit(runtime.NumCPU())

	perFileChan := make(chan map[string]*arrow.Chunked, runtime.NumCPU())
	go func() {
		for _, v := range uniqueDeletes {
			g.Go(func() error {
				deletes, err := readDeletes(fs, v)
				if deletes != nil {
					perFileChan <- deletes
				}
				return err
			})
		}

		err = g.Wait()
		close(perFileChan)
	}()

	for deletes := range perFileChan {
		for file, arr := range deletes {
			deletesPerFile[file] = append(deletesPerFile[file], arr)
		}
	}

	return deletesPerFile, err
}

func combinePositionalDeletes(deletes map[int64]struct{}, start, end int64) arrow.Array {
	bldr := array.NewInt64Builder(memory.DefaultAllocator)
	defer bldr.Release()
	for i := start; i < end; i++ {
		if _, ok := deletes[i]; !ok {
			bldr.Append(i)
		}
	}
	return bldr.NewArray()
}

type arrowScan struct {
	metadata        Metadata
	fs              iceio.IO
	projectedSchema *iceberg.Schema
	boundRowFilter  iceberg.BooleanExpression
	caseSensitive   bool
	rowLimit        int64
}

func filterRecords(ctx context.Context, recordFilter expr.Expression) func(arrow.Record) (arrow.Record, error) {
	return func(rec arrow.Record) (arrow.Record, error) {
		input := compute.NewDatumWithoutOwning(rec)
		mask, err := exprs.ExecuteScalarExpression(ctx, rec.Schema(), recordFilter, input)
		if err != nil {
			return nil, err
		}

		result, err := compute.Filter(ctx, input, mask, *compute.DefaultFilterOptions())
		if err != nil {
			return nil, err
		}

		mask.Release()

		return result.(*compute.RecordDatum).Value, nil
	}
}

func toRequestedSchema(fileSchema, projectedSchema *iceberg.Schema) func(arrow.Record) (arrow.Record, error) {
	return func(rec arrow.Record) (arrow.Record, error) {
		defer rec.Release()
		st := array.RecordToStructArray(rec)
		defer st.Release()

		result, err := iceberg.VisitWithPartner[arrow.Array, arrow.Array](
			projectedSchema, st,
			&arrowProjectionVisitor{fileSchema: fileSchema, includeFieldIDs: true, useLargeTypes: false},
			arrowAccessor{fileSchema: fileSchema})
		if err != nil {
			return nil, err
		}

		return array.RecordFromStructArray(result.(*array.Struct), nil), nil
	}
}

func processPositionalDeletes(ctx context.Context, deletes map[int64]struct{}) func(arrow.Record) (arrow.Record, error) {
	nextIdx := int64(0)
	return func(rec arrow.Record) (arrow.Record, error) {
		defer rec.Release()

		currentIdx := nextIdx
		nextIdx += rec.NumRows()

		indices := combinePositionalDeletes(deletes, currentIdx, nextIdx)
		defer indices.Release()

		out, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
			compute.NewDatumWithoutOwning(rec), compute.NewDatumWithoutOwning(indices))
		if err != nil {
			return nil, err
		}

		return out.(*compute.RecordDatum).Value, nil
	}
}

func (as *arrowScan) taskToRecordBatches(task FileScanTask, positionalDeletes []*arrow.Chunked) (iter.Seq2[arrow.Record, error], error) {
	ids, err := as.projectedFieldIDs()
	if err != nil {
		return nil, err
	}

	src := parquet.FileSource{
		Fs:   as.fs,
		File: task.File,
	}

	rdr, err := src.GetReader(false)
	if err != nil {
		return nil, err
	}
	defer rdr.ParquetReader().Close()

	extSet := exprs.NewDefaultExtensionSet()
	ctx := exprs.WithExtensionIDSet(context.Background(), extSet)

	fschema, colIds, recordFilter, err := src.Prep(ctx, rdr, ids, as.boundRowFilter)
	if err != nil {
		return nil, err
	}

	var testRg func(*metadata.RowGroupMetaData, []int) (bool, error)
	if rdr.ParquetReader().NumRowGroups() > 1 {
		testRg, err = newParquetRowGroupStatsEvaluator(fschema, as.boundRowFilter, as.caseSensitive, false)
		if err != nil {
			return nil, err
		}
	}

	recRdr, err := src.ReadData(rdr, colIds, testRg)
	if err != nil {
		return nil, err
	}

	pipeline := make([]func(arrow.Record) (arrow.Record, error), 0, 2)
	if len(positionalDeletes) > 0 {
		deletes := make(map[int64]struct{})
		for _, c := range positionalDeletes {
			for _, a := range c.Chunks() {
				for _, v := range a.(*array.Int64).Int64Values() {
					deletes[v] = struct{}{}
				}
			}
		}
		pipeline = append(pipeline, processPositionalDeletes(ctx, deletes))
	}

	pipeline = append(pipeline, filterRecords(ctx, recordFilter), toRequestedSchema(fschema, as.projectedSchema))
	return func(yield func(arrow.Record, error) bool) {
		defer recRdr.Release()
		var err error
		for recRdr.Next() {
			rec := recRdr.Record()

			for _, p := range pipeline {
				rec, err = p(rec)
				if err != nil {
					yield(nil, err)
					return
				}
			}

			if !yield(rec, nil) {
				return
			}
		}
		if recRdr.Err() != nil && !errors.Is(recRdr.Err(), io.EOF) {
			yield(nil, recRdr.Err())
		}
	}, nil
}

func (as *arrowScan) recordBatchesFromScanTasksAndDeletes(tasks []FileScanTask, deletesPerFile map[string][]*arrow.Chunked) iter.Seq2[arrow.Record, error] {
	totalRowCount := int64(0)

	drain := func(itr iter.Seq2[arrow.Record, error]) {
		for rec, _ := range itr {
			if rec != nil {
				rec.Release()
			}
		}
	}

	return func(yield func(arrow.Record, error) bool) {
		for _, t := range tasks {
			if as.rowLimit >= 0 && totalRowCount >= as.rowLimit {
				break
			}

			batchItr, err := as.taskToRecordBatches(t, deletesPerFile[t.File.FilePath()])
			if err != nil {
				yield(nil, err)
				return
			}

			for batch, err := range batchItr {
				if err != nil {
					yield(nil, err)
					defer drain(batchItr)
					return
				}

				if as.rowLimit >= 0 {
					defer batch.Release()

					if totalRowCount >= as.rowLimit {
						break
					} else if totalRowCount+batch.NumRows() >= as.rowLimit {
						batch = batch.NewSlice(0, as.rowLimit-totalRowCount)
					}
				}
				if !yield(batch, nil) {
					defer drain(batchItr)
					return
				}
				totalRowCount += batch.NumRows()
			}
		}
	}
}

func (as *arrowScan) ToTable(tasks []FileScanTask) (arrow.Table, error) {
	deletesPerFile, err := readAllDeleteFiles(as.fs, tasks)
	defer func() {
		for _, v := range deletesPerFile {
			for _, c := range v {
				c.Release()
			}
		}
	}()

	if err != nil {
		return nil, nil
	}

	records := make([]arrow.Record, 0)
	for rec, err := range as.recordBatchesFromScanTasksAndDeletes(tasks, deletesPerFile) {
		if err != nil {
			return nil, err
		}

		defer rec.Release()
		records = append(records, rec)
	}

	return array.NewTableFromRecords(records[0].Schema(), records), nil
}

func (as *arrowScan) projectedFieldIDs() (map[int]iceberg.Void, error) {
	idset := map[int]struct{}{}
	for _, field := range as.projectedSchema.Fields() {
		switch field.Type.(type) {
		case *iceberg.MapType, *iceberg.ListType:
		default:
			idset[field.ID] = struct{}{}
		}
	}

	extracted, err := iceberg.ExtractFieldIDs(as.boundRowFilter)
	if err != nil {
		return nil, err
	}

	for _, id := range extracted {
		idset[id] = struct{}{}
	}

	return idset, nil
}
