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
	"iter"
	"runtime"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/apache/arrow-go/v18/arrow/dataset"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/substrait-io/substrait-go/expr"
	"golang.org/x/sync/errgroup"
)

func constructFragment(fs io.IO, dataFile iceberg.DataFile) (dataset.Fragment, error) {
	format := dataset.ParquetFileFormat{ReaderOptions: struct{ DictCols []string }{
		[]string{"file_path"}}}

	return dataset.NewFileFragment(format, fs, dataFile.FilePath())
}

func readDeletes(fs io.IO, dataFile iceberg.DataFile) (map[string]*arrow.Chunked, error) {
	frag, err := constructFragment(fs, dataFile)
	if err != nil {
		return nil, err
	}

	scanner, err := dataset.NewScannerFromFragment(frag, &dataset.ScanOptions{
		BatchReadahead:    dataset.DefaultBatchReadahead,
		FragmentReadahead: 1,
		BatchSize:         dataset.DefaultBatchSize,
		Mem:               memory.DefaultAllocator,
	})
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	tbl, err := scanner.ToTable(context.Background())
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

func readAllDeleteFiles(fs io.IO, tasks []FileScanTask) (map[string][]*arrow.Chunked, error) {
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
	fs              io.IO
	projectedSchema *iceberg.Schema
	boundRowFilter  iceberg.BooleanExpression
	caseSensitive   bool
	rowLimit        int64
}

func (as *arrowScan) taskToRecordBatches(task FileScanTask, positionalDeletes []*arrow.Chunked) (iter.Seq2[arrow.Record, error], error) {
	arrowFmt := dataset.ParquetFileFormat{}
	frag, err := arrowFmt.FragmentFromFile(as.fs, task.File.FilePath(), nil)
	if err != nil {
		return nil, err
	}

	physicalSchema, err := frag.ReadPhysicalSchema()
	if err != nil {
		return nil, err
	}

	fileSchema, err := arrowToIceberg(physicalSchema)
	if err != nil {
		return nil, err
	}

	ids, err := as.projectedFieldIDs()
	if err != nil {
		return nil, err
	}

	outschema, err := iceberg.PruneColumns(fileSchema, ids, false)
	if err != nil {
		return nil, err
	}

	exprSchema, err := schemaToArrow(outschema, nil, true)
	if err != nil {
		return nil, err
	}

	extSet := exprs.NewDefaultExtensionSet()
	ctx := exprs.WithExtensionIDSet(context.Background(), extSet)

	var datasetFilter expr.Expression
	if as.boundRowFilter != nil && !as.boundRowFilter.Equals(iceberg.AlwaysTrue{}) {
		bldr := exprs.NewExprBuilder(extSet)
		if err := bldr.SetInputSchema(exprSchema); err != nil {
			return nil, err
		}
		datasetFilter, err = iceberg.VisitExpr(as.boundRowFilter, convertToArrowExpr{bldr: bldr})
		if err != nil {
			return nil, err
		}
	}

	cols := make([]compute.FieldPath, outschema.NumFields())
	for i, f := range outschema.Fields() {
		ref, _ := compute.FieldRefName(f.Name).FindOne(physicalSchema)
		cols[i] = ref
	}

	scanner, err := dataset.NewScannerFromFragment(frag, &dataset.ScanOptions{
		Filter:            datasetFilter,
		Columns:           cols,
		BatchReadahead:    dataset.DefaultBatchReadahead,
		FragmentReadahead: 1,
		BatchSize:         dataset.DefaultBatchSize,
		Mem:               memory.DefaultAllocator,
		FormatOptions:     arrowFmt.DefaultFragmentScanOptions(),
	})

	if err != nil {
		return nil, err
	}

	ch, err := scanner.ScanBatches(ctx)
	if err != nil {
		return nil, err
	}

	deletes := make(map[int64]struct{})
	for _, c := range positionalDeletes {
		for _, a := range c.Chunks() {
			for _, v := range a.(*array.Int64).Int64Values() {
				deletes[v] = struct{}{}
			}
		}
	}

	drain := func() {
		for rec := range ch {
			if rec.RecordBatch != nil {
				rec.RecordBatch.Release()
			}
		}
	}

	return func(yield func(arrow.Record, error) bool) {
		defer drain()

		nextIdx := int64(0)
		for rec := range ch {
			if rec.Err != nil {
				yield(nil, rec.Err)
				return
			}
			defer rec.RecordBatch.Release()

			result := rec.RecordBatch
			currentIdx := nextIdx
			nextIdx += rec.RecordBatch.NumRows()
			if len(positionalDeletes) > 0 {
				indices := combinePositionalDeletes(deletes, currentIdx, nextIdx)
				defer indices.Release()
				out, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
					compute.NewDatumWithoutOwning(rec.RecordBatch),
					compute.NewDatumWithoutOwning(indices))
				if err != nil {
					yield(nil, err)
					return
				}
				defer out.Release()

				result = out.(*compute.RecordDatum).Value
				// apply filter
			}

			result.Retain()
			if !yield(result, nil) {
				return
			}
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
