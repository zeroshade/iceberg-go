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

package parquet

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/substrait-io/substrait-go/expr"
)

type FileSource struct {
	Fs   iceio.IO
	File iceberg.DataFile

	mem memory.Allocator
}

func (f *FileSource) GetReader(positionalDeletes bool) (*pqarrow.FileReader, error) {
	pf, err := f.Fs.Open(f.File.FilePath())
	if err != nil {
		return nil, err
	}

	rdr, err := file.NewParquetReader(pf.(parquet.ReaderAtSeeker),
		file.WithReadProps(parquet.NewReaderProperties(f.mem)))
	if err != nil {
		return nil, err
	}

	arrProps := pqarrow.ArrowReadProperties{
		Parallel:  true,
		BatchSize: 1 << 17,
	}

	if positionalDeletes {
		arrProps.SetReadDict(0, true)
	}
	return pqarrow.NewFileReader(rdr, arrProps, f.mem)
}

func (f *FileSource) Prep(ctx context.Context, rdr *pqarrow.FileReader, projectedIDs map[int]struct{}, filter iceberg.BooleanExpression) (*iceberg.Schema, []int, expr.Expression, error) {
	fileSchema, colIndices, err := pruneParquetColumns(rdr.Manifest, projectedIDs, false)
	if err != nil {
		return nil, nil, nil, err
	}

	var recordFilter expr.Expression
	if filter != nil && !filter.Equals(iceberg.AlwaysTrue{}) {
		extSet := exprs.GetExtensionIDSet(ctx)
		if filter != nil && !filter.Equals(iceberg.AlwaysTrue{}) {
			bldr := exprs.NewExprBuilder(extSet)
			if err := bldr.SetInputSchema(fileSchema); err != nil {
				return nil, nil, nil, err
			}

			recordFilter, err = iceberg.VisitExpr(filter, convertToArrowExpr{bldr: bldr})
			if err != nil {
				return nil, nil, nil, err
			}
		}
	}

	icebergSchema, err := arrowToIceberg(fileSchema)
	if err != nil {
		return nil, nil, nil, err
	}

	return icebergSchema, colIndices, recordFilter, nil
}

func (f *FileSource) ReadData(rdr *pqarrow.FileReader, cols []int, testRG func(*metadata.RowGroupMetaData, []int) (bool, error)) (array.RecordReader, error) {
	var rgList []int
	if testRG != nil {
		rgList = make([]int, 0)
		fileMeta, numRg := rdr.ParquetReader().MetaData(), rdr.ParquetReader().NumRowGroups()
		for rg := 0; rg < numRg; rg++ {
			rgMeta := fileMeta.RowGroup(rg)
			use, err := testRG(rgMeta, cols)
			if err != nil {
				return nil, err
			}

			if use {
				rgList = append(rgList, rg)
			}
		}
	}

	return rdr.GetRecordReader(context.TODO(), cols, rgList)
}
