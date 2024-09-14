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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowToIceberg(t *testing.T) {
	tests := []struct {
		dt         arrow.DataType
		ice        iceberg.Type
		reciprocal bool
	}{
		{&arrow.FixedSizeBinaryType{ByteWidth: 23}, iceberg.FixedTypeOf(23), true},
		{&arrow.Decimal128Type{Precision: 26, Scale: 20}, iceberg.DecimalTypeOf(26, 20), true},
		{arrow.FixedWidthTypes.Boolean, iceberg.PrimitiveTypes.Bool, true},
		{arrow.PrimitiveTypes.Int8, iceberg.PrimitiveTypes.Int32, false},
		{arrow.PrimitiveTypes.Uint8, iceberg.PrimitiveTypes.Int32, false},
		{arrow.PrimitiveTypes.Int16, iceberg.PrimitiveTypes.Int32, false},
		{arrow.PrimitiveTypes.Uint16, iceberg.PrimitiveTypes.Int32, false},
		{arrow.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int32, true},
		{arrow.PrimitiveTypes.Uint32, iceberg.PrimitiveTypes.Int32, false},
		{arrow.PrimitiveTypes.Int64, iceberg.PrimitiveTypes.Int64, true},
		{arrow.PrimitiveTypes.Uint64, iceberg.PrimitiveTypes.Int64, false},
		{arrow.FixedWidthTypes.Float16, iceberg.PrimitiveTypes.Float32, false},
		{arrow.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float32, true},
		{arrow.PrimitiveTypes.Float64, iceberg.PrimitiveTypes.Float64, true},
		{arrow.FixedWidthTypes.Date32, iceberg.PrimitiveTypes.Date, true},
		{arrow.FixedWidthTypes.Time64us, iceberg.PrimitiveTypes.Time, true},
		{arrow.FixedWidthTypes.Timestamp_us, iceberg.PrimitiveTypes.TimestampTz, true},
		{&arrow.TimestampType{Unit: arrow.Microsecond}, iceberg.PrimitiveTypes.Timestamp, true},
		{arrow.BinaryTypes.String, iceberg.PrimitiveTypes.String, false},
		{arrow.BinaryTypes.LargeString, iceberg.PrimitiveTypes.String, true},
		{arrow.BinaryTypes.Binary, iceberg.PrimitiveTypes.Binary, false},
		{arrow.BinaryTypes.LargeBinary, iceberg.PrimitiveTypes.Binary, true},
		{extensions.NewUUIDType(), iceberg.PrimitiveTypes.UUID, true},
		{arrow.StructOf(arrow.Field{
			Name:     "foo",
			Type:     arrow.BinaryTypes.LargeString,
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				parquetFieldIDKey: "1", fieldDocKey: "foo doc"}),
		},
			arrow.Field{
				Name: "bar",
				Type: arrow.PrimitiveTypes.Int32,
				Metadata: arrow.MetadataFrom(map[string]string{
					parquetFieldIDKey: "2",
				}),
			},
			arrow.Field{
				Name:     "baz",
				Type:     arrow.FixedWidthTypes.Boolean,
				Nullable: true,
				Metadata: arrow.MetadataFrom(map[string]string{
					parquetFieldIDKey: "3",
				}),
			}), &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: "foo doc"},
				{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
				{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false},
			},
		}, true},
		{arrow.ListOfField(arrow.Field{
			Name:     "element",
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{parquetFieldIDKey: "1"}),
		}), &iceberg.ListType{
			ElementID:       1,
			Element:         iceberg.PrimitiveTypes.Int32,
			ElementRequired: true,
		}, false},
		{arrow.LargeListOfField(arrow.Field{
			Name:     "element",
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{parquetFieldIDKey: "1"}),
		}), &iceberg.ListType{
			ElementID:       1,
			Element:         iceberg.PrimitiveTypes.Int32,
			ElementRequired: true,
		}, true},
	}

	for _, tt := range tests {
		t.Run(tt.dt.String(), func(t *testing.T) {
			ice, err := arrowTypToIceberg(tt.dt)
			require.NoError(t, err)
			assert.True(t, ice.Equals(tt.ice), ice)

			if tt.reciprocal {
				out, err := typeToArrowType(ice)
				require.NoError(t, err)
				assert.Truef(t, arrow.TypeEqual(tt.dt, out), "expected: %s\ngot: %s", tt.dt, out)
			}
		})
	}
}

func TestNestedSchema(t *testing.T) {
	sc := arrow.NewSchema([]arrow.Field{
		{
			Name: "foo", Type: arrow.BinaryTypes.String,
			Metadata: arrow.MetadataFrom(map[string]string{"PARQUET:field_id": "1", "doc": "foo doc"}),
		},
		{
			Name: "quux",
			Type: arrow.MapOfWithMetadata(arrow.BinaryTypes.String, arrow.MetadataFrom(map[string]string{"PARQUET:field_id": "7"}),
				arrow.MapOfWithMetadata(arrow.BinaryTypes.String, arrow.MetadataFrom(map[string]string{"PARQUET:field_id": "9"}),
					arrow.PrimitiveTypes.Int32, arrow.MetadataFrom(map[string]string{"PARQUET:field_id": "10"})),
				arrow.MetadataFrom(map[string]string{"PARQUET:field_id": "8"})),
			Metadata: arrow.MetadataFrom(map[string]string{"PARQUET:field_id": "6", "doc": "quux doc"}),
		},
	}, nil)

	ice, err := arrowToIceberg(sc)
	require.NoError(t, err)
	assert.Equal(t, "", ice.String())
}
