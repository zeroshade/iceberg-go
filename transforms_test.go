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

package iceberg_test

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTransform(t *testing.T) {
	tests := []struct {
		toparse  string
		expected iceberg.Transform
	}{
		{"identity", iceberg.IdentityTransform{}},
		{"IdEnTiTy", iceberg.IdentityTransform{}},
		{"void", iceberg.VoidTransform{}},
		{"VOId", iceberg.VoidTransform{}},
		{"year", iceberg.YearTransform{}},
		{"yEAr", iceberg.YearTransform{}},
		{"month", iceberg.MonthTransform{}},
		{"MONtH", iceberg.MonthTransform{}},
		{"day", iceberg.DayTransform{}},
		{"DaY", iceberg.DayTransform{}},
		{"hour", iceberg.HourTransform{}},
		{"hOuR", iceberg.HourTransform{}},
		{"bucket[5]", iceberg.BucketTransform{NumBuckets: 5}},
		{"bucket[100]", iceberg.BucketTransform{NumBuckets: 100}},
		{"BUCKET[5]", iceberg.BucketTransform{NumBuckets: 5}},
		{"bUCKeT[100]", iceberg.BucketTransform{NumBuckets: 100}},
		{"truncate[10]", iceberg.TruncateTransform{Width: 10}},
		{"truncate[255]", iceberg.TruncateTransform{Width: 255}},
		{"TRUNCATE[10]", iceberg.TruncateTransform{Width: 10}},
		{"tRuNCATe[255]", iceberg.TruncateTransform{Width: 255}},
	}

	for _, tt := range tests {
		t.Run(tt.toparse, func(t *testing.T) {
			transform, err := iceberg.ParseTransform(tt.toparse)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, transform)

			txt, err := transform.MarshalText()
			assert.NoError(t, err)
			assert.Equal(t, strings.ToLower(tt.toparse), string(txt))
		})
	}

	errorTests := []struct {
		name    string
		toparse string
	}{
		{"foobar", "foobar"},
		{"bucket no brackets", "bucket"},
		{"truncate no brackets", "truncate"},
		{"bucket no val", "bucket[]"},
		{"truncate no val", "truncate[]"},
		{"bucket neg", "bucket[-1]"},
		{"truncate neg", "truncate[-1]"},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			tr, err := iceberg.ParseTransform(tt.toparse)
			assert.Nil(t, tr)
			assert.ErrorIs(t, err, iceberg.ErrInvalidTransform)
			assert.ErrorContains(t, err, tt.toparse)
		})
	}
}

func TestBucketTransformHash(t *testing.T) {
	tests := []struct {
		input    iceberg.Literal
		expected iceberg.Literal
	}{
		{iceberg.Int32Literal(1), iceberg.Int32Literal(4)},
		{iceberg.Int32Literal(34), iceberg.Int32Literal(3)},
		{iceberg.Int64Literal(34), iceberg.Int32Literal(3)},
		{iceberg.DateLiteral(iceberg.DateFromTime(time.Date(2017, 11, 16, 0, 0, 0, 0, time.UTC))),
			iceberg.Int32Literal(2)},
		{iceberg.TimeLiteral(time.Date(1970, 1, 1, 22, 31, 8, 0, time.UTC).UnixMicro()),
			iceberg.Int32Literal(3)},
		{iceberg.TimestampLiteral(time.Date(2017, 11, 16, 22, 31, 8, 0, time.UTC).UnixMicro()),
			iceberg.Int32Literal(7)},
		{iceberg.TimestampLiteral(time.Date(2017, 11, 16, 14, 31, 8, 0, time.FixedZone("8", -8*3600)).UnixMicro()),
			iceberg.Int32Literal(7)},
		{iceberg.BinaryLiteral{0x0, 0x1, 0x2, 0x3}, iceberg.Int32Literal(1)},
		{iceberg.FixedLiteral{0x0, 0x1, 0x2, 0x3}, iceberg.Int32Literal(1)},
		{iceberg.StringLiteral("iceberg"), iceberg.Int32Literal(1)},
		{iceberg.UUIDLiteral(uuid.MustParse("f79c3e09-677c-4bbd-a479-3f349cb785e7")),
			iceberg.Int32Literal(4)},
	}

	for _, tt := range tests {
		t.Run(tt.input.String(), func(t *testing.T) {
			out := iceberg.BucketTransform{NumBuckets: 8}.
				Apply(iceberg.Optional[iceberg.Literal]{Valid: true, Val: tt.input})

			assert.True(t, out.Valid)
			assert.Truef(t, tt.expected.Equals(out.Val),
				"got: %s, expected: %s", out.Val, tt.expected)
		})
	}
}

func TestNumBuckets(t *testing.T) {
	tests := []struct {
		nbuckets int
		val      iceberg.Literal
		expected int32
	}{
		{2, iceberg.Int32Literal(0), 0},
		{100, iceberg.Int32Literal(34), 79},
		{100, iceberg.Int64Literal(34), 79},
		{100, iceberg.DateLiteral(17486), 26},
		{100, iceberg.TimeLiteral(81068000000), 59},
		{100, iceberg.TimestampLiteral(1510871468000000), 7},
		{100, iceberg.DecimalLiteral{Scale: 2, Val: decimal128.FromI64(1420)}, 59},
		{100, iceberg.StringLiteral("iceberg"), 89},
		{100, iceberg.UUIDLiteral(uuid.MustParse("f79c3e09-677c-4bbd-a479-3f349cb785e7")), 40},
		{128, iceberg.FixedLiteral("foo"), 32},
		{128, iceberg.BinaryLiteral{0x0, 0x1, 0x2, 0x3}, 57},
	}

	for _, tt := range tests {
		t.Run(tt.val.String(), func(t *testing.T) {
			out := iceberg.BucketTransform{NumBuckets: tt.nbuckets}.
				Apply(iceberg.Optional[iceberg.Literal]{Valid: true, Val: tt.val})

			assert.True(t, out.Valid)
			assert.Truef(t, out.Val.Equals(iceberg.Int32Literal(tt.expected)),
				"got: %s, expected: %d", out.Val, tt.expected)
		})
	}
}

func TestDateTransform(t *testing.T) {
	tests := []struct {
		val      iceberg.Literal
		tf       iceberg.Transform
		expected iceberg.Literal
	}{
		{iceberg.DateLiteral(17501), iceberg.DayTransform{}, iceberg.Int32Literal(17501)},
		{iceberg.DateLiteral(-1), iceberg.DayTransform{}, iceberg.Int32Literal(-1)},
		{iceberg.DateLiteral(17501), iceberg.MonthTransform{}, iceberg.Int32Literal(575)},
		{iceberg.DateLiteral(-1), iceberg.MonthTransform{}, iceberg.Int32Literal(-1)},
		{iceberg.DateLiteral(17501), iceberg.YearTransform{}, iceberg.Int32Literal(47)},
		{iceberg.DateLiteral(-1), iceberg.YearTransform{}, iceberg.Int32Literal(-1)},
		{iceberg.TimestampLiteral(1512151975038194), iceberg.YearTransform{}, iceberg.Int32Literal(47)},
		{iceberg.TimestampLiteral(-1), iceberg.YearTransform{}, iceberg.Int32Literal(-1)},
		{iceberg.TimestampLiteral(1512151975038194), iceberg.MonthTransform{}, iceberg.Int32Literal(575)},
		{iceberg.TimestampLiteral(-1), iceberg.MonthTransform{}, iceberg.Int32Literal(-1)},
		{iceberg.TimestampLiteral(1512151975038194), iceberg.DayTransform{}, iceberg.Int32Literal(17501)},
		{iceberg.TimestampLiteral(-1), iceberg.DayTransform{}, iceberg.Int32Literal(-1)},
		{iceberg.DateLiteral(0), iceberg.YearTransform{}, iceberg.Int32Literal(0)},
		{iceberg.DateLiteral(0), iceberg.MonthTransform{}, iceberg.Int32Literal(0)},
		{iceberg.DateLiteral(0), iceberg.DayTransform{}, iceberg.Int32Literal(0)},
		{iceberg.TimestampLiteral(0), iceberg.YearTransform{}, iceberg.Int32Literal(0)},
		{iceberg.TimestampLiteral(0), iceberg.MonthTransform{}, iceberg.Int32Literal(0)},
		{iceberg.TimestampLiteral(0), iceberg.DayTransform{}, iceberg.Int32Literal(0)},
	}

	for _, tt := range tests {
		t.Run(tt.tf.String(), func(t *testing.T) {
			out := tt.tf.Apply(iceberg.Optional[iceberg.Literal]{
				Valid: true, Val: tt.val})

			assert.True(t, out.Valid)
			assert.Truef(t, out.Val.Equals(tt.expected), "got: %s, expected: %s",
				out.Val, tt.expected)
		})
	}
}

func TestTruncateTransform(t *testing.T) {

}
