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
	"fmt"
	"math"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNumericLiteralComparison(t *testing.T) {
	smallLit, err := iceberg.NewLiteral[int32](10)
	require.NoError(t, err)
	bigLit, err := iceberg.NewLiteral(int32(1000))
	require.NoError(t, err)

	smallOrdered := smallLit.(iceberg.OrderedLiteral)

	assert.False(t, smallOrdered.Equals(bigLit))
	assert.True(t, smallOrdered.Equals(iceberg.Int32Literal(10)))
	assert.True(t, smallOrdered.Less(bigLit))
	assert.True(t, smallOrdered.LessEqual(bigLit))
	assert.False(t, smallOrdered.Greater(bigLit))
	assert.False(t, smallOrdered.GreaterEqual(bigLit))
}

func TestLiteralConversion(t *testing.T) {
	tests := []struct {
		from     iceberg.Literal
		expected iceberg.Literal
	}{
		{iceberg.MustLiteral(int32(10)), iceberg.MustLiteral(int64(10))},
		{iceberg.MustLiteral(int32(34)), iceberg.MustLiteral(float32(34))},
		{iceberg.MustLiteral(int32(34)), iceberg.MustLiteral(float64(34))},
		{iceberg.MustLiteral(int64(34)), iceberg.MustLiteral(int32(34))},
		{iceberg.MustLiteral(int64(math.MaxInt32 + 1)), iceberg.Int32AboveMaxLiteral()},
		{iceberg.MustLiteral(int64(math.MinInt32 - 1)), iceberg.Int32BelowMinLiteral()},
		{iceberg.MustLiteral(int64(34)), iceberg.MustLiteral(float32(34))},
		{iceberg.MustLiteral(int64(34)), iceberg.MustLiteral(float64(34))},
		{iceberg.MustLiteral(int64(51661919000)), iceberg.MustLiteral(iceberg.Time(51661919000))},
		{iceberg.MustLiteral(int64(1647305201)), iceberg.MustLiteral(iceberg.Timestamp(1647305201))},
		{iceberg.MustLiteral(float32(1.5)), iceberg.MustLiteral(float64(1.5))},
		{iceberg.MustLiteral(float64(1.5)), iceberg.MustLiteral(float32(1.5))},
		{iceberg.MustLiteral(float64(math.MaxFloat32 + 1.0e37)), iceberg.Float32AboveMaxLiteral()},
		{iceberg.MustLiteral(float64(-math.MaxFloat32 - 1.0e37)), iceberg.Float32BelowMinLiteral()},
		{iceberg.MustLiteral("abc"), iceberg.MustLiteral("abc")},
		{iceberg.MustLiteral("2022-03-28"), iceberg.MustLiteral(iceberg.Date(19079))},
		{iceberg.MustLiteral("14:21:01.919"), iceberg.MustLiteral(iceberg.Time(51661919000))},
		{iceberg.MustLiteral("2017-08-18T14:21:01.919234+00:00"),
			iceberg.MustLiteral(iceberg.Timestamp(1503066061919234))},
		{iceberg.MustLiteral("2017-08-18T14:21:01.919234"),
			iceberg.MustLiteral(iceberg.Timestamp(1503066061919234))},
		{iceberg.MustLiteral("2017-08-18T14:21:01.919234-07:00"),
			iceberg.MustLiteral(iceberg.Timestamp(1503091261919234))},
		{iceberg.MustLiteral("true"), iceberg.BoolLiteral(true)},
		{iceberg.MustLiteral("True"), iceberg.BoolLiteral(true)},
		{iceberg.MustLiteral("false"), iceberg.BoolLiteral(false)},
		{iceberg.MustLiteral("False"), iceberg.BoolLiteral(false)},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s to %s", tt.from.Type(), tt.expected.Type()), func(t *testing.T) {
			out, err := tt.from.To(tt.expected.Type())
			assert.NoError(t, err)
			assert.True(t, tt.expected.Equals(out))
		})
	}
}

func TestIdentityLiteralConversions(t *testing.T) {
	tests := []iceberg.Literal{
		iceberg.BoolLiteral(true),
		iceberg.Int32Literal(34),
		iceberg.Int64Literal(340000000),
		iceberg.Float32Literal(34.11),
		iceberg.Float64Literal(3.5028235e38),
		iceberg.DateLiteral(19079),
		iceberg.TimeLiteral(51661919000),
		iceberg.TimestampLiteral(1503066061919234),
		iceberg.StringLiteral("abc"),
		iceberg.UUIDLiteral(uuid.New()),
		iceberg.BinaryLiteral([]byte{0x01, 0x02, 0x03}),
		iceberg.FixedLiteral([]byte{0x01, 0x02, 0x03}),
	}

	for _, tt := range tests {
		t.Run(tt.String(), func(t *testing.T) {
			out, err := tt.To(tt.Type())
			require.NoError(t, err)
			assert.True(t, tt.Equals(out))
		})
	}
}
