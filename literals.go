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

package iceberg

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/decimal128"
	"github.com/google/uuid"
)

var (
	ErrBadCastLiteral = errors.New("could not cast literal")
	ErrBadLiteral     = errors.New("invalid literal value")
)

type Comparator[T literalType] func(v1, v2 T) int

func nullsFirstCmp[T literalType](cmp Comparator[T], v1, v2 optional[T]) int {
	if v1.isValid {
		if v2.isValid {
			return cmp(v1.val, v2.val)
		}
		return 1
	}
	return -1
}

type Literal interface {
	fmt.Stringer

	Type() Type
	To(Type) (Literal, error)
	Equals(Literal) bool
}

type OrderedLiteral interface {
	Literal

	Less(Literal) bool
	LessEqual(Literal) bool
	Greater(Literal) bool
	GreaterEqual(Literal) bool
}

type AboveMaxLiteral interface {
	Literal

	aboveMax()
}

type BelowMinLiteral interface {
	Literal

	belowMin()
}

type literalType interface {
	bool | int32 | int64 | float32 | float64 | Date |
		Time | Timestamp | string | []byte | uuid.UUID | Decimal
}

func MustLiteral[T literalType](val T) Literal {
	l, err := NewLiteral(val)
	if err != nil {
		panic(err)
	}
	return l
}

func NewLiteral[T literalType](val T) (Literal, error) {
	switch v := any(val).(type) {
	case bool:
		return BoolLiteral(v), nil
	case int32:
		return Int32Literal(v), nil
	case int64:
		return Int64Literal(v), nil
	case float32:
		return Float32Literal(v), nil
	case float64:
		return Float64Literal(v), nil
	case Date:
		return DateLiteral(v), nil
	case Time:
		return TimeLiteral(v), nil
	case Timestamp:
		return TimestampLiteral(v), nil
	case string:
		return StringLiteral(v), nil
	case []byte:
		return BinaryLiteral(v), nil
	case uuid.UUID:
		return UUIDLiteral(v), nil
	case Decimal:
		return DecimalLiteral(v), nil
	default:
		return nil, fmt.Errorf("%w: %v", ErrBadLiteral, val)
	}
}

type TypedLiteral[T literalType] interface {
	Literal

	Value() T
	Comparator() Comparator[T]
	createSet(...T) set[T]
}

func setOfLits[T literalType](vals ...TypedLiteral[T]) set[T] {
	s := ((TypedLiteral[T])(nil)).createSet()
	for _, v := range vals {
		s.add(v.Value())
	}
	return s
}

type cmpLiteral interface {
	literalType
	comparable
	cmp.Ordered
}

func literalEq[T cmpLiteral](lhs TypedLiteral[T], other Literal) bool {
	rhs, ok := other.(TypedLiteral[T])
	if !ok {
		return false
	}

	return lhs.Value() == rhs.Value()
}

func literalLt[T cmpLiteral](lhs TypedLiteral[T], other Literal) bool {
	rhs, ok := other.(TypedLiteral[T])
	if !ok {
		return false
	}

	return lhs.Value() < rhs.Value()
}

func literalGt[T cmpLiteral](lhs TypedLiteral[T], other Literal) bool {
	rhs, ok := other.(TypedLiteral[T])
	if !ok {
		return false
	}

	return lhs.Value() > rhs.Value()
}

func literalLte[T cmpLiteral](lhs TypedLiteral[T], other Literal) bool {
	rhs, ok := other.(TypedLiteral[T])
	if !ok {
		return false
	}

	return lhs.Value() <= rhs.Value()
}

func literalGte[T cmpLiteral](lhs TypedLiteral[T], other Literal) bool {
	rhs, ok := other.(TypedLiteral[T])
	if !ok {
		return false
	}

	return lhs.Value() >= rhs.Value()
}

type aboveMaxLiteral[T int32 | int64 | float32 | float64] struct {
	value T
}

func (ab aboveMaxLiteral[T]) aboveMax() {}

func (ab aboveMaxLiteral[T]) Type() Type {
	var z T
	switch any(z).(type) {
	case int32:
		return PrimitiveTypes.Int32
	case int64:
		return PrimitiveTypes.Int64
	case float32:
		return PrimitiveTypes.Float32
	case float64:
		return PrimitiveTypes.Float64
	default:
		panic("should never happen")
	}
}

func (ab aboveMaxLiteral[T]) To(t Type) (Literal, error) {
	if ab.Type().Equals(t) {
		return ab, nil
	}
	return nil, fmt.Errorf("%w: cannot change type of AboveMaxLiteral",
		ErrBadCastLiteral)
}

func (ab aboveMaxLiteral[T]) Value() T { return ab.value }

func (ab aboveMaxLiteral[T]) String() string { return "AboveMax" }
func (ab aboveMaxLiteral[T]) Equals(other Literal) bool {
	_, ok := other.(aboveMaxLiteral[T])
	return ok
}

type belowMinLiteral[T int32 | int64 | float32 | float64] struct {
	value T
}

func (bm belowMinLiteral[T]) belowMin() {}

func (bm belowMinLiteral[T]) Type() Type {
	var z T
	switch any(z).(type) {
	case int32:
		return PrimitiveTypes.Int32
	case int64:
		return PrimitiveTypes.Int64
	case float32:
		return PrimitiveTypes.Float32
	case float64:
		return PrimitiveTypes.Float64
	default:
		panic("should never happen")
	}
}

func (bm belowMinLiteral[T]) To(t Type) (Literal, error) {
	if bm.Type().Equals(t) {
		return bm, nil
	}
	return nil, fmt.Errorf("%w: cannot change type of BelowMinLiteral",
		ErrBadCastLiteral)
}

func (bm belowMinLiteral[T]) Value() T { return bm.value }

func (bm belowMinLiteral[T]) String() string { return "BelowMin" }
func (bm belowMinLiteral[T]) Equals(other Literal) bool {
	_, ok := other.(belowMinLiteral[T])
	return ok
}

func Int32AboveMaxLiteral() Literal {
	return aboveMaxLiteral[int32]{value: math.MaxInt32}
}

func Int64AboveMaxLiteral() Literal {
	return aboveMaxLiteral[int64]{value: math.MaxInt64}
}

func Float32AboveMaxLiteral() Literal {
	return aboveMaxLiteral[float32]{value: math.MaxFloat32}
}

func Float64AboveMaxLiteral() Literal {
	return aboveMaxLiteral[float64]{value: math.MaxFloat64}
}

func Int32BelowMinLiteral() Literal {
	return belowMinLiteral[int32]{value: math.MinInt32}
}

func Int64BelowMinLiteral() Literal {
	return belowMinLiteral[int64]{value: math.MinInt64}
}

func Float32BelowMinLiteral() Literal {
	return belowMinLiteral[float32]{value: -math.MaxFloat32}
}

func Float64BelowMinLiteral() Literal {
	return belowMinLiteral[float64]{value: -math.MaxFloat64}
}

type BoolLiteral bool

func (BoolLiteral) createSet(vals ...bool) set[bool] { return newset(vals...) }
func (BoolLiteral) Comparator() Comparator[bool] {
	return func(v1, v2 bool) int {
		if v1 {
			if v2 {
				return 0
			}
			return 1
		}
		return -1
	}
}

func (b BoolLiteral) Type() Type     { return PrimitiveTypes.Bool }
func (b BoolLiteral) Value() bool    { return bool(b) }
func (b BoolLiteral) String() string { return strconv.FormatBool(bool(b)) }
func (b BoolLiteral) To(t Type) (Literal, error) {
	switch t.(type) {
	case BooleanType:
		return b, nil
	}
	return nil, fmt.Errorf("%w: BoolLiteral to %s", ErrBadCastLiteral, t)
}

func (b BoolLiteral) Equals(l Literal) bool {
	rhs, ok := l.(BoolLiteral)
	if !ok {
		return false
	}
	return b == rhs
}

type Int32Literal int32

func (Int32Literal) createSet(vals ...int32) set[int32] { return newset(vals...) }
func (Int32Literal) Comparator() Comparator[int32]      { return cmp.Compare[int32] }
func (i Int32Literal) Type() Type                       { return PrimitiveTypes.Int32 }
func (i Int32Literal) Value() int32                     { return int32(i) }
func (i Int32Literal) String() string                   { return strconv.FormatInt(int64(i), 10) }
func (i Int32Literal) To(t Type) (Literal, error) {
	switch t := t.(type) {
	case Int32Type:
		if math.MaxInt32 < i {
			return Int32AboveMaxLiteral(), nil
		} else if math.MinInt32 > i {
			return Int32BelowMinLiteral(), nil
		}
		return i, nil
	case Int64Type:
		return Int64Literal(i), nil
	case Float32Type:
		return Float32Literal(i), nil
	case Float64Type:
		return Float64Literal(i), nil
	case DateType:
		return DateLiteral(i), nil
	case TimeType:
		return TimeLiteral(i), nil
	case TimestampType:
		return TimestampLiteral(i), nil
	case TimestampTzType:
		return TimestampLiteral(i), nil
	case DecimalType:
		unscaled := Decimal{Val: decimal128.FromI64(int64(i)), Scale: 0}
		if t.scale == 0 {
			return DecimalLiteral(unscaled), nil
		}
		out, err := unscaled.Val.Rescale(0, int32(t.scale))
		if err != nil {
			return nil, fmt.Errorf("%w: failed to cast to DecimalType: %s", ErrBadCastLiteral, err.Error())
		}
		return DecimalLiteral{Val: out, Scale: t.scale}, nil
	}

	return nil, fmt.Errorf("%w: Int32Literal to %s", ErrBadCastLiteral, t)
}

func (i Int32Literal) Equals(other Literal) bool {
	return literalEq(i, other)
}
func (i Int32Literal) Less(other Literal) bool {
	return literalLt(i, other)
}
func (i Int32Literal) LessEqual(other Literal) bool {
	return literalLte(i, other)
}
func (i Int32Literal) Greater(other Literal) bool {
	return literalGt(i, other)
}
func (i Int32Literal) GreaterEqual(other Literal) bool {
	return literalGte(i, other)
}

type Int64Literal int64

func (Int64Literal) createSet(vals ...int64) set[int64] { return newset(vals...) }
func (Int64Literal) Comparator() Comparator[int64]      { return cmp.Compare[int64] }
func (i Int64Literal) Type() Type                       { return PrimitiveTypes.Int64 }
func (i Int64Literal) Value() int64                     { return int64(i) }
func (i Int64Literal) String() string                   { return strconv.FormatInt(int64(i), 10) }
func (i Int64Literal) To(t Type) (Literal, error) {
	switch t := t.(type) {
	case Int32Type:
		if math.MaxInt32 < i {
			return Int32AboveMaxLiteral(), nil
		} else if math.MinInt32 > i {
			return Int32BelowMinLiteral(), nil
		}
		return Int32Literal(i), nil
	case Int64Type:
		if math.MaxInt64 < i {
			return Int64AboveMaxLiteral(), nil
		} else if math.MinInt64 > i {
			return Int64BelowMinLiteral(), nil
		}

		return i, nil
	case Float32Type:
		return Float32Literal(i), nil
	case Float64Type:
		return Float64Literal(i), nil
	case DateType:
		return DateLiteral(i), nil
	case TimeType:
		return TimeLiteral(i), nil
	case TimestampType:
		return TimestampLiteral(i), nil
	case TimestampTzType:
		return TimestampLiteral(i), nil
	case DecimalType:
		unscaled := Decimal{Val: decimal128.FromI64(int64(i)), Scale: 0}
		if t.scale == 0 {
			return DecimalLiteral(unscaled), nil
		}
		out, err := unscaled.Val.Rescale(0, int32(t.scale))
		if err != nil {
			return nil, fmt.Errorf("%w: failed to cast to DecimalType: %s", ErrBadCastLiteral, err.Error())
		}
		return DecimalLiteral{Val: out, Scale: t.scale}, nil
	}

	return nil, fmt.Errorf("%w: Int64Literal to %s", ErrBadCastLiteral, t)
}
func (i Int64Literal) Equals(other Literal) bool {
	return literalEq(i, other)
}
func (i Int64Literal) Less(other Literal) bool {
	return literalLt(i, other)
}
func (i Int64Literal) LessEqual(other Literal) bool {
	return literalLte(i, other)
}
func (i Int64Literal) Greater(other Literal) bool {
	return literalGt(i, other)
}
func (i Int64Literal) GreaterEqual(other Literal) bool {
	return literalGte(i, other)
}

type Float32Literal float32

func (Float32Literal) createSet(vals ...float32) set[float32] { return newset(vals...) }
func (Float32Literal) Comparator() Comparator[float32]        { return cmp.Compare[float32] }
func (f Float32Literal) Type() Type                           { return PrimitiveTypes.Float32 }
func (f Float32Literal) Value() float32                       { return float32(f) }
func (f Float32Literal) String() string                       { return strconv.FormatFloat(float64(f), 'g', -1, 32) }
func (f Float32Literal) To(t Type) (Literal, error) {
	switch t := t.(type) {
	case Float32Type:
		return f, nil
	case Float64Type:
		return Float64Literal(f), nil
	case DecimalType:
		v, err := decimal128.FromFloat32(float32(f), int32(t.precision), int32(t.scale))
		if err != nil {
			return nil, err
		}
		return DecimalLiteral{Val: v, Scale: t.scale}, nil
	}

	return nil, fmt.Errorf("%w: Float32Literal to %s", ErrBadCastLiteral, t)
}
func (f Float32Literal) Equals(other Literal) bool {
	return literalEq(f, other)
}
func (f Float32Literal) Less(other Literal) bool {
	return literalLt(f, other)
}
func (f Float32Literal) LessEqual(other Literal) bool {
	return literalLte(f, other)
}
func (f Float32Literal) Greater(other Literal) bool {
	return literalGt(f, other)
}
func (f Float32Literal) GreaterEqual(other Literal) bool {
	return literalGte(f, other)
}

type Float64Literal float64

func (Float64Literal) createSet(vals ...float64) set[float64] { return newset(vals...) }
func (Float64Literal) Comparator() Comparator[float64]        { return cmp.Compare[float64] }
func (f Float64Literal) Type() Type                           { return PrimitiveTypes.Float64 }
func (f Float64Literal) Value() float64                       { return float64(f) }
func (f Float64Literal) String() string                       { return strconv.FormatFloat(float64(f), 'g', -1, 64) }
func (f Float64Literal) To(t Type) (Literal, error) {
	switch t := t.(type) {
	case Float32Type:
		if math.MaxFloat32 < f {
			return Float32AboveMaxLiteral(), nil
		} else if -math.MaxFloat32 > f {
			return Float32BelowMinLiteral(), nil
		}
		return Float32Literal(f), nil
	case Float64Type:
		return f, nil
	case DecimalType:
		v, err := decimal128.FromFloat64(float64(f), int32(t.precision), int32(t.scale))
		if err != nil {
			return nil, err
		}
		return DecimalLiteral{Val: v, Scale: t.scale}, nil
	}

	return nil, fmt.Errorf("%w: Float64Literal to %s", ErrBadCastLiteral, t)
}
func (f Float64Literal) Equals(other Literal) bool {
	return literalEq(f, other)
}
func (f Float64Literal) Less(other Literal) bool {
	return literalLt(f, other)
}
func (f Float64Literal) LessEqual(other Literal) bool {
	return literalLte(f, other)
}
func (f Float64Literal) Greater(other Literal) bool {
	return literalGt(f, other)
}
func (f Float64Literal) GreaterEqual(other Literal) bool {
	return literalGte(f, other)
}

type DateLiteral Date

func (DateLiteral) createSet(vals ...Date) set[Date] { return newset(vals...) }
func (DateLiteral) Comparator() Comparator[Date]     { return cmp.Compare[Date] }
func (d DateLiteral) Type() Type                     { return PrimitiveTypes.Date }
func (d DateLiteral) Value() Date                    { return Date(d) }
func (d DateLiteral) String() string {
	t := time.Unix(0, 0).UTC().AddDate(0, 0, int(d))
	return t.Format("2006-01-02")
}
func (d DateLiteral) To(t Type) (Literal, error) {
	switch t.(type) {
	case DateType:
		return d, nil
	}
	return nil, fmt.Errorf("%w: DateLiteral to %s", ErrBadCastLiteral, t)
}
func (d DateLiteral) Equals(other Literal) bool {
	return literalEq(d, other)
}
func (d DateLiteral) Less(other Literal) bool {
	return literalLt(d, other)
}
func (d DateLiteral) LessEqual(other Literal) bool {
	return literalLte(d, other)
}
func (d DateLiteral) Greater(other Literal) bool {
	return literalGt(d, other)
}
func (d DateLiteral) GreaterEqual(other Literal) bool {
	return literalGte(d, other)
}

type TimeLiteral Time

func (TimeLiteral) createSet(vals ...Time) set[Time] { return newset(vals...) }
func (TimeLiteral) Comparator() Comparator[Time]     { return cmp.Compare[Time] }
func (t TimeLiteral) Type() Type                     { return PrimitiveTypes.Time }
func (t TimeLiteral) Value() Time                    { return Time(t) }
func (t TimeLiteral) String() string {
	tm := time.UnixMicro(int64(t)).UTC()
	return tm.Format("15:04:05.000000")
}
func (t TimeLiteral) To(typ Type) (Literal, error) {
	switch typ.(type) {
	case TimeType:
		return t, nil
	}
	return nil, fmt.Errorf("%w: TimeLiteral to %s", ErrBadCastLiteral, typ)

}
func (t TimeLiteral) Equals(other Literal) bool {
	return literalEq(t, other)
}
func (t TimeLiteral) Less(other Literal) bool {
	return literalLt(t, other)
}
func (t TimeLiteral) LessEqual(other Literal) bool {
	return literalLte(t, other)
}
func (t TimeLiteral) Greater(other Literal) bool {
	return literalGt(t, other)
}
func (t TimeLiteral) GreaterEqual(other Literal) bool {
	return literalGte(t, other)
}

type TimestampLiteral Timestamp

func (TimestampLiteral) createSet(vals ...Timestamp) set[Timestamp] { return newset(vals...) }
func (TimestampLiteral) Comparator() Comparator[Timestamp]          { return cmp.Compare[Timestamp] }
func (t TimestampLiteral) Type() Type                               { return PrimitiveTypes.Timestamp }
func (t TimestampLiteral) Value() Timestamp                         { return Timestamp(t) }
func (t TimestampLiteral) String() string {
	tm := time.UnixMicro(int64(t)).UTC()
	return tm.Format("2006-01-02 15:04:05.000000")
}
func (t TimestampLiteral) To(typ Type) (Literal, error) {
	switch typ.(type) {
	case TimestampType:
		return t, nil
	case TimestampTzType:
		return t, nil
	case DateType:
		tm := time.UnixMicro(int64(t)).UTC()
		return DateLiteral(tm.Truncate(24*time.Hour).Unix() / int64((time.Hour * 24).Seconds())), nil
	}
	return nil, fmt.Errorf("%w: TimestampLiteral to %s", ErrBadCastLiteral, typ)
}
func (t TimestampLiteral) Equals(other Literal) bool {
	return literalEq(t, other)
}
func (t TimestampLiteral) Less(other Literal) bool {
	return literalLt(t, other)
}
func (t TimestampLiteral) LessEqual(other Literal) bool {
	return literalLte(t, other)
}
func (t TimestampLiteral) Greater(other Literal) bool {
	return literalGt(t, other)
}
func (t TimestampLiteral) GreaterEqual(other Literal) bool {
	return literalGte(t, other)
}

type StringLiteral string

func (StringLiteral) createSet(vals ...string) set[string] { return newset(vals...) }
func (StringLiteral) Comparator() Comparator[string]       { return cmp.Compare[string] }
func (s StringLiteral) Type() Type                         { return PrimitiveTypes.String }
func (s StringLiteral) Value() string                      { return string(s) }
func (s StringLiteral) String() string                     { return string(s) }
func (s StringLiteral) To(typ Type) (Literal, error) {
	switch t := typ.(type) {
	case StringType:
		return s, nil
	case Int32Type:
		n, err := strconv.ParseInt(string(s), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("%w: casting '%s' to %s - %s",
				ErrBadCastLiteral, s, typ, err.Error())
		}

		if math.MaxInt32 < n {
			return Int32AboveMaxLiteral(), nil
		} else if math.MinInt32 > n {
			return Int32BelowMinLiteral(), nil
		}

		return Int32Literal(n), nil
	case Int64Type:
		n, err := strconv.ParseInt(string(s), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: casting '%s' to %s - %s",
				ErrBadCastLiteral, s, typ, err.Error())
		}

		if math.MaxInt64 < n {
			return Int64AboveMaxLiteral(), nil
		} else if math.MinInt64 > n {
			return Int64BelowMinLiteral(), nil
		}

		return Int64Literal(n), nil
	case DateType:
		tm, err := time.Parse("2006-01-02", string(s))
		if err != nil {
			return nil, fmt.Errorf("%w: casting '%s' to %s - %s",
				ErrBadCastLiteral, s, typ, err.Error())
		}
		return DateLiteral(tm.Truncate(24*time.Hour).Unix() / int64((time.Hour * 24).Seconds())), nil
	case TimeType:
		val, err := arrow.Time64FromString(string(s), arrow.Microsecond)
		if err != nil {
			return nil, fmt.Errorf("%w: casting '%s' to %s - %s",
				ErrBadCastLiteral, s, typ, err.Error())
		}

		return TimeLiteral(val), nil
	case TimestampType:
		val, err := arrow.TimestampFromString(string(s), arrow.Microsecond)
		if err != nil {
			return nil, fmt.Errorf("%w: casting '%s' to %s - %s",
				ErrBadCastLiteral, s, typ, err.Error())
		}
		return TimestampLiteral(val), nil
	case TimestampTzType:
		val, err := arrow.TimestampFromString(string(s), arrow.Microsecond)
		if err != nil {
			return nil, fmt.Errorf("%w: casting '%s' to %s - %s",
				ErrBadCastLiteral, s, typ, err.Error())
		}
		return TimestampLiteral(val), nil
	case UUIDType:
		val, err := uuid.Parse(string(s))
		if err != nil {
			return nil, fmt.Errorf("%w: casting '%s' to %s - %s",
				ErrBadCastLiteral, s, typ, err.Error())
		}
		return UUIDLiteral(val), nil
	case DecimalType:
		n, err := decimal128.FromString(string(s), int32(t.precision), int32(t.scale))
		if err != nil {
			return nil, fmt.Errorf("%w: casting '%s' to %s - %s",
				ErrBadCastLiteral, s, typ, err.Error())
		}
		return DecimalLiteral{Val: n, Scale: t.scale}, nil
	case BooleanType:
		val, err := strconv.ParseBool(string(s))
		if err != nil {
			return nil, fmt.Errorf("%w: casting '%s' to %s - %s",
				ErrBadCastLiteral, s, typ, err.Error())
		}
		return BoolLiteral(val), nil
	}
	return nil, fmt.Errorf("%w: StringLiteral to %s", ErrBadCastLiteral, typ)
}
func (s StringLiteral) Equals(other Literal) bool {
	return literalEq(s, other)
}
func (s StringLiteral) Less(other Literal) bool {
	return literalLt(s, other)
}
func (s StringLiteral) LessEqual(other Literal) bool {
	return literalLte(s, other)
}
func (s StringLiteral) Greater(other Literal) bool {
	return literalGt(s, other)
}
func (s StringLiteral) GreaterEqual(other Literal) bool {
	return literalGte(s, other)
}

type BinaryLiteral []byte

func (BinaryLiteral) createSet(vals ...[]byte) set[[]byte] { return newByteSliceSet(vals...) }
func (BinaryLiteral) Comparator() Comparator[[]byte] {
	return bytes.Compare
}
func (b BinaryLiteral) Type() Type     { return PrimitiveTypes.Binary }
func (b BinaryLiteral) Value() []byte  { return []byte(b) }
func (b BinaryLiteral) String() string { return string(b) }
func (b BinaryLiteral) To(typ Type) (Literal, error) {
	switch t := typ.(type) {
	case UUIDType:
		val, err := uuid.FromBytes(b)
		if err != nil {
			return nil, fmt.Errorf("%w: cannot convert BinaryLiteral to UUID - %s",
				ErrBadCastLiteral, err.Error())
		}
		return UUIDLiteral(val), nil
	case FixedType:
		if len(b) == t.len {
			return FixedLiteral(b), nil
		}

		return nil, fmt.Errorf("%w: cannot convert BinaryLiteral to %s, different length - %d <> %d",
			ErrBadCastLiteral, typ, len(b), t.len)
	case BinaryType:
		return b, nil
	}

	return nil, fmt.Errorf("%w: BinaryLiteral to %s", ErrBadCastLiteral, typ)
}
func (b BinaryLiteral) Equals(other Literal) bool {
	rhs, ok := other.(BinaryLiteral)
	if !ok {
		return false
	}

	return bytes.Equal([]byte(b), rhs)
}
func (b BinaryLiteral) Less(other Literal) bool {
	rhs, ok := other.(BinaryLiteral)
	if !ok {
		return false
	}

	return bytes.Compare([]byte(b), rhs) < 0
}
func (b BinaryLiteral) LessEqual(other Literal) bool {
	rhs, ok := other.(BinaryLiteral)
	if !ok {
		return false
	}

	return bytes.Compare([]byte(b), rhs) <= 0
}
func (b BinaryLiteral) Greater(other Literal) bool {
	rhs, ok := other.(BinaryLiteral)
	if !ok {
		return false
	}

	return bytes.Compare([]byte(b), rhs) > 0
}
func (b BinaryLiteral) GreaterEqual(other Literal) bool {
	rhs, ok := other.(BinaryLiteral)
	if !ok {
		return false
	}

	return bytes.Compare([]byte(b), rhs) >= 0
}

type FixedLiteral []byte

func (FixedLiteral) createSet(vals ...[]byte) set[[]byte] { return newByteSliceSet(vals...) }
func (FixedLiteral) Comparator() Comparator[[]byte]       { return bytes.Compare }
func (f FixedLiteral) Type() Type                         { return FixedTypeOf(len(f)) }
func (f FixedLiteral) Value() []byte                      { return []byte(f) }
func (f FixedLiteral) String() string                     { return string(f) }
func (f FixedLiteral) To(typ Type) (Literal, error) {
	switch t := typ.(type) {
	case UUIDType:
		val, err := uuid.FromBytes(f)
		if err != nil {
			return nil, fmt.Errorf("%w: cannot convert BinaryLiteral to UUID - %s",
				ErrBadCastLiteral, err.Error())
		}
		return UUIDLiteral(val), nil
	case FixedType:
		if len(f) == t.len {
			return FixedLiteral(f), nil
		}

		return nil, fmt.Errorf("%w: cannot convert BinaryLiteral to %s, different length - %d <> %d",
			ErrBadCastLiteral, typ, len(f), t.len)
	case BinaryType:
		return f, nil
	}

	return nil, fmt.Errorf("%w: FixedLiteral[%d] to %s",
		ErrBadCastLiteral, len(f), typ)
}
func (f FixedLiteral) Equals(other Literal) bool {
	rhs, ok := other.(FixedLiteral)
	if !ok {
		return false
	}

	return bytes.Equal([]byte(f), rhs)
}
func (f FixedLiteral) Less(other Literal) bool {
	rhs, ok := other.(FixedLiteral)
	if !ok {
		return false
	}

	return bytes.Compare([]byte(f), rhs) < 0
}
func (f FixedLiteral) LessEqual(other Literal) bool {
	rhs, ok := other.(FixedLiteral)
	if !ok {
		return false
	}

	return bytes.Compare([]byte(f), rhs) <= 0
}
func (f FixedLiteral) Greater(other Literal) bool {
	rhs, ok := other.(FixedLiteral)
	if !ok {
		return false
	}

	return bytes.Compare([]byte(f), rhs) > 0
}
func (f FixedLiteral) GreaterEqual(other Literal) bool {
	rhs, ok := other.(FixedLiteral)
	if !ok {
		return false
	}

	return bytes.Compare([]byte(f), rhs) >= 0
}

type UUIDLiteral uuid.UUID

func (UUIDLiteral) createSet(vals ...uuid.UUID) set[uuid.UUID] { return newset(vals...) }
func (UUIDLiteral) Comparator() Comparator[uuid.UUID] {
	return func(v1, v2 uuid.UUID) int {
		return bytes.Compare(v1[:], v2[:])
	}
}

func (UUIDLiteral) Type() Type         { return PrimitiveTypes.UUID }
func (u UUIDLiteral) Value() uuid.UUID { return uuid.UUID(u) }
func (u UUIDLiteral) String() string   { return uuid.UUID(u).String() }
func (u UUIDLiteral) To(typ Type) (Literal, error) {
	switch t := typ.(type) {
	case UUIDType:
		return u, nil
	case FixedType:
		if len(u) == t.len {
			v, _ := uuid.UUID(u).MarshalBinary()
			return FixedLiteral(v), nil
		}

		return nil, fmt.Errorf("%w: cannot convert UUIDLiteral to %s, different length - %d <> %d",
			ErrBadCastLiteral, typ, len(u), t.len)
	case BinaryType:
		v, _ := uuid.UUID(u).MarshalBinary()
		return BinaryLiteral(v), nil
	}

	return nil, fmt.Errorf("%w: UUIDLiteral to %s", ErrBadCastLiteral, typ)
}
func (u UUIDLiteral) Equals(other Literal) bool {
	rhs, ok := other.(UUIDLiteral)
	if !ok {
		return false
	}

	return uuid.UUID(u) == uuid.UUID(rhs)
}

type DecimalLiteral Decimal

func (DecimalLiteral) createSet(vals ...Decimal) set[Decimal] { return newset(vals...) }
func (DecimalLiteral) Comparator() Comparator[Decimal] {
	return func(v1, v2 Decimal) int {
		if v1.Scale == v2.Scale {
			return v1.Val.Cmp(v2.Val)
		}

		rescaled, err := v2.Val.Rescale(int32(v2.Scale), int32(v1.Scale))
		if err != nil {
			return -1
		}

		return v1.Val.Cmp(rescaled)
	}
}
func (d DecimalLiteral) Type() Type     { return DecimalTypeOf(9, d.Scale) }
func (d DecimalLiteral) Value() Decimal { return Decimal(d) }
func (d DecimalLiteral) String() string {
	return d.Val.ToString(int32(d.Scale))
}

func (d DecimalLiteral) To(t Type) (Literal, error) {
	switch t := t.(type) {
	case DecimalType:
		if d.Scale == t.scale {
			return d, nil
		}

		return nil, fmt.Errorf("%w: could not convert %v to %s",
			ErrBadCastLiteral, d, t)
	case Int32Type:
		v := d.Val.BigInt().Int64()
		if v > math.MaxInt32 {
			return Int32AboveMaxLiteral(), nil
		} else if v < math.MinInt32 {
			return Int32BelowMinLiteral(), nil
		}

		return Int32Literal(int32(v)), nil
	case Int64Type:
		v := d.Val.BigInt().Int64()
		if v > math.MaxInt64 {
			return Int64AboveMaxLiteral(), nil
		} else if v < math.MinInt64 {
			return Int64BelowMinLiteral(), nil
		}

		return Int64Literal(v), nil
	case Float32Type:
		v := d.Val.ToFloat64(int32(d.Scale))
		if v > math.MaxFloat32 {
			return Float32AboveMaxLiteral(), nil
		} else if v < -math.MaxFloat32 {
			return Float32BelowMinLiteral(), nil
		}

		return Float32Literal(float32(v)), nil
	case Float64Type:
		return Float64Literal(d.Val.ToFloat64(int32(d.Scale))), nil
	}

	return nil, fmt.Errorf("%w: DecimalLiteral to %s", ErrBadCastLiteral, t)
}

func (d DecimalLiteral) Equals(other Literal) bool {
	rhs, ok := other.(DecimalLiteral)
	if !ok {
		return false
	}

	rescaled, err := rhs.Val.Rescale(int32(rhs.Scale), int32(d.Scale))
	if err != nil {
		return false
	}
	return d.Val == rescaled
}

func (d DecimalLiteral) Less(other Literal) bool {
	rhs, ok := other.(DecimalLiteral)
	if !ok {
		return false
	}

	rescaled, err := rhs.Val.Rescale(int32(rhs.Scale), int32(d.Scale))
	if err != nil {
		return false
	}

	return d.Val.Less(rescaled)
}
func (d DecimalLiteral) LessEqual(other Literal) bool {
	rhs, ok := other.(DecimalLiteral)
	if !ok {
		return false
	}

	rescaled, err := rhs.Val.Rescale(int32(rhs.Scale), int32(d.Scale))
	if err != nil {
		return false
	}

	return d.Val.LessEqual(rescaled)
}

func (d DecimalLiteral) Greater(other Literal) bool {
	rhs, ok := other.(DecimalLiteral)
	if !ok {
		return false
	}

	rescaled, err := rhs.Val.Rescale(int32(rhs.Scale), int32(d.Scale))
	if err != nil {
		return false
	}

	return d.Val.Greater(rescaled)
}

func (d DecimalLiteral) GreaterEqual(other Literal) bool {
	rhs, ok := other.(DecimalLiteral)
	if !ok {
		return false
	}

	rescaled, err := rhs.Val.Rescale(int32(rhs.Scale), int32(d.Scale))
	if err != nil {
		return false
	}

	return d.Val.GreaterEqual(rescaled)
}
