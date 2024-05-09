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
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/decimal128"
	"github.com/google/uuid"
)

type LiteralType interface {
	bool | int32 | int64 | float32 | float64 | Date |
		Time | Timestamp | string | []byte | uuid.UUID | Decimal
}

type Comparator[T LiteralType] func(v1, v2 T) int

func nullsFirst[T LiteralType](cmp Comparator[T]) func(v1, v2 Optional[T]) int {
	return func(v1, v2 Optional[T]) int {
		return nullsFirstCmp(cmp, v1, v2)
	}
}

func nullsFirstCmp[T LiteralType](cmp Comparator[T], v1, v2 Optional[T]) int {
	if !v1.Valid {
		if !v2.Valid {
			return 0
		}
		return -1
	}

	if !v2.Valid {
		return 1
	}

	return cmp(v1.Val, v2.Val)
}

type Literal interface {
	fmt.Stringer

	Type() Type
	To(Type) (Literal, error)
	Equals(Literal) bool
}

type TypedLiteral[T LiteralType] interface {
	Literal

	Value() T
	Comparator() Comparator[T]

	createSet(...T) set[T]
}

func MustLiteral[T LiteralType](val T) Literal {
	l, err := NewLiteral(val)
	if err != nil {
		panic(err)
	}
	return l
}

func NewLiteralFromType(v any, typ Type) (Literal, error) {
	switch typ := typ.(type) {
	case BooleanType:
		v, ok := v.(bool)
		if !ok {
			return nil, ErrBadLiteral
		}
		return BoolLiteral(v), nil
	case Int32Type:
		switch v := v.(type) {
		case int32:
			return Int32Literal(v), nil
		case int:
			if bits.UintSize == 32 {
				return Int32Literal(v), nil
			}
			// handle AboveMax / BelowMin
			return Int64Literal(v).To(PrimitiveTypes.Int32)
		case int64:
			return Int64Literal(v).To(PrimitiveTypes.Int32)
		case uint32:
			return Int64Literal(v).To(PrimitiveTypes.Int32)
		case uint64:
			return Int64Literal(v).To(PrimitiveTypes.Int32)
		case uint:
			return Int64Literal(v).To(PrimitiveTypes.Int32)
		}

		return nil, ErrBadLiteral
	case Int64Type:
		switch v := v.(type) {
		case int:
			return Int64Literal(v), nil
		case int32:
			return Int64Literal(v), nil
		case int64:
			return Int64Literal(v), nil
		case uint:
			if bits.UintSize == 32 {
				return Int64Literal(v), nil
			}

			if v > math.MaxInt64 {
				return Int64AboveMaxLiteral(), nil
			}
			return Int64Literal(v), nil
		case uint32:
			return Int64Literal(v), nil
		case uint64:
			if v > math.MaxInt64 {
				return Int64AboveMaxLiteral(), nil
			}
			return Int64Literal(v), nil
		}

		return nil, ErrBadLiteral
	case Float32Type:
		switch v := v.(type) {
		case float32:
			return Float32Literal(v), nil
		case float64:
			// handle abovemax/belowmin
			return Float64Literal(v).To(PrimitiveTypes.Float32)
		}

		return nil, ErrBadLiteral
	case Float64Type:
		switch v := v.(type) {
		case float32:
			return Float64Literal(v), nil
		case float64:
			return Float64Literal(v), nil
		}

		return nil, ErrBadLiteral
	case DateType:
		switch v := v.(type) {
		case Date:
			return DateLiteral(v), nil
		case int32:
			return DateLiteral(v), nil
		case int:
			return DateLiteral(v), nil
		case uint32:
			return DateLiteral(v), nil
		case uint:
			return DateLiteral(v), nil
		}

		return nil, ErrBadLiteral
	case TimeType:
		switch v := v.(type) {
		case Time:
			return TimeLiteral(v), nil
		case int32:
			return TimeLiteral(v), nil
		case int:
			return TimeLiteral(v), nil
		case int64:
			return TimeLiteral(v), nil
		case uint32:
			return TimeLiteral(v), nil
		case uint:
			return TimeLiteral(v), nil
		case uint64:
			return TimeLiteral(v), nil
		}

		return nil, ErrBadLiteral
	case TimestampType, TimestampTzType:
		switch v := v.(type) {
		case Timestamp:
			return TimestampLiteral(v), nil
		case int32:
			return TimestampLiteral(v), nil
		case int:
			return TimestampLiteral(v), nil
		case int64:
			return TimestampLiteral(v), nil
		case uint32:
			return TimestampLiteral(v), nil
		case uint:
			return TimestampLiteral(v), nil
		case uint64:
			return TimestampLiteral(v), nil
		}

		return nil, ErrBadLiteral
	case BinaryType:
		v, ok := v.([]byte)
		if !ok {
			return nil, ErrBadLiteral
		}

		return BinaryLiteral(v), nil
	case FixedType:
		v, ok := v.([]byte)
		if !ok {
			return nil, ErrBadLiteral
		}

		if len(v) != typ.Len() {
			return nil, ErrBadLiteral
		}

		return FixedLiteral(v), nil
	case StringType:
		v, ok := v.(string)
		if !ok {
			return nil, ErrBadLiteral
		}

		return StringLiteral(v), nil
	case UUIDType:
		v, ok := v.(uuid.UUID)
		if !ok {
			return nil, ErrBadLiteral
		}

		return UUIDLiteral(v), nil
	case DecimalType:
		switch v := v.(type) {
		case Decimal:
			if v.Scale != typ.scale {
				out, err := v.Val.Rescale(int32(v.Scale), int32(typ.scale))
				if err != nil {
					return nil, err
				}
				return DecimalLiteral(Decimal{Val: out, Scale: typ.scale}), nil
			}

			return DecimalLiteral(v), nil
		case decimal128.Num:
			return DecimalLiteral(Decimal{Val: v, Scale: typ.scale}), nil
		}

		return nil, ErrBadLiteral
	default:
		return nil, ErrBadLiteral
	}
}

func NewLiteral[T LiteralType](val T) (Literal, error) {
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

func createLiteralSet(t Type, vals ...Literal) (literalSet, error) {
	switch t.(type) {
	case BooleanType:
		return toLiteralSet[bool](t, vals...)
	case Int32Type:
		return toLiteralSet[int32](t, vals...)
	case Int64Type:
		return toLiteralSet[int64](t, vals...)
	case Float32Type:
		return toLiteralSet[float32](t, vals...)
	case Float64Type:
		return toLiteralSet[float64](t, vals...)
	case DateType:
		return toLiteralSet[Date](t, vals...)
	case TimeType:
		return toLiteralSet[Time](t, vals...)
	case TimestampType, TimestampTzType:
		return toLiteralSet[Timestamp](t, vals...)
	case DecimalType:
		return toLiteralSet[Decimal](t, vals...)
	case StringType:
		return toLiteralSet[string](t, vals...)
	case BinaryType, FixedType:
		return toLiteralSet[[]byte](t, vals...)
	case UUIDType:
		return toLiteralSet[uuid.UUID](t, vals...)
	}
	return nil, ErrType
}

func toLiteralSet[T LiteralType](t Type, vals ...Literal) (set[T], error) {
	if len(vals) == 0 {
		return nil, nil
	}

	s := vals[0].(TypedLiteral[T]).createSet()
	for _, v := range vals {
		typed, err := v.To(t)
		if err != nil {
			return nil, err
		}

		s.add(typed.(TypedLiteral[T]).Value())
	}

	return s, nil
}

// func setOfLits[T LiteralType](vals ...TypedLiteral[T]) set[T] {
// 	s := ((TypedLiteral[T])(nil)).createSet()
// 	for _, v := range vals {
// 		s.add(v.Value())
// 	}
// 	return s
// }

func literalEq[L interface {
	comparable
	LiteralType
}, T TypedLiteral[L]](lhs T, other Literal) bool {
	rhs, ok := other.(T)
	if !ok {
		return false
	}

	return lhs.Value() == rhs.Value()
}

type AboveMaxLiteral interface {
	Literal

	aboveMax()
}

type BelowMinLiteral interface {
	Literal

	belowMin()
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
	return nil, fmt.Errorf("%w: cannot change type of AboveMax%sLiteral",
		ErrBadCast, reflect.TypeOf(T(0)).String())
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
	return nil, fmt.Errorf("%w: cannot change type of BelowMin%sLiteral",
		ErrBadCast, reflect.TypeOf(T(0)).String())
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

func (BoolLiteral) createSet(vals ...bool) set[bool] { return newPrimitiveSet(vals...) }
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
	return nil, fmt.Errorf("%w: BoolLiteral to %s", ErrBadCast, t)
}

func (b BoolLiteral) Equals(l Literal) bool {
	rhs, ok := l.(BoolLiteral)
	if !ok {
		return false
	}
	return b == rhs
}

type Int32Literal int32

func (Int32Literal) createSet(vals ...int32) set[int32] { return newPrimitiveSet(vals...) }
func (Int32Literal) Comparator() Comparator[int32]      { return cmp.Compare[int32] }
func (i Int32Literal) Type() Type                       { return PrimitiveTypes.Int32 }
func (i Int32Literal) Value() int32                     { return int32(i) }
func (i Int32Literal) String() string                   { return strconv.FormatInt(int64(i), 10) }
func (i Int32Literal) To(t Type) (Literal, error) {
	switch t := t.(type) {
	case Int32Type:
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
			return nil, fmt.Errorf("%w: failed to cast to DecimalType: %s", ErrBadCast, err.Error())
		}
		return DecimalLiteral{Val: out, Scale: t.scale}, nil
	}

	return nil, fmt.Errorf("%w: Int32Literal to %s", ErrBadCast, t)
}

func (i Int32Literal) Equals(other Literal) bool {
	return literalEq(i, other)
}

type Int64Literal int64

func (Int64Literal) createSet(vals ...int64) set[int64] { return newPrimitiveSet(vals...) }
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
			return nil, fmt.Errorf("%w: failed to cast to DecimalType: %s", ErrBadCast, err.Error())
		}
		return DecimalLiteral{Val: out, Scale: t.scale}, nil
	}

	return nil, fmt.Errorf("%w: Int64Literal to %s", ErrBadCast, t)
}
func (i Int64Literal) Equals(other Literal) bool {
	return literalEq(i, other)
}

type Float32Literal float32

func (Float32Literal) createSet(vals ...float32) set[float32] { return newPrimitiveSet(vals...) }
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

	return nil, fmt.Errorf("%w: Float32Literal to %s", ErrBadCast, t)
}
func (f Float32Literal) Equals(other Literal) bool {
	return literalEq(f, other)
}

type Float64Literal float64

func (Float64Literal) createSet(vals ...float64) set[float64] { return newPrimitiveSet(vals...) }
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

	return nil, fmt.Errorf("%w: Float64Literal to %s", ErrBadCast, t)
}
func (f Float64Literal) Equals(other Literal) bool {
	return literalEq(f, other)
}

type DateLiteral Date

func (DateLiteral) createSet(vals ...Date) set[Date] { return newPrimitiveSet(vals...) }
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
	return nil, fmt.Errorf("%w: DateLiteral to %s", ErrBadCast, t)
}
func (d DateLiteral) Equals(other Literal) bool {
	return literalEq(d, other)
}

type TimeLiteral Time

func (TimeLiteral) createSet(vals ...Time) set[Time] { return newPrimitiveSet(vals...) }
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
	return nil, fmt.Errorf("%w: TimeLiteral to %s", ErrBadCast, typ)

}
func (t TimeLiteral) Equals(other Literal) bool {
	return literalEq(t, other)
}

type TimestampLiteral Timestamp

func (TimestampLiteral) createSet(vals ...Timestamp) set[Timestamp] { return newPrimitiveSet(vals...) }
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
	return nil, fmt.Errorf("%w: TimestampLiteral to %s", ErrBadCast, typ)
}
func (t TimestampLiteral) Equals(other Literal) bool {
	return literalEq(t, other)
}

type StringLiteral string

func (StringLiteral) createSet(vals ...string) set[string] { return newPrimitiveSet(vals...) }
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
				ErrBadCast, s, typ, err.Error())
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
				ErrBadCast, s, typ, err.Error())
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
				ErrBadCast, s, typ, err.Error())
		}
		return DateLiteral(tm.Truncate(24*time.Hour).Unix() / int64((time.Hour * 24).Seconds())), nil
	case TimeType:
		val, err := arrow.Time64FromString(string(s), arrow.Microsecond)
		if err != nil {
			return nil, fmt.Errorf("%w: casting '%s' to %s - %s",
				ErrBadCast, s, typ, err.Error())
		}

		return TimeLiteral(val), nil
	case TimestampType:
		val, err := arrow.TimestampFromString(string(s), arrow.Microsecond)
		if err != nil {
			return nil, fmt.Errorf("%w: casting '%s' to %s - %s",
				ErrBadCast, s, typ, err.Error())
		}
		return TimestampLiteral(val), nil
	case TimestampTzType:
		val, err := arrow.TimestampFromString(string(s), arrow.Microsecond)
		if err != nil {
			return nil, fmt.Errorf("%w: casting '%s' to %s - %s",
				ErrBadCast, s, typ, err.Error())
		}
		return TimestampLiteral(val), nil
	case UUIDType:
		val, err := uuid.Parse(string(s))
		if err != nil {
			return nil, fmt.Errorf("%w: casting '%s' to %s - %s",
				ErrBadCast, s, typ, err.Error())
		}
		return UUIDLiteral(val), nil
	case DecimalType:
		n, err := decimal128.FromString(string(s), int32(t.precision), int32(t.scale))
		if err != nil {
			return nil, fmt.Errorf("%w: casting '%s' to %s - %s",
				ErrBadCast, s, typ, err.Error())
		}
		return DecimalLiteral{Val: n, Scale: t.scale}, nil
	case BooleanType:
		val, err := strconv.ParseBool(string(s))
		if err != nil {
			return nil, fmt.Errorf("%w: casting '%s' to %s - %s",
				ErrBadCast, s, typ, err.Error())
		}
		return BoolLiteral(val), nil
	}
	return nil, fmt.Errorf("%w: StringLiteral to %s", ErrBadCast, typ)
}
func (s StringLiteral) Equals(other Literal) bool {
	return literalEq(s, other)
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
				ErrBadCast, err.Error())
		}
		return UUIDLiteral(val), nil
	case FixedType:
		if len(b) == t.len {
			return FixedLiteral(b), nil
		}

		return nil, fmt.Errorf("%w: cannot convert BinaryLiteral to %s, different length - %d <> %d",
			ErrBadCast, typ, len(b), t.len)
	case BinaryType:
		return b, nil
	}

	return nil, fmt.Errorf("%w: BinaryLiteral to %s", ErrBadCast, typ)
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
				ErrBadCast, err.Error())
		}
		return UUIDLiteral(val), nil
	case FixedType:
		if len(f) == t.len {
			return FixedLiteral(f), nil
		}

		return nil, fmt.Errorf("%w: cannot convert BinaryLiteral to %s, different length - %d <> %d",
			ErrBadCast, typ, len(f), t.len)
	case BinaryType:
		return f, nil
	}

	return nil, fmt.Errorf("%w: FixedLiteral[%d] to %s",
		ErrBadCast, len(f), typ)
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

func (UUIDLiteral) createSet(vals ...uuid.UUID) set[uuid.UUID] { return newPrimitiveSet(vals...) }
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
			ErrBadCast, typ, len(u), t.len)
	case BinaryType:
		v, _ := uuid.UUID(u).MarshalBinary()
		return BinaryLiteral(v), nil
	}

	return nil, fmt.Errorf("%w: UUIDLiteral to %s", ErrBadCast, typ)
}
func (u UUIDLiteral) Equals(other Literal) bool {
	rhs, ok := other.(UUIDLiteral)
	if !ok {
		return false
	}

	return uuid.UUID(u) == uuid.UUID(rhs)
}

type DecimalLiteral Decimal

func (DecimalLiteral) createSet(vals ...Decimal) set[Decimal] { return newPrimitiveSet(vals...) }
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
			ErrBadCast, d, t)
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

	return nil, fmt.Errorf("%w: DecimalLiteral to %s", ErrBadCast, t)
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
