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
	"encoding"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unsafe"

	"github.com/google/uuid"
	"github.com/spaolacci/murmur3"
)

// ParseTransform takes the string representation of a transform as
// defined in the iceberg spec, and produces the appropriate Transform
// object or an error if the string is not a valid transform string.
func ParseTransform(s string) (Transform, error) {
	s = strings.ToLower(s)
	switch {
	case strings.HasPrefix(s, "bucket"):
		matches := regexFromBrackets.FindStringSubmatch(s)
		if len(matches) != 2 {
			break
		}

		n, _ := strconv.Atoi(matches[1])
		return BucketTransform{NumBuckets: n}, nil
	case strings.HasPrefix(s, "truncate"):
		matches := regexFromBrackets.FindStringSubmatch(s)
		if len(matches) != 2 {
			break
		}

		n, _ := strconv.Atoi(matches[1])
		return TruncateTransform{Width: n}, nil
	default:
		switch s {
		case "identity":
			return IdentityTransform{}, nil
		case "void":
			return VoidTransform{}, nil
		case "year":
			return YearTransform{}, nil
		case "month":
			return MonthTransform{}, nil
		case "day":
			return DayTransform{}, nil
		case "hour":
			return HourTransform{}, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrInvalidTransform, s)
}

// Transform is an interface for the various Transformation types
// in partition specs. Currently, they do not yet provide actual
// transformation functions or implementation. That will come later as
// data reading gets implemented.
type Transform interface {
	fmt.Stringer
	encoding.TextMarshaler
	ResultType(t Type) Type
}

type TypedTransform[S, T LiteralType] interface {
	fmt.Stringer
	encoding.TextMarshaler

	Apply(Optional[S]) Optional[T]
}

// IdentityTransform uses the identity function, performing no transformation
// but instead partitioning on the value itself.
type IdentityTransform struct{}

func (t IdentityTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (IdentityTransform) String() string { return "identity" }

func (IdentityTransform) ResultType(t Type) Type { return t }

type identTransformImpl[T LiteralType] struct {
	IdentityTransform
}

func (identTransformImpl[T]) Apply(v Optional[T]) Optional[T] {
	return v
}

func (IdentityTransform) Project(name string, pred BoundPredicate) UnboundPredicate {
	switch p := pred.Term().(type) {
	case BoundTransform:
		return projectTransformPredicate(IdentityTransform{}, name, pred)
	case BoundUnaryPredicate:
		return p.AsUnbound(Reference(name))
	case BoundLiteralPredicate:
		return p.AsUnbound(Reference(name), p.Literal())
	case BoundSetPredicate:
		if p.Op() == OpIn || p.Op() == OpNotIn {
			return p.AsUnbound(Reference(name), p.Literals())
		}
	}
	panic("could not project")
}

func (IdentityTransform) Bind(t Type) (Transform, error) {
	switch t.(type) {
	case BooleanType:
		return identTransformImpl[bool]{}, nil
	case Int32Type:
		return identTransformImpl[int32]{}, nil
	case Int64Type:
		return identTransformImpl[int64]{}, nil
	case Float32Type:
		return identTransformImpl[float32]{}, nil
	case Float64Type:
		return identTransformImpl[float64]{}, nil
	case DateType:
		return identTransformImpl[Date]{}, nil
	case TimeType:
		return identTransformImpl[Time]{}, nil
	case TimestampType:
		return identTransformImpl[Timestamp]{}, nil
	case TimestampTzType:
		return identTransformImpl[Timestamp]{}, nil
	case StringType:
		return identTransformImpl[string]{}, nil
	case BinaryType:
		return identTransformImpl[[]byte]{}, nil
	case UUIDType:
		return identTransformImpl[uuid.UUID]{}, nil
	case FixedType:
		return identTransformImpl[[]byte]{}, nil
	case DecimalType:
		return identTransformImpl[Decimal]{}, nil
	}
	panic("unhandled type in IdentityTransform")
}

// VoidTransform is a transformation that always returns nil.
type VoidTransform struct{}

func (t VoidTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (VoidTransform) String() string { return "void" }

func (VoidTransform) ResultType(t Type) Type { return t }

func (VoidTransform) Transform(t Type) (any, error) { return nil, nil }

// BucketTransform transforms values into a bucket partition value. It is
// parameterized by a number of buckets. Bucket partition transforms use
// a 32-bit hash of the source value to produce a positive value by mod
// the bucket number.
type BucketTransform struct {
	NumBuckets int
}

func (t BucketTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t BucketTransform) String() string { return fmt.Sprintf("bucket[%d]", t.NumBuckets) }

func (BucketTransform) ResultType(Type) Type { return PrimitiveTypes.Int32 }

func (t BucketTransform) getBucket(hashValue uint32) int32 {
	return int32(hashValue&math.MaxInt32) % int32(t.NumBuckets)
}

type bucketTransform[T int32 | int64 | Date | Time | Timestamp] struct {
	BucketTransform
}

func (b bucketTransform[T]) Apply(v Optional[T]) Optional[int32] {
	if !v.Valid {
		return Optional[int32]{}
	}

	raw := unsafe.Slice((*byte)(unsafe.Pointer(&v.Val)), unsafe.Sizeof(v.Val))
	h := murmur3.Sum32WithSeed(raw, 0)
	return Optional[int32]{Valid: true, Val: b.getBucket(h)}
}

type strBucketTransform struct {
	BucketTransform
}

func (b strBucketTransform) Apply(v Optional[string]) Optional[int32] {
	if !v.Valid {
		return Optional[int32]{}
	}

	h := murmur3.Sum32WithSeed(unsafe.Slice(unsafe.StringData(v.Val), len(v.Val)), 0)
	return Optional[int32]{Valid: true, Val: b.getBucket(h)}
}

type byteBucketTransform struct {
	BucketTransform
}

func (b byteBucketTransform) Apply(v Optional[[]byte]) Optional[int32] {
	if !v.Valid {
		return Optional[int32]{}
	}

	h := murmur3.Sum32WithSeed(v.Val, 0)
	return Optional[int32]{Valid: true, Val: b.getBucket(h)}
}

type uuidBucketTransform struct {
	BucketTransform
}

func (b uuidBucketTransform) Apply(v Optional[uuid.UUID]) Optional[int32] {
	if !v.Valid {
		return Optional[int32]{}
	}

	h := murmur3.Sum32WithSeed(v.Val[:], 0)
	return Optional[int32]{Valid: true, Val: b.getBucket(h)}
}

func (t BucketTransform) Bind(typ Type) (Transform, error) {
	switch typ.(type) {
	case Int32Type:
		return bucketTransform[int32]{t}, nil
	case Int64Type:
		return bucketTransform[int64]{t}, nil
	case DateType:
		return bucketTransform[Date]{t}, nil
	case TimeType:
		return bucketTransform[Time]{t}, nil
	case TimestampType, TimestampTzType:
		return bucketTransform[Timestamp]{t}, nil
	// case DecimalType:
	// 	return bucketTransform[Decimal]{t}, nil
	case StringType:
		return strBucketTransform{t}, nil
	case FixedType:
		return byteBucketTransform{t}, nil
	case BinaryType:
		return byteBucketTransform{t}, nil
	case UUIDType:
		return uuidBucketTransform{t}, nil
	}
	return nil, fmt.Errorf("%w: bucket transform does not accept type %s",
		ErrType, typ)
}

func (t BucketTransform) Project(name string, pred BoundPredicate) UnboundPredicate {
	transformer, err := t.Bind(pred.Ref().Field().Type)
	if err != nil {
		return nil
	}

	switch p := pred.(type) {
	case BoundTransform:
		return projectTransformPredicate(t, name, pred)
	case BoundUnaryPredicate:
		return p.AsUnbound(Reference(name))
	case BoundLiteralPredicate:
		if p.Op() == OpEQ {
			return p.AsUnbound(Reference(name), transformLiteralTo[int32](transformer, p.Literal()))
		}
	case BoundSetPredicate:
		if p.Op() == OpIn {
			transformed := transformLiteralSliceTo[int32](transformer, p.Literals())
			return p.AsUnbound(Reference(name), transformed)
		}
	}

	return nil
}

// TruncateTransform is a transformation for truncating a value to a specified width.
type TruncateTransform struct {
	Width int
}

func (t TruncateTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t TruncateTransform) String() string { return fmt.Sprintf("truncate[%d]", t.Width) }

func (TruncateTransform) ResultType(t Type) Type { return t }

func (t TruncateTransform) Transform(typ Type) (any, error) { return nil, nil }

// YearTransform transforms a datetime value into a year value.
type YearTransform struct{}

func (t YearTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (YearTransform) String() string { return "year" }

func (YearTransform) ResultType(Type) Type { return PrimitiveTypes.Int32 }

func (YearTransform) Transform(typ Type) (any, error) { return nil, nil }

// MonthTransform transforms a datetime value into a month value.
type MonthTransform struct{}

func (t MonthTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (MonthTransform) String() string { return "month" }

func (MonthTransform) ResultType(Type) Type { return PrimitiveTypes.Int32 }

func (MonthTransform) Transform(typ Type) (any, error) { return nil, nil }

// DayTransform transforms a datetime value into a date value.
type DayTransform struct{}

func (t DayTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (DayTransform) String() string { return "day" }

func (DayTransform) ResultType(Type) Type { return PrimitiveTypes.Date }

func (DayTransform) Transform(typ Type) (any, error) { return nil, nil }

// HourTransform transforms a datetime value into an hour value.
type HourTransform struct{}

func (t HourTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (HourTransform) String() string { return "hour" }

func (HourTransform) ResultType(Type) Type { return PrimitiveTypes.Int32 }

func (HourTransform) Transform(typ Type) (any, error) { return nil, nil }
