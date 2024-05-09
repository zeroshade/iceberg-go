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
	"github.com/google/uuid"
)

type TransformFn[S, T LiteralType] func(Optional[S]) Optional[T]

func transformLiteral[S, T LiteralType](fn TransformFn[S, T], lit TypedLiteral[S]) Literal {
	in := Optional[S]{Valid: true, Val: lit.Value()}
	out, _ := NewLiteral(fn(in).Val)
	return out
}

func transformLiteralSlice[S, T LiteralType](fn TransformFn[S, T], lits []Literal) []Literal {
	out := make([]Literal, len(lits))
	for i, l := range lits {
		out[i] = transformLiteral(fn, l.(TypedLiteral[S]))
	}
	return out
}

func transformLiteralSliceTo[T LiteralType](transformer any, lits []Literal) []Literal {
	switch fn := transformer.(type) {
	case TransformFn[bool, T]:
		return transformLiteralSlice(fn, lits)
	case TransformFn[int32, T]:
		return transformLiteralSlice(fn, lits)
	case TransformFn[int64, T]:
		return transformLiteralSlice(fn, lits)
	case TransformFn[float32, T]:
		return transformLiteralSlice(fn, lits)
	case TransformFn[float64, T]:
		return transformLiteralSlice(fn, lits)
	case TransformFn[Date, T]:
		return transformLiteralSlice(fn, lits)
	case TransformFn[Time, T]:
		return transformLiteralSlice(fn, lits)
	case TransformFn[Timestamp, T]:
		return transformLiteralSlice(fn, lits)
	case TransformFn[string, T]:
		return transformLiteralSlice(fn, lits)
	case TransformFn[[]byte, T]:
		return transformLiteralSlice(fn, lits)
	case TransformFn[Decimal, T]:
		return transformLiteralSlice(fn, lits)
	case TransformFn[uuid.UUID, T]:
		return transformLiteralSlice(fn, lits)
	}
	panic("type not handled for transforming literals")
}

func transformLiteralTo[T LiteralType](transformer any, lit Literal) Literal {
	switch fn := transformer.(type) {
	case TransformFn[bool, T]:
		return transformLiteral(fn, lit.(TypedLiteral[bool]))
	case TransformFn[int32, T]:
		return transformLiteral(fn, lit.(TypedLiteral[int32]))
	case TransformFn[int64, T]:
		return transformLiteral(fn, lit.(TypedLiteral[int64]))
	case TransformFn[float32, T]:
		return transformLiteral(fn, lit.(TypedLiteral[float32]))
	case TransformFn[float64, T]:
		return transformLiteral(fn, lit.(TypedLiteral[float64]))
	case TransformFn[Date, T]:
		return transformLiteral(fn, lit.(TypedLiteral[Date]))
	case TransformFn[Time, T]:
		return transformLiteral(fn, lit.(TypedLiteral[Time]))
	case TransformFn[Timestamp, T]:
		return transformLiteral(fn, lit.(TypedLiteral[Timestamp]))
	case TransformFn[string, T]:
		return transformLiteral(fn, lit.(TypedLiteral[string]))
	case TransformFn[[]byte, T]:
		return transformLiteral(fn, lit.(TypedLiteral[[]byte]))
	case TransformFn[Decimal, T]:
		return transformLiteral(fn, lit.(TypedLiteral[Decimal]))
	case TransformFn[uuid.UUID, T]:
		return transformLiteral(fn, lit.(TypedLiteral[uuid.UUID]))
	}
	panic("type not handled for transforming literals")
}

func removeTransform(partName string, pred BoundPredicate) UnboundPredicate {
	switch p := pred.(type) {
	case BoundUnaryPredicate:
		return p.AsUnbound(Reference(partName))
	case BoundLiteralPredicate:
		return p.AsUnbound(Reference(partName), p.Literal())
	case BoundSetPredicate:
		// validate BoundIn or BoundNotIn
		return p.AsUnbound(Reference(partName), p.Literals())
	}

	panic("cannot replace transform in unknown predicate: " + pred.String())
}

func projectTransformPredicate(transform Transform, partitionName string, pred BoundPredicate) UnboundPredicate {
	term := pred.Term()
	if bt, ok := term.(BoundTransform); ok && bt.Transform() == transform {
		return removeTransform(partitionName, pred)
	}
	return nil
}
