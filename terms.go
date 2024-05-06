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
	"fmt"
	"math"
	"strings"
)

type StructLike interface {
	Size() int
	Get(pos int) any
	Set(pos int, val any) error
}

//go:generate stringer -type=Operation -linecomment

type Operation int

const (
	OpTrue          Operation = iota // True
	OpFalse                          // False
	OpIsNull                         // IsNull
	OpNotNull                        // NotNull
	OpIsNan                          // IsNaN
	OpNotNan                         // NotNaN
	OpLT                             // LessThan
	OpLTEQ                           // LessThanEqual
	OpGT                             // GreaterThan
	OpGTEQ                           // GreaterThanEqual
	OpEQ                             // Equal
	OpNotEQ                          // NotEqual
	OpIn                             // In
	OpNotIn                          // NotIn
	OpNot                            // Not
	OpAnd                            // And
	OpOr                             // Or
	OpStartsWith                     // StartsWith
	OpNotStartsWith                  // NotStartsWith
	OpCount                          // Count
	OpCountStar                      // CountStar
	OpMax                            // Max
	OpMin                            // Min
)

func (op Operation) Negate() Operation {
	switch op {
	case OpIsNull:
		return OpNotNull
	case OpNotNull:
		return OpIsNull
	case OpIsNan:
		return OpNotNan
	case OpNotNan:
		return OpIsNan
	case OpLT:
		return OpGTEQ
	case OpLTEQ:
		return OpGT
	case OpGT:
		return OpLTEQ
	case OpGTEQ:
		return OpLT
	case OpEQ:
		return OpNotEQ
	case OpNotEQ:
		return OpEQ
	case OpIn:
		return OpNotIn
	case OpNotIn:
		return OpIn
	case OpStartsWith:
		return OpNotStartsWith
	case OpNotStartsWith:
		return OpStartsWith
	default:
		panic("no negation for operation " + op.String())
	}
}

func (op Operation) FlipLR() Operation {
	switch op {
	case OpLT:
		return OpGT
	case OpLTEQ:
		return OpGTEQ
	case OpGT:
		return OpLT
	case OpGTEQ:
		return OpLTEQ
	case OpAnd:
		return OpAnd
	case OpOr:
		return OpOr
	default:
		panic("no left-right flip for operation: " + op.String())
	}
}

type optional[T literalType] struct {
	val     T
	isValid bool
}

type Term interface {
	Equivalent(other Term) bool
}

type Bound[T literalType] interface {
	Eval(st StructLike) (optional[T], error)

	Ref() Reference
}

type BoundTerm[T literalType] interface {
	Term
	Bound[T]

	Type() Type
}

type Unbound[T literalType, B any] interface {
	Bind(st StructType, caseSensitive bool) (B, error)
	Ref() Reference
}

type UnboundTerm[T literalType] interface {
	Term
	Unbound[T, BoundTerm[T]]
}

type predicate[T literalType, C Term] struct {
	op   Operation
	term C
}

func (p *predicate[T, C]) Op() Operation { return p.op }
func (p *predicate[T, C]) Term() C       { return p.term }

func newUnboundPredicate[T literalType](op Operation, t UnboundTerm[T], lits ...Literal) *UnboundPredicate[T] {
	return &UnboundPredicate[T]{
		predicate: predicate[T, UnboundTerm[T]]{op: op, term: t},
		literals:  lits}
}

type UnboundPredicate[T literalType] struct {
	predicate[T, UnboundTerm[T]]

	literals []Literal
}

func (up *UnboundPredicate[T]) Ref() Reference {
	return up.Term().Ref()
}

func (up *UnboundPredicate[T]) Bind(st StructType, caseSensitive bool) (Expression, error) {
	b, err := up.Term().Bind(st, caseSensitive)
	if err != nil {
		return nil, err
	}

	if len(up.literals) == 0 {
		return up.bindUnaryOperation(b)
	}

	if up.Op() == OpIn || up.Op() == OpNotIn {
		return up.bindInOperation(b)
	}

	return up.bindLiteralOperation(b)
}

func (up *UnboundPredicate[T]) bindUnaryOperation(bound BoundTerm[T]) (Expression, error) {
	switch up.op {
	case OpIsNull:
		if bound.Ref().(BoundRef).Field().Required {
			return AlwaysFalse, nil
		}

		return newBoundUnaryPredicate(OpIsNull, bound), nil
	case OpNotNull:
		if bound.Ref().(BoundRef).Field().Required {
			return AlwaysTrue, nil
		}

		return newBoundUnaryPredicate(OpNotNull, bound), nil
	case OpIsNan:
		if up.isFloatingType(bound.Type()) {
			return newBoundUnaryPredicate(OpIsNan, bound), nil
		}

		return nil, fmt.Errorf("IsNaN cannot be used with non-floating-point column")
	case OpNotNan:
		if up.isFloatingType(bound.Type()) {
			return newBoundUnaryPredicate(OpNotNan, bound), nil
		}

		return nil, fmt.Errorf("IsNaN cannot be used with non-floating-point column")
	}

	return nil, fmt.Errorf("unhandled operation in unbound predicate.bindUnaryOperation: %s", up.op)
}

func (up *UnboundPredicate[T]) isFloatingType(t Type) bool {
	switch t.(type) {
	case Float32Type, Float64Type:
		return true
	}
	return false
}

func (up *UnboundPredicate[T]) bindInOperation(bound BoundTerm[T]) (Expression, error) {
	convertedLits := make([]TypedLiteral[T], 0, len(up.literals))
	for _, lit := range up.literals {
		conv, err := lit.To(bound.Type())
		if err != nil {
			return nil, err
		}

		if _, ok := conv.(AboveMaxLiteral); ok {
			continue
		}
		if _, ok := conv.(BelowMinLiteral); ok {
			continue
		}

		convertedLits = append(convertedLits, conv.(TypedLiteral[T]))
	}

	switch len(convertedLits) {
	case 0:
		switch up.op {
		case OpIn:
			return AlwaysFalse, nil
		case OpNotIn:
			return AlwaysTrue, nil
		default:
			return nil, fmt.Errorf("Operation must be In or NotIn")
		}
	case 1:
		switch up.op {
		case OpIn:
			return newBoundLiteralPredicate(OpEQ, bound, convertedLits[0]), nil
		case OpNotIn:
			return newBoundLiteralPredicate(OpNotEQ, bound, convertedLits[0]), nil
		default:
			return nil, fmt.Errorf("Operation must be In or NotIn")
		}
	default:
		return newBoundSetPredicate(up.op, bound, setOfLits(convertedLits...)), nil
	}
}

func (up *UnboundPredicate[T]) bindLiteralOperation(bound BoundTerm[T]) (Expression, error) {
	if len(up.literals) != 1 {
		return nil, fmt.Errorf("bindLiteralOperation expects 1 literal, found %d", len(up.literals))
	}

	lit, err := up.literals[0].To(bound.Type())
	if err != nil {
		return nil, err
	}

	if _, aboveMax := lit.(AboveMaxLiteral); aboveMax {
		switch up.op {
		case OpLT, OpLTEQ, OpNotEQ:
			return AlwaysTrue, nil
		case OpGT, OpGTEQ, OpEQ:
			return AlwaysFalse, nil
		}
	} else if _, belowMin := lit.(BelowMinLiteral); belowMin {
		switch up.op {
		case OpLT, OpLTEQ, OpNotEQ:
			return AlwaysFalse, nil
		case OpGT, OpGTEQ, OpEQ:
			return AlwaysTrue, nil
		}
	}

	return newBoundLiteralPredicate(up.op, bound, lit.(TypedLiteral[T])), nil
}

type tester[T literalType] interface {
	Expression

	test(optional[T]) (bool, error)
	Term() BoundTerm[T]
}

type BoundPredicate[T literalType] struct {
	tester[T]
}

func (bp BoundPredicate[T]) Eval(st StructLike) (optional[bool], error) {
	t, err := bp.Term().Eval(st)
	if err != nil {
		return optional[bool]{}, err
	}

	res, err := bp.tester.test(t)
	return optional[bool]{val: res, isValid: true}, err
}

func (bp BoundPredicate[T]) test(st StructLike) (bool, error) {
	t, err := bp.Term().Eval(st)
	if err != nil {
		return false, err
	}
	return bp.tester.test(t)
}

func (bp BoundPredicate[T]) Ref() Reference {
	return bp.Term().Ref()
}

func newBoundUnaryPredicate[T literalType](op Operation, b BoundTerm[T]) Expression {
	return BoundPredicate[T]{
		tester: &boundUnaryPredicate[T]{
			predicate: predicate[T, BoundTerm[T]]{op: op, term: b}}}
}

type boundUnaryPredicate[T literalType] struct {
	predicate[T, BoundTerm[T]]
}

func (bp *boundUnaryPredicate[T]) Negate() Expression {
	return BoundPredicate[T]{
		tester: &boundUnaryPredicate[T]{
			predicate: predicate[T, BoundTerm[T]]{op: bp.op.Negate(), term: bp.term},
		},
	}
}

func (bp *boundUnaryPredicate[T]) Equivalent(other Expression) bool {
	if bp.op == other.Op() {
		rhs, ok := other.(BoundPredicate[T])
		if !ok {
			return false
		}
		return bp.term.Equivalent(rhs.Term())
	}

	return false
}

func isNan[T literalType](val optional[T]) bool {
	if !val.isValid {
		return false
	}

	switch v := any(val.val).(type) {
	case float32:
		return math.IsNaN(float64(v))
	case float64:
		return math.IsNaN(v)
	default:
		return false
	}
}

func (bp *boundUnaryPredicate[T]) test(val optional[T]) (bool, error) {
	switch bp.op {
	case OpIsNull:
		return !val.isValid, nil
	case OpNotNull:
		return val.isValid, nil
	case OpIsNan:
		return isNan(val), nil
	case OpNotNan:
		return !isNan(val), nil
	default:
		return false, fmt.Errorf("invalid operation for boundUnaryPredicate: %s", bp.op)
	}
}

func newBoundLiteralPredicate[T literalType](op Operation, b BoundTerm[T], lit TypedLiteral[T]) Expression {
	if op == OpIn || op == OpNotIn {
		panic("bound literal predicate does not support In or NotIn")
	}

	return BoundPredicate[T]{
		tester: &boundLiteralPredicate[T]{
			lit:       lit,
			predicate: predicate[T, BoundTerm[T]]{op: op, term: b}}}
}

type boundLiteralPredicate[T literalType] struct {
	predicate[T, BoundTerm[T]]

	lit TypedLiteral[T]
}

func isIntegralType(t Type) bool {
	switch t.(type) {
	case Int32Type, Int64Type, DateType, TimeType, TimestampType, TimestampTzType:
		return true
	}

	return false
}

func toInt64[T literalType](lit TypedLiteral[T]) int64 {
	switch v := any(lit.Value()).(type) {
	case int32:
		return int64(v)
	case int64:
		return v
	case Date:
		return int64(v)
	case Timestamp:
		return int64(v)
	case Time:
		return int64(v)
	}

	panic("can only call toInt64 with integral types")
}

func (bp *boundLiteralPredicate[T]) test(val optional[T]) (bool, error) {
	cmp := bp.lit.Comparator()
	lit := optional[T]{isValid: true, val: bp.lit.Value()}
	switch bp.op {
	case OpLT:
		return nullsFirstCmp(cmp, val, lit) < 0, nil
	case OpLTEQ:
		return nullsFirstCmp(cmp, val, lit) <= 0, nil
	case OpGT:
		return nullsFirstCmp(cmp, val, lit) > 0, nil
	case OpGTEQ:
		return nullsFirstCmp(cmp, val, lit) >= 0, nil
	case OpEQ:
		return nullsFirstCmp(cmp, val, lit) == 0, nil
	case OpNotEQ:
		return nullsFirstCmp(cmp, val, lit) != 0, nil
	case OpStartsWith:
		if !val.isValid {
			return false, nil
		}

		switch v := any(val.val).(type) {
		case string:
			return strings.HasPrefix(v, bp.lit.String()), nil
		}
		return false, fmt.Errorf("startswith called with non-string column")
	case OpNotStartsWith:
		if !val.isValid {
			return false, nil
		}

		switch v := any(val.val).(type) {
		case string:
			return !strings.HasPrefix(v, bp.lit.String()), nil
		}
		return false, fmt.Errorf("notstartswith called with non-string column")
	}

	return false, fmt.Errorf("invalid operation for BoundLiteralPredicate")
}

func (bp *boundLiteralPredicate[T]) Negate() Expression {
	return newBoundLiteralPredicate(bp.op.Negate(), bp.term, bp.lit)
}

func (bp *boundLiteralPredicate[T]) Equivalent(expr Expression) bool {
	other, ok := expr.(BoundPredicate[T])
	if !ok {
		return false
	}

	rhs, ok := other.tester.(*boundLiteralPredicate[T])
	if !bp.term.Equivalent(rhs.Term()) {
		return false
	}

	if bp.op == expr.Op() {
		return bp.lit.Equals(rhs.lit)
	}

	if !isIntegralType(bp.term.Type()) {
		return false
	}

	switch bp.op {
	case OpLT:
		if rhs.op == OpLTEQ {
			// <6 equivalent to <= 5
			return toInt64(bp.lit) == toInt64(rhs.lit)+1
		}
	case OpLTEQ:
		if rhs.op == OpLT {
			// <=5 equivalent to <6
			return toInt64(bp.lit) == toInt64(rhs.lit)-1
		}
	case OpGT:
		if rhs.op == OpGTEQ {
			// >5 equivalent to >= 6
			return toInt64(bp.lit) == toInt64(rhs.lit)-1
		}
	case OpGTEQ:
		if rhs.op == OpGT {
			// >5 equivalent to >=4
			return toInt64(bp.lit) == toInt64(rhs.lit)+1
		}
	}

	return false
}

func newBoundSetPredicate[T literalType](op Operation, b BoundTerm[T], lits set[T]) Expression {
	if op != OpIn && op != OpNotIn {
		panic("BoundSetLiteralPredicate must be In or NotIn")
	}

	return BoundPredicate[T]{
		tester: &boundSetPredicate[T]{
			literalSet: lits,
			predicate:  predicate[T, BoundTerm[T]]{op: op, term: b}}}
}

type boundSetPredicate[T literalType] struct {
	predicate[T, BoundTerm[T]]

	literalSet set[T]
}

func (bp *boundSetPredicate[T]) test(val optional[T]) (bool, error) {
	switch bp.op {
	case OpIn:
		if !val.isValid {
			return false, nil
		}
		return bp.literalSet.contains(val.val), nil
	case OpNotIn:
		if !val.isValid {
			return true, nil
		}
		return !bp.literalSet.contains(val.val), nil
	default:
		return false, fmt.Errorf("invalid operation BoundSetPredicate: %s", bp.op)
	}
}

func (bp *boundSetPredicate[T]) Negate() Expression {
	return newBoundSetPredicate(bp.op.Negate(), bp.term, bp.literalSet)
}

func (bp *boundSetPredicate[T]) Equivalent(expr Expression) bool {
	if rhs, ok := expr.(*boundSetPredicate[T]); ok {
		return bp.literalSet.equals(rhs.literalSet)
	}

	return false
}
