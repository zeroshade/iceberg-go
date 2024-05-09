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

	"github.com/google/uuid"
)

type BooleanExprVisitor[T any] interface {
	VisitTrue() T
	VisitFalse() T
	VisitNot(childResult T) T
	VisitAnd(left, right T) T
	VisitOr(left, right T) T
	VisitUnbound(UnboundPredicate) T
	VisitBound(BoundPredicate) T
}

func VisitExpr[T any](expr BooleanExpression, visitor BooleanExprVisitor[T]) (res T, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case string:
				err = fmt.Errorf("error encounted during visitExpr: %s", e)
			case error:
				err = e
			}
		}
	}()
	return visitBoolExpr(expr, visitor), err
}

func visitBoolExpr[T any](e BooleanExpression, visitor BooleanExprVisitor[T]) T {
	switch e := e.(type) {
	case AlwaysFalse:
		return visitor.VisitFalse()
	case AlwaysTrue:
		return visitor.VisitTrue()
	case AndExpr:
		left, right := visitBoolExpr(e.left, visitor), visitBoolExpr(e.right, visitor)
		return visitor.VisitAnd(left, right)
	case OrExpr:
		left, right := visitBoolExpr(e.left, visitor), visitBoolExpr(e.right, visitor)
		return visitor.VisitOr(left, right)
	case NotExpr:
		child := visitBoolExpr(e.child, visitor)
		return visitor.VisitNot(child)
	case UnboundPredicate:
		return visitor.VisitUnbound(e)
	case BoundPredicate:
		return visitor.VisitBound(e)
	}
	panic(fmt.Errorf("%w: VisitBooleanExpression type %s", ErrNotImplemented, e))
}

func visitBoundPredicate[T any](e BoundPredicate, visitor BoundBooleanExprVisitor[T]) T {
	switch e.Op() {
	case OpIn:
		return visitor.VisitIn(e.Term(), e.(BoundSetPredicate).literalSet())
	case OpNotIn:
		return visitor.VisitNotIn(e.Term(), e.(BoundSetPredicate).literalSet())
	case OpIsNan:
		return visitor.VisitIsNan(e.Term())
	case OpNotNan:
		return visitor.VisitNotNan(e.Term())
	case OpIsNull:
		return visitor.VisitIsNull(e.Term())
	case OpNotNull:
		return visitor.VisitNotNull(e.Term())
	case OpEQ:
		return visitor.VisitEqual(e.Term(), e.(BoundLiteralPredicate).Literal())
	case OpNotEQ:
		return visitor.VisitNotEqual(e.Term(), e.(BoundLiteralPredicate).Literal())
	case OpGTEQ:
		return visitor.VisitGreaterEqual(e.Term(), e.(BoundLiteralPredicate).Literal())
	case OpGT:
		return visitor.VisitGreater(e.Term(), e.(BoundLiteralPredicate).Literal())
	case OpLTEQ:
		return visitor.VisitLessEqual(e.Term(), e.(BoundLiteralPredicate).Literal())
	case OpLT:
		return visitor.VisitLess(e.Term(), e.(BoundLiteralPredicate).Literal())
	case OpStartsWith:
		return visitor.VisitStartsWith(e.Term(), e.(BoundLiteralPredicate).Literal())
	case OpNotStartsWith:
		return visitor.VisitNotStartsWith(e.Term(), e.(BoundLiteralPredicate).Literal())
	}
	panic("unhandled bound predicate type: " + e.String())
}

func BindExpr(s *Schema, expr BooleanExpression, caseSensitive bool) (BooleanExpression, error) {
	return VisitExpr(expr, &bindVisitor{schema: s, caseSensitive: caseSensitive})
}

type bindVisitor struct {
	schema        *Schema
	caseSensitive bool
}

func (b *bindVisitor) VisitTrue() BooleanExpression {
	return AlwaysTrue{}
}

func (b *bindVisitor) VisitFalse() BooleanExpression {
	return AlwaysFalse{}
}

func (b *bindVisitor) VisitNot(childResult BooleanExpression) BooleanExpression {
	return NewNot(childResult)
}

func (b *bindVisitor) VisitAnd(left, right BooleanExpression) BooleanExpression {
	return NewAnd(left, right)
}

func (b *bindVisitor) VisitOr(left, right BooleanExpression) BooleanExpression {
	return NewOr(left, right)
}

func (b *bindVisitor) VisitUnbound(predicate UnboundPredicate) BooleanExpression {
	expr, err := predicate.Bind(b.schema, b.caseSensitive)
	if err != nil {
		panic(err)
	}
	return expr
}

func (b *bindVisitor) VisitBound(p BoundPredicate) BooleanExpression {
	panic("found already bound predicate: " + p.String())
}

func RewriteNotExpr(e BooleanExpression) (BooleanExpression, error) {
	return VisitExpr(e, rewriteNotVisitor{})
}

type rewriteNotVisitor struct{}

func (rewriteNotVisitor) VisitTrue() BooleanExpression {
	return AlwaysTrue{}
}

func (rewriteNotVisitor) VisitFalse() BooleanExpression {
	return AlwaysFalse{}
}

func (rewriteNotVisitor) VisitNot(child BooleanExpression) BooleanExpression {
	return child.Negate()
}

func (rewriteNotVisitor) VisitAnd(left, right BooleanExpression) BooleanExpression {
	return NewAnd(left, right)
}

func (rewriteNotVisitor) VisitOr(left, right BooleanExpression) BooleanExpression {
	return NewOr(left, right)
}

func (rewriteNotVisitor) VisitUnbound(pred UnboundPredicate) BooleanExpression {
	return pred
}

func (rewriteNotVisitor) VisitBound(pred BoundPredicate) BooleanExpression {
	return pred
}

type BoundBooleanExprVisitor[T any] interface {
	BooleanExprVisitor[T]

	VisitIn(BoundTerm, literalSet) T
	VisitNotIn(BoundTerm, literalSet) T
	VisitIsNan(BoundTerm) T
	VisitNotNan(BoundTerm) T
	VisitIsNull(BoundTerm) T
	VisitNotNull(BoundTerm) T
	VisitEqual(BoundTerm, Literal) T
	VisitNotEqual(BoundTerm, Literal) T
	VisitGreaterEqual(BoundTerm, Literal) T
	VisitGreater(BoundTerm, Literal) T
	VisitLessEqual(BoundTerm, Literal) T
	VisitLess(BoundTerm, Literal) T
	VisitStartsWith(BoundTerm, Literal) T
	VisitNotStartsWith(BoundTerm, Literal) T
}

func ExpressionEvaluator(s *Schema, unbound BooleanExpression, caseSensitive bool) (func(structLike) (bool, error), error) {
	bound, err := BindExpr(s, unbound, caseSensitive)
	if err != nil {
		return nil, err
	}

	return (&exprEvaluator{bound: bound}).Eval, nil
}

type exprEvaluator struct {
	bound BooleanExpression
	st    structLike
}

func (e *exprEvaluator) Eval(st structLike) (bool, error) {
	e.st = st
	return VisitExpr(e.bound, e)
}

func evalInType[T LiteralType](st structLike, term BoundTerm, literals literalSet) bool {
	v := term.(Bound[T]).eval(st)
	if !v.Valid {
		return false
	}

	return literals.(set[T]).contains(v.Val)
}

func evalIn(st structLike, term BoundTerm, literals literalSet) bool {
	switch term.Type().(type) {
	case BooleanType:
		return evalInType[bool](st, term, literals)
	case Int32Type:
		return evalInType[int32](st, term, literals)
	case Int64Type:
		return evalInType[int64](st, term, literals)
	case Float32Type:
		return evalInType[float32](st, term, literals)
	case Float64Type:
		return evalInType[float64](st, term, literals)
	case DateType:
		return evalInType[Date](st, term, literals)
	case TimeType:
		return evalInType[Time](st, term, literals)
	case TimestampType, TimestampTzType:
		return evalInType[Timestamp](st, term, literals)
	case BinaryType, FixedType:
		return evalInType[[]byte](st, term, literals)
	case StringType:
		return evalInType[string](st, term, literals)
	case UUIDType:
		return evalInType[uuid.UUID](st, term, literals)
	case DecimalType:
		return evalInType[Decimal](st, term, literals)
	}
	panic("invalid type")
}

func (e *exprEvaluator) VisitIn(term BoundTerm, literals literalSet) bool {
	return evalIn(e.st, term, literals)
}

func (e *exprEvaluator) VisitNotIn(term BoundTerm, literals literalSet) bool {
	return !evalIn(e.st, term, literals)
}

func (e *exprEvaluator) VisitIsNan(term BoundTerm) bool {
	switch term.Type().(type) {
	case Float32Type:
		v := term.(Bound[float32]).eval(e.st)
		if !v.Valid {
			break
		}
		return math.IsNaN(float64(v.Val))
	case Float64Type:
		v := term.(Bound[float64]).eval(e.st)
		if !v.Valid {
			break
		}
		return math.IsNaN(v.Val)
	}

	return false
}

func (e *exprEvaluator) VisitNotNan(term BoundTerm) bool {
	switch term.Type().(type) {
	case Float32Type:
		v := term.(Bound[float32]).eval(e.st)
		if !v.Valid {
			break
		}

		return !math.IsNaN(float64(v.Val))
	case Float64Type:
		v := term.(Bound[float64]).eval(e.st)
		if !v.Valid {
			break
		}
		return !math.IsNaN(v.Val)
	}

	return true
}

func (e *exprEvaluator) VisitIsNull(term BoundTerm) bool {
	return term.evalIsNull(e.st)
}

func (e *exprEvaluator) VisitNotNull(term BoundTerm) bool {
	return !term.evalIsNull(e.st)
}

func (e *exprEvaluator) VisitEqual(term BoundTerm, lit Literal) bool {
	return term.evalToLiteral(e.st).Equals(lit)
}

func (e *exprEvaluator) VisitNotEqual(term BoundTerm, lit Literal) bool {
	return !term.evalToLiteral(e.st).Equals(lit)
}

func typedDoCmp[T LiteralType](st structLike, term Bound[T], lit TypedLiteral[T]) int {
	v := term.eval(st)
	var l Optional[T]
	if lit != nil {
		l.Valid = true
		l.Val = lit.Value()
	}

	return nullsFirst(lit.Comparator())(v, l)
}

func doCmp(st structLike, term BoundTerm, lit Literal) int {
	switch term.Type().(type) {
	case BooleanType:
		return typedDoCmp(st, term.(Bound[bool]), lit.(TypedLiteral[bool]))
	case Int32Type:
		return typedDoCmp(st, term.(Bound[int32]), lit.(TypedLiteral[int32]))
	case Int64Type:
		return typedDoCmp(st, term.(Bound[int64]), lit.(TypedLiteral[int64]))
	case Float32Type:
		return typedDoCmp(st, term.(Bound[float32]), lit.(TypedLiteral[float32]))
	case Float64Type:
		return typedDoCmp(st, term.(Bound[float64]), lit.(TypedLiteral[float64]))
	case DateType:
		return typedDoCmp(st, term.(Bound[Date]), lit.(TypedLiteral[Date]))
	case TimeType:
		return typedDoCmp(st, term.(Bound[Time]), lit.(TypedLiteral[Time]))
	case TimestampType, TimestampTzType:
		return typedDoCmp(st, term.(Bound[Timestamp]), lit.(TypedLiteral[Timestamp]))
	case BinaryType, FixedType:
		return typedDoCmp(st, term.(Bound[[]byte]), lit.(TypedLiteral[[]byte]))
	case StringType:
		return typedDoCmp(st, term.(Bound[string]), lit.(TypedLiteral[string]))
	case UUIDType:
		return typedDoCmp(st, term.(Bound[uuid.UUID]), lit.(TypedLiteral[uuid.UUID]))
	case DecimalType:
		return typedDoCmp(st, term.(Bound[Decimal]), lit.(TypedLiteral[Decimal]))
	}
	panic("invalid type")
}

func (e *exprEvaluator) VisitGreater(term BoundTerm, lit Literal) bool {
	return doCmp(e.st, term, lit) > 0
}

func (e *exprEvaluator) VisitGreaterEqual(term BoundTerm, lit Literal) bool {
	return doCmp(e.st, term, lit) >= 0
}

func (e *exprEvaluator) VisitLess(term BoundTerm, lit Literal) bool {
	return doCmp(e.st, term, lit) < 0
}

func (e *exprEvaluator) VisitLessEqual(term BoundTerm, lit Literal) bool {
	return doCmp(e.st, term, lit) <= 0
}

func (e *exprEvaluator) VisitStartsWith(term BoundTerm, lit Literal) bool {
	val := term.(Bound[string]).eval(e.st)
	if !val.Valid {
		return false
	}

	return strings.HasPrefix(val.Val, lit.(StringLiteral).Value())
}

func (e *exprEvaluator) VisitNotStartsWith(term BoundTerm, lit Literal) bool {
	return !e.VisitStartsWith(term, lit)
}

func (e *exprEvaluator) VisitTrue() bool                { return true }
func (e *exprEvaluator) VisitFalse() bool               { return false }
func (e *exprEvaluator) VisitNot(child bool) bool       { return !child }
func (e *exprEvaluator) VisitAnd(left, right bool) bool { return left && right }
func (e *exprEvaluator) VisitOr(left, right bool) bool  { return left || right }
func (e *exprEvaluator) VisitUnbound(UnboundPredicate) bool {
	panic("found unbound predicate when evaluating expression")
}
func (e *exprEvaluator) VisitBound(p BoundPredicate) bool {
	return visitBoundPredicate(p, e)
}

const (
	RowsMightMatch    = true
	RowsMustMatch     = true
	RowsCannotMatch   = false
	RowsMightNotMatch = false
)

type metricsEvaluator struct {
	valueCounts map[int]int64
	nullCounts  map[int]int64
	nanCounts   map[int]int64
	lowerBounds map[int][]byte
	upperBounds map[int][]byte
}

func (me *metricsEvaluator) VisitUnbound(UnboundPredicate) bool {
	panic("must be called with a bound expression")
}

func (me *metricsEvaluator) VisitTrue() bool  { return RowsMightMatch }
func (me *metricsEvaluator) VisitFalse() bool { return RowsCannotMatch }
func (me *metricsEvaluator) VisitNot(childResult bool) bool {
	panic(fmt.Errorf("NOT should be rewritten: %t", childResult))
}

func (me *metricsEvaluator) VisitAnd(left, right bool) bool { return left && right }
func (me *metricsEvaluator) VisitOr(left, right bool) bool  { return left || right }

func (me *metricsEvaluator) containsNullsOnly(fieldID int) bool {
	if valCount, ok := me.valueCounts[fieldID]; ok {
		if nullCount, ok := me.nullCounts[fieldID]; ok {
			return valCount == nullCount
		}
	}

	return false
}

func (me *metricsEvaluator) containsNansOnly(fieldID int) bool {
	if valCount, ok := me.valueCounts[fieldID]; ok {
		if nanCount, ok := me.nanCounts[fieldID]; ok {
			return valCount == nanCount
		}
	}
	return false
}

// func newInclusiveMetricsEvaluator(s *Schema, expr BooleanExpression, caseSensitive, includeEmptyFiles bool) (BoundBooleanExprVisitor[bool], error) {
// 	rewritten, err := RewriteNotExpr(expr)
// 	if err != nil {
// 		return nil, err
// 	}

// 	final, err := BindExpr(s, rewritten, caseSensitive)
// 	if err != nil {
// 		return nil, err
// 	}

// 	ev := &inclusiveMetricsEvaluator{
// 		st:                s.AsStruct(),
// 		includeEmptyFiles: includeEmptyFiles,
// 		expr:              final,
// 	}

// 	return ev, nil
// }

type inclusiveMetricsEvaluator struct {
	metricsEvaluator

	st                StructType
	expr              BooleanExpression
	includeEmptyFiles bool
}

// func (ime *inclusiveMetricsEvaluator) eval(file DataFile) bool {
// 	if !ime.includeEmptyFiles && file.Count() == 0 {
// 		return RowsCannotMatch
// 	}

// 	if file.Count() < 0 {
// 		// older versions don't correctly implement record counts from avro files
// 		// and thus set record count to -1 when importing avro tables to iceberg
// 		// this should be updated once we implement and set the correct count
// 		return RowsMightMatch
// 	}

// 	ime.valueCounts = file.ValueCounts()
// 	ime.nullCounts = file.NullValueCounts()
// 	ime.nanCounts = file.NaNValueCounts()
// 	ime.lowerBounds = file.LowerBoundValues()
// 	ime.upperBounds = file.UpperBoundValues()

// 	return visitBoolExpr(ime.expr, ime)
// }

// func (ime *inclusiveMetricsEvaluator) VisitBound(p BoundPredicate) bool {
// 	return visitBoundPredicate(p, ime)
// }

func (ime *inclusiveMetricsEvaluator) mayContainNull(fieldID int) bool {
	if ime.nullCounts == nil {
		return true
	}

	nullCount, ok := ime.nullCounts[fieldID]
	return ok && nullCount > 0
}

// func (ime *inclusiveMetricsEvaluator) VisitIn(BoundTerm, literalSet) bool    {}
// func (ime *inclusiveMetricsEvaluator) VisitNotIn(BoundTerm, literalSet) bool {}
func (ime *inclusiveMetricsEvaluator) VisitIsNan(term BoundTerm) bool {
	fieldID := term.Ref().Field().ID
	if ime.nanCounts[fieldID] == 0 {
		return RowsCannotMatch
	}

	// when there's no nancounts info but we already know the column only
	// contains nulls, its guaranteed there's no NAN
	if ime.containsNullsOnly(fieldID) {
		return RowsCannotMatch
	}

	return RowsMightMatch
}

func (ime *inclusiveMetricsEvaluator) VisitNotNan(term BoundTerm) bool {
	fieldID := term.Ref().Field().ID
	if ime.containsNansOnly(fieldID) {
		return RowsCannotMatch
	}
	return RowsMightMatch
}

func (ime *inclusiveMetricsEvaluator) VisitIsNull(term BoundTerm) bool {
	fieldID := term.Ref().Field().ID
	if ime.nullCounts[fieldID] == 0 {
		return RowsCannotMatch
	}

	return RowsMightMatch
}

func (ime *inclusiveMetricsEvaluator) VisitNotNull(term BoundTerm) bool {
	// no need to check whether the field is required because binding evaluates
	// taht case if the column has no null values, the expression won't match
	fieldID := term.Ref().Field().ID
	if ime.containsNullsOnly(fieldID) {
		return RowsCannotMatch
	}
	return RowsMightMatch
}

// func (ime *inclusiveMetricsEvaluator) VisitEqual(BoundTerm, Literal) bool {}

// func (ime *inclusiveMetricsEvaluator) VisitNotEqual(BoundTerm, Literal) bool {
// 	return RowsMightMatch
// }

// func (ime *inclusiveMetricsEvaluator) VisitGreaterEqual(BoundTerm, Literal) bool  {}
// func (ime *inclusiveMetricsEvaluator) VisitGreater(BoundTerm, Literal) bool       {}
// func (ime *inclusiveMetricsEvaluator) VisitLessEqual(BoundTerm, Literal) bool     {}
// func (ime *inclusiveMetricsEvaluator) VisitLess(BoundTerm, Literal) bool          {}
// func (ime *inclusiveMetricsEvaluator) VisitStartsWith(BoundTerm, Literal) bool    {}
// func (ime *inclusiveMetricsEvaluator) VisitNotStartsWith(BoundTerm, Literal) bool {}

var (
	_ BooleanExprVisitor[BooleanExpression] = (*bindVisitor)(nil)
)
