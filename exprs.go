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
	"slices"
	"sync"

	"github.com/google/uuid"
)

type BooleanExpression interface {
	fmt.Stringer
	Op() Operation
	Negate() BooleanExpression
	Equivalent(BooleanExpression) bool
}

//go:generate stringer -type=Operation -linecomment

type Operation int

const (
	// do not change the order of these enum constants
	// they are grouped for quick validation of operation type

	OpTrue  Operation = iota // True
	OpFalse                  // False
	// unary ops
	OpIsNull  // IsNull
	OpNotNull // NotNull
	OpIsNan   // IsNaN
	OpNotNan  // NotNaN
	// literal ops
	OpLT            // LessThan
	OpLTEQ          // LessThanEqual
	OpGT            // GreaterThan
	OpGTEQ          // GreaterThanEqual
	OpEQ            // Equal
	OpNotEQ         // NotEqual
	OpStartsWith    // StartsWith
	OpNotStartsWith // NotStartsWith
	// set ops
	OpIn    // In
	OpNotIn // NotIn
	// bool ops
	OpNot // Not
	OpAnd // And
	OpOr  // Or
	// OpCount                          // Count
	// OpCountStar                      // CountStar
	// OpMax                            // Max
	// OpMin                            // Min
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

type AlwaysTrue struct{}

func (AlwaysTrue) String() string            { return "AlwaysTrue()" }
func (AlwaysTrue) Op() Operation             { return OpTrue }
func (AlwaysTrue) Negate() BooleanExpression { return AlwaysFalse{} }
func (AlwaysTrue) Equivalent(other BooleanExpression) bool {
	_, ok := other.(AlwaysTrue)
	return ok
}

type AlwaysFalse struct{}

func (AlwaysFalse) String() string            { return "AlwaysFalse()" }
func (AlwaysFalse) Op() Operation             { return OpFalse }
func (AlwaysFalse) Negate() BooleanExpression { return AlwaysTrue{} }
func (AlwaysFalse) Equivalent(other BooleanExpression) bool {
	_, ok := other.(AlwaysFalse)
	return ok
}

func NewNot(child BooleanExpression) BooleanExpression {
	switch t := child.(type) {
	case NotExpr:
		return t.child
	case AlwaysTrue:
		return AlwaysFalse{}
	case AlwaysFalse:
		return AlwaysTrue{}
	}

	return NotExpr{child: child}
}

type NotExpr struct {
	child BooleanExpression
}

func (n NotExpr) String() string {
	return "Not(child=" + n.child.String() + ")"
}

func (n NotExpr) Equivalent(other BooleanExpression) bool {
	rhs, ok := other.(NotExpr)
	if !ok {
		return false
	}

	return n.child.Equivalent(rhs.child)
}

func (NotExpr) Op() Operation { return OpNot }
func (n NotExpr) Negate() BooleanExpression {
	return n.child
}

func and(left, right BooleanExpression) BooleanExpression {
	if left == nil || right == nil {
		panic("cannot construct expression with nil arguments")
	}

	switch {
	case left == AlwaysFalse{} || right == AlwaysFalse{}:
		return AlwaysFalse{}
	case left == AlwaysTrue{}:
		return right
	case right == AlwaysTrue{}:
		return left
	}

	return AndExpr{left: left, right: right}
}

func NewAnd(left, right BooleanExpression, addl ...BooleanExpression) BooleanExpression {
	folded := and(left, right)
	for _, a := range addl {
		folded = and(folded, a)
	}
	return folded
}

type AndExpr struct {
	left, right BooleanExpression
}

func (a AndExpr) String() string {
	return "And(left=" + a.left.String() + ", right=" + a.right.String() + ")"
}

func (a AndExpr) Equivalent(other BooleanExpression) bool {
	rhs, ok := other.(AndExpr)
	if !ok {
		return false
	}

	return (a.left.Equivalent(rhs.left) && a.right.Equivalent(rhs.right)) ||
		(a.left.Equivalent(rhs.right) && a.right.Equivalent(rhs.left))
}

func (AndExpr) Op() Operation { return OpAnd }
func (a AndExpr) Negate() BooleanExpression {
	return NewOr(a.left.Negate(), a.right.Negate())
}

func or(left, right BooleanExpression) BooleanExpression {
	if left == nil || right == nil {
		panic("cannot construct expression with nil arguments")
	}

	switch {
	case left == AlwaysTrue{} || right == AlwaysTrue{}:
		return AlwaysTrue{}
	case left == AlwaysFalse{}:
		return right
	case right == AlwaysFalse{}:
		return left
	}

	return OrExpr{left: left, right: right}
}

func NewOr(left, right BooleanExpression, addl ...BooleanExpression) BooleanExpression {
	folded := or(left, right)
	for _, a := range addl {
		folded = or(folded, a)
	}
	return folded
}

type OrExpr struct {
	left, right BooleanExpression
}

func (o OrExpr) String() string {
	return "Or(left=" + o.left.String() + ", right=" + o.right.String() + ")"
}

func (o OrExpr) Equivalent(other BooleanExpression) bool {
	rhs, ok := other.(OrExpr)
	if !ok {
		return false
	}

	return (o.left.Equivalent(rhs.left) && o.right.Equivalent(rhs.right)) ||
		(o.left.Equivalent(rhs.right) && o.right.Equivalent(rhs.left))
}

func (OrExpr) Op() Operation { return OpOr }
func (o OrExpr) Negate() BooleanExpression {
	return NewAnd(o.left.Negate(), o.right.Negate())
}

type Reference string

func (r Reference) String() string {
	return "Reference(name='" + string(r) + "')"
}

func (r Reference) Equals(other UnboundTerm) bool {
	rhs, ok := other.(Reference)
	if !ok {
		return false
	}
	return r == rhs
}

func (r Reference) Bind(s *Schema, caseSensitive bool) (BoundTerm, error) {
	var (
		field NestedField
		found bool
	)

	if caseSensitive {
		field, found = s.FindFieldByName(string(r))
	} else {
		field, found = s.FindFieldByNameCaseInsensitive(string(r))
	}
	if !found {
		return nil, fmt.Errorf("%w: could not bind reference '%s', caseSensitive=%t",
			ErrInvalidSchema, r, caseSensitive)
	}

	acc, ok := s.accessorForField(field.ID)
	if !ok {
		return nil, ErrInvalidSchema
	}

	return createBoundRef(field, acc), nil
}

type unbound[B any] interface {
	Bind(schema *Schema, caseSensitive bool) (B, error)
}

type UnboundPredicate interface {
	BooleanExpression
	unbound[BooleanExpression]
	Term() Term
}

type unboundUnaryPredicate struct {
	op   Operation
	term UnboundTerm
}

func (up *unboundUnaryPredicate) String() string {
	return fmt.Sprintf("%s(term=%s)", up.op, up.term)
}

func (up *unboundUnaryPredicate) Equivalent(other BooleanExpression) bool {
	rhs, ok := other.(*unboundUnaryPredicate)
	if !ok {
		return false
	}

	return up.op == rhs.op && up.term.Equals(rhs.term)
}

func (up *unboundUnaryPredicate) Op() Operation { return up.op }
func (up *unboundUnaryPredicate) Negate() BooleanExpression {
	return &unboundUnaryPredicate{op: up.op.Negate(), term: up.term}
}
func (up *unboundUnaryPredicate) Term() Term { return up.term }
func (up *unboundUnaryPredicate) Bind(schema *Schema, caseSensitive bool) (BooleanExpression, error) {
	bound, err := up.term.Bind(schema, caseSensitive)
	if err != nil {
		return nil, err
	}

	switch up.op {
	case OpIsNull:
		if bound.Ref().Field().Required {
			return AlwaysFalse{}, nil
		}
	case OpNotNull:
		if bound.Ref().Field().Required {
			return AlwaysTrue{}, nil
		}
	case OpIsNan:
		if !bound.Type().Equals(PrimitiveTypes.Float32) && !bound.Type().Equals(PrimitiveTypes.Float64) {
			return AlwaysFalse{}, nil
		}
	case OpNotNan:
		if !bound.Type().Equals(PrimitiveTypes.Float32) && !bound.Type().Equals(PrimitiveTypes.Float64) {
			return AlwaysTrue{}, nil
		}
	}

	return createBoundUnaryPredicate(up.op, bound), nil
}
func (up *unboundUnaryPredicate) Equals(other UnboundPredicate) bool {
	if up.op != other.Op() {
		return false
	}

	// same op means we can assume it's a unary predicate
	return up.term.Equals(other.(*unboundUnaryPredicate).term)
}

type unboundLiteralPredicate struct {
	op   Operation
	term UnboundTerm
	lit  Literal
}

func (ul *unboundLiteralPredicate) String() string {
	return fmt.Sprintf("%s(term=%s, literal=%s)",
		ul.op, ul.term, ul.lit)
}

func (ul *unboundLiteralPredicate) Equivalent(other BooleanExpression) bool {
	rhs, ok := other.(*unboundLiteralPredicate)
	if !ok {
		return false
	}

	return ul.op == rhs.op && ul.term.Equals(rhs.term) && ul.lit.Equals(rhs.lit)
}

func (ul *unboundLiteralPredicate) Op() Operation { return ul.op }
func (ul *unboundLiteralPredicate) Negate() BooleanExpression {
	return &unboundLiteralPredicate{op: ul.op.Negate(), term: ul.term, lit: ul.lit}
}
func (ul *unboundLiteralPredicate) Term() Term { return ul.term }
func (ul *unboundLiteralPredicate) Bind(schema *Schema, caseSensitive bool) (BooleanExpression, error) {
	bound, err := ul.term.Bind(schema, caseSensitive)
	if err != nil {
		return nil, err
	}

	if (ul.op == OpStartsWith || ul.op == OpNotStartsWith) &&
		!bound.Type().Equals(PrimitiveTypes.String) {
		return nil, ErrType
	}

	lit, err := ul.lit.To(bound.Type())
	if err != nil {
		return nil, err
	}

	switch lit.(type) {
	case AboveMaxLiteral:
		switch ul.op {
		case OpLT, OpLTEQ, OpNotEQ:
			return AlwaysTrue{}, nil
		case OpGT, OpGTEQ, OpEQ:
			return AlwaysFalse{}, nil
		}
	case BelowMinLiteral:
		switch ul.op {
		case OpLT, OpLTEQ, OpNotEQ:
			return AlwaysFalse{}, nil
		case OpGT, OpGTEQ, OpEQ:
			return AlwaysTrue{}, nil
		}
	}

	return createBoundLiteralPredicate(ul.op, bound, lit)
}

type unboundSetPredicate struct {
	op   Operation
	term UnboundTerm
	lits []Literal
}

func (usp *unboundSetPredicate) String() string {
	return fmt.Sprintf("%s(term=%s, {%v})", usp.op, usp.term, usp.lits)
}

func (usp *unboundSetPredicate) Equivalent(other BooleanExpression) bool {
	rhs, ok := other.(*unboundSetPredicate)
	if !ok {
		return false
	}

	if usp.op != rhs.op || !usp.term.Equals(rhs.term) {
		return false
	}

	return slices.EqualFunc(usp.lits, rhs.lits, func(a, b Literal) bool {
		return a.Equals(b)
	})
}

func (usp *unboundSetPredicate) Op() Operation { return usp.op }
func (usp *unboundSetPredicate) Negate() BooleanExpression {
	return &unboundSetPredicate{op: usp.op.Negate(), term: usp.term, lits: usp.lits}
}
func (usp *unboundSetPredicate) Term() Term { return usp.term }
func (usp *unboundSetPredicate) Bind(schema *Schema, caseSensitive bool) (BooleanExpression, error) {
	bound, err := usp.term.Bind(schema, caseSensitive)
	if err != nil {
		return nil, err
	}

	return createBoundSetPredicate(usp.op, bound, usp.lits)
}

func UnaryPredicate(op Operation, t UnboundTerm) UnboundPredicate {
	if op < OpIsNull || op > OpNotNan {
		panic("invalid operation for Unary Predicate: " + op.String())
	}
	return &unboundUnaryPredicate{op: op, term: t}
}

func LiteralPredicate(op Operation, t UnboundTerm, lit Literal) UnboundPredicate {
	if op < OpLT || op > OpNotStartsWith {
		panic("invalid operation for LiteralPredicate: " + op.String())
	}
	return &unboundLiteralPredicate{op: op, term: t, lit: lit}
}

func SetPredicate(op Operation, t UnboundTerm, lits []Literal) BooleanExpression {
	if op < OpIn || op > OpNotIn {
		panic("invalid operation for SetPredicate: " + op.String())
	}

	switch len(lits) {
	case 0:
		if op == OpIn {
			return AlwaysFalse{}
		} else if op == OpNotIn {
			return AlwaysTrue{}
		}
	case 1:
		if op == OpIn {
			return LiteralPredicate(OpEQ, t, lits[0])
		} else if op == OpNotIn {
			return LiteralPredicate(OpNotEQ, t, lits[0])
		}
	}

	return &unboundSetPredicate{op: op, term: t, lits: lits}
}

type Term interface {
	fmt.Stringer
}

type UnboundTerm interface {
	Term

	Equals(UnboundTerm) bool
	Bind(schema *Schema, caseSensitive bool) (BoundTerm, error)
}

type BoundTerm interface {
	Term

	fmt.Stringer
	Equals(BoundTerm) bool
	Ref() BoundReference
	Type() Type
	evalToLiteral(structLike) Literal
	evalIsNull(structLike) bool
}

type Bound[T LiteralType] interface {
	BoundTerm

	eval(structLike) Optional[T]
}

type BoundReference interface {
	BoundTerm

	Field() NestedField
}

func createBoundRef(field NestedField, acc accessor) BoundReference {
	switch field.Type.(type) {
	case BooleanType:
		return &boundRef[bool]{field: field, acc: acc}
	case Int32Type:
		return &boundRef[int32]{field: field, acc: acc}
	case Int64Type:
		return &boundRef[int64]{field: field, acc: acc}
	case Float32Type:
		return &boundRef[float32]{field: field, acc: acc}
	case Float64Type:
		return &boundRef[float64]{field: field, acc: acc}
	case DateType:
		return &boundRef[Date]{field: field, acc: acc}
	case TimeType:
		return &boundRef[Time]{field: field, acc: acc}
	case TimestampType, TimestampTzType:
		return &boundRef[Timestamp]{field: field, acc: acc}
	case StringType:
		return &boundRef[string]{field: field, acc: acc}
	case FixedType, BinaryType:
		return &boundRef[[]byte]{field: field, acc: acc}
	case DecimalType:
		return &boundRef[Decimal]{field: field, acc: acc}
	case UUIDType:
		return &boundRef[uuid.UUID]{field: field, acc: acc}
	}
	panic("unhandled bound reference type")
}

type boundRef[T LiteralType] struct {
	field NestedField
	acc   accessor
}

func (b *boundRef[T]) String() string {
	return fmt.Sprintf("BoundReference(field=%s, accessor=%s)",
		b.field, &b.acc)
}

func (b *boundRef[T]) Equals(other BoundTerm) bool {
	rhs, ok := other.(*boundRef[T])
	if !ok {
		return false
	}

	return b.field.Equals(rhs.field)
}

func (b *boundRef[T]) Ref() BoundReference { return b }
func (b *boundRef[T]) eval(st structLike) Optional[T] {
	v := b.acc.Get(st)
	switch v := v.(type) {
	case nil:
		return Optional[T]{}
	case T:
		return Optional[T]{Valid: true, Val: v}
	}
	panic("unexpected type returned for bound ref")
}

func (b *boundRef[T]) Field() NestedField { return b.field }
func (b *boundRef[T]) Type() Type         { return b.field.Type }

func (b *boundRef[T]) evalToLiteral(st structLike) Literal {
	v := b.eval(st)
	lit, _ := NewLiteral[T](v.Val)
	if !lit.Type().Equals(b.field.Type) {
		lit, _ = lit.To(b.field.Type)
	}
	return lit
}

func (b *boundRef[T]) evalIsNull(st structLike) bool {
	v := b.eval(st)
	return !v.Valid
}

type BoundPredicate interface {
	BooleanExpression

	fmt.Stringer

	Ref() BoundReference
	Term() BoundTerm
}

type BoundTransform interface {
	BoundPredicate

	Transform() Transform
}

type BoundUnaryPredicate interface {
	BoundPredicate

	AsUnbound(Reference) UnboundPredicate
}

func createBoundUnaryPredicate(op Operation, bound BoundTerm) BoundUnaryPredicate {
	switch bound.Type().(type) {
	case BooleanType:
		return &boundUnaryPredicate[bool]{
			op: op, term: bound.(Bound[bool])}
	case Int32Type:
		return &boundUnaryPredicate[int32]{
			op: op, term: bound.(Bound[int32])}
	case Int64Type:
		return &boundUnaryPredicate[int64]{
			op: op, term: bound.(Bound[int64])}
	case Float32Type:
		return &boundUnaryPredicate[float32]{
			op: op, term: bound.(Bound[float32])}
	case Float64Type:
		return &boundUnaryPredicate[float64]{
			op: op, term: bound.(Bound[float64])}
	case DateType:
		return &boundUnaryPredicate[Date]{
			op: op, term: bound.(Bound[Date])}
	case TimeType:
		return &boundUnaryPredicate[Time]{
			op: op, term: bound.(Bound[Time])}
	case TimestampType, TimestampTzType:
		return &boundUnaryPredicate[Timestamp]{
			op: op, term: bound.(Bound[Timestamp])}
	case StringType:
		return &boundUnaryPredicate[string]{
			op: op, term: bound.(Bound[string])}
	case FixedType, BinaryType:
		return &boundUnaryPredicate[[]byte]{
			op: op, term: bound.(Bound[[]byte])}
	case DecimalType:
		return &boundUnaryPredicate[Decimal]{
			op: op, term: bound.(Bound[Decimal])}
	case UUIDType:
		return &boundUnaryPredicate[uuid.UUID]{
			op: op, term: bound.(Bound[uuid.UUID])}
	}
	panic("unhandled bound reference type")
}

type boundUnaryPredicate[T LiteralType] struct {
	op   Operation
	term Bound[T]
}

func (bp *boundUnaryPredicate[T]) AsUnbound(r Reference) UnboundPredicate {
	return &unboundUnaryPredicate{op: bp.op, term: r}
}

func (bp *boundUnaryPredicate[T]) Equivalent(other BooleanExpression) bool {
	rhs, ok := other.(*boundUnaryPredicate[T])
	if !ok {
		return false
	}

	return bp.op == rhs.op && bp.term.Equals(rhs.term)
}

func (bp *boundUnaryPredicate[T]) Op() Operation { return bp.op }
func (bp *boundUnaryPredicate[T]) Negate() BooleanExpression {
	return &boundUnaryPredicate[T]{op: bp.op.Negate(), term: bp.term}
}
func (bp *boundUnaryPredicate[T]) Term() BoundTerm     { return bp.term }
func (bp *boundUnaryPredicate[T]) Ref() BoundReference { return bp.term.Ref() }
func (bp *boundUnaryPredicate[T]) String() string {
	return fmt.Sprintf("Bound%s(term=%s)", bp.op, bp.term)
}

type BoundLiteralPredicate interface {
	BoundPredicate

	Literal() Literal
	AsUnbound(Reference, Literal) UnboundPredicate
}

func createBoundLiteralPredicate(op Operation, bound BoundTerm, lit Literal) (BoundPredicate, error) {
	finalLit, err := lit.To(bound.Type())
	if err != nil {
		return nil, err
	}

	switch bound.Type().(type) {
	case BooleanType:
		return &boundLiteralPredicate[bool]{
			op: op, term: bound.(Bound[bool]),
			lit: finalLit.(TypedLiteral[bool]),
		}, nil
	case Int32Type:
		return &boundLiteralPredicate[int32]{
			op: op, term: bound.(Bound[int32]),
			lit: finalLit.(TypedLiteral[int32]),
		}, nil
	case Int64Type:
		return &boundLiteralPredicate[int64]{
			op: op, term: bound.(Bound[int64]),
			lit: finalLit.(TypedLiteral[int64]),
		}, nil
	case Float32Type:
		return &boundLiteralPredicate[float32]{
			op: op, term: bound.(Bound[float32]),
			lit: finalLit.(TypedLiteral[float32]),
		}, nil
	case Float64Type:
		return &boundLiteralPredicate[float64]{
			op: op, term: bound.(Bound[float64]),
			lit: finalLit.(TypedLiteral[float64]),
		}, nil
	case DateType:
		return &boundLiteralPredicate[Date]{
			op: op, term: bound.(Bound[Date]),
			lit: finalLit.(TypedLiteral[Date]),
		}, nil
	case TimeType:
		return &boundLiteralPredicate[Time]{
			op: op, term: bound.(Bound[Time]),
			lit: finalLit.(TypedLiteral[Time]),
		}, nil
	case TimestampType, TimestampTzType:
		return &boundLiteralPredicate[Timestamp]{
			op: op, term: bound.(Bound[Timestamp]),
			lit: finalLit.(TypedLiteral[Timestamp]),
		}, nil
	case StringType:
		return &boundLiteralPredicate[string]{
			op: op, term: bound.(Bound[string]),
			lit: finalLit.(TypedLiteral[string]),
		}, nil
	case FixedType, BinaryType:
		return &boundLiteralPredicate[[]byte]{
			op: op, term: bound.(Bound[[]byte]),
			lit: finalLit.(TypedLiteral[[]byte]),
		}, nil
	case DecimalType:
		return &boundLiteralPredicate[Decimal]{
			op: op, term: bound.(Bound[Decimal]),
			lit: finalLit.(TypedLiteral[Decimal]),
		}, nil
	case UUIDType:
		return &boundLiteralPredicate[uuid.UUID]{
			op: op, term: bound.(Bound[uuid.UUID]),
			lit: finalLit.(TypedLiteral[uuid.UUID]),
		}, nil
	}
	return nil, ErrInvalidArgument
}

type boundLiteralPredicate[T LiteralType] struct {
	op   Operation
	term Bound[T]
	lit  TypedLiteral[T]
}

func (blp *boundLiteralPredicate[T]) Equivalent(other BooleanExpression) bool {
	rhs, ok := other.(*boundLiteralPredicate[T])
	if !ok {
		return false
	}

	return blp.op == rhs.op && blp.term.Equals(rhs.term) && blp.lit.Equals(rhs.lit)
}

func (blp *boundLiteralPredicate[T]) Op() Operation { return blp.op }
func (blp *boundLiteralPredicate[T]) Negate() BooleanExpression {
	return &boundLiteralPredicate[T]{op: blp.op.Negate(), term: blp.term, lit: blp.lit}
}
func (blp *boundLiteralPredicate[T]) Term() BoundTerm     { return blp.term }
func (blp *boundLiteralPredicate[T]) Ref() BoundReference { return blp.term.Ref() }
func (blp *boundLiteralPredicate[T]) String() string {
	return fmt.Sprintf("Bound%s(term=%s, literal=%s)", blp.op, blp.term, blp.lit)
}

func (blp *boundLiteralPredicate[T]) AsUnbound(r Reference, l Literal) UnboundPredicate {
	return &unboundLiteralPredicate{op: blp.op, term: r, lit: l}
}
func (blp *boundLiteralPredicate[T]) Literal() Literal { return blp.lit }

type BoundSetPredicate interface {
	BoundPredicate

	Literals() []Literal
	literalSet() literalSet
	AsUnbound(Reference, []Literal) UnboundPredicate
}

func createBoundSetPredicate(op Operation, bound BoundTerm, lits []Literal) (BooleanExpression, error) {
	switch bound.Type().(type) {
	case BooleanType:
		return toBsp[bool](op, bound, lits)
	case Int32Type:
		return toBsp[int32](op, bound, lits)
	case Int64Type:
		return toBsp[int64](op, bound, lits)
	case Float32Type:
		return toBsp[float32](op, bound, lits)
	case Float64Type:
		return toBsp[float64](op, bound, lits)
	case DateType:
		return toBsp[Date](op, bound, lits)
	case TimeType:
		return toBsp[Time](op, bound, lits)
	case TimestampType, TimestampTzType:
		return toBsp[Timestamp](op, bound, lits)
	case StringType:
		return toBsp[string](op, bound, lits)
	case BinaryType, FixedType:
		return toBsp[[]byte](op, bound, lits)
	case DecimalType:
		return toBsp[Decimal](op, bound, lits)
	case UUIDType:
		return toBsp[uuid.UUID](op, bound, lits)
	}

	return nil, ErrType
}

func toBsp[T LiteralType](op Operation, bound BoundTerm, lits []Literal) (BooleanExpression, error) {
	s, err := toLiteralSet[T](bound.Type(), lits...)
	if err != nil {
		return nil, err
	}

	switch s.len() {
	case 0:
		if op == OpIn {
			return AlwaysFalse{}, nil
		} else if op == OpNotIn {
			return AlwaysTrue{}, nil
		}
	case 1:
		if op == OpIn {
			return createBoundLiteralPredicate(OpEQ, bound, MustLiteral(s.members()[0]))
		} else if op == OpNotIn {
			return createBoundLiteralPredicate(OpNotEQ, bound, MustLiteral(s.members()[0]))
		}
	}

	return newBoundSetPredicate(op, bound.(Bound[T]), s), nil
}

func newBoundSetPredicate[T LiteralType](op Operation, term Bound[T], lits set[T]) *boundSetPredicate[T] {
	return &boundSetPredicate[T]{
		op:   op,
		term: term,
		lits: lits,
		getMembers: sync.OnceValue[[]Literal](func() []Literal {
			values, typ := lits.members(), term.Type()
			out := make([]Literal, len(values))
			for i, v := range values {
				out[i], _ = NewLiteralFromType(v, typ)
			}
			return out
		}),
	}
}

type boundSetPredicate[T LiteralType] struct {
	op   Operation
	term Bound[T]
	lits set[T]

	getMembers func() []Literal
}

func (bsp *boundSetPredicate[T]) Equivalent(other BooleanExpression) bool {
	rhs, ok := other.(*boundSetPredicate[T])
	if !ok {
		return false
	}

	if bsp.op != rhs.op || !bsp.term.Equals(rhs.term) {
		return false
	}

	return bsp.lits.equals(rhs.lits)
}

func (bsp *boundSetPredicate[T]) Op() Operation { return bsp.op }
func (bsp *boundSetPredicate[T]) Negate() BooleanExpression {
	return &boundSetPredicate[T]{op: bsp.op.Negate(), term: bsp.term,
		lits: bsp.lits, getMembers: bsp.getMembers}
}
func (bsp *boundSetPredicate[T]) Term() BoundTerm     { return bsp.term }
func (bsp *boundSetPredicate[T]) Ref() BoundReference { return bsp.term.Ref() }
func (bsp *boundSetPredicate[T]) String() string {
	return fmt.Sprintf("Bound%s(term=%s, {%v})", bsp.op, bsp.term, bsp.getMembers())
}
func (bsp *boundSetPredicate[T]) AsUnbound(r Reference, lits []Literal) UnboundPredicate {
	return &unboundSetPredicate{op: bsp.op, term: r, lits: lits}
}
func (bsp *boundSetPredicate[T]) Literals() []Literal {
	return bsp.getMembers()
}
func (bsp *boundSetPredicate[T]) literalSet() literalSet { return bsp.lits }
