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

type Expression interface {
	Op() Operation
	Negate() Expression
	Equivalent(Expression) bool
}

var (
	AlwaysTrue  = alwaysTrue{}
	AlwaysFalse = alwaysFalse{}
)

type alwaysFalse struct{}

func (alwaysFalse) Op() Operation      { return OpFalse }
func (alwaysFalse) Negate() Expression { return AlwaysTrue }
func (alwaysFalse) Equivalent(other Expression) bool {
	return other.Op() == OpFalse
}

type alwaysTrue struct{}

func (alwaysTrue) Op() Operation      { return OpTrue }
func (alwaysTrue) Negate() Expression { return AlwaysFalse }
func (alwaysTrue) Equivalent(other Expression) bool {
	return other.Op() == OpTrue
}

type UnboundTransform[S, T literalType] struct {
	ref       *NamedReference[S]
	transform TypedTransform[S, T]
}

func (ut *UnboundTransform[S, T]) Bind(st StructType, caseSensitive bool) (BoundTerm[T], error) {
	boundRef, err := ut.ref.Bind(st, caseSensitive)
	if err != nil {
		return nil, err
	}

	return newBoundTransform(boundRef.(*BoundReference[S]), ut.transform)
}

func (ut *UnboundTransform[S, T]) Transform() TypedTransform[S, T] { return ut.transform }

func (ut *UnboundTransform[S, T]) Ref() Reference { return ut.ref }

func newBoundTransform[S, T literalType](ref *BoundReference[S], t TypedTransform[S, T]) (*BoundTransform[S, T], error) {
	fn, err := t.Bind(ref.Type())
	if err != nil {
		return nil, err
	}

	return &BoundTransform[S, T]{
		ref: ref, transform: t,
		fn: fn,
	}, nil
}

type BoundTransform[S, T literalType] struct {
	ref       *BoundReference[S]
	transform TypedTransform[S, T]
	fn        func(optional[S]) (optional[T], error)
}

func (bt *BoundTransform[S, T]) Type() Type {
	return bt.transform.ResultType(bt.ref.Type())
}

func (bt *BoundTransform[S, T]) Eval(st StructLike) (optional[T], error) {
	v, err := bt.ref.Eval(st)
	if err != nil {
		return optional[T]{}, err
	}
	return bt.fn(v)
}

func (bt *BoundTransform[S, T]) Ref() Reference {
	return bt.ref
}

func (bt *BoundTransform[S, T]) Equivalent(other Term) bool {
	if rhs, ok := other.(*BoundTransform[S, T]); ok {
		return bt.ref.Equivalent(bt.ref) && bt.transform.Equals(rhs.transform)
	}

	if rhs, ok := other.(*BoundReference[S]); ok && bt.transform.String() == "identity" {
		return bt.ref.Equivalent(rhs)
	}

	return false
}

func and(left, right Expression) Expression {
	if left == nil || right == nil {
		panic("cannot construct expression with nil arguments")
	}

	switch {
	case left == AlwaysFalse || right == AlwaysFalse:
		return AlwaysFalse
	case left == AlwaysTrue:
		return right
	case right == AlwaysTrue:
		return left
	}

	return &AndExpr{left: left, right: right}
}

func And(left, right Expression, addl ...Expression) Expression {
	folded := and(left, right)
	for _, a := range addl {
		folded = and(folded, a)
	}
	return folded
}

type AndExpr struct {
	left, right Expression
}

func (a *AndExpr) Left() Expression  { return a.left }
func (a *AndExpr) Right() Expression { return a.right }
func (a *AndExpr) Op() Operation     { return OpAnd }
func (a *AndExpr) Negate() Expression {
	return &OrExpr{left: a.left, right: a.right}
}

func (a *AndExpr) Equivalent(other Expression) bool {
	rhs, ok := other.(*AndExpr)
	if !ok {
		return false
	}
	return (a.left.Equivalent(rhs.left) && a.right.Equivalent(rhs.right)) ||
		(a.left.Equivalent(rhs.right) && a.right.Equivalent(rhs.left))
}

func or(left, right Expression) Expression {
	if left == nil || right == nil {
		panic("cannot construct expression with nil arguments")
	}

	switch {
	case left == AlwaysTrue || right == AlwaysTrue:
		return AlwaysTrue
	case left == AlwaysFalse:
		return right
	case right == AlwaysFalse:
		return left
	}

	return &OrExpr{left: left, right: right}
}

func Or(left, right Expression, addl ...Expression) Expression {
	folded := or(left, right)
	for _, a := range addl {
		folded = or(folded, a)
	}
	return folded
}

type OrExpr struct {
	left, right Expression
}

func (a *OrExpr) Left() Expression  { return a.left }
func (a *OrExpr) Right() Expression { return a.right }
func (a *OrExpr) Op() Operation     { return OpOr }
func (a *OrExpr) Negate() Expression {
	return &AndExpr{left: a.left, right: a.right}
}

func (a *OrExpr) Equivalent(other Expression) bool {
	rhs, ok := other.(*OrExpr)
	if !ok {
		return false
	}
	return (a.left.Equivalent(rhs.left) && a.right.Equivalent(rhs.right)) ||
		(a.left.Equivalent(rhs.right) && a.right.Equivalent(rhs.left))
}

func Not(child Expression) Expression {
	if child == nil {
		panic("cannot construct expression from nil argument")
	}

	switch {
	case child == AlwaysTrue:
		return AlwaysFalse
	case child == AlwaysFalse:
		return AlwaysTrue
	}

	if rhs, ok := child.(*NotExpr); ok {
		return rhs.child
	}

	return &NotExpr{child: child}
}

type NotExpr struct {
	child Expression
}

func (n *NotExpr) Child() Expression  { return n.child }
func (n *NotExpr) Op() Operation      { return OpNot }
func (n *NotExpr) Negate() Expression { return n.child }
func (n *NotExpr) Equivalent(other Expression) bool {
	rhs, ok := other.(*NotExpr)
	if !ok {
		return false
	}
	return n.child.Equivalent(rhs.child)
}

func isNull[T literalType](name string) *UnboundPredicate[T] {
	return newUnboundPredicate(OpIsNull, &NamedReference[T]{name: name})
}

func isNullExpr[T literalType](expr UnboundTerm[T]) *UnboundPredicate[T] {
	return newUnboundPredicate(OpIsNull, expr)
}

func notNull[T literalType](name string) *UnboundPredicate[T] {
	return newUnboundPredicate(OpNotNull, &NamedReference[T]{name: name})
}

type Aggregate[C Term] interface {
	Expression

	Term() C
}
