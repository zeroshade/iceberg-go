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
	"reflect"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnaryExpr(t *testing.T) {
	t.Run("negate", func(t *testing.T) {
		n := iceberg.IsNull(iceberg.Reference("a")).Negate()
		exp := iceberg.NotNull(iceberg.Reference("a"))

		assert.True(t, reflect.DeepEqual(n, exp))
	})

	sc := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 2, Name: "a", Type: iceberg.PrimitiveTypes.Int32})
	sc2 := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 2, Name: "a", Type: iceberg.PrimitiveTypes.Float64})
	sc3 := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 2, Name: "a", Type: iceberg.PrimitiveTypes.Int32, Required: true})
	sc4 := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 2, Name: "a", Type: iceberg.PrimitiveTypes.Float32, Required: true})

	t.Run("isnull and notnull", func(t *testing.T) {
		t.Run("bind", func(t *testing.T) {
			n, err := iceberg.IsNull(iceberg.Reference("a")).Bind(sc, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpIsNull, n.Op())

			assert.Implements(t, (*iceberg.BoundPredicate)(nil), n)
			p := n.(iceberg.BoundPredicate)
			assert.IsType(t, iceberg.PrimitiveTypes.Int32, p.Term().Type())
			assert.Same(t, p.Ref(), p.Term().Ref())
			assert.Same(t, p.Ref(), p.Ref().Ref())

			f := p.Ref().Field()
			assert.True(t, f.Equals(sc.Field(0)))
		})

		t.Run("negate and bind", func(t *testing.T) {
			n1, err := iceberg.IsNull(iceberg.Reference("a")).Bind(sc, true)
			require.NoError(t, err)

			n2, err := iceberg.NotNull(iceberg.Reference("a")).Bind(sc, true)
			require.NoError(t, err)

			assert.True(t, n1.Negate().Equivalent(n2))
			assert.True(t, n2.Negate().Equivalent(n1))
		})

		t.Run("null bind required", func(t *testing.T) {
			n1, err := iceberg.IsNull(iceberg.Reference("a")).Bind(sc3, true)
			require.NoError(t, err)

			n2, err := iceberg.NotNull(iceberg.Reference("a")).Bind(sc3, true)
			require.NoError(t, err)

			assert.True(t, n1.Equivalent(iceberg.AlwaysFalse{}))
			assert.True(t, n2.Equivalent(iceberg.AlwaysTrue{}))
		})
	})

	t.Run("isnan notnan", func(t *testing.T) {
		t.Run("negate and bind", func(t *testing.T) {
			n1, err := iceberg.IsNaN(iceberg.Reference("a")).Bind(sc2, true)
			require.NoError(t, err)

			n2, err := iceberg.NotNaN(iceberg.Reference("a")).Bind(sc2, true)
			require.NoError(t, err)

			assert.True(t, n1.Negate().Equivalent(n2))
			assert.True(t, n2.Negate().Equivalent(n1))
		})

		t.Run("bind float", func(t *testing.T) {
			n, err := iceberg.IsNaN(iceberg.Reference("a")).Bind(sc4, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpIsNan, n.Op())
			assert.Implements(t, (*iceberg.BoundPredicate)(nil), n)
			p := n.(iceberg.BoundPredicate)
			assert.IsType(t, iceberg.PrimitiveTypes.Float32, p.Term().Type())

			n2, err := iceberg.NotNaN(iceberg.Reference("a")).Bind(sc4, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpNotNan, n2.Op())
			assert.Implements(t, (*iceberg.BoundPredicate)(nil), n2)
			p2 := n2.(iceberg.BoundPredicate)
			assert.IsType(t, iceberg.PrimitiveTypes.Float32, p2.Term().Type())
		})

		t.Run("bind double", func(t *testing.T) {
			n, err := iceberg.IsNaN(iceberg.Reference("a")).Bind(sc2, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpIsNan, n.Op())
			assert.Implements(t, (*iceberg.BoundPredicate)(nil), n)
			p := n.(iceberg.BoundPredicate)
			assert.IsType(t, iceberg.PrimitiveTypes.Float64, p.Term().Type())

			n2, err := iceberg.NotNaN(iceberg.Reference("a")).Bind(sc2, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpNotNan, n2.Op())
			assert.Implements(t, (*iceberg.BoundPredicate)(nil), n2)
			p2 := n2.(iceberg.BoundPredicate)
			assert.IsType(t, iceberg.PrimitiveTypes.Float64, p2.Term().Type())
		})

		t.Run("bind non float", func(t *testing.T) {
			n1, err := iceberg.IsNaN(iceberg.Reference("a")).Bind(sc, true)
			require.NoError(t, err)

			n2, err := iceberg.NotNaN(iceberg.Reference("a")).Bind(sc, true)
			require.NoError(t, err)

			assert.True(t, n1.Equivalent(iceberg.AlwaysFalse{}))
			assert.True(t, n2.Equivalent(iceberg.AlwaysTrue{}))
		})
	})
}

func TestRefBindingCaseSensitive(t *testing.T) {
	ref1 := iceberg.Reference("foo")
	ref2 := iceberg.Reference("Foo")

	bound1, err := ref1.Bind(tableSchemaSimple, true)
	require.NoError(t, err)
	assert.True(t, bound1.Type().Equals(iceberg.PrimitiveTypes.String))

	_, err = ref2.Bind(tableSchemaSimple, true)
	assert.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.ErrorContains(t, err, "could not bind reference 'Foo', caseSensitive=true")

	bound2, err := ref2.Bind(tableSchemaSimple, false)
	require.NoError(t, err)
	assert.True(t, bound1.Equals(bound2))

	_, err = iceberg.Reference("foot").Bind(tableSchemaSimple, false)
	assert.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.ErrorContains(t, err, "could not bind reference 'foot', caseSensitive=false")
}

func TestInNotInSimplification(t *testing.T) {
	t.Run("in to eq", func(t *testing.T) {
		a := iceberg.IsIn(iceberg.Reference("x"), 34.56)
		b := iceberg.EqualTo(iceberg.Reference("x"), iceberg.MustLiteral(34.56))
		assert.True(t, a.Equivalent(b))
	})

	t.Run("empty in", func(t *testing.T) {
		a := iceberg.IsIn[float32](iceberg.Reference("x"))
		assert.Equal(t, iceberg.AlwaysFalse{}, a)
	})

	t.Run("empty not in", func(t *testing.T) {
		a := iceberg.NotIn[float32](iceberg.Reference("x"))
		assert.Equal(t, iceberg.AlwaysTrue{}, a)
	})

	t.Run("bind not in equal", func(t *testing.T) {
		ex := iceberg.NotIn(iceberg.Reference("foo"), "hello")
		bound, err := ex.(iceberg.UnboundPredicate).Bind(tableSchemaSimple, true)
		require.NoError(t, err)

		assert.Equal(t, iceberg.OpNotEQ, bound.Op())
		p := bound.(iceberg.BoundLiteralPredicate)
		assert.True(t, p.Literal().Equals(iceberg.MustLiteral("hello")))
		assert.Equal(t, tableSchemaSimple.Field(0), p.Ref().Field())
	})

	t.Run("bind and negate", func(t *testing.T) {
		inexp := iceberg.IsIn(iceberg.Reference("foo"), "hello", "world")

		notin := iceberg.NotIn(iceberg.Reference("foo"), "hello", "world")

		assert.True(t, inexp.Negate().Equivalent(notin))
		assert.True(t, notin.Negate().Equivalent(inexp))

		boundin, err := inexp.(iceberg.UnboundPredicate).Bind(tableSchemaSimple, true)
		require.NoError(t, err)

		boundnot, err := notin.(iceberg.UnboundPredicate).Bind(tableSchemaSimple, true)
		require.NoError(t, err)

		assert.True(t, boundin.Negate().Equivalent(boundnot))
		assert.True(t, boundnot.Negate().Equivalent(boundin))
	})

	t.Run("bind dedup", func(t *testing.T) {
		isin := iceberg.IsIn(iceberg.Reference("foo"), "hello", "world", "world")
		bound, err := isin.(iceberg.UnboundPredicate).Bind(tableSchemaSimple, true)
		require.NoError(t, err)

		assert.Equal(t, []iceberg.Literal{
			iceberg.MustLiteral("hello"), iceberg.MustLiteral("world"),
		}, bound.(iceberg.BoundSetPredicate).Literals())
	})

	t.Run("bind dedup to eq", func(t *testing.T) {
		isin := iceberg.IsIn(iceberg.Reference("foo"), "world", "world")
		bound, err := isin.(iceberg.UnboundPredicate).Bind(tableSchemaSimple, true)
		require.NoError(t, err)

		assert.Equal(t, iceberg.OpEQ, bound.Op())
		assert.Equal(t, iceberg.MustLiteral("world"), bound.(iceberg.BoundLiteralPredicate).Literal())
	})
}

func TestNegations(t *testing.T) {
	ref, lit := iceberg.Reference("foo"), iceberg.MustLiteral("hello")

	tests := []struct {
		name     string
		ex1, ex2 iceberg.UnboundPredicate
	}{
		{"equal-not", iceberg.EqualTo(ref, lit), iceberg.NotEqualTo(ref, lit)},
		{"greater-equal-less", iceberg.GreaterThanEqual(ref, lit), iceberg.LessThan(ref, lit)},
		{"greater-less-equal", iceberg.GreaterThan(ref, lit), iceberg.LessThanEqual(ref, lit)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.False(t, tt.ex1.Equivalent(tt.ex2))
			assert.False(t, tt.ex2.Equivalent(tt.ex1))
			assert.True(t, tt.ex1.Negate().Equivalent(tt.ex2))
			assert.True(t, tt.ex2.Negate().Equivalent(tt.ex1))

			b1, err := tt.ex1.Bind(tableSchemaSimple, true)
			require.NoError(t, err)
			b2, err := tt.ex2.Bind(tableSchemaSimple, true)
			require.NoError(t, err)

			assert.False(t, b1.Equivalent(b2))
			assert.False(t, b2.Equivalent(b1))
			assert.True(t, b1.Negate().Equivalent(b2))
			assert.True(t, b2.Negate().Equivalent(b1))
		})
	}
}

type ExprA struct{}

func (ExprA) String() string                    { return "ExprA" }
func (ExprA) Op() iceberg.Operation             { return iceberg.OpFalse }
func (ExprA) Negate() iceberg.BooleanExpression { return ExprB{} }
func (ExprA) Equivalent(o iceberg.BooleanExpression) bool {
	_, ok := o.(ExprA)
	return ok
}

type ExprB struct{}

func (ExprB) String() string                    { return "ExprB" }
func (ExprB) Op() iceberg.Operation             { return iceberg.OpTrue }
func (ExprB) Negate() iceberg.BooleanExpression { return ExprA{} }
func (ExprB) Equivalent(o iceberg.BooleanExpression) bool {
	_, ok := o.(ExprB)
	return ok
}

func TestBoolExprEQ(t *testing.T) {
	tests := []struct {
		exp, testexpra, testexprb iceberg.BooleanExpression
	}{
		{iceberg.NewAnd(ExprA{}, ExprB{}),
			iceberg.NewAnd(ExprA{}, ExprB{}),
			iceberg.NewOr(ExprA{}, ExprB{})},
		{iceberg.NewOr(ExprA{}, ExprB{}),
			iceberg.NewOr(ExprA{}, ExprB{}),
			iceberg.NewAnd(ExprA{}, ExprB{})},
		{iceberg.NewAnd(ExprA{}, ExprB{}),
			iceberg.NewAnd(ExprB{}, ExprA{}),
			iceberg.NewOr(ExprB{}, ExprA{})},
		{iceberg.NewOr(ExprA{}, ExprB{}),
			iceberg.NewOr(ExprB{}, ExprA{}),
			iceberg.NewAnd(ExprB{}, ExprA{})},
		{iceberg.NewNot(ExprA{}), iceberg.NewNot(ExprA{}), ExprB{}},
		{ExprA{}, ExprA{}, ExprB{}},
		{ExprB{}, ExprB{}, ExprA{}},
		{iceberg.IsIn(iceberg.Reference("foo"), "hello", "world"),
			iceberg.IsIn(iceberg.Reference("foo"), "hello", "world"),
			iceberg.IsIn(iceberg.Reference("not_foo"), "hello", "world")},
		{iceberg.IsIn(iceberg.Reference("foo"), "hello", "world"),
			iceberg.IsIn(iceberg.Reference("foo"), "hello", "world"),
			iceberg.IsIn(iceberg.Reference("foo"), "goodbye", "world")},
	}

	for _, tt := range tests {
		assert.True(t, tt.exp.Equivalent(tt.testexpra))
		assert.False(t, tt.exp.Equivalent(tt.testexprb))
	}
}

func TestBoolExprNegate(t *testing.T) {
	tests := []struct {
		lhs, rhs iceberg.BooleanExpression
	}{
		{iceberg.NewAnd(ExprA{}, ExprB{}), iceberg.NewOr(ExprB{}, ExprA{})},
		{iceberg.NewOr(ExprB{}, ExprA{}), iceberg.NewAnd(ExprA{}, ExprB{})},
		{iceberg.NewNot(ExprA{}), ExprA{}},
		{iceberg.IsIn(iceberg.Reference("foo"), "hello", "world"),
			iceberg.NotIn(iceberg.Reference("foo"), "hello", "world")},
		{iceberg.NotIn(iceberg.Reference("foo"), "hello", "world"),
			iceberg.IsIn(iceberg.Reference("foo"), "hello", "world")},
		{iceberg.GreaterThan(iceberg.Reference("foo"), iceberg.MustLiteral(int32(5))),
			iceberg.LessThanEqual(iceberg.Reference("foo"), iceberg.MustLiteral(int32(5)))},
		{iceberg.LessThan(iceberg.Reference("foo"), iceberg.MustLiteral(int32(5))),
			iceberg.GreaterThanEqual(iceberg.Reference("foo"), iceberg.MustLiteral(int32(5)))},
		{iceberg.EqualTo(iceberg.Reference("foo"), iceberg.MustLiteral(int32(5))),
			iceberg.NotEqualTo(iceberg.Reference("foo"), iceberg.MustLiteral(int32(5)))},
		{ExprA{}, ExprB{}},
	}

	for _, tt := range tests {
		assert.True(t, tt.lhs.Negate().Equivalent(tt.rhs))
	}
}

func TestExprFolding(t *testing.T) {
	tests := []struct {
		lhs, rhs iceberg.BooleanExpression
	}{
		{iceberg.NewAnd(ExprA{}, ExprB{}, ExprA{}),
			iceberg.NewAnd(iceberg.NewAnd(ExprA{}, ExprB{}), ExprA{})},
		{iceberg.NewOr(ExprA{}, ExprB{}, ExprA{}),
			iceberg.NewOr(iceberg.NewOr(ExprA{}, ExprB{}), ExprA{})},
		{iceberg.NewNot(iceberg.NewNot(ExprA{})), ExprA{}},
	}

	for _, tt := range tests {
		assert.True(t, tt.lhs.Equivalent(tt.rhs))
	}
}

func TestBaseAlwaysTrueAlwaysFalse(t *testing.T) {
	tests := []struct {
		lhs, rhs iceberg.BooleanExpression
	}{
		{iceberg.NewAnd(iceberg.AlwaysTrue{}, ExprB{}), ExprB{}},
		{iceberg.NewAnd(iceberg.AlwaysFalse{}, ExprB{}), iceberg.AlwaysFalse{}},
		{iceberg.NewAnd(ExprB{}, iceberg.AlwaysTrue{}), ExprB{}},
		{iceberg.NewOr(iceberg.AlwaysTrue{}, ExprB{}), iceberg.AlwaysTrue{}},
		{iceberg.NewOr(iceberg.AlwaysFalse{}, ExprB{}), ExprB{}},
		{iceberg.NewOr(ExprA{}, iceberg.AlwaysFalse{}), ExprA{}},
		{iceberg.NewNot(iceberg.NewNot(ExprA{})), ExprA{}},
		{iceberg.NewNot(iceberg.AlwaysTrue{}), iceberg.AlwaysFalse{}},
		{iceberg.NewNot(iceberg.AlwaysFalse{}), iceberg.AlwaysTrue{}},
	}

	for _, tt := range tests {
		assert.True(t, tt.lhs.Equivalent(tt.rhs))
	}
}

func TestBoundReference(t *testing.T) {
	ref, err := iceberg.Reference("foo").Bind(tableSchemaSimple, true)
	require.NoError(t, err)

	assert.Equal(t, "BoundReference(field=1: foo: optional string, accessor=Accessor(position=0,inner=<nil>))", ref.String())
}

func TestToString(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "a", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "b", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "c", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 4, Name: "d", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 5, Name: "e", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 6, Name: "f", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 7, Name: "g", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 8, Name: "h", Type: iceberg.DecimalTypeOf(8, 4)},
		iceberg.NestedField{ID: 9, Name: "i", Type: iceberg.PrimitiveTypes.UUID},
		iceberg.NestedField{ID: 10, Name: "j", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 11, Name: "k", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 12, Name: "l", Type: iceberg.PrimitiveTypes.Binary})

	null := iceberg.IsNull(iceberg.Reference("a"))
	nan := iceberg.IsNaN(iceberg.Reference("g"))
	boundNull, _ := null.Bind(schema, true)
	boundNan, _ := nan.Bind(schema, true)

	isin := iceberg.IsIn(iceberg.Reference("b"), "a", "b", "c")
	boundIn, _ := isin.(iceberg.UnboundPredicate).Bind(schema, true)

	equal := iceberg.EqualTo(iceberg.Reference("c"), iceberg.MustLiteral("a"))
	grtequal := iceberg.GreaterThanEqual(iceberg.Reference("a"), iceberg.MustLiteral("a"))
	greater := iceberg.GreaterThan(iceberg.Reference("a"), iceberg.MustLiteral("a"))

	boundEqual, _ := equal.Bind(schema, true)
	boundGrtEqual, _ := grtequal.Bind(schema, true)
	boundGreater, _ := greater.Bind(schema, true)

	tests := []struct {
		e        iceberg.BooleanExpression
		expected string
	}{
		{iceberg.NewAnd(null, nan),
			"And(left=IsNull(term=Reference(name='a')), right=IsNaN(term=Reference(name='g')))"},
		{iceberg.NewOr(null, nan),
			"Or(left=IsNull(term=Reference(name='a')), right=IsNaN(term=Reference(name='g')))"},
		{iceberg.NewNot(null),
			"Not(child=IsNull(term=Reference(name='a')))"},
		{iceberg.AlwaysTrue{}, "AlwaysTrue()"},
		{iceberg.AlwaysFalse{}, "AlwaysFalse()"},
		{boundNull,
			"BoundIsNull(term=BoundReference(field=1: a: optional string, accessor=Accessor(position=0,inner=<nil>)))"},
		{boundNull.Negate(),
			"BoundNotNull(term=BoundReference(field=1: a: optional string, accessor=Accessor(position=0,inner=<nil>)))"},
		{boundNan,
			"BoundIsNaN(term=BoundReference(field=7: g: optional float, accessor=Accessor(position=6,inner=<nil>)))"},
		{boundNan.Negate(),
			"BoundNotNaN(term=BoundReference(field=7: g: optional float, accessor=Accessor(position=6,inner=<nil>)))"},
		{isin,
			"In(term=Reference(name='b'), {[a b c]})"},
		{isin.Negate(),
			"NotIn(term=Reference(name='b'), {[a b c]})"},
		{boundIn,
			"BoundIn(term=BoundReference(field=2: b: optional string, accessor=Accessor(position=1,inner=<nil>)), {[a b c]})"},
		{boundIn.Negate(),
			"BoundNotIn(term=BoundReference(field=2: b: optional string, accessor=Accessor(position=1,inner=<nil>)), {[a b c]})"},
		{equal,
			"Equal(term=Reference(name='c'), literal=a)"},
		{equal.Negate(),
			"NotEqual(term=Reference(name='c'), literal=a)"},
		{grtequal,
			"GreaterThanEqual(term=Reference(name='a'), literal=a)"},
		{grtequal.Negate(),
			"LessThan(term=Reference(name='a'), literal=a)"},
		{greater,
			"GreaterThan(term=Reference(name='a'), literal=a)"},
		{greater.Negate(),
			"LessThanEqual(term=Reference(name='a'), literal=a)"},
		{boundEqual,
			"BoundEqual(term=BoundReference(field=3: c: optional string, accessor=Accessor(position=2,inner=<nil>)), literal=a)"},
		{boundEqual.Negate(),
			"BoundNotEqual(term=BoundReference(field=3: c: optional string, accessor=Accessor(position=2,inner=<nil>)), literal=a)"},
		{boundGreater,
			"BoundGreaterThan(term=BoundReference(field=1: a: optional string, accessor=Accessor(position=0,inner=<nil>)), literal=a)"},
		{boundGreater.Negate(),
			"BoundLessThanEqual(term=BoundReference(field=1: a: optional string, accessor=Accessor(position=0,inner=<nil>)), literal=a)"},
		{boundGrtEqual,
			"BoundGreaterThanEqual(term=BoundReference(field=1: a: optional string, accessor=Accessor(position=0,inner=<nil>)), literal=a)"},
		{boundGrtEqual.Negate(),
			"BoundLessThan(term=BoundReference(field=1: a: optional string, accessor=Accessor(position=0,inner=<nil>)), literal=a)"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, tt.e.String())
	}
}

// TODO ADD TESTS FOR ABOVEMAX/BELOWMIN
// test_expressions.py:1135
