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

package parquet

import (
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/apache/iceberg-go"
	"github.com/substrait-io/substrait-go/expr"
	"github.com/substrait-io/substrait-go/types"
)

type convertToArrowExpr struct {
	bldr exprs.ExprBuilder
}

func toSubstraitLiteral(lit iceberg.Literal) expr.Literal {
	switch lit := lit.(type) {
	case iceberg.BoolLiteral:
		return expr.NewPrimitiveLiteral(bool(lit), false)
	case iceberg.Int32Literal:
		return expr.NewPrimitiveLiteral(lit, false)
	case iceberg.Int64Literal:
		return expr.NewPrimitiveLiteral(lit, false)
	case iceberg.Float32Literal:
		return expr.NewPrimitiveLiteral(float32(lit), false)
	case iceberg.Float64Literal:
		return expr.NewPrimitiveLiteral(float64(lit), false)
	case iceberg.StringLiteral:
		return expr.NewPrimitiveLiteral(string(lit), false)
	case iceberg.BinaryLiteral:
		return expr.NewByteSliceLiteral([]byte(lit), false)
	case iceberg.TimestampLiteral:
		return expr.NewPrimitiveLiteral(types.Timestamp(lit), false)
	case iceberg.DateLiteral:
		return expr.NewPrimitiveLiteral(types.Date(lit), false)
	case iceberg.TimeLiteral:
		return expr.NewPrimitiveLiteral(types.Time(lit), false)
	case iceberg.FixedLiteral:
		return expr.NewFixedCharLiteral(types.FixedChar(lit), false)
	case iceberg.UUIDLiteral:
		return expr.NewByteSliceLiteral(types.UUID(lit[:]), false)
	case iceberg.DecimalLiteral:
		byts, _ := lit.MarshalBinary()
		result, _ := expr.NewLiteral(&types.Decimal{
			Scale:     int32(lit.Scale),
			Value:     byts,
			Precision: 38,
		}, false)
		return result
	}
	panic("invalid literal type")
}

func (c convertToArrowExpr) VisitIn(term iceberg.BoundTerm, literals iceberg.Set[iceberg.Literal]) expr.Expression {
	val := make(expr.ListLiteralValue, literals.Len())
	for i, lit := range literals.Members() {
		val[i] = toSubstraitLiteral(lit)
	}

	result, err := c.bldr.MustCallScalar("is_in", nil,
		c.bldr.FieldRef(term.Ref().Field().Name),
		c.bldr.Literal(expr.NewNestedLiteral(val, false))).BuildExpr()
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitNotIn(term iceberg.BoundTerm, literals iceberg.Set[iceberg.Literal]) expr.Expression {
	val := make(expr.ListLiteralValue, literals.Len())
	for i, lit := range literals.Members() {
		val[i] = toSubstraitLiteral(lit)
	}

	result, err := c.bldr.MustCallScalar("not", nil,
		c.bldr.MustCallScalar("is_in", nil,
			c.bldr.FieldRef(term.Ref().Field().Name),
			c.bldr.Literal(expr.NewNestedLiteral(val, false)))).BuildExpr()
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitIsNan(term iceberg.BoundTerm) expr.Expression {
	result, err := c.bldr.MustCallScalar("is_nan", nil,
		c.bldr.FieldRef(term.Ref().Field().Name)).BuildExpr()
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitNotNan(term iceberg.BoundTerm) expr.Expression {
	result, err := c.bldr.MustCallScalar("not", nil,
		c.bldr.MustCallScalar("is_nan", nil,
			c.bldr.FieldRef(term.Ref().Field().Name))).BuildExpr()
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitIsNull(term iceberg.BoundTerm) expr.Expression {
	result, err := c.bldr.MustCallScalar("is_null", nil,
		c.bldr.FieldRef(term.Ref().Field().Name)).BuildExpr()
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitNotNull(term iceberg.BoundTerm) expr.Expression {
	result, err := c.bldr.MustCallScalar("is_not_null", nil,
		c.bldr.FieldRef(term.Ref().Field().Name)).BuildExpr()
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitEqual(term iceberg.BoundTerm, lit iceberg.Literal) expr.Expression {
	result, err := c.bldr.MustCallScalar("equal", nil,
		c.bldr.FieldRef(term.Ref().Field().Name),
		c.bldr.Literal(toSubstraitLiteral(lit))).BuildExpr()
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitNotEqual(term iceberg.BoundTerm, lit iceberg.Literal) expr.Expression {
	result, err := c.bldr.MustCallScalar("not_equal", nil,
		c.bldr.FieldRef(term.Ref().Field().Name),
		c.bldr.Literal(toSubstraitLiteral(lit))).BuildExpr()
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitGreaterEqual(term iceberg.BoundTerm, lit iceberg.Literal) expr.Expression {
	result, err := c.bldr.MustCallScalar("greater_equal", nil,
		c.bldr.FieldRef(term.Ref().Field().Name),
		c.bldr.Literal(toSubstraitLiteral(lit))).BuildExpr()
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitGreater(term iceberg.BoundTerm, lit iceberg.Literal) expr.Expression {
	result, err := c.bldr.MustCallScalar("greater", nil,
		c.bldr.FieldRef(term.Ref().Field().Name),
		c.bldr.Literal(toSubstraitLiteral(lit))).BuildExpr()
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitLessEqual(term iceberg.BoundTerm, lit iceberg.Literal) expr.Expression {
	result, err := c.bldr.MustCallScalar("less_equal", nil,
		c.bldr.FieldRef(term.Ref().Field().Name),
		c.bldr.Literal(toSubstraitLiteral(lit))).BuildExpr()
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitLess(term iceberg.BoundTerm, lit iceberg.Literal) expr.Expression {
	result, err := c.bldr.MustCallScalar("less", nil,
		c.bldr.FieldRef(term.Ref().Field().Name),
		c.bldr.Literal(toSubstraitLiteral(lit))).BuildExpr()
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitStartsWith(term iceberg.BoundTerm, lit iceberg.Literal) expr.Expression {
	result, err := c.bldr.MustCallScalar("starts_with", nil,
		c.bldr.FieldRef(term.Ref().Field().Name),
		c.bldr.Literal(toSubstraitLiteral(lit))).BuildExpr()
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitNotStartsWith(term iceberg.BoundTerm, lit iceberg.Literal) expr.Expression {
	result, err := c.bldr.MustCallScalar("not", nil,
		c.bldr.MustCallScalar("starts_with", nil,
			c.bldr.FieldRef(term.Ref().Field().Name),
			c.bldr.Literal(toSubstraitLiteral(lit)))).BuildExpr()
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitTrue() expr.Expression {
	return expr.NewPrimitiveLiteral(true, false)
}

func (c convertToArrowExpr) VisitFalse() expr.Expression {
	return expr.NewPrimitiveLiteral(false, false)
}

func (c convertToArrowExpr) VisitNot(child expr.Expression) expr.Expression {
	result, err := exprs.NewScalarCall(c.bldr.ExtIDSet(), "not", nil, child)
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitAnd(left, right expr.Expression) expr.Expression {
	result, err := exprs.NewScalarCall(c.bldr.ExtIDSet(), "and_kleene", nil, left, right)
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitOr(left, right expr.Expression) expr.Expression {
	result, err := exprs.NewScalarCall(c.bldr.ExtIDSet(), "or_kleene", nil, left, right)
	if err != nil {
		panic(err)
	}
	return result
}

func (c convertToArrowExpr) VisitUnbound(iceberg.UnboundPredicate) expr.Expression {
	panic("should not receive unbound predicate for arrow conversion")
}

func (c convertToArrowExpr) VisitBound(pred iceberg.BoundPredicate) expr.Expression {
	return iceberg.VisitBoundPredicate[expr.Expression](pred, c)
}

// func toSubstrait(sub types.NamedStruct, cond iceberg.BooleanExpression) {
// 	bldr := plan.NewBuilderDefault()
// 	input := bldr.NamedScan(nil, sub)
// 	filterRel, err := bldr.Filter(input, )
// 	bldr.Project(filterRel, )
// }
