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

func IsNull(t UnboundTerm) UnboundPredicate {
	return UnaryPredicate(OpIsNull, t)
}

func NotNull(t UnboundTerm) UnboundPredicate {
	return UnaryPredicate(OpNotNull, t)
}

func IsNaN(t UnboundTerm) UnboundPredicate {
	return UnaryPredicate(OpIsNan, t)
}

func NotNaN(t UnboundTerm) UnboundPredicate {
	return UnaryPredicate(OpNotNan, t)
}

func LessThan(t UnboundTerm, lit Literal) UnboundPredicate {
	return LiteralPredicate(OpLT, t, lit)
}

func LessThanEqual(t UnboundTerm, lit Literal) UnboundPredicate {
	return LiteralPredicate(OpLTEQ, t, lit)
}

func GreaterThan(t UnboundTerm, lit Literal) UnboundPredicate {
	return LiteralPredicate(OpGT, t, lit)
}

func GreaterThanEqual(t UnboundTerm, lit Literal) UnboundPredicate {
	return LiteralPredicate(OpGTEQ, t, lit)
}

func EqualTo(t UnboundTerm, lit Literal) UnboundPredicate {
	return LiteralPredicate(OpEQ, t, lit)
}

func NotEqualTo(t UnboundTerm, lit Literal) UnboundPredicate {
	return LiteralPredicate(OpNotEQ, t, lit)
}

func StartsWith(t UnboundTerm, v string) UnboundPredicate {
	return LiteralPredicate(OpStartsWith, t, StringLiteral(v))
}

func NotStartsWith(t UnboundTerm, v string) UnboundPredicate {
	return LiteralPredicate(OpNotStartsWith, t, StringLiteral(v))
}

func IsIn[T LiteralType](t UnboundTerm, vals ...T) BooleanExpression {
	lits := make([]Literal, 0, len(vals))
	for _, v := range vals {
		lits = append(lits, MustLiteral(v))
	}
	return SetPredicate(OpIn, t, lits)
}

func NotIn[T LiteralType](t UnboundTerm, vals ...T) BooleanExpression {
	lits := make([]Literal, 0, len(vals))
	for _, v := range vals {
		lits = append(lits, MustLiteral(v))
	}

	return SetPredicate(OpNotIn, t, lits)
}
