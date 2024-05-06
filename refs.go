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

import "fmt"

type Reference interface {
	Term

	Name() string
}

type BoundRef interface {
	Field() NestedField
	Type() Type
	Name() string
	Ref() Reference
	Equivalent(Term) bool
}

type BoundReference[T literalType] struct {
	field NestedField
	acc   accessor[StructLike]
	name  string
}

func (br *BoundReference[T]) Type() Type   { return br.field.Type }
func (br *BoundReference[T]) Name() string { return br.name }
func (br *BoundReference[T]) Eval(st StructLike) (optional[T], error) {
	v := br.acc.Get(st)
	switch v := v.(type) {
	case nil:
		return optional[T]{}, nil
	case T:
		return optional[T]{val: v, isValid: true}, nil
	case *T:
		return optional[T]{val: *v, isValid: true}, nil
	}

	return optional[T]{}, fmt.Errorf("incorrect type returned for reference: %+v", v)
}

func (br *BoundReference[T]) Field() NestedField { return br.field }

func (br *BoundReference[T]) Ref() Reference { return br }
func (br *BoundReference[T]) Equivalent(other Term) bool {
	rhs, ok := other.(*BoundReference[T])
	if ok {
		otherField := rhs.field
		return br.field.ID == otherField.ID &&
			br.field.Type.Equals(otherField.Type) &&
			br.field.Required == otherField.Required
	}

	return other.Equivalent(br)
}

type NamedReference[T literalType] struct {
	name string
}

func (nr *NamedReference[T]) Name() string              { return nr.name }
func (nr *NamedReference[T]) Equivalent(term Term) bool { return false }
func (nr *NamedReference[T]) Ref() Reference {
	return nr
}
func (nr *NamedReference[T]) Bind(st StructType, caseSensitive bool) (BoundTerm[T], error) {
	return nil, nil
}
