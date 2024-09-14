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

package table

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/iceberg-go"
	"github.com/substrait-io/substrait-go/expr"
	"github.com/substrait-io/substrait-go/types"
)

const (
	fieldDocKey       = "doc"
	parquetFieldIDKey = "PARQUET:field_id"
)

type arrowSchemaVisitor[T any] interface {
	Schema(*arrow.Schema, T) T
	Struct(*arrow.StructType, []T) T
	Field(arrow.Field, T) T
	List(arrow.ListLikeType, T) T
	Map(*arrow.MapType, T, T) T
	Primitive(arrow.DataType) T
}

type beforeFieldVisitor interface {
	BeforeField(arrow.Field)
}

type afterFieldVisitor interface {
	AfterField(arrow.Field)
}

type beforeListElemVisitor interface {
	BeforeListElement(arrow.Field)
}

type afterListElemVisitor interface {
	AfterListElement(arrow.Field)
}

type beforeMapKeyVisitor interface {
	BeforeMapKey(arrow.Field)
}

type afterMapKeyVisitor interface {
	AfterMapKey(arrow.Field)
}

type beforeMapValueVisitor interface {
	BeforeMapValue(arrow.Field)
}

type afterMapValueVisitor interface {
	AfterMapValue(arrow.Field)
}

func visitArrow[T any](sc *arrow.Schema, visitor arrowSchemaVisitor[T]) (res T, err error) {
	if sc == nil {
		err = fmt.Errorf("%w: cannot visit nil arrow schema", iceberg.ErrInvalidArgument)
	}

	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case string:
				err = fmt.Errorf("error encountered during arrow schema visitor: %s", e)
			case error:
				err = fmt.Errorf("error encountered during arrow shcema visitor: %w", e)
			}
		}
	}()

	return visitor.Schema(sc, visitArrowStruct(arrow.StructOf(sc.Fields()...), visitor)), nil
}

func visitArrowStruct[T any](obj *arrow.StructType, visitor arrowSchemaVisitor[T]) T {
	results := make([]T, obj.NumFields())
	bf, _ := visitor.(beforeFieldVisitor)
	af, _ := visitor.(afterFieldVisitor)

	for i, f := range obj.Fields() {
		if bf != nil {
			bf.BeforeField(f)
		}

		res := visitArrowField(f, visitor)

		if af != nil {
			af.AfterField(f)
		}

		results[i] = visitor.Field(f, res)
	}

	return visitor.Struct(obj, results)
}

func visitArrowField[T any](obj arrow.Field, visitor arrowSchemaVisitor[T]) T {
	switch typ := obj.Type.(type) {
	case *arrow.StructType:
		return visitArrowStruct(typ, visitor)
	case *arrow.MapType:
		return visitArrowMap(typ, visitor)
	case arrow.ListLikeType:
		return visitArrowList(typ, visitor)
	default:
		return visitor.Primitive(typ)
	}
}

func visitArrowMap[T any](obj *arrow.MapType, visitor arrowSchemaVisitor[T]) T {
	key, val := obj.KeyField(), obj.ItemField()

	if bmk, ok := visitor.(beforeMapKeyVisitor); ok {
		bmk.BeforeMapKey(key)
	}

	keyResult := visitArrowField(key, visitor)

	if amk, ok := visitor.(afterMapKeyVisitor); ok {
		amk.AfterMapKey(key)
	}

	if bmv, ok := visitor.(beforeMapValueVisitor); ok {
		bmv.BeforeMapValue(val)
	}

	valueResult := visitArrowField(val, visitor)

	if amv, ok := visitor.(afterMapValueVisitor); ok {
		amv.AfterMapValue(val)
	}

	return visitor.Map(obj, keyResult, valueResult)
}

func visitArrowList[T any](obj arrow.ListLikeType, visitor arrowSchemaVisitor[T]) T {
	elemField := obj.ElemField()

	if bl, ok := visitor.(beforeListElemVisitor); ok {
		bl.BeforeListElement(elemField)
	}

	res := visitArrowField(elemField, visitor)

	if al, ok := visitor.(afterListElemVisitor); ok {
		al.AfterListElement(elemField)
	}

	return visitor.List(obj, res)
}

func getFieldID(f arrow.Field) *int {
	if !f.HasMetadata() {
		return nil
	}

	fieldIdStr, ok := f.Metadata.GetValue("PARQUET:field_id")
	if !ok {
		return nil
	}

	id, err := strconv.Atoi(fieldIdStr)
	if err != nil {
		return nil
	}

	return &id
}

type hasIDs struct{}

func (hasIDs) Schema(sc *arrow.Schema, result bool) bool {
	return result
}

func (hasIDs) Struct(st *arrow.StructType, results []bool) bool {
	return !slices.Contains(results, false)
}

func (hasIDs) Field(f arrow.Field, result bool) bool {
	return getFieldID(f) != nil
}

func (hasIDs) List(l arrow.ListLikeType, elem bool) bool {
	elemField := l.ElemField()
	return elem && getFieldID(elemField) != nil
}

func (hasIDs) Map(m *arrow.MapType, key, val bool) bool {
	return key && val &&
		getFieldID(m.KeyField()) != nil && getFieldID(m.ItemField()) != nil
}

func (hasIDs) Primitive(arrow.DataType) bool {
	return true
}

func arrowTypToIceberg(dt arrow.DataType) (iceberg.Type, error) {
	sc := arrow.NewSchema([]arrow.Field{{Type: dt,
		Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"1"})}}, nil)

	iceSchema, err := arrowToIceberg(sc)
	if err != nil {
		return nil, err
	}
	return iceSchema.Field(0).Type, nil
}

func arrowToIceberg(sc *arrow.Schema) (*iceberg.Schema, error) {
	hasIDs, err := visitArrow(sc, hasIDs{})
	if err != nil {
		return nil, err
	}

	if hasIDs {
		out, err := visitArrow(sc, &convertToIceberg{fieldNames: make([]string, 0)})
		if err != nil {
			return nil, err
		}

		return iceberg.NewSchema(0, out.Type.(*iceberg.StructType).FieldList...), nil
	} else {
		return nil, fmt.Errorf("%w: parquet file does not have field-ids and the iceberg table does not have 'schema.name-mapping.default' defined",
			ErrInvalidMetadata)
	}
}

type convertToIceberg struct {
	fieldNames []string
}

func (c *convertToIceberg) fieldID(field arrow.Field) int {
	if id := getFieldID(field); id != nil {
		return *id
	}

	panic(fmt.Errorf("%w: cannot convert %s to Iceberg field, missing field_id",
		iceberg.ErrInvalidSchema, field))
}

func (c *convertToIceberg) Schema(_ *arrow.Schema, result iceberg.NestedField) iceberg.NestedField {
	return result
}

func (c *convertToIceberg) Struct(_ *arrow.StructType, results []iceberg.NestedField) iceberg.NestedField {
	return iceberg.NestedField{
		Type: &iceberg.StructType{FieldList: results},
	}
}

func (c *convertToIceberg) Field(field arrow.Field, result iceberg.NestedField) iceberg.NestedField {
	result.ID = c.fieldID(field)
	if field.HasMetadata() {
		if doc, ok := field.Metadata.GetValue("doc"); ok {
			result.Doc = doc
		}
	}
	result.Required = !field.Nullable
	result.Name = field.Name
	return result
}

func (c *convertToIceberg) List(listTyp arrow.ListLikeType, elemResult iceberg.NestedField) iceberg.NestedField {
	elemField := listTyp.ElemField()
	elemID := c.fieldID(elemField)

	return iceberg.NestedField{
		Type: &iceberg.ListType{
			ElementID:       elemID,
			Element:         elemResult.Type,
			ElementRequired: !elemField.Nullable,
		},
	}
}

func (c *convertToIceberg) Map(m *arrow.MapType, keyResult, valueResult iceberg.NestedField) iceberg.NestedField {
	keyField, valField := m.KeyField(), m.ItemField()
	keyId, valId := c.fieldID(keyField), c.fieldID(valField)

	return iceberg.NestedField{
		Type: &iceberg.MapType{
			KeyID:         keyId,
			KeyType:       keyResult.Type,
			ValueID:       valId,
			ValueType:     valueResult.Type,
			ValueRequired: !valField.Nullable,
		},
	}
}

var (
	utcAliases = []string{"UTC", "+00:00", "Etc/UTC", "Z"}
)

func (c *convertToIceberg) Primitive(dt arrow.DataType) iceberg.NestedField {
	switch dt := dt.(type) {
	case *arrow.BooleanType:
		return iceberg.NestedField{Type: iceberg.PrimitiveTypes.Bool}
	case *arrow.Uint8Type, *arrow.Int8Type, *arrow.Uint16Type, *arrow.Int16Type,
		*arrow.Uint32Type, *arrow.Int32Type:
		return iceberg.NestedField{Type: iceberg.PrimitiveTypes.Int32}
	case *arrow.Uint64Type, *arrow.Int64Type:
		return iceberg.NestedField{Type: iceberg.PrimitiveTypes.Int64}
	case *arrow.Float16Type, *arrow.Float32Type:
		return iceberg.NestedField{Type: iceberg.PrimitiveTypes.Float32}
	case *arrow.Float64Type:
		return iceberg.NestedField{Type: iceberg.PrimitiveTypes.Float64}
	case *arrow.Decimal128Type:
		return iceberg.NestedField{
			Type: iceberg.DecimalTypeOf(int(dt.GetPrecision()), int(dt.GetScale()))}
	case *arrow.StringType, *arrow.LargeStringType:
		return iceberg.NestedField{Type: iceberg.PrimitiveTypes.String}
	case *arrow.BinaryType, *arrow.LargeBinaryType:
		return iceberg.NestedField{Type: iceberg.PrimitiveTypes.Binary}
	case *arrow.Date32Type:
		return iceberg.NestedField{Type: iceberg.PrimitiveTypes.Date}
	case *arrow.Time64Type:
		if dt.Unit == arrow.Microsecond {
			return iceberg.NestedField{Type: iceberg.PrimitiveTypes.Time}
		}
	case *arrow.TimestampType:
		if dt.Unit == arrow.Nanosecond {
			panic(fmt.Errorf("%w: 'ns' timestamp precision not supported",
				iceberg.ErrType))
		}

		if slices.Contains(utcAliases, dt.TimeZone) {
			return iceberg.NestedField{Type: iceberg.PrimitiveTypes.TimestampTz}
		}
		return iceberg.NestedField{Type: iceberg.PrimitiveTypes.Timestamp}
	case *arrow.FixedSizeBinaryType:
		return iceberg.NestedField{Type: iceberg.FixedTypeOf(dt.ByteWidth)}
	case arrow.ExtensionType:
		if dt.ExtensionName() == "arrow.uuid" {
			return iceberg.NestedField{Type: iceberg.PrimitiveTypes.UUID}
		}
	}

	panic(fmt.Errorf("%w: unsupported arrow type - %s", iceberg.ErrInvalidSchema, dt))
}

func typeToArrowType(typ iceberg.Type) (arrow.DataType, error) {
	result, err := schemaToArrow(iceberg.NewSchema(0, iceberg.NestedField{Type: typ}), nil, true)
	if err != nil {
		return nil, err
	}

	return result.Field(0).Type, nil
}

func schemaToArrow(sc *iceberg.Schema, metadata map[string]string, includeFieldIDs bool) (*arrow.Schema, error) {
	out, err := iceberg.Visit(sc, &convertToArrow{metadata: metadata, includeFieldIDs: includeFieldIDs})
	if err != nil {
		return nil, err
	}

	return arrow.NewSchema(out.Type.(*arrow.StructType).Fields(), &out.Metadata), nil
}

type convertToArrow struct {
	metadata        map[string]string
	includeFieldIDs bool
}

func (c *convertToArrow) Schema(_ *iceberg.Schema, result arrow.Field) arrow.Field {
	result.Metadata = arrow.MetadataFrom(c.metadata)
	return result
}

func (c *convertToArrow) Struct(_ iceberg.StructType, results []arrow.Field) arrow.Field {
	return arrow.Field{Type: arrow.StructOf(results...)}
}

func (c *convertToArrow) Field(field iceberg.NestedField, result arrow.Field) arrow.Field {
	meta := map[string]string{}
	if len(field.Doc) > 0 {
		meta[fieldDocKey] = field.Doc
	}

	if c.includeFieldIDs {
		meta[parquetFieldIDKey] = strconv.Itoa(field.ID)
	}

	if len(meta) > 0 {
		result.Metadata = arrow.MetadataFrom(meta)
	}

	result.Name = field.Name
	result.Nullable = !field.Required
	return result
}

func (c *convertToArrow) List(list iceberg.ListType, elemResult arrow.Field) arrow.Field {
	elemField := c.Field(list.ElementField(), elemResult)
	return arrow.Field{Type: arrow.LargeListOfField(elemField)}
}

func (c *convertToArrow) Map(mapType iceberg.MapType, keyResult, valueResult arrow.Field) arrow.Field {
	keyField := c.Field(mapType.KeyField(), keyResult)
	valField := c.Field(mapType.ValueField(), valueResult)
	return arrow.Field{Type: arrow.MapOfWithMetadata(keyField.Type,
		keyField.Metadata, valField.Type, valField.Metadata)}
}

func (c *convertToArrow) Primitive(iceberg.PrimitiveType) arrow.Field { panic("shouldn't be called") }

func (c *convertToArrow) VisitFixed(f iceberg.FixedType) arrow.Field {
	return arrow.Field{Type: &arrow.FixedSizeBinaryType{ByteWidth: f.Len()}}
}

func (c *convertToArrow) VisitDecimal(d iceberg.DecimalType) arrow.Field {
	return arrow.Field{Type: &arrow.Decimal128Type{Precision: int32(d.Precision()), Scale: int32(d.Scale())}}
}

func (c *convertToArrow) VisitBoolean() arrow.Field {
	return arrow.Field{Type: arrow.FixedWidthTypes.Boolean}
}

func (c *convertToArrow) VisitInt32() arrow.Field {
	return arrow.Field{Type: arrow.PrimitiveTypes.Int32}
}

func (c *convertToArrow) VisitInt64() arrow.Field {
	return arrow.Field{Type: arrow.PrimitiveTypes.Int64}
}

func (c *convertToArrow) VisitFloat32() arrow.Field {
	return arrow.Field{Type: arrow.PrimitiveTypes.Float32}
}

func (c *convertToArrow) VisitFloat64() arrow.Field {
	return arrow.Field{Type: arrow.PrimitiveTypes.Float64}
}

func (c *convertToArrow) VisitDate() arrow.Field {
	return arrow.Field{Type: arrow.FixedWidthTypes.Date32}
}

func (c *convertToArrow) VisitTime() arrow.Field {
	return arrow.Field{Type: arrow.FixedWidthTypes.Time64us}
}

func (c *convertToArrow) VisitTimestampTz() arrow.Field {
	return arrow.Field{Type: arrow.FixedWidthTypes.Timestamp_us}
}

func (c *convertToArrow) VisitTimestamp() arrow.Field {
	return arrow.Field{Type: &arrow.TimestampType{Unit: arrow.Microsecond}}
}

func (c *convertToArrow) VisitString() arrow.Field {
	return arrow.Field{Type: arrow.BinaryTypes.LargeString}
}

func (c *convertToArrow) VisitBinary() arrow.Field {
	return arrow.Field{Type: arrow.BinaryTypes.LargeBinary}
}

func (c *convertToArrow) VisitUUID() arrow.Field {
	return arrow.Field{Type: extensions.NewUUIDType()}
}

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
