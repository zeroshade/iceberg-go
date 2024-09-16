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
	"fmt"
	"slices"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
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
			iceberg.ErrInvalidArgument)
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

type manifestVisitor[T any] interface {
	Manifest(*pqarrow.SchemaManifest, []T) T
	Field(pqarrow.SchemaField, T) T
	Struct(pqarrow.SchemaField, []T) T
	List(pqarrow.SchemaField, T) T
	Map(pqarrow.SchemaField, T, T) T
	Primitive(pqarrow.SchemaField) T
}

func visitParquetManifest[T any](manifest *pqarrow.SchemaManifest, visitor manifestVisitor[T]) (res T, err error) {
	if manifest == nil {
		err = fmt.Errorf("%w: cannot visit nil manifest", iceberg.ErrInvalidArgument)
		return
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s", r)
		}
	}()

	results := make([]T, len(manifest.Fields))
	for i, f := range manifest.Fields {
		res := visitManifestField(f, visitor)
		results[i] = visitor.Field(f, res)
	}
	return visitor.Manifest(manifest, results), nil
}

func visitParquetManifestStruct[T any](field pqarrow.SchemaField, visitor manifestVisitor[T]) T {
	results := make([]T, len(field.Children))

	for i, f := range field.Children {
		results[i] = visitManifestField(f, visitor)
	}

	return visitor.Struct(field, results)
}

func visitManifestList[T any](field pqarrow.SchemaField, visitor manifestVisitor[T]) T {
	elemField := field.Children[0]
	res := visitManifestField(elemField, visitor)
	return visitor.List(field, res)
}

func visitManifestMap[T any](field pqarrow.SchemaField, visitor manifestVisitor[T]) T {
	kvfield := field.Children[0]
	keyField, valField := kvfield.Children[0], kvfield.Children[1]

	return visitor.Map(field, visitManifestField(keyField, visitor), visitManifestField(valField, visitor))
}

func visitManifestField[T any](field pqarrow.SchemaField, visitor manifestVisitor[T]) T {
	switch field.Field.Type.(type) {
	case *arrow.StructType:
		return visitParquetManifestStruct(field, visitor)
	case *arrow.MapType:
		return visitManifestMap(field, visitor)
	case arrow.ListLikeType:
		return visitManifestList(field, visitor)
	default:
		return visitor.Primitive(field)
	}
}

func pruneParquetColumns(manifest *pqarrow.SchemaManifest, selected map[int]struct{}, selectFullTypes bool) (*arrow.Schema, []int, error) {
	visitor := &pruneParquetSchema{
		selected:  selected,
		manifest:  manifest,
		fullTypes: selectFullTypes,
		indices:   []int{},
	}

	result, err := visitParquetManifest[arrow.Field](manifest, visitor)
	if err != nil {
		return nil, nil, err
	}

	return arrow.NewSchema(result.Type.(*arrow.StructType).Fields(), &result.Metadata),
		visitor.indices, nil
}

type pruneParquetSchema struct {
	selected  map[int]struct{}
	fullTypes bool
	manifest  *pqarrow.SchemaManifest

	indices []int
}

func (p *pruneParquetSchema) fieldID(field arrow.Field) int {
	if id := getFieldID(field); id != nil {
		return *id
	}

	panic(fmt.Errorf("%w: cannot convert %s to Iceberg field, missing field_id",
		iceberg.ErrInvalidSchema, field))
}

func (p *pruneParquetSchema) Manifest(manifest *pqarrow.SchemaManifest, fields []arrow.Field) arrow.Field {
	finalFields := slices.DeleteFunc(fields, func(f arrow.Field) bool { return f.Type == nil })
	result := arrow.Field{
		Type: arrow.StructOf(finalFields...),
	}
	if manifest.SchemaMeta != nil {
		result.Metadata = *manifest.SchemaMeta
	}

	return result
}

func (p *pruneParquetSchema) Struct(field pqarrow.SchemaField, children []arrow.Field) arrow.Field {
	selected, fields := []arrow.Field{}, field.Children
	sameType := true

	for i, t := range children {
		field := fields[i]
		if arrow.TypeEqual(field.Field.Type, t.Type) {
			selected = append(selected, *field.Field)
		} else if t.Type == nil {
			sameType = false
			// type has changed, create a new field with the projected type
			selected = append(selected, arrow.Field{
				Name:     field.Field.Name,
				Type:     field.Field.Type,
				Nullable: field.Field.Nullable,
				Metadata: field.Field.Metadata,
			})
		}
	}

	if len(selected) > 0 {
		if len(selected) == len(fields) && sameType {
			// nothing changed, return the original
			return *field.Field
		} else {
			result := *field.Field
			result.Type = arrow.StructOf(selected...)
			return result
		}
	}

	return arrow.Field{}
}

func (p *pruneParquetSchema) Field(field pqarrow.SchemaField, result arrow.Field) arrow.Field {
	_, ok := p.selected[p.fieldID(*field.Field)]
	if !ok {
		if result.Type != nil {
			return result
		}

		return arrow.Field{}
	}

	if p.fullTypes {
		return *field.Field
	}

	if _, ok := field.Field.Type.(*arrow.StructType); ok {
		result := *field.Field
		result.Type = p.projectSelectedStruct(result.Type)
		return result
	}

	if !field.IsLeaf() {
		panic(fmt.Errorf("cannot explicitly project list or map types"))
	}

	p.indices = append(p.indices, field.ColIndex)
	return *field.Field
}

func (p *pruneParquetSchema) List(field pqarrow.SchemaField, elemResult arrow.Field) arrow.Field {
	_, ok := p.selected[p.fieldID(*field.Children[0].Field)]
	if !ok {
		if elemResult.Type != nil {
			result := *field.Field
			result.Type = p.projectList(field.Field.Type.(arrow.ListLikeType), elemResult.Type)
			return result
		}

		return arrow.Field{}
	}

	if p.fullTypes {
		return *field.Field
	}

	_, ok = field.Children[0].Field.Type.(*arrow.StructType)
	if field.Children[0].Field.Type != nil && ok {
		result := *field.Field
		projected := p.projectSelectedStruct(elemResult.Type)
		result.Type = p.projectList(field.Field.Type.(arrow.ListLikeType), projected)
		return result
	}

	if !field.Children[0].IsLeaf() {
		panic(fmt.Errorf("cannot explicitly project list or map types"))
	}

	p.indices = append(p.indices, field.ColIndex)
	return *field.Field
}

func (p *pruneParquetSchema) Map(field pqarrow.SchemaField, keyResult, valResult arrow.Field) arrow.Field {
	_, ok := p.selected[p.fieldID(*field.Children[0].Children[1].Field)]
	if !ok {
		if valResult.Type != nil {
			result := *field.Field
			result.Type = p.projectMap(field.Field.Type.(*arrow.MapType), valResult.Type)
			return result
		}

		if _, ok = p.selected[p.fieldID(*field.Children[0].Children[1].Field)]; ok {
			return *field.Field
		}

		return arrow.Field{}
	}

	if p.fullTypes {
		return *field.Field
	}

	_, ok = field.Children[0].Children[1].Field.Type.(*arrow.StructType)
	if ok {
		result := *field.Field
		projected := p.projectSelectedStruct(valResult.Type)
		result.Type = p.projectMap(field.Field.Type.(*arrow.MapType), projected)
		return result
	}

	if !field.Children[0].Children[1].IsLeaf() {
		panic("cannot explicitly project list or map types")
	}

	return *field.Field
}

func (p *pruneParquetSchema) Primitive(field pqarrow.SchemaField) arrow.Field {
	return arrow.Field{}
}

func (p *pruneParquetSchema) projectSelectedStruct(projected arrow.DataType) *arrow.StructType {
	if projected == nil {
		return &arrow.StructType{}
	}

	if ty, ok := projected.(*arrow.StructType); ok {
		return ty
	}

	panic("expected a struct")
}

func (p *pruneParquetSchema) projectList(listType arrow.ListLikeType, elemResult arrow.DataType) arrow.ListLikeType {
	if arrow.TypeEqual(listType.Elem(), elemResult) {
		return listType
	}

	origField := listType.ElemField()
	origField.Type = elemResult

	switch listType.(type) {
	case *arrow.ListType:
		return arrow.ListOfField(origField)
	case *arrow.LargeListType:
		return arrow.LargeListOfField(origField)
	case *arrow.ListViewType:
		return arrow.ListViewOfField(origField)
	}

	n := listType.(*arrow.FixedSizeListType).Len()
	return arrow.FixedSizeListOfField(n, origField)
}

func (p *pruneParquetSchema) projectMap(m *arrow.MapType, valResult arrow.DataType) *arrow.MapType {
	if arrow.TypeEqual(m.ItemType(), valResult) {
		return m
	}

	return arrow.MapOf(m.KeyType(), valResult)
}
