package sql_planner

import (
	"errors"
	"fmt"
)

type ColumnType int

func (c ColumnType) String() string {
	switch c {
	case INT:
		return "int"
	case STRING:
		return "string"
	case BOOL:
		return "bool"
	default:
		return "unknown"
	}
}

type Field interface {
	lessThan(a interface{}) bool
	equals(b interface{}) bool
	columnType() ColumnType
}

// int values in columns
type IntField int64

func (f IntField) lessThan(a interface{}) bool {
	return f < a.(IntField)
}
func (f IntField) equals(a interface{}) bool {
	return f == a.(IntField)
}
func (f IntField) String() string {
	return fmt.Sprintf("%d", int(f))
}
func (f IntField) columnType() ColumnType {
	return INT
}

// string values in columns
type StringField string

func (f StringField) lessThan(a interface{}) bool {
	return f < a.(StringField)
}
func (f StringField) equals(a interface{}) bool {
	return f == a.(StringField)
}
func (f StringField) columnType() ColumnType {
	return STRING
}

// boolean values in columns
type BoolField bool

func (f BoolField) lessThan(a interface{}) bool {
	return !bool(f) && bool(a.(BoolField))
}
func (f BoolField) equals(a interface{}) bool {
	return f == a.(BoolField)
}
func (f BoolField) columnType() ColumnType {
	return BOOL
}

const (
	INT    ColumnType = 1
	STRING ColumnType = 2
	BOOL   ColumnType = 3
)

const DefaultBatchSize = 5

type Column struct {
	Name       string
	ColumnType ColumnType
}

type Table struct {
	// schema is an ordered list of (column name, type)
	schema []Column
	// map from primary key to data
	primaryIndex *Index
	// ordered list of indices, first one being the primary key, required
	indices []*Index
}

func (t Table) String() string {
	return fmt.Sprintf("Schema: %v\nPrimary index:%s\nIndices:%v", t.schema, t.primaryIndex, t.indices)
}

type Index struct {
	// list of columns to build an index with
	schema []Column
	// B-Tree
	btree *BTree
}

func (i *Index) String() string {
	return fmt.Sprintf("{schema: %v, data:\n%s\n}", i.schema, i.btree)
}

// append s to list only if s not already in list
func appendUnique(list []string, s string) []string {
	for _, sInList := range list {
		if sInList == s {
			return list
		}
	}
	return append(list, s)
}

func namesToSchema(
	names []string,
	nameToType map[string]ColumnType,
) ([]Column, error) {
	schema := make([]Column, 0, len(names))
	if len(names) == 0 {
		return nil, errors.New("list of column names can not be empty")
	}
	for _, colName := range names {
		if colType, ok := nameToType[colName]; ok {
			schema = append(schema, Column{Name: colName, ColumnType: colType})
		} else {
			return nil, errors.New("index column names must exist in schema")
		}
	}
	return schema, nil
}

func CreateTable(schema []Column, primaryIndex []string, indices ...[]string) (*Table, error) {
	if len(schema) == 0 {
		return nil, errors.New("schema can not be empty")
	}
	nameToType := make(map[string]ColumnType, len(schema))
	for _, col := range schema {
		nameToType[col.Name] = col.ColumnType
	}
	fullIndices := make([]*Index, 0, len(indices))
	for _, index := range indices {
		for _, primaryIndexName := range primaryIndex {
			index = appendUnique(index, primaryIndexName)
		}
		indexSchema, err := namesToSchema(index, nameToType)
		if err != nil {
			return nil, err
		}
		fullIndices = append(fullIndices, &Index{
			schema: indexSchema,
			btree:  new(BTree),
		})
	}
	// add all fields in the schema to primary index
	for _, col := range schema {
		primaryIndex = appendUnique(primaryIndex, col.Name)
	}
	primaryIndexSchema, err := namesToSchema(primaryIndex, nameToType)
	if err != nil {
		return nil, err
	}
	return &Table{
		schema:       schema,
		primaryIndex: &Index{schema: primaryIndexSchema, btree: new(BTree)},
		indices:      fullIndices,
	}, nil
}

func rowMatchSchema(row Row, schema []Column) error {
	if len(schema) != len(row) {
		return errors.New("row and table schema length mismatch")
	}
	for i, col := range row {
		if col.columnType() != schema[i].ColumnType {
			return errors.New("row and table schema type mismatch")
		}
	}
	return nil
}

func (r Row) copy() Row {
	newRow := make(Row, len(r))
	copy(newRow, r)
	return newRow
}

func (t Table) Insert(row Row) error {
	// validate input row against table schema
	if err := rowMatchSchema(row, t.schema); err != nil {
		return err
	}
	t.primaryIndex.insert(row, t.schema)
	// insert into indices
	for _, index := range t.indices {
		index.insert(row, t.schema)
	}
	return nil
}

func (t Table) BatchInsert(rows []Row) error {
	for _, row := range rows {
		err := t.Insert(row)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t Table) Delete(index *Index, prefix Row) error {
	output := make(chan []Row)
	var err error
	go func() {
		defer close(output)
		err = t.TraverseWithIndexPaginated(index, QueryPredicate{
			UpperBound: ExclusiveBound(prefix),
			LowerBound: InclusiveBound(prefix),
			Limit:      NoLimit,
		}, DefaultBatchSize, output)
	}()

	for rowBatch := range output {
		for _, row := range rowBatch {
			t.primaryIndex.btree = t.primaryIndex.btree.Delete(
				reorderRowBySchema(row, t.schema, t.primaryIndex.schema),
			)
			for _, i := range t.indices {
				i.btree = i.btree.Delete(
					reorderRowBySchema(row, t.schema, i.schema),
				)
			}
		}
	}
	return err
}

func (t Table) Update(index *Index, pred QueryPredicate, vals map[Column]Field) error {
	output := make(chan []Row)
	var err error
	go func() {
		defer close(output)
		err = t.TraverseWithIndexPaginated(index, pred, DefaultBatchSize, output)
	}()

	for rowBatch := range output {
		for _, row := range rowBatch {
			// delete
			t.primaryIndex.btree = t.primaryIndex.btree.Delete(
				reorderRowBySchema(row, t.schema, t.primaryIndex.schema),
			)
			for _, i := range t.indices {
				i.btree = i.btree.Delete(
					reorderRowBySchema(row, t.schema, i.schema),
				)
			}
			// update
			var newRow Row
			for i, field := range row {
				if col, exists := vals[t.schema[i]]; exists {
					newRow = append(newRow, col)
				} else {
					newRow = append(newRow, field)
				}
			}
			t.primaryIndex.btree = t.primaryIndex.btree.Insert(
				reorderRowBySchema(newRow, t.schema, t.primaryIndex.schema),
			)
			for _, i := range t.indices {
				i.btree = i.btree.Insert(
					reorderRowBySchema(newRow, t.schema, i.schema),
				)
			}
		}
	}
	return err
}

func (t Table) TraverseWithIndexPaginated(index *Index, pred QueryPredicate, batchSize int, output chan<- []Row) error {
	indexOutput := make(chan []Row)
	var err error
	go func() {
		defer close(indexOutput)
		err = index.btree.TraversePaginated(pred, batchSize, indexOutput)
	}()

	for rowBatch := range indexOutput {
		rowFromTableList := make([]Row, 0, batchSize)
		for _, rowFromIndex := range rowBatch {
			rowFromTable := rowFromIndex
			if index != t.primaryIndex {
				primaryIndexPrefix := reorderRowBySchema(rowFromIndex, index.schema, t.primaryIndex.schema)
				rowFromTable = t.searchPrimaryIndex(primaryIndexPrefix)
			}
			rowFromTable = reorderRowBySchema(rowFromTable, t.primaryIndex.schema, t.schema)
			rowFromTableList = append(rowFromTableList, rowFromTable)
		}

		// output each batch to the channel
		output <- rowFromTableList
	}
	return err
}

// input prefix row is in the order of the index. output rows are from the main table.
func (t Table) TraverseWithIndex(index *Index, prefix Row, output chan<- Row) {
	indexOutput := make(chan Row)
	go func() {
		defer close(indexOutput)
		index.traversePrefix(prefix, indexOutput)
	}()

	for rowFromIndex := range indexOutput {
		rowFromTable := rowFromIndex
		if index != t.primaryIndex {
			primaryIndexPrefix := reorderRowBySchema(rowFromIndex, index.schema, t.primaryIndex.schema)
			rowFromTable = t.searchPrimaryIndex(primaryIndexPrefix)
		}
		rowFromTable = reorderRowBySchema(rowFromTable, t.primaryIndex.schema, t.schema)

		output <- rowFromTable
	}
}

func (t Table) ListWithIndex(index *Index, prefix Row) []Row {
	allRows := make(chan Row)
	go func() {
		defer close(allRows)
		t.TraverseWithIndex(index, prefix, allRows)
	}()
	rowList := make([]Row, 0)
	for row := range allRows {
		rowList = append(rowList, row)
	}
	return rowList
}

// prefix must contain all fields in the primary index
func (t Table) searchPrimaryIndex(prefix Row) Row {
	// TODO: enforce primary index is unique at write time. otherwise this will deadlock.
	primaryIndexOutput := make(chan Row, 1)
	t.primaryIndex.traversePrefix(prefix, primaryIndexOutput)
	return <-primaryIndexOutput
}

// general function for reordering a row from one schema to another, possibly yielding only a prefix
func reorderRowBySchema(row Row, rowSchema []Column, newSchema []Column) Row {
	columnSet := make(map[Column]int, len(rowSchema))
	// column -> index in indexSchema
	for j, col := range rowSchema {
		columnSet[col] = j
	}

	newRow := make(Row, 0, len(newSchema))
	for _, col := range newSchema {
		if i, exists := columnSet[col]; exists {
			// insert it
			newRow = append(newRow, row[i])
		} else {
			break
		}
	}

	return newRow
}

// inserting row into index, where the row is in the order of the table schema
func (i *Index) insert(row Row, tableSchema []Column) {
	rowToInsert := reorderRowBySchema(row, tableSchema, i.schema)
	if len(rowToInsert) < len(i.schema) {
		panic("row and index schema type mismatch")
	}

	i.btree = i.btree.Insert(rowToInsert)
}

// input prefix is in the order of the index's schema
func (i *Index) traversePrefix(prefix Row, output chan<- Row) {
	i.btree.TraversePrefix(prefix, output)
}
