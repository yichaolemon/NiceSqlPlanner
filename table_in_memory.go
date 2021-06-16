package sql_planner
import (
  "fmt"
  "errors"
)

type ColumnType int
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
  INT ColumnType = 1
  STRING ColumnType = 2
  BOOL ColumnType = 3
)

type Column struct {
  Name string
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

type Index struct {
  // list of columns to build an index with
  schema []Column
  // B-Tree
  btree *BTree
}

func CreateTable(schema []Column, primaryIndex []string, indices ...[]string) (*Table, error) {
  if len(schema) == 0 {
    return nil, errors.New("schema can not be empty")
  }
  // TODO build primaryIndex
  primaryKey := schema[0]
  nameToType := make(map[string]ColumnType, len(schema))
  for _, col := range schema {
    nameToType[col.Name] = col.ColumnType
  }
  fullIndices := make([]*Index, 0, len(indices))
  for _, index := range indices {
    indexSchema := make([]Column, 0, len(index))
    if len(index) == 0 {
      return nil, errors.New("index can not be empty")
    }
    for _, indexCol := range index {
      if colType, ok := nameToType[indexCol]; ok {
        indexSchema = append(indexSchema, Column{Name: indexCol, ColumnType: colType})
      } else {
        return nil, errors.New("index column names must exist in schema")
      }
    }
    // append primary key to the end of each index
    indexSchema = append(indexSchema, primaryKey)
    fullIndices = append(fullIndices, &Index{schema: indexSchema, btree: new(BTree)})
  }
  return &Table{
    schema: schema,
    primaryIndex: &Index{schema: schema, btree: new(BTree)},
    indices: fullIndices,
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

//func (t Table) Delete(key IntField) error {
//}

func (i *Index) insert(row Row, tableSchema []Column) {
  indexSchema := i.schema
  // column name -> index in indexSchema
  columnSet := make(map[string]int, len(tableSchema))
  for j, col := range indexSchema {
    columnSet[col.Name] = j
  }
  // loop over the row 
  rowToInsert := make(Row, len(indexSchema))
  for j, col := range row {
    if indexIndex, exists := columnSet[tableSchema[j].Name]; exists {
      // insert it 
      rowToInsert[indexIndex] = col
    } else {
      panic("row and index schema type mismatch")
    }
  }
  i.btree = i.btree.Insert(rowToInsert)
}

func (i *Index) delete(row Row, tableSchema []Column) {

}
