package sql_planner
import (
)

type ColumnType int
type Field interface {
  lessThan(a interface{}) bool
  equals(b interface{}) bool
}

// int values in columns
type IntField int
func (f IntField) lessThan(a interface{}) bool {
  return f < a.(IntField)
}
func (f IntField) equals(a interface{}) bool {
  return f == a.(IntField)
}


// string values in columns
type StringField string
func (f StringField) lessThan(a interface{}) bool {
  return f < a.(StringField)
}
func (f StringField) equals(a interface{}) bool {
  return f == a.(StringField)
}

// boolean values in columns
type BoolField bool
func (f BoolField) lessThan(a interface{}) bool {
  return !bool(f) && bool(a.(BoolField))
}
func (f BoolField) equals(a interface{}) bool {
  return f == a.(BoolField)
}

const (
  INT ColumnType = 1
  STRING ColumnType = 2
  BOOL ColumnType = 3
)

type Column struct {
  name string
  columnType ColumnType
}

type Table struct {
  // schema is an ordered list of (column name, type)
  schema []Column
  // map from primary key to data
  dataMap map[int64][]byte
  // ordered list of indices, first one being the primary key, required
  indices []Index
}

type Index struct {
  // list of columns to build an index with
  schema []Column
  // B-Tree
  btree BTree
}
