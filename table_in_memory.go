import (
  "fmt"
)

type ColumnType int
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

type  struct {

}
