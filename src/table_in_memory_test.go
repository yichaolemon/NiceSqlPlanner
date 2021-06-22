package sql_planner

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateTable(t *testing.T) {
  table := createTable(t)

  require.Equal(t, table.schema, []Column{
    {Name: "email", ColumnType: STRING},
    {Name: "age", ColumnType: INT},
    {Name: "id", ColumnType: INT},
    {Name: "isActive", ColumnType: BOOL},
  })
  require.Equal(t, table.primaryIndex.schema, []Column{
    {Name: "id", ColumnType: INT},
    {Name: "isActive", ColumnType: BOOL},
    {Name: "email", ColumnType: STRING},
    {Name: "age", ColumnType: INT},
  })
  require.Len(t, table.indices, 1)
  require.Equal(t, table.indices[0].schema, []Column{
    {Name: "email", ColumnType: STRING},
    {Name: "id", ColumnType: INT},
    {Name: "isActive", ColumnType: BOOL},
  })
}

func TestInsertTable(t *testing.T) {
  table := createTable(t)
  rows := insertManyToTable(t, table, 4)
  fmt.Println(table)

  // search
  require.Equal(t,
    table.ListWithIndex(table.indices[0], Row{StringField("doodle@sheen.com")}),
    []Row{rows[0], rows[3]},
  )
  require.Equal(t,
    table.ListWithIndex(table.primaryIndex, Row{IntField(2)}),
    []Row{rows[2], rows[1]},
  )
}

func TestDeleteTable(t *testing.T) {
  table := createTable(t)

  // insert a lot of pusheens
  insertManyToTable(t, table, 100)

  fmt.Println(table)

  // delete by email
  require.NoError(t, table.Delete(table.indices[0], Row{StringField("toto@sheen.com")}))
  require.Len(t, table.ListWithIndex(table.indices[0], Row{StringField("toto@sheen.com")}), 0)
  require.Len(t, table.ListWithIndex(table.indices[0], Row{StringField("doodle@sheen.com")}), 50)

  // delete by primary key
  table.Delete(table.primaryIndex, Row{IntField(1)})
  require.Len(t, table.ListWithIndex(table.indices[0], Row{StringField("doodle@sheen.com")}), 49)
}

func insertManyToTable(t *testing.T, table *Table, count int) []Row {
  var err error
  rowTemplates := []Row{
    {StringField("doodle@sheen.com"), IntField(3), IntField(1), BoolField(true)},
    {StringField("toto@sheen.com"), IntField(21), IntField(2), BoolField(true)},
    {StringField("toto@sheen.com"), IntField(1), IntField(2), BoolField(false)},
    {StringField("doodle@sheen.com"), IntField(1), IntField(8), BoolField(true)},
  }
  rows := make([]Row, count)
  for i := range rows {
    copyTemplate := make(Row, 4)
    copy(copyTemplate, rowTemplates[i % len(rowTemplates)])
    copyTemplate[2] = copyTemplate[2].(IntField) + IntField((i / len(rowTemplates)) * 10)
    rows[i] = copyTemplate
  }
  err = table.BatchInsert(rows)
  require.NoError(t, err)
  return rows
}

// helper functions 
func createTable(t *testing.T) *Table {
  table, err := CreateTable(
    []Column{
      {Name: "email", ColumnType: STRING},
      {Name: "age", ColumnType: INT},
      {Name: "id", ColumnType: INT},
      {Name: "isActive", ColumnType: BOOL},
    },
    []string{"id", "isActive"},
    []string{"email"},
  )
  require.NoError(t, err)
  return table
}

func TestUpdate(t *testing.T) {
  table := createTable(t)
  insertManyToTable(t, table, 4)
  require.NoError(t, table.Update(table.primaryIndex, QueryPredicate{
    LowerBound: InclusiveBound(Row{IntField(1)}),
    UpperBound: ExclusiveBound(Row{IntField(1)}),
    Limit: Limit(1),
  }, map[Column]Field{{Name: "age", ColumnType: INT}: IntField(4)}))
  require.Equal(t, table.ListWithIndex(table.primaryIndex, Row{IntField(1)}),
    []Row{{StringField("doodle@sheen.com"), IntField(4), IntField(1), BoolField(true)}})
}

func TestMultipleConcurrentOperations(t *testing.T) {
  table := createTable(t)
  insertManyToTable(t, table, 4)
  // long-running update
  insertChan := make(chan struct{})
  insertInjection = func() chan struct{} {
    return insertChan
  }
  updateDone := make(chan struct{})
  go func() {
    require.NoError(t, table.Update(table.primaryIndex, QueryPredicate{
      LowerBound: InclusiveBound(Row{IntField(1)}),
      UpperBound: ExclusiveBound(Row{IntField(1)}),
      Limit: Limit(1),
    }, map[Column]Field{{Name: "age", ColumnType: INT}: IntField(4)}))
    require.Equal(t, table.ListWithIndex(table.primaryIndex, Row{IntField(1)}),
      []Row{{StringField("doodle@sheen.com"), IntField(4), IntField(1), BoolField(true)}})
    updateDone <- struct{}{}
  }()
  insertChan <- struct{}{}
  // now the btree.go code is stuck
  insertInjection = nil
  
  // Try a point-read.
  rows := table.ListWithIndex(table.primaryIndex, Row{IntField(8)})
  require.Equal(t, rows, []Row{
    {StringField("doodle@sheen.com"), IntField(1), IntField(8), BoolField(true)},
  })

  
  // allow the original update to finish
  insertChan <- struct{}{}
  <-updateDone
}
