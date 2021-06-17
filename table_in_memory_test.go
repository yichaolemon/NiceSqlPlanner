package sql_planner

import (
  "fmt"
  "testing"
  "github.com/stretchr/testify/require"
)

func TestCreateTable(t *testing.T) {
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

  doodlesheen1 := Row{
    StringField("doodle@sheen.com"), IntField(3), IntField(1), BoolField(true),
  }
  err = table.Insert(doodlesheen1)
  require.NoError(t, err)

  totosheen1 := Row{
    StringField("toto@sheen.com"), IntField(21), IntField(2), BoolField(true),
  }
  table.Insert(totosheen1)
  require.NoError(t, err)

  totosheen2 := Row{
    StringField("toto@sheen.com"), IntField(1), IntField(2), BoolField(false),
  }
  table.Insert(totosheen2)
  require.NoError(t, err)

  doodlesheen2 := Row{
    StringField("doodle@sheen.com"), IntField(1), IntField(8), BoolField(true),
  }
  table.Insert(doodlesheen2)
  require.NoError(t, err)
  fmt.Println(table)

  // search
  require.Equal(t,
    table.ListWithIndex(table.indices[0], Row{StringField("doodle@sheen.com")}),
    []Row{doodlesheen1, doodlesheen2},
  )
  require.Equal(t,
    table.ListWithIndex(table.primaryIndex, Row{IntField(2)}),
    []Row{totosheen2, totosheen1},
  )
}


