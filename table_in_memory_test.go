package sql_planner

import (
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
