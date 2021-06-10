package sql_planner

import (
  "fmt"
  "testing"
)

func intKey(i int) Row {
  return Row{IntField(i)}
}

func allKeys(t *BTree) []Row {
  output := make(chan Row)
  go func() {
    defer close(output)
    t.Traverse(output)
  }()
  var all []Row
  for r := range output {
    all = append(all, r)
  }
  return all
}

func assertRowsEqual(t *testing.T, rows1 []Row, rows2 []Row) {
  if len(rows1) != len(rows2) {
    t.Error(len(rows1), len(rows2))
  }
  for i := range rows1 {
    if !rows1[i].equals(rows2[i]) {
      t.Error(i, rows1[i], rows2[i])
    }
  }
}

func TestInsert(t *testing.T) {
  tree := &BTree{}
  tree.AssertWellFormed()
  for i := 0; i < 10; i++ {
    tree = tree.Insert(intKey(i*2))
    tree.AssertWellFormed()
  }
  for i := 0; i < 10; i++ {
    tree = tree.Insert(intKey(i*2+1))
    tree.AssertWellFormed()
  }
  var rows []Row
  for i := 0; i < 20; i++ {
    rows = append(rows, intKey(i))
  }
  assertRowsEqual(t, allKeys(tree), rows)
}

func TestDelete(t *testing.T) {
  tree := &BTree{}
  tree.AssertWellFormed()
  for i := 0; i < 20; i++ {
    tree = tree.Insert(intKey(i))
    tree.AssertWellFormed()
    fmt.Println(tree)
  }
  for i := 0; i < 10; i++ {
    tree = tree.Delete(intKey(i))
    tree.AssertWellFormed()
    fmt.Println(tree)
  }
  var rows []Row
  for i := 10; i < 20; i++ {
    rows = append(rows, intKey(i))
  }
  assertRowsEqual(t, allKeys(tree), rows)
  for i := 10; i < 20; i++ {
    tree = tree.Delete(intKey(i))
    tree.AssertWellFormed()
    fmt.Println(tree)
  }
  assertRowsEqual(t, allKeys(tree), []Row{})
}
