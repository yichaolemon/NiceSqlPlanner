package sql_planner
import (
)

type Row []Field
const MAX_NODE_SIZE = 100

type BTree struct {
  // keys end with the primary key field
  keys []Row
  children []*BTree
}

func (r Row) lessThan(a Row) bool {
  for i, f := range r {
    if (f.lessThan(a[i])) {
      return true
    } else if (!f.equals(a[i])) {
      return false
    }
  }
  return false
}

func (r Row) equals(a Row) bool {
  for i, f := range r {
    if (!f.equals(a[i])) {
      return false
    }
  }
  return true
}

func (t *BTree) Insert(k Row) (*BTree) {
  lTree, rTree, r := t.insert(k)

  // root has split, need to create a new root
  if rTree != nil {
    //
    return &BTree{keys: []Row{r}, children: []*BTree{lTree, rTree}}
  }
  return t
}

// helper function to Insert
func (t *BTree) insert(k Row) (*BTree, *BTree, Row) {
  isLeaf := len(t.children) == 0
  found := false
  for i, key := range t.keys {
    if k.lessThan(key) {
      if isLeaf {
        t.keys = append(append(t.keys[:i], k), t.keys[i:]...)
      } else {
        lTree, rTree, newK := t.children[i].insert(k)
        if rTree == nil {
          t.children[i] = lTree
        } else {
          // split happened
          t.keys = append(append(t.keys[:i], newK), t.keys[i:]...)
          t.children = append(append(t.children[:i], lTree, rTree), t.children[i+1:]...)
        }
      }
      found = true
      break
    } else if k.equals(key) {
      found = true
      break
    }
  }

  // rightmost child 
  if !found {
    if isLeaf {
      t.keys = append(t.keys, k)
    } else {
      i := len(t.children)-1
      lTree, rTree, newK := t.children[i].insert(k)
      if rTree == nil {
        t.children[i] = lTree
      } else {
        // split happened
        t.keys = append(t.keys, newK)
        t.children = append(t.children[:i-1], lTree, rTree)
      }
    }
  }

  if len(t.keys) > MAX_NODE_SIZE {
    // need to split
    lTree := BTree {
      keys: t.keys[:MAX_NODE_SIZE / 2],
      children: t.children[:MAX_NODE_SIZE / 2 + 1],
    }
    rTree := BTree {
      keys: t.keys[MAX_NODE_SIZE / 2+1:],
      children: t.children[MAX_NODE_SIZE / 2 + 1:],
    }
    return &lTree, &rTree, t.keys[MAX_NODE_SIZE / 2]
  }

  return t, nil, nil
}
