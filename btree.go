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

func (t *BTree) delete(k Row) {
  isLeaf := len(t.children) == 0
  found := false
  childIndex := 0
  for i, key := range t.keys {
    if k.lessThan(key) {
      if isLeaf {
        // it's not in the tree => no-op
      } else {
        t.children[i].delete(k)
        childIndex = i
      }
      found = true
      break
    } else if k.equals(key) {
      if isLeaf {
        t.keys = append(t.keys[:i], t.keys[i+1:]...)
      } else {
        // move up the largest element in the left subtree
        movingKey := t.children[i].max()
        t.keys[i] = movingKey
        t.children[i].delete(movingKey)
        childIndex = i
      }
      found = true
      break
    }
  }
  // it's in the rightmost subtree
  if !found {
    childIndex = len(t.children)-1
    if isLeaf {
      // it's not in the tree => no-op
    } else {
      t.children[childIndex].delete(k)
    }
  }
  if !isLeaf {
    child := t.children[childIndex]
    // child.keys might be too small
    if len(child.keys) < MAX_NODE_SIZE/2 {
      // need to rebalance with sibling
      siblingIndex, keyIndex := childIndex+1, childIndex
      if siblingIndex >= len(t.children) {
        // rebalance with sibling to the left
        siblingIndex, keyIndex = childIndex-1, childIndex-1
      }
      if len(t.children[siblingIndex].keys) == MAX_NODE_SIZE/2 {
        // can't shuffle keys around in existing nodes, have to merge nodes
        t.keys = append(t.keys[:keyIndex], t.keys[keyIndex+1:]...)
        t.children = append(append(t.children[:keyIndex], &BTree{
          keys: append(append(t.children[keyIndex].keys, t.keys[keyIndex]), t.children[keyIndex+1].keys...),
          children: append(t.children[keyIndex].children, t.children[keyIndex+1:]...),
        }), t.children[keyIndex+1:]...)
      } else {
        // shuffle key from sibling to child.
        sibling := t.children[siblingIndex]
        if childIndex < siblingIndex {
          child.keys = append(child.keys, t.keys[keyIndex])
          t.keys[keyIndex] = sibling.keys[0]
          sibling.keys = sibling.keys[1:]
          if len(sibling.children) > 0 {
            child.children = append(child.children, sibling.children[0])
            sibling.children = sibling.children[1:]
          }
        } else {
          child.keys = append([]Row{t.keys[keyIndex]}, child.keys...)
          t.keys[keyIndex] = sibling.keys[len(sibling.keys)-1]
          sibling.keys = sibling.keys[:len(sibling.keys)-1]
          if len(sibling.children) > 0 {
            child.children = append([]*BTree{sibling.children[len(sibling.children)-1]}, child.children...)
            sibling.children = sibling.children[:len(sibling.children)-1]
          }
        }
      }
    }
  }
}

func (t *BTree) Delete(k Row) *BTree {
  t.delete(k)
  if len(t.keys) == 0 {
    return t.children[0]
  }
  return t
}

func (t *BTree) max() Row {
  if len(t.children) == 0 {
    return t.keys[len(t.keys)-1]
  }
  return t.children[len(t.children)-1].max()
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
