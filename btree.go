package sql_planner
import (
  "fmt"
)

type Row []Field
const MAX_NODE_SIZE = 6

type BTree struct {
  // keys end with the primary key field
  keys []Row
  children []*BTree
}

func (t *BTree) String() string {
  s := "{"
  for i, key := range t.keys {
    if t.IsLeaf() {
      s += fmt.Sprintf("%v", key)
      if i < len(t.keys)-1 {
        s += " "
      }
    } else {
      s += fmt.Sprintf("%s %v ", t.children[i], key)
    }
  }
  if !t.IsLeaf() {
    s += fmt.Sprintf("%s", t.children[len(t.children)-1])
  }
  return s+"}"
}

func (t *BTree) IsLeaf() bool {
  return len(t.children) == 0
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
        suffix := copyKeys(t.keys[i+1:])
        t.keys = append(t.keys[:i], suffix...)
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
      sibling := t.children[siblingIndex]
      if len(sibling.keys) == MAX_NODE_SIZE/2 {
        // can't shuffle keys around in existing nodes, have to merge nodes
        mergedChild := &BTree{
          keys: append(append(t.children[keyIndex].keys, t.keys[keyIndex]), t.children[keyIndex+1].keys...),
          children: append(t.children[keyIndex].children, t.children[keyIndex+1].children...),
        }
        t.keys = append(t.keys[:keyIndex], t.keys[keyIndex+1:]...)
        t.children = append(append(t.children[:keyIndex], mergedChild), t.children[keyIndex+2:]...)
      } else {
        // shuffle key from sibling to child.
        if childIndex < siblingIndex {
          child.keys = append(child.keys, t.keys[keyIndex])
          t.keys[keyIndex] = sibling.keys[0]
          sibling.keys = sibling.keys[1:]
          if !sibling.IsLeaf() {
            child.children = append(child.children, sibling.children[0])
            sibling.children = sibling.children[1:]
          }
        } else {
          child.keys = append([]Row{t.keys[keyIndex]}, child.keys...)
          t.keys[keyIndex] = sibling.keys[len(sibling.keys)-1]
          sibling.keys = sibling.keys[:len(sibling.keys)-1]
          if !sibling.IsLeaf() {
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
  if !t.IsLeaf() && len(t.keys) == 0 {
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

func (t *BTree) min() Row {
  if len(t.children) == 0 {
    return t.keys[0]
  }
  return t.children[0].min()
}

func (t *BTree) height() int {
  if len(t.children) == 0 {
    return 1
  }
  return 1 + t.children[0].height()
}

func (t *BTree) AssertWellFormed() {
  t.assertWellFormed(true)
}

func (t *BTree) assertWellFormed(isRoot bool) {
  if len(t.keys) > MAX_NODE_SIZE {
    panic(fmt.Sprintf("too many keys in node %s", t))
  }
  if !isRoot {
    if len(t.keys) < MAX_NODE_SIZE/2 {
      panic(fmt.Sprintf("too few keys in node %s", t))
    }
  }
  height := t.height()
  if len(t.children) > 0 {
    if len(t.children) != len(t.keys)+1 {
      panic(fmt.Sprintf("wrong number of children in node %s", t))
    }
    for i, k := range t.keys {
      t.children[i].assertWellFormed(false)
      if t.children[i].height() + 1 != height {
        panic(fmt.Sprintf("tree height uneven at index %d in node %s", i, t))
      }
      if !t.children[i].max().lessThan(k) {
        panic(fmt.Sprintf("tree out of order at index %d in node %s", i, t))
      }
      if !k.lessThan(t.children[i+1].min()) {
        panic(fmt.Sprintf("tree out of order (type 2) at index %d in node %s", i, t))
      }
    }
  } else {
    for i := 0; i < len(t.keys)-1; i++ {
      if !t.keys[i].lessThan(t.keys[i+1]) {
        panic(fmt.Sprintf("tree out of order at index %d in leaf %s", i, t))
      }
    }
  }
}

func (t *BTree) TraverseAll(output chan<- Row) {
  t.TraverseBounded(NegativeInfinity{}, Infinity{}, output)
}

// traverse every row with the given prefix, in order
func (t *BTree) TraversePrefix(prefix Row, output chan<- Row) {
  t.TraverseBounded(PrefixBoundLower{prefix: prefix}, PrefixBoundUpper{prefix: prefix}, output)
}

// Can be compared to Rows
type RowBound interface {
  // Row is greater than (to the right of) bound
  rowGreaterThan(Row) bool
  // A bound cannot be "equal to" a row, so rowLessThan = !rowGreaterThan
}

// infemum for all rows that have a given prefix
type PrefixBoundLower struct {
  prefix Row
}
func (p PrefixBoundLower) rowGreaterThan(r Row) bool {
  for i, field := range p.prefix {
    if r[i].lessThan(field) {
      return false
    } else if !r[i].equals(field) {
      return true
    }
  }
  return true
}
// supremum for all rows that have a given prefix
type PrefixBoundUpper struct {
  prefix Row
}
func (p PrefixBoundUpper) rowGreaterThan(r Row) bool {
  for i, field := range p.prefix {
    if r[i].lessThan(field) {
      return false
    } else if !r[i].equals(field) {
      return true
    }
  }
  return false
}
type Infinity struct {}
func (i Infinity) rowGreaterThan(r Row) bool { return false }
type NegativeInfinity struct {}
func (i NegativeInfinity) rowGreaterThan(r Row) bool { return true }

// Returns everything to output between lower and upper
func (t *BTree) TraverseBounded(lower RowBound, upper RowBound, output chan<- Row) {
  for i, k := range t.keys {
    // look to the left of k if k > lower.
    if !t.IsLeaf() && lower.rowGreaterThan(k) {
      t.children[i].TraverseBounded(lower, upper, output)
    }
    // if k > upper, we're done.
    if upper.rowGreaterThan(k) {
      fmt.Println("greater than ", i, upper, k)
      return
    }
    // k is in range if k > lower.
    if lower.rowGreaterThan(k) {
      output <- k
    }
  }
  if !t.IsLeaf() {
    t.children[len(t.children)-1].TraverseBounded(lower, upper, output)
  }
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

func copyKeys(keys []Row) []Row {
  c := make([]Row, len(keys))
  copy(c, keys)
  return c
}

// shallow copy
func copyNodes(nodes []*BTree) []*BTree {
  c := make([]*BTree, len(nodes))
  copy(c, nodes)
  return c
}


// helper function to Insert
func (t *BTree) insert(k Row) (*BTree, *BTree, Row) {
  isLeaf := t.IsLeaf()
  found := false
  for i, key := range t.keys {
    if k.lessThan(key) {
      if isLeaf {
        suffix := copyKeys(t.keys[i:])
        t.keys = append(append(t.keys[:i], k), suffix...)
      } else {
        lTree, rTree, newK := t.children[i].insert(k)
        if rTree == nil {
          t.children[i] = lTree
        } else {
          // split happened
          suffix := copyKeys(t.keys[i:])
          t.keys = append(append(t.keys[:i], newK), suffix...)
          childrenSuffix := copyNodes(t.children[i+1:])
          t.children = append(append(t.children[:i], lTree, rTree), childrenSuffix...)
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
        t.children = append(t.children[:i], lTree, rTree)
      }
    }
  }

  if len(t.keys) > MAX_NODE_SIZE {
    // need to split
    lTree := BTree {
      keys: copyKeys(t.keys[:MAX_NODE_SIZE / 2]),
    }
    rTree := BTree {
      keys: copyKeys(t.keys[MAX_NODE_SIZE / 2+1:]),
    }
    if !t.IsLeaf() {
      lTree.children = copyNodes(t.children[:MAX_NODE_SIZE / 2 + 1])
      rTree.children = copyNodes(t.children[MAX_NODE_SIZE / 2 + 1:])
    }
    return &lTree, &rTree, t.keys[MAX_NODE_SIZE / 2]
  }

  return t, nil, nil
}

