package radix

import (
	"fmt"
	"sort"
	"strings"
)

// Value represents the value to be stored in the radix tree
type Value interface{}

// WalkFn is used when walking the tree. It takes a key and a value, returning
// if iteration should be terminated.
type WalkFn func(string, Value) bool

// leafNode is used to represent a value
type leafNode struct {
	key string
	val Value
}

func newLeafNode(key string, val Value) *leafNode {
	return &leafNode{
		key: key,
		val: val,
	}
}

// edge is used to represent an edge node
type edge struct {
	label byte
	node  *node
}

func makeEdge(prefix string, leaf *leafNode) edge {
	return edge{
		label: prefix[0],
		node: &node{
			leaf:   leaf,
			prefix: prefix,
		},
	}
}

type node struct {
	// leaf is used to store possible leaf
	leaf *leafNode

	// prefix is the common prefix we ignore
	prefix string

	// edges should be store in-order first iteration.
	edges edges
}

func (n *node) isLeaf() bool {
	return n.leaf != nil
}

func (n *node) addEdge(e edge) {
	n.edges = append(n.edges, e)
	n.edges.Sort()
}

func (n *node) replaceEdge(e edge) error {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= e.label
	})
	if idx < num && n.edges[idx].label == e.label {
		n.edges[idx].node = e.node
		return nil
	}
	return fmt.Errorf("replacing missing edge")
}

func (n *node) getEdge(label byte) *node {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= label
	})
	if idx < num && n.edges[idx].label == label {
		return n.edges[idx].node
	}
	return nil
}

func (n *node) deleteEdge(label byte) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= label
	})
	if idx < num && n.edges[idx].label == label {
		copy(n.edges[idx:], n.edges[idx+1:])
		n.edges[len(n.edges)-1] = edge{}
		n.edges = n.edges[:len(n.edges)-1]
	}
}

func (n *node) mergeChild() {
	var (
		e     = n.edges[0]
		child = e.node
	)

	n.prefix = n.prefix + child.prefix
	n.leaf = child.leaf
	n.edges = child.edges
}

type edges []edge

func (e edges) Sort() {
	sort.Sort(edgesSortable(e))
}

type edgesSortable edges

func (e edgesSortable) Len() int {
	return len(e)
}

func (e edgesSortable) Less(i, j int) bool {
	return e[i].label < e[j].label
}

func (e edgesSortable) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

// Tree implements a radix tree. This can be treated as a Dictionary abstract
// data type. The main advantage over a standard hash map is prefix-based
// lookups and ordered iteration
type Tree struct {
	root *node
	size int
}

// New returns an empty Tree with sane defaults
func New() *Tree {
	return &Tree{
		root: &node{},
	}
}

// Len is used to return the number of elements in the tree
func (t *Tree) Len() int {
	return t.size
}

// Insert is used to add or update a new entry into the Tree.
// Returns true if the Tree was updated
// Returns error if there is an error replacing an edge. If this happens, the
// tree is considered broken.
func (t *Tree) Insert(s string, v Value) (Value, bool, error) {
	var (
		parent *node

		n      = t.root
		search = s
	)

	for {
		// Handle key exhaustion / exit
		if len(search) == 0 {
			if n.isLeaf() {
				old := n.leaf.val
				n.leaf.val = v
				return old, true, nil
			}

			n.leaf = &leafNode{
				key: s,
				val: v,
			}
			t.size++
			return nil, false, nil
		}

		// Look for the edge
		parent = n
		head := search[0]
		n = n.getEdge(head)

		// No valid edge, create one
		if n == nil {
			e := makeEdge(search, newLeafNode(s, v))
			parent.addEdge(e)
			t.size++
			return nil, false, nil
		}

		// Determine the longest prefix of the search key on match
		commonPrefix := longestPrefix(search, n.prefix)
		if commonPrefix == len(n.prefix) {
			search = search[commonPrefix:]
			continue
		}

		// Split the node
		t.size++
		child := &node{
			prefix: search[:commonPrefix],
		}
		if err := parent.replaceEdge(edge{
			label: head,
			node:  child,
		}); err != nil {
			return nil, false, err
		}

		// Restore the existing node
		child.addEdge(edge{
			label: n.prefix[commonPrefix],
			node:  n,
		})
		n.prefix = n.prefix[commonPrefix:]

		// Create a new leaf node
		leaf := &leafNode{
			key: s,
			val: v,
		}

		// If the new key is a subset, add to this node as well
		search = search[commonPrefix:]
		if len(search) == 0 {
			child.leaf = leaf
			return nil, false, nil
		}

		// Create a new edge for the node
		child.addEdge(makeEdge(search, leaf))
		return nil, false, nil
	}
}

// Delete is used to delete a key, returns the previous value and if it was
// deleted
func (t *Tree) Delete(s string) (Value, bool) {
	var (
		parent *node
		label  byte

		n      = t.root
		search = s
	)
	for {
		// Handle key exhaustion / exit
		if len(search) == 0 {
			if !n.isLeaf() {
				break
			}
			goto DELETE
		}

		// Look for an edge
		parent = n
		label = search[0]
		n = n.getEdge(label)
		if n == nil {
			break
		}

		// Consume the search prefix
		if !strings.HasPrefix(search, n.prefix) {
			break
		}
		search = search[len(n.prefix):]
	}
	return nil, false

DELETE:
	// Delete the leaf
	leaf := n.leaf
	n.leaf = nil
	t.size--

	// Check if we should delete this node from the parent
	if parent != nil && len(n.edges) == 0 {
		parent.deleteEdge(label)
	}

	// Check if we should merge this node
	if n != t.root && len(n.edges) == 1 {
		n.mergeChild()
	}

	// Check if we should merge the parent's other child
	if parent != nil && parent != t.root && len(parent.edges) == 1 && !parent.isLeaf() {
		parent.mergeChild()
	}

	return leaf.val, true
}

// Get is used to lookup a specific key, returning the value and if it was
// found
func (t *Tree) Get(s string) (Value, bool) {
	var (
		n      = t.root
		search = s
	)
	for {
		// Handle key exhaustion / exit
		if len(search) == 0 {
			if n.isLeaf() {
				return n.leaf.val, true
			}
			break
		}

		// Look for an edge
		n = n.getEdge(search[0])
		if n == nil {
			break
		}

		// Consume the search prefix
		if !strings.HasPrefix(search, n.prefix) {
			break
		}

		search = search[len(n.prefix):]
	}
	return nil, false
}

// LongestPrefix is like Get, but instead of an exact match, it will return
// the longest prefix match.
func (t *Tree) LongestPrefix(s string) (string, Value, bool) {
	var (
		last   *leafNode
		n      = t.root
		search = s
	)
	for {
		// Look for a leaf node
		if n.isLeaf() {
			last = n.leaf
		}

		// Handle key exhaustion / exit
		if len(search) == 0 {
			break
		}

		// Look for an edge
		n = n.getEdge(search[0])
		if n == nil {
			break
		}

		if !strings.HasPrefix(search, n.prefix) {
			break
		}

		search = search[len(n.prefix):]
	}

	if last != nil {
		return last.key, last.val, true
	}
	return "", nil, false
}

// Walk is used to walk the tree
func (t *Tree) Walk(fn WalkFn) {
	recursiveWalk(t.root, fn)
}

// WalkPrefix is used to walk the tree under a prefix
func (t *Tree) WalkPrefix(prefix string, fn WalkFn) {
	var (
		n      = t.root
		search = prefix
	)
	for {
		// Handle key exhaustion / exit
		if len(search) == 0 {
			recursiveWalk(n, fn)
			return
		}

		// Look for an edge
		n = n.getEdge(search[0])
		if n == nil {
			break
		}

		// Consume the search prefix
		if strings.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]
			continue

		} else if strings.HasPrefix(n.prefix, search) {
			// Child may be under our search prefix
			recursiveWalk(n, fn)
			return
		}

		break
	}
}

// longestPrefox finds the length of the shared prefix of two strings
func longestPrefix(k1, k2 string) int {
	max := len(k1)
	if l := len(k2); l < max {
		max = l
	}

	var i int
	for i = 0; i < max; i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	return i
}

// recursiveWalk is used to do a pre-order walk of a node recursively.
// Returns true if the walk should be aborted
func recursiveWalk(n *node, fn WalkFn) bool {
	// Visit the leaf values if any
	if n.leaf != nil && fn(n.leaf.key, n.leaf.val) {
		return true
	}

	// Recursive walk over the children
	for _, e := range n.edges {
		if recursiveWalk(e.node, fn) {
			return true
		}
	}
	return false
}
