package lru

// Field holds the field alias to the underlying type
type Field = int

// Value holds the value alias to the underlying type
type Value = string

// FieldValue is a tuple to both Field and Value type
type FieldValue struct {
	Field Field
	Value Value
}

type element struct {
	key   Field
	value Value

	next, prev *element
	list       *list
}

func (e *element) Prev() *element {
	if n := e.prev; e.list != nil && n != &e.list.root {
		return n
	}
	return nil
}

type list struct {
	root element
	size int
}

// Unshift puts an item at the front of the list
func (l *list) Unshift(e *element) {
	if l.root.next == nil {
		l.Reset()
	}
	l.insert(e, &l.root)
}

// Mark an element in the list as used, which has the side-effect of
// moving the element to the front of the list
func (l *list) Mark(e *element) {
	if e.list != l || l.root.next == e {
		return
	}
	l.insert(l.remove(e), &l.root)
}

// Remove removes an element from the list
func (l *list) Remove(e *element) bool {
	if e.list == l {
		l.remove(e)
		return true
	}
	return false
}

// Walk iterates over the list with a key, value
func (l *list) Walk(fn func(Field, Value) error) error {
	for elem := l.Back(); elem != nil; elem = elem.Prev() {
		if err := fn(elem.key, elem.value); err != nil {
			return err
		}
	}
	return nil
}

// Reset drops the list and starts it from a clean slate
func (l *list) Reset() {
	l.root.next = &l.root
	l.root.prev = &l.root
	l.size = 0
}

// Len returns the size of the list
func (l *list) Len() int {
	return l.size
}

// Back returns the last element of the list
func (l *list) Back() *element {
	if l.size == 0 {
		return nil
	}
	return l.root.prev
}

// Dequeue walks over one item at a time, removing each one from the list
func (l *list) Dequeue(fn func(elem *element) error) error {
	for elem := l.Back(); elem != nil; elem = elem.Prev() {
		if err := fn(elem); err != nil {
			return err
		}
	}
	return nil
}

func (l *list) insert(e, ptr *element) *element {
	next := ptr.next

	ptr.next = e

	e.prev = ptr
	e.next = next

	next.prev = e

	e.list = l

	l.size++

	return e
}

func (l *list) remove(e *element) *element {
	e.prev.next = e.next
	e.next.prev = e.prev

	e.next = nil
	e.prev = nil
	e.list = nil

	l.size--
	return e
}
