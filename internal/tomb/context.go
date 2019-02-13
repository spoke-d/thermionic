package tomb

import "context"

// WithContext returns a new tomb that is killed when the provided parent
// context is canceled, and a copy of parent with a replaced Done channel
// that is closed when either the tomb is dying or the parent is canceled.
// The returned context may also be obtained via the tomb's Context method.
func WithContext(parent context.Context) (*Tomb, context.Context) {
	t := New()
	if parent.Done() != nil {
		go func() {
			select {
			case <-t.Dying():
			case <-parent.Done():
				t.Kill(parent.Err())
			}
		}()
	}
	t.parent = parent
	child, cancel := context.WithCancel(parent)
	t.addChild(parent, child, cancel)
	return t, child
}
