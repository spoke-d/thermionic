package cluster

import "github.com/pkg/errors"

// ErrSomeNodesAreBehind states if some nodes are behind and can be used to
// identify when this happens.
var ErrSomeNodesAreBehind = errors.New("some nodes are behind this node's version")
