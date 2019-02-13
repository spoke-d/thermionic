package db

func NewNodeWithQuery(
	node QueryNode,
	transaction Transaction,
	builder nodeTxBuilder,
) *Node {
	return &Node{
		node:        node,
		transaction: transaction,
		builder:     builder,
	}
}
