package transport

type NodeTransport struct {
	nodeID   string
	transport *InProcTransport
	node     *InProcNode
}

func NewNodeTransport(nodeID string, t *InProcTransport) *NodeTransport {
	node := t.RegisterNode(nodeID)
	return &NodeTransport{
		nodeID:   nodeID,
		transport: t,
		node:     node,
	}
}

func (nt *NodeTransport) SendRPC(targetID string, method string, args interface{}, resp interface{}) error {
	return nt.node.SendRPC(targetID, method, args, resp)
}

func (nt *NodeTransport) RegisterHandler(method string, handler func(interface{}) (interface{}, error)) {
	nt.node.RegisterHandler(method, handler)
}

func (nt *NodeTransport) Incoming() <-chan interface{} {
	return nt.node.Incoming()
}

