package transport

type Transport interface {
	SendRPC(targetID string, method string, args interface{}, resp interface{}) error
	RegisterHandler(method string, handler func(interface{}) (interface{}, error))
	Incoming() <-chan interface{}
}

