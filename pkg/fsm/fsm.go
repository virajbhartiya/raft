package fsm

type FSM interface {
	Apply(command []byte) (result []byte)
	Snapshot() ([]byte, error)
	Restore(snapshot []byte) error
}

