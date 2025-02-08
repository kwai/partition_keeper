package rpc

type ConnPoolBuilder interface {
	Build() *ConnPool
}
