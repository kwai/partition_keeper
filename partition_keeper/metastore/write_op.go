package metastore

type WriteOp interface {
	OpPath() string
	Name() string
}

type CreateOp struct {
	Path string
	Data []byte
}

func (o *CreateOp) OpPath() string {
	return o.Path
}

func (o *CreateOp) Name() string {
	return "create"
}

type SetOp struct {
	Path string
	Data []byte
}

func (o *SetOp) OpPath() string {
	return o.Path
}

func (o *SetOp) Name() string {
	return "set"
}

type DeleteOp struct {
	Path string
}

func (o *DeleteOp) OpPath() string {
	return o.Path
}

func (o *DeleteOp) Name() string {
	return "delete"
}
