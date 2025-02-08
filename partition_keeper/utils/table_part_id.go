package utils

import "fmt"

type TblPartID int64

const (
	InvalidTPID TblPartID = -1
)

func MakeTblPartID(table, part int32) TblPartID {
	return (TblPartID(table) << 32) + TblPartID(part)
}

func (tp TblPartID) Table() int32 {
	return int32(tp >> 32)
}

func (tp TblPartID) Part() int32 {
	return int32(tp & 0xFFFFFFFF)
}

func (tp TblPartID) String() string {
	return fmt.Sprintf("t%d.p%d", tp.Table(), tp.Part())
}
