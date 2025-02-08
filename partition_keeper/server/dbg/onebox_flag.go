package dbg

import "flag"

var (
	flagDebugOnebox = flag.Bool("debug_onebox", false, "debug onebox")
)

func RunOnebox() bool {
	return *flagDebugOnebox
}
