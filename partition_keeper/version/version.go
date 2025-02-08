package version

import (
	"flag"
	"fmt"
	"os"
)

var (
	// please refer to Makefile to see how GitCommitId is assigned
	GitCommitId string
	flagVersion = flag.Bool("version", false, "print version")
)

func MayPrintVersionAndExit() {
	if *flagVersion {
		fmt.Printf("git commit id: %s\n", GitCommitId)
		os.Exit(0)
	}
}
