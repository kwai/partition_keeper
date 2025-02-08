package utils

import (
	"regexp"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
)

func IsValidName(name string) bool {
	validNameExp := `([a-zA-Z0-9\.\-_]+)`
	reg := regexp.MustCompile(validNameExp)
	locs := reg.FindStringIndex(name)
	if locs == nil {
		logging.Info("can't find match in name %s", name)
		return false
	}
	logging.Info("match %v in name: %s", locs, name)
	return locs[0] == 0 && locs[1] == len(name)
}
