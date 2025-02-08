package utils

import (
	"strings"
)

func KrpAzToStandard(krpAz string) string {
	if strings.HasPrefix(krpAz, "_") {
		return strings.ToUpper(krpAz[1:])
	}
	return strings.ToUpper(krpAz)
}
