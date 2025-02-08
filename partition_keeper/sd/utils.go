package sd

import "strings"

func KessLocationAz(location string) string {
	tokens := strings.Split(location, ".")
	ans := strings.ToUpper(tokens[len(tokens)-1])
	if ans == "SGP" {
		return "TXSGP1"
	} else {
		return ans
	}
}

func GetHostnameByKessId(id string) string {
	// should be format <protocol>:<hostname>:<port>
	tokens := strings.Split(id, ":")
	if len(tokens) != 3 {
		return ""
	}
	return tokens[1]
}

func Bool2String(flag bool) string {
	if flag {
		return "true"
	}
	return "false"
}

func AddKeyValue(opts map[string]string, key, value string) map[string]string {
	if opts == nil {
		return map[string]string{key: value}
	}
	opts[key] = value
	return opts
}
