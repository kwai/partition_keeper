package utils

func ContainsKey(m map[string]string, key string) bool {
	if len(m) == 0 {
		return false
	}
	_, ok := m[key]
	return ok
}
