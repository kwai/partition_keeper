package utils

func MapAddTo(to, from map[string]int) {
	for key, value := range from {
		to[key] += value
	}
}

func Gcd(a, b int) int {
	for a%b != 0 {
		a, b = b, a%b
	}
	return b
}
