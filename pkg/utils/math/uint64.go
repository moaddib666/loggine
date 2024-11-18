package math

// TODO: use Generics
func MintUnit64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func MaxUnit64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
