package utils

const (
	// use lower 14 bits in hash as group
	groupBits = 14
	// max 16 bits for group
	maxGroupBits = 16
)

// ComputeHLLValue compute hll value based on hash value
func ComputeHLLValue(hash uint64) uint32 {
	group := uint32(hash & ((1 << groupBits) - 1))
	var rho uint32
	for {
		h := hash & (1 << (rho + groupBits))
		if rho+groupBits < 64 && h == 0 {
			rho++
		} else {
			break
		}
	}
	return rho<<maxGroupBits | group
}
