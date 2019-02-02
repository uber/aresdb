package utils

// ComputeHLLValue compute hll value based on hash value
func ComputeHLLValue(hash uint64) uint32 {
	p := uint32(14) // max 16
	group := uint32(hash & ((1 << p) - 1))
	var rho uint32
	for {
		h := hash & (1 << (rho + p))
		if rho+p < 64 && h == 0 {
			rho++
		} else {
			break
		}
	}
	return rho<<16 | group
}
