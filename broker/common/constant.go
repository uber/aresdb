package common

type AggType int
const (
	Count AggType = iota
	Sum
	Max
	Min
	Avg
	Hll //todo
)