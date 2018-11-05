package utils

import "os"

type AresEnv string

const (
	EnvProd    AresEnv = "production"
	EnvStaging AresEnv = "staging"
	EnvDev     AresEnv = "development"
	EnvTest    AresEnv = "test"
)

func GetAresEnv() AresEnv {
	aresEnv := AresEnv(os.Getenv("ARES_ENV"))
	switch aresEnv {
	case EnvProd:
		break
	case EnvStaging:
		break
	case EnvDev:
		break
	case EnvTest:
		break
	default:
		return EnvDev
	}
	return aresEnv
}

func IsTest() bool {
	return GetAresEnv() == EnvTest
}

func IsDev() bool {
	return GetAresEnv() == EnvDev
}

func IsProd() bool {
	return GetAresEnv() == EnvProd
}

func IsStaging() bool {
	return GetAresEnv() == EnvStaging
}
