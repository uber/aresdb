package utils

import (
	"github.com/spf13/viper"
)

// BindEnvironments binds environment variables to viper
func BindEnvironments(v *viper.Viper) {
	v.SetEnvPrefix("ares")
	v.BindEnv("env")
}
