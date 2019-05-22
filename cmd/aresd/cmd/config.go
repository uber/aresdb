package cmd

import (
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/utils"
	"os"
)

// AddFlags addsx flags to command
func AddFlags(cmd *cobra.Command) {
	cmd.Flags().String("config", "config/ares.yaml", "Ares config file")
	cmd.Flags().IntP("port", "p", 0, "Ares service port")
	cmd.Flags().IntP("debug_port", "d", 0, "Ares service debug port")
	cmd.Flags().StringP("root_path", "r", "ares-root", "Root path of the data directory")
	cmd.Flags().Bool("scheduler_off", false, "Start server with scheduler off, no archiving and backfill")
}

// ReadConfig populate AresServerConfig
func ReadConfig(defaultCfg map[string]interface{}, flags *pflag.FlagSet) (common.AresServerConfig, error) {
	v := viper.New()
	v.SetConfigType("yaml")
	// bind command flags
	v.BindPFlags(flags)

	utils.BindEnvironments(v)

	// set defaults
	v.SetDefault("root_path", "ares-root")
	hostname, err := os.Hostname()
	if err != nil {
		panic(utils.StackError(err, "cannot get host name"))
	}
	v.SetDefault("cluster", map[string]interface{}{
		"instance_name": hostname,
	})
	v.MergeConfigMap(defaultCfg)

	// merge in config file
	if cfgFile, err := flags.GetString("config"); err == nil && cfgFile != "" {
		v.SetConfigFile(cfgFile)
	} else {
		v.SetConfigName("ares")
		v.AddConfigPath("./config")
	}

	if err := v.MergeInConfig(); err == nil {
		fmt.Println("Using config file: ", v.ConfigFileUsed())
	}

	var cfg common.AresServerConfig
	err = v.Unmarshal(&cfg, func(config *mapstructure.DecoderConfig) {
		config.TagName = "yaml"
	})
	return cfg, err
}
