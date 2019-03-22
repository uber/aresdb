package utils

type AresEnv string

const (
	EnvProd    AresEnv = "production"
	EnvStaging AresEnv = "staging"
	EnvDev     AresEnv = "development"
	EnvTest    AresEnv = "test"

	EnvironmentKey        = "ENVIRONMENT"
	RuntimeEnvironmentKey = "RUNTIME_ENVIRONMENT"
	ZoneKey               = "DATACENTER_ZONE"
	DataCenterKey         = "DATACENTER"
	DeploymentKey         = "DEPLOYMENT_NAME"
	HostnameKey           = "HOSTNAME"
	PortSystemKey         = "PORT_SYSTEM"
	AppIDKey              = "APP_ID"
	InstanceIDKey         = "INSTANCE_ID"
	ConfigDirKey          = "UBER_CONFIG_DIR"
)

type EnvironmentContext struct {
	// Environment is // enum for host-level environment (development, test, production, staging)
	Environment string
	// RuntimeEnvironment is user-specified service runtime environment
	RuntimeEnvironment string
	// Zone is data center
	Zone string
	// Hostname is the host name
	Hostname string
	// Deployment is the deployment name
	Deployment string // t.uber.com/udeploy_env
	// SystemPort is for health checks and introspection
	SystemPort string
	// ApplicationID is application  name
	ApplicationID string
	// InstanceID is the ID of an application instance
	InstanceID string
}

// GetAresEnv gets the running environment setting for ares
func GetAresEnv() AresEnv {
	aresEnv := AresEnv(GetConfig().Env)
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
