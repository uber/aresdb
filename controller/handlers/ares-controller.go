package handler

import (
	"github.com/uber/aresdb/utils"
	"net/http"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"go.uber.org/config"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// ServerParams defines params needed to init server
type ServerParams struct {
	fx.In

	Config            config.Provider
	Logger            *zap.SugaredLogger
	HealthHandler     HealthHandler
	ConfigHandler     ConfigHandler
	SchemaHandler     SchemaHandler
	NamespaceHandler  NamespaceHandler
	MembershipHandler MembershipHandler
	AssignmentHandler AssignmentHandler
	PlacementHandler  PlacementHandler
	WrapperProvider   utils.MetricsLoggingMiddleWareProvider
}

// NewCompositeHandler is the provider for http server
func NewCompositeHandler(p ServerParams) http.Handler {
	router := mux.NewRouter()

	p.HealthHandler.Register(router)
	p.ConfigHandler.Register(router.PathPrefix("/config").Subrouter(), p.WrapperProvider.WithMetrics, p.WrapperProvider.WithLogging)
	p.SchemaHandler.Register(router.PathPrefix("/schema").Subrouter(), p.WrapperProvider.WithMetrics, p.WrapperProvider.WithLogging)
	p.MembershipHandler.Register(router.PathPrefix("/membership").Subrouter(), p.WrapperProvider.WithMetrics, p.WrapperProvider.WithLogging)
	p.AssignmentHandler.Register(router.PathPrefix("/assignment").Subrouter(), p.WrapperProvider.WithMetrics, p.WrapperProvider.WithLogging)
	p.PlacementHandler.Register(router.PathPrefix("/placement").Subrouter(), p.WrapperProvider.WithMetrics, p.WrapperProvider.WithLogging)
	p.NamespaceHandler.Register(router, p.WrapperProvider.WithMetrics, p.WrapperProvider.WithLogging)

	// handlers for swagger
	swaggerHanlder := http.StripPrefix("/swagger/", http.FileServer(http.Dir("./ui/swagger/")))
	nodeModulesHandler := http.StripPrefix("/node_modules/", http.FileServer(http.Dir("./ui/node_modules/")))
	router.PathPrefix("/swagger/").Handler(swaggerHanlder)
	router.PathPrefix("/node_modules").Handler(nodeModulesHandler)

	return handlers.CORS()(router)
}
