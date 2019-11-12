package handler

import (
	"go.uber.org/fx"
)

// Module is handler fx module
var Module = fx.Provide(
		NewHealthHandler,
		NewConfigHandler,
		NewSchemaHandler,
		NewMembershipHandler,
		NewNamespaceHandler,
		NewAssignmentHandler,
		NewPlacementHandler,
	)
