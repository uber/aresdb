package handler

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/uber/aresdb/controller/models"
	mutatorCom "github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

// AssignmentHandler serves requests for ingestion job assignments
type AssignmentHandler struct {
	logger *zap.SugaredLogger

	assignmentMutator mutatorCom.IngestionAssignmentMutator
	schemaMutator     mutatorCom.TableSchemaMutator
	membershipMutator mutatorCom.MembershipMutator
}

// NewAssignmentHandler creates a new AssignmentHandler
func NewAssignmentHandler(logger *zap.SugaredLogger, assignmentMutator mutatorCom.IngestionAssignmentMutator, schemaMutator mutatorCom.TableSchemaMutator, membershipMutator mutatorCom.MembershipMutator) AssignmentHandler {
	return AssignmentHandler{
		logger:            logger,
		assignmentMutator: assignmentMutator,
		schemaMutator:     schemaMutator,
		membershipMutator: membershipMutator,
	}
}

// Register adds paths to router
func (h AssignmentHandler) Register(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper2) {
	router.HandleFunc("/{namespace}/assignments/{subscriber}", utils.ApplyHTTPWrappers2(h.GetAssignment, wrappers...)).Methods(http.MethodGet)
	router.HandleFunc("/{namespace}/assignments", utils.ApplyHTTPWrappers2(h.GetAssignments, wrappers...)).Methods(http.MethodGet)
	router.HandleFunc("/{namespace}/hash/{subscriber}", utils.ApplyHTTPWrappers2(h.GetHash, wrappers...)).Methods(http.MethodGet)
}

// GetAssignment swagger:route GET /assignment/{namespace}/assignments/{subscriber} getAssignment
// gets assignment by subscriber name
func (h AssignmentHandler) GetAssignment(w *utils.ResponseWriter, r *http.Request) {
	var req GetAssignmentRequest
	err := ReadRequest(r, &req, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	assignment, err := h.assignmentMutator.GetIngestionAssignment(req.Namespace, req.Subscriber)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if mutatorCom.IsNonExist(err) {
			statusCode = http.StatusBadRequest
			err = mutatorCom.ErrIngestionAssignmentDoesNotExist
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}

	for i, job := range assignment.Jobs {
		table, err := h.schemaMutator.GetTable(req.Namespace, job.AresTableConfig.Name)
		if err != nil {
			w.WriteError(ErrFailedToFetchTableSchemaForJobConfig)
			return
		}

		assignment.Jobs[i].AresTableConfig.Table = table
	}

	var instances []models.Instance
	if h.membershipMutator != nil {
		instances, err = h.membershipMutator.GetInstances(req.Namespace)
		if err != nil {
			w.WriteError(err)
			return
		}
	}
	w.WriteObject(composeSingleAssignment(instances, assignment))
}

// GetAssignments swagger:route GET /assignment/{namespace}/assignments getAssignments
// returns all assignments
func (h AssignmentHandler) GetAssignments(w *utils.ResponseWriter, r *http.Request) {
	var req GetJobsRequest
	err := ReadRequest(r, &req, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	assignments, err := h.assignmentMutator.GetIngestionAssignments(req.Namespace)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if mutatorCom.IsNonExist(err) {
			statusCode = http.StatusBadRequest
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}

	results := make([]models.IngestionAssignment, len(assignments))

	for i, assignment := range assignments {

		for i, job := range assignment.Jobs {
			table, err := h.schemaMutator.GetTable(req.Namespace, job.AresTableConfig.Name)
			if err != nil {
				w.WriteError(ErrFailedToFetchTableSchemaForJobConfig)
				return
			}
			assignment.Jobs[i].AresTableConfig.Table = table
		}

		var instances []models.Instance
		if h.membershipMutator != nil {
			instances, err = h.membershipMutator.GetInstances(req.Namespace)
			if err != nil {
				w.WriteError(err)
				return
			}
		}
		results[i] = composeSingleAssignment(instances, assignment)
	}
	w.WriteObject(results)
}

// GetHash swagger:route GET /assignment/{namespace}/hash/{subscriber} getAssignmentHash
// returns hash that will be different if any thing changed for given assignment
func (h AssignmentHandler) GetHash(w *utils.ResponseWriter, r *http.Request) {
	var req GetAssignmentHashRequest
	err := ReadRequest(r, &req, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	hash, err := h.assignmentMutator.GetHash(req.Namespace, req.Subscriber)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if mutatorCom.IsNonExist(err) {
			statusCode = http.StatusNotFound
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}
	w.WriteJSONBytesWithCode(http.StatusOK, []byte(hash), nil)
}

func composeSingleAssignment(instances []models.Instance, assignment models.IngestionAssignment) models.IngestionAssignment {
	instanceViews := make(map[string]models.Instance, len(instances))
	for _, instance := range instances {
		instance.Address = fmt.Sprintf("%s:%d", instance.Host, instance.Port)
		instanceViews[instance.Name] = instance
	}
	return models.IngestionAssignment{
		Subscriber: assignment.Subscriber,
		Jobs:       assignment.Jobs,
		Instances:  instanceViews,
	}
}
