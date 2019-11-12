package handler

import (
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/uber/aresdb/controller/models"
	"github.com/uber/aresdb/metastore/common"
)

// Define request and response struct for each endpoint

// Schema endpoints models

// AddTableRequest is the AddTable request
type AddTableRequest struct {
	Namespace string       `path:"namespace"`
	Body      common.Table `body:""`
}

// GetTableRequest is the GetTable request
type GetTableRequest struct {
	Namespace string `path:"namespace"`
	TableName string `path:"table"`
}

// GetTableResponse is the GetTable response
type GetTableResponse struct {
	Table common.Table `json:"table"`
}

// GetTablesRequest is the request for GetTables
type GetTablesRequest struct {
	InstanceName string `header:"AresDB-InstanceName,optional"`
	Namespace    string `path:"namespace"`
}

// GetTablesResponse is the response for GetTables
type GetTablesResponse struct {
	Tables []common.Table `json:"tables"`
}

// DeleteTableRequest is the request for DeleteTable
type DeleteTableRequest struct {
	Namespace string `path:"namespace"`
	TableName string `path:"table"`
}

// UpdateTableRequest is the UpdateTable request
type UpdateTableRequest struct {
	Namespace string       `path:"namespace"`
	TableName string       `path:"table"`
	Body      common.Table `body:""`
}

// UpdateTableResponse is the UpdateTable response
type UpdateTableResponse struct {
	Table common.Table `json:"table"`
}

// GetHashRequest is the GetHash request
type GetHashRequest struct {
	InstanceName string `header:"AresDB-InstanceName,optional"`
	Namespace    string `path:"namespace"`
}

// GetHashResponse is the GetHash response
type GetHashResponse struct {
	Hash string `json:"hash"`
}

// Job endpoints models

// GetJobRequest is the request for GetJob
type GetJobRequest struct {
	Namespace string `path:"namespace"`
	JobName   string `path:"job"`
}

// GetJobResponse is the response for GetJob
type GetJobResponse struct {
	Job models.JobConfig `json:"job"`
}

// GetJobsRequest is the request for GetJobs
type GetJobsRequest struct {
	Namespace string `path:"namespace"`
}

// GetJobsResponse is the response for GetJobs
type GetJobsResponse struct {
	Jobs []models.JobConfig `json:"jobs"`
}

// DeleteJobRequest is the request for DeleteJob
type DeleteJobRequest struct {
	Namespace string `path:"namespace"`
	JobName   string `path:"job"`
}

// UpdateJobRequest is the request for UpdateJob
type UpdateJobRequest struct {
	Namespace string           `path:"namespace"`
	Body      models.JobConfig `body:""`
}

// UpdateJobResponse is the response for UpdateJob
type UpdateJobResponse struct {
	Job models.JobConfig `json:"job"`
}

// AddJobRequest is the request for AddJob
type AddJobRequest struct {
	Namespace string           `path:"namespace"`
	Body      models.JobConfig `body:""`
}

// AddJobResponse is the response for AddJob
type AddJobResponse struct {
	Job models.JobConfig `json:"job"`
}

// Namespace endpoints models

// CreateNamespaceRequest is the request for CreateNamespace
// swagger:parameters createNamespace
type CreateNamespaceRequest struct {
	// in: body
	Body struct {
		Namespace string `json:"namespace"`
	} `body:""`
}

// Membership endpoints models

// GetInstanceRequest is the request for GetInstance
type GetInstanceRequest struct {
	Namespace    string `path:"namespace"`
	InstanceName string `path:"instance"`
}

// Instance is the external view of instances
type Instance struct {
	Address string `json:"address"`
}

// Assignment is the external view of assignment
// TODO: finer grain assignment for each aresdb instance
type Assignment struct {
	Subscriber string              `json:"subscriber"`
	Jobs       []models.JobConfig  `json:"jobs"`
	Instances  map[string]Instance `json:"instances"`
}

// GetInstanceResponse is the response for GetInstance
type GetInstanceResponse struct {
	Instance Instance `json:"instance"`
}

// GetInstancesRequest is the request for GetInstances
type GetInstancesRequest struct {
	Namespace string `path:"namespace"`
}

// GetInstancesResponse is the response for GetInstances
type GetInstancesResponse struct {
	Instances map[string]Instance `json:"instances"`
}

// LeaveRequest is the request for Leave
type LeaveRequest struct {
	Namespace    string `path:"namespace"`
	InstanceName string `path:"instance"`
}

// JoinRequest is the request for Join
type JoinRequest struct {
	Namespace string          `path:"namespace"`
	Body      models.Instance `body:""`
}

// GetAssignmentRequest is the request for GetAssignment
// swagger:parameters getAssignment
type GetAssignmentRequest struct {
	Namespace  string `path:"namespace"`
	Subscriber string `path:"subscriber"`
}

// GetAssignmentHashRequest is the GetAssignmentHash request
type GetAssignmentHashRequest struct {
	Namespace  string `path:"namespace"`
	Subscriber string `path:"subscriber"`
}

// ExtendEnumCaseRequest is the ExtendEnumCase request
type ExtendEnumCaseRequest struct {
	GetTableRequest
	Column string   `path:"column"`
	Body   []string `body:""`
}

// GetEnumCaseRequest is the GetEnumCase request
type GetEnumCaseRequest struct {
	GetTableRequest
	Column string `path:"column"`
}

// NamespaceRequest represents request on specified namespace
type NamespaceRequest struct {
	Namespace string `path:"namespace"`
}

// InstanceRequest represents request on specified instance
type InstanceRequest struct {
	NamespaceRequest
	Instance string `path:"instance"`
}

// InitPlacementRequest represents placement initialization request
type InitPlacementRequest struct {
	NamespaceRequest
	Body struct {
		NumShards    int                    `json:"numShards"`
		NumReplica   int                    `json:"numReplica"`
		NewInstances []placementpb.Instance `json:"newInstances"`
	} `body:""`
}

// AddInstancesRequest represents request to add new instances to placement
type AddInstancesRequest struct {
	NamespaceRequest
	Body struct {
		NewInstances []placementpb.Instance `json:"newInstances"`
	} `body:""`
}

// ReplaceInstanceRequest represents request to replace instances
type ReplaceInstanceRequest struct {
	NamespaceRequest
	Body struct {
		LeavingInstances []string               `json:"leavingInstances"`
		NewInstances     []placementpb.Instance `json:"newInstances"`
	} `body:""`
}

// RemoveInstanceRequest represents request to remove instance
type RemoveInstanceRequest struct {
	NamespaceRequest
	Body struct {
		LeavingInstances []string `json:"leavingInstances"`
	} `body:""`
}

// MarkAvailableRequest marks specified shards within specified instance as available
type MarkAvailableRequest struct {
	InstanceRequest
	Body struct {
		// if all specified, then mark all shards within instance as available
		AllShards bool     `json:"all"`
		Shards    []uint32 `json:"shards"`
	} `body:""`
}
