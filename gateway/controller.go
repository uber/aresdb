package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/uber/aresdb/client"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/utils"
)

const (
	// InstanceNameHeaderKey is the key for instance name http header
	InstanceNameHeaderKey = "AresDB-InstanceName"
)

// ControllerClient defines methods to communicate with ares-controller
type ControllerClient interface {
	client.SchemaFetcher

	GetSchemaHash(namespace string) (string, error)
	GetAllSchema(namespace string) ([]common.Table, error)
	GetAssignmentHash(jobNamespace, instance string) (string, error)
	GetAssignment(jobNamespace, instance string) (*rules.Assignment, error)
}

// ControllerHTTPClient implements ControllerClient over http
type ControllerHTTPClient struct {
	c         *http.Client
	address   string
	headers   http.Header
	namespace string
}

// NewControllerHTTPClient returns new ControllerHTTPClient
func NewControllerHTTPClient(address string, timeoutSec time.Duration, headers http.Header) *ControllerHTTPClient {
	return &ControllerHTTPClient{
		c: &http.Client{
			Timeout: timeoutSec,
		},
		address: address,
		headers: headers,
	}
}

// buildRequest builds an http.Request with headers.
func (c *ControllerHTTPClient) buildRequest(method, path string, body io.Reader) (req *http.Request, err error) {
	path = strings.TrimPrefix(path, "/")
	url := fmt.Sprintf("http://%s/%s", c.address, path)
	req, err = http.NewRequest(method, url, body)
	if err != nil {
		req = nil
		return
	}

	headersCopy := http.Header{}
	for k, vs := range c.headers {
		for _, v := range vs {
			headersCopy.Add(k, v)
		}
	}
	headersCopy.Add("RPC-Procedure", path)
	req.Header = headersCopy
	return
}

func (c *ControllerHTTPClient) getResponse(request *http.Request) (respBytes []byte, err error) {
	resp, err := c.c.Do(request)
	if err != nil {
		return
	}

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("aresDB controller return status: %d", resp.StatusCode)
		return
	}

	respBytes, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		respBytes = nil
		return
	}

	return
}

func (c *ControllerHTTPClient) getJSONResponse(request *http.Request, output interface{}) error {
	bytes, err := c.getResponse(request)
	if err != nil {
		return err
	}

	err = json.Unmarshal(bytes, output)
	return err
}

func (c *ControllerHTTPClient) GetSchemaHash(namespace string) (hash string, err error) {
	request, err := c.buildRequest(http.MethodGet, fmt.Sprintf("/schema/%s/hash", namespace), nil)
	if err != nil {
		return
	}
	bytes, err := c.getResponse(request)
	if err != nil {
		err = utils.StackError(err, "controller client error fetching hash")
		return
	}

	hash = string(bytes)
	return
}

func (c *ControllerHTTPClient) GetAllSchema(namespace string) (tables []common.Table, err error) {
	request, err := c.buildRequest(http.MethodGet, fmt.Sprintf("/schema/%s/tables", namespace), nil)
	if err != nil {
		return
	}
	err = c.getJSONResponse(request, &tables)
	if err != nil {
		err = utils.StackError(err, "controller client error fetching schema")
		return
	}

	return
}

// GetAssignmentHash get hash code of assignment
func (c *ControllerHTTPClient) GetAssignmentHash(jobNamespace, instance string) (hash string, err error) {
	request, err := c.buildRequest(http.MethodGet, fmt.Sprintf("assignment/%s/hash/%s", jobNamespace, instance), nil)
	if err != nil {
		return
	}

	bytes, err := c.getResponse(request)
	if err != nil {
		err = utils.StackError(err, "controller client error fetching assignment hash")
		return
	}

	hash = string(bytes)
	return
}

// GetAssignment gets the job assignment of the ares-subscriber
func (c *ControllerHTTPClient) GetAssignment(jobNamespace, instance string) (assignment *rules.Assignment, err error) {
	request, err := c.buildRequest(http.MethodGet, fmt.Sprintf("assignment/%s/assignments/%s", jobNamespace, instance), nil)
	if err != nil {
		err = utils.StackError(err, "Failed to buildRequest")
		return
	}

	request.Header.Add("content-type", "application/json")
	assignment = &rules.Assignment{}
	err = c.getJSONResponse(request, assignment)
	if err != nil {
		err = utils.StackError(err, "Failed to GetAssignment")
		return
	}

	for _, jobConfig := range assignment.Jobs {
		if jobConfig.PopulateAresTableConfig() != nil {
			err = utils.StackError(err, "Failed to PopulateAresTableConfig")
			return
		}
	}
	return
}

// SetNamespace sets the namespace which the ControllerHTTPClient connects to
func (c *ControllerHTTPClient) SetNamespace(namespace string) {
	c.namespace = namespace
}

// FetchAllSchemas fetches all schemas
func (c *ControllerHTTPClient) FetchAllSchemas() (tables []*common.Table, err error) {
	var schemas []common.Table
	schemas, err = c.GetAllSchema(c.namespace)
	if err != nil {
		return
	}
	tables = make([]*common.Table, 0, len(schemas))
	for i, _ := range schemas {
		tables = append(tables, &schemas[i])
	}
	return
}

// FetchSchema fetch one schema for given table
func (c *ControllerHTTPClient) FetchSchema(tableName string) (table *common.Table, err error) {
	request, err := c.buildRequest(http.MethodGet, fmt.Sprintf("schema/%s/tables/%s", c.namespace, tableName), nil)
	if err != nil {
		return
	}
	table = &common.Table{}
	err = c.getJSONResponse(request, table)
	if err != nil {
		err = utils.StackError(err, "controller client error fetching schema for table: %s", tableName)
		return
	}
	return
}

// FetchAllEnums fetches all enums for given table and column
func (c *ControllerHTTPClient) FetchAllEnums(tableName string, columnName string) (enumDictReponse []string, err error) {
	request, err := c.buildRequest(http.MethodGet, fmt.Sprintf("schema/%s/tables/%s/columns/%s/enum-cases", c.namespace, tableName, columnName), nil)
	if err != nil {
		return
	}

	err = c.getJSONResponse(request, &enumDictReponse)
	if err != nil {
		err = utils.StackError(err, "controller client error fetching schema for table: %s", tableName)
		return
	}
	return
}

// ExtendEnumCases extends enum cases to given table column
func (c *ControllerHTTPClient) ExtendEnumCases(tableName, columnName string, enumCases []string) (enumIDs []int, err error) {
	enumCasesBytes, err := json.Marshal(enumCases)
	if err != nil {
		return nil, utils.StackError(err, "Failed to marshal enum cases")
	}
	
	request, err := c.buildRequest(http.MethodPost, fmt.Sprintf("schema/%s/tables/%s/columns/%s/enum-cases", c.namespace, tableName, columnName), bytes.NewReader(enumCasesBytes))
	if err != nil {
		return
	}

	err = c.getJSONResponse(request, &enumIDs)
	return

}
