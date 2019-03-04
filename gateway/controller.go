package gateway

import (
	"encoding/json"
	"fmt"
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
	GetSchemaHash(namespace string) (string, error)
	GetAllSchema(namespace string) ([]common.Table, error)
	GetAssignmentHash(jobNamespace, instance string) (string, error)
	GetAssignment(jobNamespace, instance string) (*rules.Assignment, error)
}

// ControllerHTTPClient implements ControllerClient over http
type ControllerHTTPClient struct {
	c       *http.Client
	address string
	headers http.Header
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
func (c *ControllerHTTPClient) buildRequest(method, path string) (req *http.Request, err error) {
	path = strings.TrimPrefix(path, "/")
	url := fmt.Sprintf("http://%s/%s", c.address, path)
	req, err = http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header = c.headers
	return
}

func (c *ControllerHTTPClient) getResponse(request *http.Request) ([]byte, error) {
	resp, err := c.c.Do(request)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("aresDB controller return status: %d", resp.StatusCode)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return respBytes, nil
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
	request, err := c.buildRequest(http.MethodGet, fmt.Sprintf("/schema/%s/hash", namespace))
	if err != nil {
		return
	}
	bytes, err := c.getResponse(request)
	if err != nil {
		return "", utils.StackError(err, "controller client error fetching hash")
	}
	return string(bytes), nil
}

func (c *ControllerHTTPClient) GetAllSchema(namespace string) (tables []common.Table, err error) {
	request, err := c.buildRequest(http.MethodGet, fmt.Sprintf("/schema/%s/tables", namespace))
	if err != nil {
		return
	}
	err = c.getJSONResponse(request, &tables)
	if err != nil {
		return nil, utils.StackError(err, "controller client error fetching schema")
	}

	return
}

// GetAssignmentHash get hash code of assignment
func (c *ControllerHTTPClient) GetAssignmentHash(jobNamespace, instance string) (hash string, err error) {
	request, err := c.buildRequest(http.MethodGet, fmt.Sprintf("assignment/%s/hash/%s", jobNamespace, instance))
	if err != nil {
		return "", err
	}

	bytes, err := c.getResponse(request)
	if err != nil {
		return "", utils.StackError(err, "controller client error fetching assignment hash")
	}

	return string(bytes), nil
}

// GetAssignment gets the job assignment of the ares-subscriber
func (c *ControllerHTTPClient) GetAssignment(jobNamespace, instance string) (assignment *rules.Assignment, err error) {
	request, err := c.buildRequest(http.MethodGet, fmt.Sprintf("assignment/%s/assignments/%s", jobNamespace, instance))
	if err != nil {
		return nil, utils.StackError(err, "Failed to buildRequest")
	}

	request.Header.Add("content-type", "application/json")
	assignment = &rules.Assignment{}
	err = c.getJSONResponse(request, assignment)
	if err != nil {
		return nil, utils.StackError(err, "Failed to GetAssignment")
	}

	for _, jobConfig := range assignment.Jobs {
		if jobConfig.PopulateAresTableConfig() != nil {
			return nil, utils.StackError(err, "Failed to PopulateAresTableConfig")
		}
	}
	return
}
