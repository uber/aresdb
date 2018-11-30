package clients

import (
	"encoding/json"
	"fmt"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/utils"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	defaultRequestTimeout = 5
)

const (
	// InstanceNameHeaderKey is the key for instance name http header
	InstanceNameHeaderKey = "AresDB-InstanceName"
)

// ControllerClient defines methods to communicate with ares-controller
type ControllerClient interface {
	GetSchemaHash(namespace string) (string, error)
	GetAllSchema(namespace string) ([]metaCom.Table, error)
}

// ControllerHTTPClient implements ControllerClient over http
type ControllerHTTPClient struct {
	c              *http.Client
	controllerHost string
	controllerPort int
	headers        http.Header
}

// NewControllerHTTPClient returns new ControllerHTTPClient
func NewControllerHTTPClient(cfg common.ControllerConfig) *ControllerHTTPClient {
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultRequestTimeout
	}

	return &ControllerHTTPClient{
		c:              &http.Client{
			Timeout: time.Duration(cfg.Timeout) * time.Second,
		},
		controllerHost: cfg.Host,
		controllerPort: cfg.Port,
		headers:        cfg.Headers,
	}
}

func (c *ControllerHTTPClient) GetSchemaHash(namespace string) (hash string, err error) {
	var req *http.Request
	req, err = c.getRequest(namespace, true)
	if err != nil {
		return
	}
	var resp *http.Response
	resp, err = c.c.Do(req)
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = utils.StackError(nil, fmt.Sprintf("controller client error fetching hash, status code %d", resp.StatusCode))
	}

	var b []byte
	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	hash = string(b)
	return
}

func (c *ControllerHTTPClient) GetAllSchema(namespace string) (tables []metaCom.Table, err error) {
	var req *http.Request
	req, err = c.getRequest(namespace, false)
	if err != nil {
		return
	}
	var resp *http.Response
	resp, err = c.c.Do(req)
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = utils.StackError(nil, fmt.Sprintf("controller client error fetching schema, status code %d", resp.StatusCode))
	}

	var b []byte
	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(b, &tables)
	if err != nil {
		return
	}

	return
}

func (c *ControllerHTTPClient) getRequest(namespace string, hash bool) (req *http.Request, err error) {
	suffix := "tables"
	if hash {
		suffix = "hash"
	}
	url := fmt.Sprintf("http://%s:%d/schema/%s/%s", c.controllerHost, c.controllerPort, namespace, suffix)
	req, err = http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}
	req.Header = c.headers
	return
}
