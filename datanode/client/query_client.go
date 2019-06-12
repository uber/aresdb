//  Copyright (c) 2017-2018 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/uber/aresdb/cluster/topology"
	queryCom "github.com/uber/aresdb/query/common"
	. "io/ioutil"
	"net/http"
)

func NewDataNodeQueryClient() DataNodeQueryClient {
	return &dataNodeQueryClientImpl{
		client: http.Client{},
	}
}

type dataNodeQueryClientImpl struct {
	client http.Client
}

type aqlRequestBody struct {
	Queries []queryCom.AQLQuery `json:"queries"`
}

type aqlRespBody struct {
	Results []queryCom.AQLQueryResult `json:"results"`
}

func (dc *dataNodeQueryClientImpl) Query(ctx context.Context, host topology.Host, query queryCom.AQLQuery)  (result queryCom.AQLQueryResult, err error) {
	url := fmt.Sprintf("http://%s/query/aql", host.Address())
	aqlRequestBody := aqlRequestBody{
		[]queryCom.AQLQuery{query},
	}
	var bodyBytes []byte
	bodyBytes, err = json.Marshal(aqlRequestBody)
	if err != nil {
		return
	}
	var req *http.Request
	req, err = http.NewRequest(http.MethodPost, url, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return
	}

	req = req.WithContext(ctx)
	var res *http.Response
	res, err = dc.client.Do(req)
	if res != nil {
		defer res.Body.Close()
	}
	if err != nil {
		return
	}
	if res.StatusCode != http.StatusOK {
		err = errors.New(fmt.Sprintf("got status code %d from datanode", res.StatusCode))
		return
	}
	var respBytes []byte
	respBytes, err = ReadAll(res.Body)
	if err != nil {
		respBytes = nil
		return
	}

	var respBody aqlRespBody
	err = json.Unmarshal(respBytes, &respBody)
	if err != nil || len(respBody.Results) != 1{
		err = errors.New(fmt.Sprintf("invalid response from datanode, resp: %s", respBytes))
		return
	}
	result = respBody.Results[0]
	return
}