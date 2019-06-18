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
	"github.com/uber/aresdb/utils"
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

func (dc *dataNodeQueryClientImpl) Query(ctx context.Context, host topology.Host, query queryCom.AQLQuery, hll bool) (result queryCom.AQLQueryResult, err error) {
	var bs []byte
	bs, err = dc.queryRaw(ctx, host, query, hll)
	if err != nil {
		return
	}

	if hll {
		var results []queryCom.AQLQueryResult
		var errs []error
		results, errs, err = queryCom.ParseHLLQueryResults(bs)
		if err != nil {
			utils.GetLogger().With("host", host, "query", query, "error", err, "errors", errs, "hll", hll).Error("datanode query client Query failed")
			return
		}
		if len(results) != 1 {
			errors.New(fmt.Sprintf("invalid response from datanode, resp: %s", bs))
		}
		result = results[0]
	} else {
		var respBody aqlRespBody
		err = json.Unmarshal(bs, &respBody)
		if err != nil || len(respBody.Results) != 1 {
			err = errors.New(fmt.Sprintf("invalid response from datanode, resp: %s", bs))
			return
		}
		result = respBody.Results[0]
	}

	utils.GetLogger().With("host", host, "query", query, "result", result, "hll", hll).Debug("datanode query client Query succeeded")
	return
}

func (dc *dataNodeQueryClientImpl) QueryRaw(ctx context.Context, host topology.Host, query queryCom.AQLQuery) (bs []byte, err error) {
	bs, err = dc.queryRaw(ctx, host, query, false)
	if err == nil {
		utils.GetLogger().With("host", host, "query", query).Debug("datanode query client QueryRaw succeeded")
	}
	return
}

func (dc *dataNodeQueryClientImpl) queryRaw(ctx context.Context, host topology.Host, query queryCom.AQLQuery, hll bool) (bs []byte, err error) {
	url := fmt.Sprintf("http://%s/query/aql?dataonly=1", host.Address())
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
	if hll {
		req.Header.Add(utils.HTTPAcceptTypeHeaderKey, utils.HTTPContentTypeHyperLogLog)
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
	bs, err = ReadAll(res.Body)
	if err != nil {
		bs = nil
	}

	return
}
