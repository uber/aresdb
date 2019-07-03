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

package broker

import (
	"fmt"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/expr"
	"github.com/uber/aresdb/utils"
	"net/http"
)

const (
	nonAggregationQueryLimit = 1000
)

// QueryContext is broker query context
type QueryContext struct {
	AQLQuery              *common.AQLQuery
	IsNonAggregationQuery bool
	Writer                http.ResponseWriter
	Error                 error
}

// NewQueryContext creates new query context
func NewQueryContext(aql *common.AQLQuery, w http.ResponseWriter) *QueryContext {
	ctx := QueryContext{
		AQLQuery: aql,
		Writer:   w,
	}
	return &ctx
}

// Compile sql to AQL and extract information for routing
func (c *QueryContext) Compile(schemaReader metaCom.TableSchemaReader) {

	// validate main table
	var err error
	mainTableName := c.AQLQuery.Table
	_, err = schemaReader.GetTable(mainTableName)
	if err != nil {
		c.Error = utils.StackError(err, fmt.Sprintf("err finding main table %s", mainTableName))
		return
	}
	// validate foreign table names
	for _, join := range c.AQLQuery.Joins {
		_, err = schemaReader.GetTable(join.Table)
		if err != nil {
			c.Error = utils.StackError(err, fmt.Sprintf("err finding join table %s", join.Table))
			return
		}
	}

	c.proessMeasures()

	return
}

func (c *QueryContext) proessMeasures() {
	var err error

	// Measures.
	for i, measure := range c.AQLQuery.Measures {
		measure.ExprParsed, err = expr.ParseExpr(measure.Expr)
		if err != nil {
			c.Error = utils.StackError(err, "Failed to parse measure: %s", measure.Expr)
			return
		}
		c.AQLQuery.Measures[i] = measure
	}
	utils.GetLogger().Debug(c.AQLQuery.Measures[0].ExprParsed)

	// ony support 1 measure for now
	if len(c.AQLQuery.Measures) != 1 {
		c.Error = utils.StackError(nil, "expect one measure per query, but got %d",
			len(c.AQLQuery.Measures))
		return
	}

	if _, ok := c.AQLQuery.Measures[0].ExprParsed.(*expr.NumberLiteral); ok {
		c.IsNonAggregationQuery = true
		// in case user forgot to provide limit
		if c.AQLQuery.Limit == 0 {
			c.AQLQuery.Limit = nonAggregationQueryLimit
		}
		return
	}

	aggregate, ok := c.AQLQuery.Measures[0].ExprParsed.(*expr.Call)
	if !ok {
		c.Error = utils.StackError(nil, "expect aggregate function, but got %s",
			c.AQLQuery.Measures[0].Expr)
		return
	}

	if len(aggregate.Args) != 1 {
		c.Error = utils.StackError(nil,
			"expect one parameter for aggregate function %s, but got %d",
			aggregate.Name, len(aggregate.Args))
		return
	}
}
