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

package common

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/uber/aresdb/utils"
)

// ReadRequest reads request.
// obj passed into this method has to be a pointer to a struct of request object
// Each request object will have path params tagged as `path:""` if needed
// and post body tagged as `body:""` if needed
// path tag must have parameter name, which will be used to read path param
// body tag field has to be a struct.
// eg.
//      type AddEnumCaseRequest struct {
//      	TableName string `path:"table"`
//      	ColumnName string `path:"column"`
//      	Body struct {
//      		EnumCase string `json:"enumCase"`
//      	} `body:""`
//      }
func ReadRequest(r *http.Request, obj interface{}, rw *utils.ResponseWriter) error {
	vValue := reflect.ValueOf(obj)
	vType := reflect.TypeOf(obj)
	if vType.Kind() != reflect.Ptr || vType.Elem().Kind() != reflect.Struct {
		return utils.APIError{
			Code:    http.StatusInternalServerError,
			Message: "Expecting request object to be a pointer to struct",
		}
	}

	var formParsed bool
	for i := 0; i < vType.Elem().NumField(); i++ {
		var isPathParam, isQueryParam, isHeaderParam, optional bool
		var paramName, paramValue string

		field := vType.Elem().Field(i)
		valueField := vValue.Elem().Field(i)
		// If it's anonymous field, we apply ReadRequest to this struct directly.
		if field.Type.Kind() == reflect.Struct && field.Anonymous {
			if err := ReadRequest(r, valueField.Addr().Interface(), nil); err != nil {
				return err
			}
		}

		if paramName, isHeaderParam = field.Tag.Lookup("header"); isHeaderParam {
			tagValues := strings.Split(paramName, ",")
			paramName = tagValues[0]
			if len(tagValues) == 2 && tagValues[1] == "optional" {
				optional = true
			}
			paramValue = r.Header.Get(paramName)
		} else if paramName, isPathParam = field.Tag.Lookup("path"); isPathParam {
			vars := mux.Vars(r)
			if vars == nil {
				return ErrMissingParameter
			}
			paramValue = vars[paramName]
		} else if paramName, isQueryParam = field.Tag.Lookup("query"); isQueryParam {
			tagValues := strings.Split(paramName, ",")
			paramName = tagValues[0]
			if len(tagValues) == 2 && tagValues[1] == "optional" {
				optional = true
			}
			if !formParsed {
				if err := r.ParseForm(); err != nil && !optional {
					return ErrMissingParameter
				}
				formParsed = true
			}
			paramValue = r.Form.Get(paramName)
		}

		if isPathParam || isQueryParam || isHeaderParam {
			if paramValue == "" {
				if optional {
					continue
				}
				return ErrMissingParameter
			}
			// Only string and int is supported in request path fields.
			switch field.Type.Kind() {
			case reflect.String:
				valueField.SetString(paramValue)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				intVal, err := strconv.ParseInt(paramValue, 10, 64)
				if err != nil {
					return ErrMissingParameter
				}
				valueField.SetInt(intVal)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				uintVal, err := strconv.ParseUint(paramValue, 10, 64)
				if err != nil {
					return ErrMissingParameter
				}
				valueField.SetUint(uintVal)
			default:
				return ErrMissingParameter
			}
		} else if _, isPostBody := field.Tag.Lookup("body"); isPostBody {
			requestBody, err := ioutil.ReadAll(r.Body)
			if err != nil {
				return utils.APIError{
					Code:    http.StatusBadRequest,
					Message: ErrMsgFailedToReadRequestBody,
					Cause:   err,
				}
			}

			switch valueField.Addr().Interface().(type) {
			case *[]byte:
				valueField.SetBytes(requestBody)
			case *json.RawMessage:
				valueField.SetBytes(requestBody)
			default:
				err = json.Unmarshal(requestBody, valueField.Addr().Interface())
				if err != nil {
					return utils.APIError{
						Code:    http.StatusBadRequest,
						Message: ErrMsgFailedToUnmarshalRequest,
						Cause:   err,
					}
				}
			}
		}
	}
	// set request to response writer for logging purpose
	if rw != nil {
		rw.WriteRequest(vValue.Elem().Interface())
	}
	return nil
}
