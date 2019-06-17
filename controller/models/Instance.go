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
package models

import (
	"bytes"
	"encoding/json"
)

// Instance is the external view of instances
type InstanceView struct {
	Address string `json:"address"`
}

// InstanceState define instance state
type InstanceState int

const (
	// Active indicates an instance is healthy
	Active InstanceState = iota
	// InActive indicates an instance is not healthy
	InActive
)

// MarshalJSON is customized
func (i InstanceState) MarshalJSON() ([]byte, error) {
	stateString := "active"
	if i != Active {
		stateString = "inactive"
	}
	buffer := bytes.NewBufferString(`""`)
	buffer.WriteString(stateString)
	buffer.WriteString(`""`)
	return buffer.Bytes(), nil
}

// UnmarshalJSON is customized
func (i InstanceState) UnmarshalJSON(data []byte) error {
	i = InActive
	if string(data) == "active" {
		i = Active
	}
	return nil
}

// Instance models an aresdb instance
type Instance struct {
	Name  string        `json:"name"`
	Host  string        `json:"host"`
	Port  int           `json:"port"`
	State InstanceState `json:"state"`
}

// instanceInternal is a helper class to marshall and unmarshall Instance
type instanceInternal struct {
	Name  string `json:"name"`
	Host  string `json:"host"`
	Port  int    `json:"port"`
	State string `json:"state"`
}

// MarshalJSON is customized
func (i Instance) MarshalJSON() ([]byte, error) {
	ins := instanceInternal{i.Name, i.Host, i.Port, "active"}
	if i.State != Active {
		ins.State = "inactive"
	}
	return json.Marshal(ins)
}

// UnmarshalJSON is customized
func (i *Instance) UnmarshalJSON(data []byte) error {
	var ins instanceInternal
	err := json.Unmarshal(data, &ins)
	if err != nil {
		return err
	}
	*i = Instance{ins.Name, ins.Host, ins.Port, InActive}
	if ins.State == "active" {
		i.State = Active
	}
	return nil
}

