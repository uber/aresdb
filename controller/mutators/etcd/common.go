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
package etcd

import (
	"fmt"
	"github.com/uber/aresdb/controller/mutators/common"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3/src/cluster/kv"
	pb "github.com/uber/aresdb/controller/generated/proto"
	"github.com/uber/aresdb/utils"
)

func addEntity(entityList pb.EntityList, name string) (res pb.EntityList, incarnation int, exist bool) {
	nowTs := utils.Now().UnixNano() / int64(time.Millisecond)
	for _, entityName := range entityList.Entities {
		if name == entityName.Name {
			if !entityName.Tomstoned {
				exist = true
				return
			}
			entityName.Incarnation++
			incarnation = int(entityName.Incarnation)
			entityName.Tomstoned = false
			entityName.LastUpdatedAt = nowTs
			entityList.LastUpdatedAt = nowTs
			return entityList, incarnation, false
		}
	}

	entityList.Entities = append(entityList.Entities, &pb.EntityName{
		Name:          name,
		Tomstoned:     false,
		Incarnation:   0,
		LastUpdatedAt: nowTs,
	})
	entityList.LastUpdatedAt = nowTs
	return entityList, incarnation, false
}

func readValue(etcdStore kv.TxnStore, key string, out proto.Message) (version int, err error) {
	var value kv.Value
	value, err = etcdStore.Get(key)
	if err != nil {
		return
	}

	version = value.Version()
	err = value.Unmarshal(out)
	return
}

func readEntityList(etcdStore kv.TxnStore, key string) (entityList pb.EntityList, version int, err error) {
	version, err = readValue(etcdStore, key, &entityList)
	if common.IsNonExist(err) {
		err = common.ErrNamespaceDoesNotExist
		return
	}
	return
}

func deleteEntity(entityList pb.EntityList, name string) (pb.EntityList, bool) {
	nowTs := utils.Now().UnixNano() / int64(time.Millisecond)
	entityList, found := find(entityList, name, func(entity *pb.EntityName) {
		entity.Tomstoned = true
		entity.LastUpdatedAt = nowTs
	})

	if found {
		entityList.LastUpdatedAt = nowTs
	}
	return entityList, found
}

func updateEntity(entityList pb.EntityList, name string) (pb.EntityList, bool) {
	nowTs := utils.Now().UnixNano() / int64(time.Millisecond)
	return find(entityList, name, func(entity *pb.EntityName) {
		entity.LastUpdatedAt = nowTs
	})
}

func find(entityList pb.EntityList, name string, doWithEntity func(*pb.EntityName)) (pb.EntityList, bool) {
	found := false
	for _, entity := range entityList.Entities {
		if entity.Name == name && !entity.Tomstoned {
			found = true
			doWithEntity(entity)
			break
		}
	}
	return entityList, found
}

func getHash(etcdStore kv.TxnStore, key string) (string, error) {
	entityList, _, err := readEntityList(etcdStore, key)
	if err != nil {
		return "", err
	}
	var latestUpdateAt int64
	for _, entity := range entityList.Entities {
		if entity.LastUpdatedAt > latestUpdateAt {
			latestUpdateAt = entity.LastUpdatedAt
		}
	}
	return fmt.Sprintf("%dcv%d", latestUpdateAt, entityList.LastUpdatedAt), nil
}
