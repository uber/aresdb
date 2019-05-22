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
	"github.com/uber/aresdb/controller/generated/proto"
	"path"
	"strconv"
	"sync"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/utils"
)

const maxEnumCasePerNode = 1000

type enumCache struct {
	enumCases     map[string]int
	currentNodeID int
}

type enumMutator struct {
	sync.RWMutex

	schemaMutator common.TableSchemaMutator
	// namesapce to columnID to enum cases set
	// key {namespace}/{table}/{incarnation}/{columnID}
	enumCacheMap map[string]enumCache
	txnStore     kv.TxnStore
}

// NewEnumMutator creates EnumMutator
func NewEnumMutator(store kv.TxnStore, schemaMutator common.TableSchemaMutator) common.EnumMutator {
	mutator := &enumMutator{
		schemaMutator: schemaMutator,
		txnStore:      store,
		enumCacheMap:  make(map[string]enumCache),
	}
	return mutator
}

func (e *enumMutator) ExtendEnumCases(namespace, tableName, columnName string, enumCases []string) ([]int, error) {
	schema, err := e.schemaMutator.GetTable(namespace, tableName)
	if err != nil {
		return nil, err
	}
	columnID := -1
	for id, column := range schema.Columns {
		if column.Name == columnName && !column.Deleted && column.IsEnumColumn() {
			columnID = id
			break
		}
	}
	if columnID < 0 {
		return nil, metastore.ErrColumnDoesNotExist
	}

	e.RLock()
	enumCache, exist := e.enumCacheMap[getCacheKey(namespace, tableName, schema.Incarnation, columnID)]
	if !exist {
		e.RUnlock()
		// fetch enum case from etcd and update cache
		return e.extendEnumCase(namespace, tableName, schema.Incarnation, columnID, 0, enumCases)
	}

	currentNodeID := enumCache.currentNodeID
	// check enum cache for existing enums
	enumIDs := make([]int, len(enumCases))
	newEnumCases := make([]string, 0)
	for index, enum := range enumCases {
		enumID, exist := enumCache.enumCases[enum]
		if !exist {
			enumIDs[index] = -1
			newEnumCases = append(newEnumCases, enum)
		} else {
			enumIDs[index] = enumID
		}
	}
	e.RUnlock()

	if len(newEnumCases) > 0 {
		// fetch enum cases from etcd and update cache
		missingIDs, err := e.extendEnumCase(namespace, tableName, schema.Incarnation, columnID, currentNodeID, newEnumCases)
		if err != nil {
			return nil, err
		}
		// missingID is ordered the same as newEnumCases
		// which is the same order in enumIDs
		i := 0
		for index, enumID := range enumIDs {
			if enumID < 0 {
				enumIDs[index] = missingIDs[i]
				i++
			}
		}
	}
	return enumIDs, nil
}

func getCacheKey(namespace, tableName string, incarnation, columnID int) string {
	return path.Join(namespace, tableName, strconv.Itoa(incarnation), strconv.Itoa(columnID))
}

func getEnumID(nodeID int, innerID int) int {
	return maxEnumCasePerNode*nodeID + innerID
}

func (e *enumMutator) extendEnumCase(namespace, tableName string, incarnation, columnID int, fromEnumNodeID int, newEnumCases []string) ([]int, error) {
	// track result resolvedEnumIDs
	resolvedEnumIDs := make([]int, len(newEnumCases))
	// newEnumCaseDict records the resolved resolvedEnumIDs for newEnumCases
	newEnumCaseDict := make(map[string]int)

	// fetch current enum node list
	nodeListKey := utils.EnumNodeListKey(namespace, tableName, incarnation, columnID)
	v, err := e.txnStore.Get(nodeListKey)
	if err != nil {
		return nil, err
	}

	var (
		enumNodeList proto.EnumNodeList
		// track enumNode's enum cases
		enumCases []string
		// track enumNode's version
		nodeVersion int
		// track enumNode's key in etcd
		nodeKey string
	)

	nodeListVersion := v.Version()
	err = v.Unmarshal(&enumNodeList)
	if err != nil {
		return nil, err
	}

	// it is guaranteed to have at least one enum node
	lastEnumNodeID := int(enumNodeList.NumEnumNodes - 1)

	// fetch all enum cases from fromEnumNode to lastEnumNode
	// and update newEnumCaseDict
	for i := fromEnumNodeID; i <= lastEnumNodeID; i++ {
		nodeKey = utils.EnumNodeKey(namespace, tableName, incarnation, columnID, i)
		enumCases, nodeVersion, err = e.fetchEnumCases(nodeKey)
		if err != nil {
			return nil, err
		}
		for id, enum := range enumCases {
			enumID := getEnumID(i, id)
			newEnumCaseDict[enum] = enumID
		}
	}

	// variables needed for write transaction to etcd
	var (
		// track the last enum node
		lastEnumNode = &proto.EnumCases{Cases: enumCases}
		// transaction
		txn = newTransaction().addKeyValue(nodeKey, nodeVersion, lastEnumNode)
		// only when there are new enum cases not in cache, we need to write to etcd for update
		updated = false
	)

	for index, newCase := range newEnumCases {
		if enumID, exist := newEnumCaseDict[newCase]; exist {
			resolvedEnumIDs[index] = enumID
		} else {
			updated = true
			// once last node is full, append the last finished node
			if len(lastEnumNode.Cases) >= maxEnumCasePerNode {
				// create new node
				lastEnumNode = &proto.EnumCases{
					Cases: make([]string, 0, maxEnumCasePerNode),
				}
				// advance lastEnumNodeID
				lastEnumNodeID++
				// create last node transaction
				txn.addKeyValue(utils.EnumNodeKey(namespace, tableName, incarnation, columnID, lastEnumNodeID), kv.UninitializedVersion, lastEnumNode)
				enumNodeList.NumEnumNodes++
			}
			enumID := getEnumID(lastEnumNodeID, len(lastEnumNode.Cases))
			lastEnumNode.Cases = append(lastEnumNode.Cases, newCase)
			resolvedEnumIDs[index] = enumID
			newEnumCaseDict[newCase] = enumID
		}
	}

	if updated {
		// append nodeList to the transaction
		err = txn.addKeyValue(nodeListKey, nodeListVersion, &enumNodeList).writeTo(e.txnStore)
		if err != nil {
			return nil, err
		}
	}
	e.updateCache(getCacheKey(namespace, tableName, incarnation, columnID), lastEnumNodeID, newEnumCaseDict)
	return resolvedEnumIDs, nil
}

func (e *enumMutator) fetchEnumCases(key string) ([]string, int, error) {
	v, err := e.txnStore.Get(key)
	if err != nil {
		return nil, 0, err
	}
	var enumCases proto.EnumCases
	err = v.Unmarshal(&enumCases)
	if err != nil {
		return nil, 0, err
	}
	return enumCases.Cases, v.Version(), nil
}

func (e *enumMutator) updateCache(cacheKey string, nodeID int, dict map[string]int) {
	e.Lock()
	cache, ok := e.enumCacheMap[cacheKey]
	if !ok {
		cache = enumCache{
			enumCases: make(map[string]int),
		}
	}
	// update currentNodeID for cache
	cache.currentNodeID = nodeID
	// merge newEnumCaseDict to cache
	for enumCase, enumID := range dict {
		if _, exist := cache.enumCases[enumCase]; !exist {
			cache.enumCases[enumCase] = enumID
		}
	}
	e.enumCacheMap[cacheKey] = cache
	e.Unlock()
}

func (e *enumMutator) GetEnumCases(namespace, tableName, columnName string) ([]string, error) {
	schema, err := e.schemaMutator.GetTable(namespace, tableName)
	if err != nil {
		return nil, err
	}
	columnID := -1
	for id, column := range schema.Columns {
		if column.Name == columnName && !column.Deleted && column.IsEnumColumn() {
			columnID = id
			break
		}
	}
	if columnID < 0 {
		return nil, metastore.ErrColumnDoesNotExist
	}

	cacheKey := getCacheKey(namespace, tableName, schema.Incarnation, columnID)
	enumDict := make(map[string]int)
	currentNodeID := 0
	e.RLock()
	cache, exist := e.enumCacheMap[cacheKey]
	if exist {
		currentNodeID = cache.currentNodeID
		for enumCase, enumID := range cache.enumCases {
			enumDict[enumCase] = enumID
		}
	}
	e.RUnlock()

	nodeListKey := utils.EnumNodeListKey(namespace, tableName, schema.Incarnation, columnID)
	v, err := e.txnStore.Get(nodeListKey)
	if err != nil {
		return nil, err
	}

	var nodeList proto.EnumNodeList

	err = v.Unmarshal(&nodeList)
	if err != nil {
		return nil, err
	}

	lastEnumNodeID := int(nodeList.NumEnumNodes - 1)
	for i := currentNodeID; i <= lastEnumNodeID; i++ {
		enumCases, _, err := e.fetchEnumCases(utils.EnumNodeKey(namespace, tableName, schema.Incarnation, columnID, i))
		if err != nil {
			return nil, err
		}
		for id, enumCase := range enumCases {
			enumID := getEnumID(i, id)
			enumDict[enumCase] = enumID
		}
	}

	enumCases := make([]string, len(enumDict))
	for enumCase, enumID := range enumDict {
		enumCases[enumID] = enumCase
	}

	e.updateCache(cacheKey, lastEnumNodeID, enumDict)
	return enumCases, nil
}
