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

package query

import (
	"sync"

	"github.com/uber-common/bark"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/memutils"
	"github.com/uber/aresdb/utils"
	"math"
	"strconv"
	"time"
)

const (
	mb2bytes                 = 1 << 20
	defaultDeviceUtilization = 1
	defaultTimeout           = 10
)

// DeviceInfo stores memory information per device
type DeviceInfo struct {
	// device id
	DeviceID int `json:"deviceID"`
	// number of queries being served by device
	QueryCount int `json:"queryCount"`
	// device capacity.
	TotalMemory int `json:"totalMemory"`
	// device available capacity.
	TotalAvailableMemory int `json:"totalAvailableMemory"`
	// total free memory
	FreeMemory int `json:"totalFreeMemory"`
	// query to memory map
	QueryMemoryUsageMap map[*AQLQuery]int `json:"-"`
}

// DeviceManager has the following functionalities:
// 1. Keep track of number of queries being served by this device and memory usage info
// 2. Estimate the memory requirement for a given query and determine if a device has enough memory to process a query
// 3. Assign queries to chosen device according to routing strategy specified
type DeviceManager struct {
	// lock to sync ops.
	sync.RWMutex `json:"-"`
	// device to DeviceInfo map
	DeviceInfos []*DeviceInfo `json:"deviceInfos"`
	// default DeviceChoosingTimeout for finding a device
	Timeout int `json:"timeout"`
	// Max available memory, this can be used to early determined whether a query can be satisfied or not.
	MaxAvailableMemory int `json:"maxAvailableMemory"`
	deviceAvailable    *sync.Cond
	// device choose strategy
	strategy deviceChooseStrategy
}

// NewDeviceManager is used to init a DeviceManager.
func NewDeviceManager(cfg common.QueryConfig) *DeviceManager {
	deviceMemoryUtilization := cfg.DeviceMemoryUtilization
	if deviceMemoryUtilization <= 0 || deviceMemoryUtilization > 1 {
		utils.GetLogger().WithField("deviceMemoryUtilization", deviceMemoryUtilization).
			Error("Invalid deviceMemoryUtilization config, setting to default")
		deviceMemoryUtilization = defaultDeviceUtilization
	}

	timeout := cfg.DeviceChoosingTimeout
	if timeout <= 0 {
		utils.GetLogger().WithField("timeout", timeout).
			Error("Invalid timeout config, setting to default")
		timeout = defaultTimeout
	}

	// retrieve device counts
	deviceCount := memutils.GetDeviceCount()
	utils.GetLogger().WithFields(bark.Fields{"utilization": deviceMemoryUtilization,
		"timeout": timeout}).Info("Initialized device manager")

	deviceInfos := make([]*DeviceInfo, deviceCount)
	maxAvailableMem := 0
	for device := 0; device < deviceCount; device++ {
		deviceInfos[device] = getDeviceInfo(device, deviceMemoryUtilization)
		if deviceInfos[device].TotalAvailableMemory >= maxAvailableMem {
			maxAvailableMem = deviceInfos[device].TotalAvailableMemory
		}
	}

	deviceManager := &DeviceManager{
		DeviceInfos:        deviceInfos,
		MaxAvailableMemory: maxAvailableMem,
		Timeout:            timeout,
	}

	deviceManager.strategy = leastQueryCountAndMemoryStrategy{
		deviceManager: deviceManager,
	}

	deviceManager.deviceAvailable = sync.NewCond(deviceManager)

	// Bootstrap device.
	utils.GetLogger().Info("Bootstrapping device")
	bootstrapDevice()
	utils.GetLogger().Info("Finish bootstrapping device")
	return deviceManager
}

// getDeviceInfo returns the DeviceInfo struct for a given deviceID.
func getDeviceInfo(device int, deviceMemoryUtilization float32) *DeviceInfo {
	totalGlobalMem := memutils.GetDeviceGlobalMemoryInMB(device) * mb2bytes
	totalAvailableMem := int(float32(totalGlobalMem) * deviceMemoryUtilization)

	deviceInfo := DeviceInfo{
		DeviceID:             device,
		QueryCount:           0,
		TotalMemory:          totalGlobalMem,
		TotalAvailableMemory: totalAvailableMem,
		FreeMemory:           totalAvailableMem,
		QueryMemoryUsageMap:  make(map[*AQLQuery]int, 0),
	}
	utils.GetLogger().Infof("DeviceInfo[%d]=%+v\n", device, deviceInfo)
	return &deviceInfo
}

// FindDevice finds a device to run a given query. If a device is not found, it will wait until
// the DeviceChoosingTimeout seconds elapse.
func (d *DeviceManager) FindDevice(query *AQLQuery, requiredMem int, preferredDevice int, timeout int) int {
	if requiredMem > d.MaxAvailableMemory {
		utils.GetQueryLogger().WithFields(
			bark.Fields{
				"query":           query,
				"requiredMem":     requiredMem,
				"preferredDevice": preferredDevice,
				"maxAvailableMem": d.MaxAvailableMemory,
			}).Warn("exceeds max memory")
		return -1
	}

	// no DeviceChoosingTimeout passed by request, using default DeviceChoosingTimeout.
	if timeout <= 0 {
		timeout = d.Timeout
	}

	timeoutDuration := time.Duration(timeout) * time.Second

	start := utils.Now()
	d.Lock()
	device := -1
	for {
		if utils.Now().Sub(start) >= timeoutDuration {
			utils.GetQueryLogger().WithFields(
				bark.Fields{
					"query":           query,
					"requiredMem":     requiredMem,
					"preferredDevice": preferredDevice,
					"timeout":         timeout,
				},
			).Error("DeviceChoosingTimeout when choosing the device for the query")
			break
		}

		device = d.findDevice(query, requiredMem, preferredDevice)
		if device >= 0 {
			break
		}
		d.deviceAvailable.Wait()
	}
	d.Unlock()
	utils.GetRootReporter().GetTimer(utils.QueryWaitForMemoryDuration).Record(utils.Now().Sub(start))
	return device
}

// findDevice finds a device to run a given query according to certain strategy.If no such device can't
// be found, return -1. Caller needs to hold the write lock.
func (d *DeviceManager) findDevice(query *AQLQuery, requiredMem int, preferredDevice int) int {
	utils.GetQueryLogger().WithFields(
		bark.Fields{
			"query":           query,
			"requiredMem":     requiredMem,
			"preferredDevice": preferredDevice,
		},
	).Debug("trying to find device for query")
	candidateDevice := -1

	// try to choose preferredDevice if it meets requirements.
	if preferredDevice >= 0 && preferredDevice < len(d.DeviceInfos) &&
		d.DeviceInfos[preferredDevice].FreeMemory >= requiredMem {
		candidateDevice = preferredDevice
	}

	// choose candidateDevice if preferredDevice does not meet requirements
	if candidateDevice < 0 {
		candidateDevice = d.strategy.chooseDevice(requiredMem)
	}

	if candidateDevice < 0 {
		return candidateDevice
	}

	// reserve memory for this query.
	deviceInfo := d.DeviceInfos[candidateDevice]
	deviceInfo.QueryCount++
	deviceInfo.QueryMemoryUsageMap[query] = requiredMem
	deviceInfo.FreeMemory -= requiredMem
	deviceInfo.reportMemoryUsage()

	utils.GetLogger().Debugf("Assign device '%d' for query", candidateDevice)
	utils.GetLogger().Debugf("DeviceInfo=%+v", deviceInfo)
	return candidateDevice
}

// ReleaseReservedMemory adjust total free global memory for a given device after a query is complete
func (d *DeviceManager) ReleaseReservedMemory(device int, query *AQLQuery) {
	// Don't even need the lock,
	if device < 0 || device >= len(d.DeviceInfos) {
		return
	}

	d.Lock()
	defer d.Unlock()
	deviceInfo := d.DeviceInfos[device]
	usage, ok := deviceInfo.QueryMemoryUsageMap[query]
	if ok {
		utils.GetLogger().Debugf("Freed %d bytes memory on device %d", usage, device)
		deviceInfo.FreeMemory += usage
		deviceInfo.reportMemoryUsage()
		delete(deviceInfo.QueryMemoryUsageMap, query)
		deviceInfo.QueryCount--
		d.deviceAvailable.Broadcast()
	}
}

// reportMemoryUsage reports the memory usage of specified device. Caller needs to hold the lock.
func (deviceInfo *DeviceInfo) reportMemoryUsage() {
	utils.GetRootReporter().GetChildGauge(map[string]string{
		"device": strconv.Itoa(deviceInfo.DeviceID),
	}, utils.EstimatedDeviceMemory).Update(
		float64(deviceInfo.TotalAvailableMemory - deviceInfo.FreeMemory))
}

// deviceChooseStrategy defines the interface to choose an available device for
// specific query.
type deviceChooseStrategy interface {
	chooseDevice(requiredMem int) int
}

// leastAvailableMemoryStrategy is to pick up device with least query count and
// least memory that's larger than required memory of the query.
type leastQueryCountAndMemoryStrategy struct {
	deviceManager *DeviceManager
}

// chooseDevice finds a device to run a given query according to certain strategy
// If no such device, return -1.
func (s leastQueryCountAndMemoryStrategy) chooseDevice(requiredMem int) int {
	candidateDevice := -1
	leastMemory := int(math.MaxInt64)
	leastQueryCount := int(math.MaxInt32)
	for device, deviceInfo := range s.deviceManager.DeviceInfos {
		if deviceInfo.FreeMemory >= requiredMem && (deviceInfo.QueryCount < leastQueryCount ||
			(deviceInfo.QueryCount == leastQueryCount && deviceInfo.FreeMemory <= leastMemory)) {
			candidateDevice = device
			leastQueryCount = deviceInfo.QueryCount
			leastMemory = deviceInfo.FreeMemory
		}
	}
	return candidateDevice
}
