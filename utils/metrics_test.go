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

package utils

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
)

var _ = ginkgo.Describe("metrics", func() {
	ginkgo.It("all cached metrics definitions should be properly initialized", func() {
		reporter := GetRootReporter()
		Ω(reporter.cachedDefinitions).Should(HaveLen(int(DataNodeMetricNamesSentinel)))
		for _, def := range reporter.cachedDefinitions {
			Ω(def).ShouldNot(BeNil())
			switch def.metricType {
			case Counter:
				Ω(def.counter).ShouldNot(BeNil())
			case Gauge:
				Ω(def.gauge).ShouldNot(BeNil())
			case Timer:
				Ω(def.timer).ShouldNot(BeNil())
			}
		}
	})

	ginkgo.It("NewReporterFactory should work", func() {
		scope := tally.NewTestScope("test", nil)
		rf := NewReporterFactory(scope, ReporterTypeDataNode)
		Ω(rf.GetRootReporter().GetRootScope()).Should(Equal(scope))
		// since we've not add any table shard yet, we should get a root reporter back.
		Ω(rf.GetReporter("test", 1)).Should(Equal(rf.GetRootReporter()))
	})

	ginkgo.It("AddTableShard should work", func() {
		scope := tally.NewTestScope("test", nil)
		rf := NewReporterFactory(scope, ReporterTypeDataNode)
		tableName := "test"
		shardID := 1
		rf.AddTableShard(tableName, shardID)
		testScope := rf.GetReporter(tableName, shardID).GetRootScope().(tally.TestScope)
		Ω(testScope.Snapshot().Counters()).
			Should(HaveKey("test.ingested_records+component=memstore,operation=ingestion,shard=1,table=test"))
	})

	ginkgo.It("DeleteTableShard should work", func() {
		scope := tally.NewTestScope("test", nil)
		rf := NewReporterFactory(scope, ReporterTypeDataNode)
		tableName := "test"
		shardID := 1
		rf.AddTableShard(tableName, shardID)
		Ω(rf.GetReporter(tableName, shardID)).ShouldNot(Equal(rf.GetRootReporter()))
		rf.DeleteTableShard(tableName, shardID)
		Ω(rf.GetReporter(tableName, shardID)).Should(Equal(rf.GetRootReporter()))
	})

	ginkgo.It("NewReporter should work", func() {
		scope := tally.NewTestScope("test", nil)
		r := NewReporter(scope, ReporterTypeDataNode)
		Ω(r.GetRootScope()).Should(Equal(scope))
	})

	ginkgo.It("GetCounter should work", func() {
		scope := tally.NewTestScope("test", nil)
		r := NewReporter(scope, ReporterTypeDataNode)
		counter := r.GetCounter(BackfillCount)
		Ω(counter).ShouldNot(BeNil())

		// Not a counter
		Ω(func() { r.GetCounter(BackfillLockTiming) }).Should(Panic())
	})

	ginkgo.It("GetGauge should work", func() {
		scope := tally.NewTestScope("test", nil)
		r := NewReporter(scope, ReporterTypeDataNode)
		gauge := r.GetGauge(ArchivingLowWatermark)
		Ω(gauge).ShouldNot(BeNil())

		// Not a gauge.
		Ω(func() { r.GetGauge(BackfillLockTiming) }).Should(Panic())
	})

	ginkgo.It("GetTimer should work", func() {
		scope := tally.NewTestScope("test", nil)
		r := NewReporter(scope, ReporterTypeDataNode)
		timer := r.GetTimer(BackfillLockTiming)
		Ω(timer).ShouldNot(BeNil())

		// Not a timer.
		Ω(func() { r.GetTimer(ArchivingLowWatermark) }).Should(Panic())
	})

	ginkgo.It("GetChildGauge should work", func() {
		scope := tally.NewTestScope("test", nil)
		r := NewReporter(scope, ReporterTypeDataNode)
		gauge := r.GetChildGauge(map[string]string{
			"test_field": "test_value",
		}, ArchivingLowWatermark)
		Ω(gauge).ShouldNot(BeNil())

		// Not a gauge.
		Ω(func() {
			r.GetChildGauge(map[string]string{
				"test_field": "test_value",
			}, BackfillLockTiming)
		}).Should(Panic())
	})
})
