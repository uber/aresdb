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

package redolog

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/testing"
	metaCom "github.com/uber/aresdb/metastore/common"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	diskMocks "github.com/uber/aresdb/diskstore/mocks"

)


var _ = ginkgo.Describe("redolog manager master tests", func() {
	table := "table1"
	shard := 0
	tableConfig := &metaCom.TableConfig{
		RedoLogRotationInterval: 1,
		MaxRedoLogFileSize:      100000,
	}
	metaStore := &metaMocks.MetaStore{}
	diskStore := &diskMocks.DiskStore{}
	namespace := ""

	ginkgo.It("NewRedoLogManagerMaster", func() {
		// nil config should work as before
		f, err := NewRedoLogManagerMaster(namespace, nil, diskStore, metaStore)
		Ω(err).Should(BeNil())
		Ω(f).ShouldNot(BeNil())

		// empty config should work too
		c := &common.RedoLogConfig{}
		f, err = NewRedoLogManagerMaster(namespace, c, diskStore, metaStore)
		Ω(err).Should(BeNil())
		Ω(f).ShouldNot(BeNil())
		m, err := f.NewRedologManager(table, shard, tableConfig)
		Ω(m.(*FileRedoLogManager)).ShouldNot(BeNil())

		c = &common.RedoLogConfig{
			DiskConfig: common.DiskRedoLogConfig{
				Disabled: false,
			},
			KafkaConfig: common.KafkaRedoLogConfig{
				Enabled: false,
				Brokers: []string{},
			},
		}
		f, err = NewRedoLogManagerMaster(namespace, c, diskStore, metaStore)
		Ω(err).Should(BeNil())
		m, err = f.NewRedologManager(table, shard, tableConfig)
		Ω(m.(*FileRedoLogManager)).ShouldNot(BeNil())

		c = &common.RedoLogConfig{
			DiskConfig: common.DiskRedoLogConfig{
				Disabled: false,
			},
			KafkaConfig: common.KafkaRedoLogConfig{
				Enabled: true,
				Brokers: []string{},
			},
		}
		f, err = NewRedoLogManagerMaster(namespace, c, diskStore, metaStore)
		Ω(err).ShouldNot(BeNil())
		Ω(err.Error()).Should(ContainSubstring("No kafka broker"))

		consumer, _ := testing.MockKafkaConsumerFunc(nil)
		f, err = NewKafkaRedoLogManagerMaster(namespace, c, diskStore, metaStore, consumer)
		Ω(err).Should(BeNil())

		c = &common.RedoLogConfig{
			DiskConfig: common.DiskRedoLogConfig{
				Disabled: false,
			},
			KafkaConfig: common.KafkaRedoLogConfig{
				Enabled: true,
				Brokers: []string{
					"host1",
					"host2",
				},
			},
		}
		// real kafka consumer creation will fail
		f, err = NewRedoLogManagerMaster(namespace, c, diskStore, metaStore)
		Ω(err).ShouldNot(BeNil())

		// mock kafka consumer will success
		f, err = NewKafkaRedoLogManagerMaster(namespace, c, diskStore, metaStore, consumer)
		Ω(err).Should(BeNil())
		Ω(f).ShouldNot(BeNil())
		Ω(f.consumer).ShouldNot(BeNil())
		m, err = f.NewRedologManager(table, shard, tableConfig)
		Ω(m.(*compositeRedoLogManager)).ShouldNot(BeNil())

		c = &common.RedoLogConfig{
			DiskConfig: common.DiskRedoLogConfig{
				Disabled: true,
			},
			KafkaConfig: common.KafkaRedoLogConfig{
				Enabled: true,
				Brokers: []string{
					"host1",
					"host2",
				},
			},
		}
		f, err = NewKafkaRedoLogManagerMaster(namespace, c, diskStore, metaStore, consumer)
		Ω(err).Should(BeNil())
		Ω(f).ShouldNot(BeNil())
		Ω(f.consumer).ShouldNot(BeNil())
		m, err = f.NewRedologManager(table, shard, tableConfig)
		Ω(m.(*kafkaRedoLogManager)).ShouldNot(BeNil())
	})

	ginkgo.It("NewRedologManager and close", func() {
		consumer, _ := testing.MockKafkaConsumerFunc(nil)
		f, _ := NewKafkaRedoLogManagerMaster(namespace, nil, diskStore, metaStore, consumer)
		_, err := f.NewRedologManager("table1", 0, &metaCom.TableConfig{})
		Ω(err).Should(BeNil())
		f.NewRedologManager("table1", 1, &metaCom.TableConfig{})
		f.NewRedologManager("table2", 0, &metaCom.TableConfig{})
		f.NewRedologManager("table2", 1, &metaCom.TableConfig{})
		_, err = f.NewRedologManager("table2", 1, &metaCom.TableConfig{})
		// repeat create redolog manager on same table/shard should fail
		Ω(err).ShouldNot(BeNil())

		Ω(len(f.managers)).Should(Equal(2))
		Ω(len(f.managers["table1"])).Should(Equal(2))
		Ω(len(f.managers["table2"])).Should(Equal(2))
		f.Close("table1" ,0)
		f.Close("table1", 1)
		Ω(len(f.managers)).Should(Equal(1))
		Ω(len(f.managers["table2"])).Should(Equal(2))
		f.Close("table2", 0)
		Ω(len(f.managers["table2"])).Should(Equal(1))
		f.Stop()
		Ω(len(f.managers)).Should(Equal(0))
	})
})
