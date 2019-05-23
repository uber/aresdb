package imports

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/testing"
	metaCom "github.com/uber/aresdb/metastore/common"
)


var _ = ginkgo.Describe("redolog manager factory tests", func() {

	ginkgo.It("NewRedologManagerFactory", func() {
		// nil config should work as before
		f, err := NewRedologManagerFactory(nil, nil, nil)
		Ω(err).Should(BeNil())
		Ω(f).ShouldNot(BeNil())
		Ω(f.enableLocalRedoLog).Should(BeTrue())

		// empty config should work too
		c := &common.ImportsConfig{}
		f, err = NewRedologManagerFactory(c, nil, nil)
		Ω(err).Should(BeNil())
		Ω(f).ShouldNot(BeNil())
		Ω(f.enableLocalRedoLog).Should(BeTrue())

		// not kafka but disabled redolog, should work and enable redolog
		c = &common.ImportsConfig{
			Source: "others",
			RedoLog: common.RedoLogConfig{
				Disabled: true,
			},
			KafkaConfig: common.KafkaConfig{
				Brokers: []string{},
			},
		}
		f, err = NewRedologManagerFactory(c, nil, nil)
		Ω(err).Should(BeNil())

		// kafka configured and disabled redolog, wrong config
		c = &common.ImportsConfig{
			Source: common.KafkaSoureOnly,
			RedoLog: common.RedoLogConfig{
				Disabled: true,
			},
			KafkaConfig: common.KafkaConfig{
				Brokers: []string{},
			},
		}
		consumer, _ := testing.MockKafkaConsumerFunc(nil)
		f, err = NewKafkaRedologManagerFactory(c, nil, nil, consumer)
		Ω(err).ShouldNot(BeNil())
		Ω(err.Error()).Should(ContainSubstring("No kafka broker"))

		// kafka configured and disabled redolog, will fail on create consumer
		c = &common.ImportsConfig{
			Source: common.KafkaSoureOnly,
			RedoLog: common.RedoLogConfig{
				Disabled: true,
			},
			KafkaConfig: common.KafkaConfig{
				Brokers: []string{
					"host1",
					"host2",
				},
			},
		}
		f, err = NewKafkaRedologManagerFactory(c, nil, nil, consumer)
		Ω(err).Should(BeNil())
		Ω(f).ShouldNot(BeNil())
		Ω(f.enableLocalRedoLog).Should(BeFalse())
		Ω(f.consumer).ShouldNot(BeNil())

		// kafka configured and disabled redolog, will fail on create consumer
		c = &common.ImportsConfig{
			Source: common.KafkaSoureOnly,
			RedoLog: common.RedoLogConfig{
				Disabled: false,
			},
			KafkaConfig: common.KafkaConfig{
				Brokers: []string{
					"host1",
					"host2",
				},
			},
		}
		f, err = NewKafkaRedologManagerFactory(c, nil, nil, consumer)
		Ω(err).Should(BeNil())
		Ω(f).ShouldNot(BeNil())
		Ω(f.enableLocalRedoLog).Should(BeTrue())
		Ω(f.consumer).ShouldNot(BeNil())
	})

	ginkgo.It("NewRedologManager and close", func() {
		f, _ := NewRedologManagerFactory(nil, nil, nil)
		m1, err := f.NewRedologManager("table1", 0, &metaCom.TableConfig{}, nil)
		Ω(err).Should(BeNil())
		m2, _ := f.NewRedologManager("table1", 1, &metaCom.TableConfig{}, nil)
		f.NewRedologManager("table2", 0, &metaCom.TableConfig{}, nil)
		f.NewRedologManager("table2", 1, &metaCom.TableConfig{}, nil)
		Ω(len(f.managers)).Should(Equal(2))
		Ω(len(f.managers["table1"])).Should(Equal(2))
		Ω(len(f.managers["table2"])).Should(Equal(2))
		m1.Close()
		m2.Close()
		Ω(len(f.managers)).Should(Equal(1))
		Ω(len(f.managers["table2"])).Should(Equal(2))
		f.Close("table2", 0)
		Ω(len(f.managers["table2"])).Should(Equal(1))
		f.Stop()
		Ω(len(f.managers)).Should(Equal(0))
	})
})
