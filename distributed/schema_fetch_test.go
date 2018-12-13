package distributed

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/metastore/common"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	"github.com/samuel/go-zookeeper/zk"
	"encoding/json"
	"time"
)

var _ = ginkgo.Describe("schema fetch job", func() {
	var job *SchemaFetchJob
	var mockSchemaMutator metaMocks.TableSchemaMutator
	var mockSchemaValidator metaMocks.TableSchemaValidator
	var ts *zk.TestCluster
	var conn *zk.Conn
	acls := zk.WorldACL(zk.PermAll)

	testTable1 := common.Table{
		Name: "testTable1",
		Columns: []common.Column{
			{
				Name: "col1",
				Type: "Int32",
			},
		},
		Version: 1,
	}

	testTable2 := common.Table{
		Name: "testTable2",
		Columns: []common.Column{
			{
				Name: "col1",
				Type: "Int32",
			},
		},
		Version: 2,
	}

	testTable2m := common.Table{
		Name: "testTable2",
		Columns: []common.Column{
			{
				Name: "col1",
				Type: "Int32",
			},
		},
		Version: 3,
	}

	testTable3 := common.Table{
		Name: "testTable3",
		Columns: []common.Column{
			{
				Name: "col1",
				Type: "Int32",
			},
		},
		Version: 2,
	}

	ginkgo.BeforeEach(func() {
		mockSchemaMutator = metaMocks.TableSchemaMutator{}
		mockSchemaValidator = metaMocks.TableSchemaValidator{}

		ts, _ = zk.StartTestCluster(1, nil, nil)
		conn, _, _ = ts.ConnectAll()

		job = NewSchemaFetchJob(&mockSchemaMutator, &mockSchemaValidator, "cluster1", conn)
	})

	ginkgo.It("should work", func() {
		Ω(job.schemaRootZnode).Should(Equal("/ares_controller/cluster1/schema"))
		Ω(job.clusterName).Should(Equal("cluster1"))

		conn.Create("/ares_controller", nil, int32(0), acls)
		conn.Create("/ares_controller/cluster1", nil, int32(0), acls)
		conn.Create("/ares_controller/cluster1/schema", nil, int32(0), acls)

		//                   creation     update      no-op        deletion
		// existing tables [          , testTable2, testTable3, testTable4]
		// from zk        [testTable1, testTable2m,testTable3, (deletion)]
		mockSchemaMutator.On("ListTables").Return([]string{"testTable2", "testTable3", "testTable4"}, nil).Once()
		mockSchemaMutator.On("CreateTable", mock.Anything).Return(nil).Once()
		mockSchemaMutator.On("GetTable", "testTable2").Return(&testTable2, nil).Once()
		mockSchemaMutator.On("GetTable", "testTable3").Return(&testTable3, nil).Once()
		mockSchemaMutator.On("UpdateTable", mock.Anything).Return(nil).Twice()
		mockSchemaMutator.On("DeleteTable", "testTable4").Return(nil).Once()
		mockSchemaValidator.On("SetNewTable", mock.Anything).Return(nil)
		mockSchemaValidator.On("SetOldTable", mock.Anything).Return(nil)
		mockSchemaValidator.On("Validate").Return(nil)

		for _, table := range []common.Table{testTable1, testTable2m, testTable3} {
			tableBytes, _ := json.Marshal(table)
			conn.Create("/ares_controller/cluster1/schema/"+table.Name, tableBytes, int32(0), acls)
		}

		// bootstrap
		job.FetchApplySchema(true)

		// initial fetch
		go job.Run()

		mockSchemaMutator.On("GetTable", "testTable3").Return(&testTable2, nil).Once()
		mockSchemaMutator.On("UpdateTable", mock.Anything).Return(nil).Once()

		// test schema update
		testTable3.Version=100
		tableBytes, _ := json.Marshal(testTable3)
		_,stat,_:=conn.Get("/ares_controller/cluster1/schema/"+testTable3.Name)
		conn.Set("/ares_controller/cluster1/schema/"+testTable3.Name, tableBytes, stat.Version)

		// had to wait a bit before killing the job, so the table update above have a chance to be reacted on
		time.Sleep(time.Duration(250)*time.Microsecond)
		job.Stop()
	})
})
