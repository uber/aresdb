package distributed

import (
	"github.com/onsi/ginkgo"
	"github.com/stretchr/testify/mock"
	clientsMocks "github.com/uber/aresdb/clients/mocks"
	"github.com/uber/aresdb/metastore/common"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
)

var _ = ginkgo.Describe("schema fetch job", func() {
	var job *SchemaFetchJob
	var mockSchemaMutator metaMocks.TableSchemaMutator
	var mockSchemaValidator metaMocks.TableSchemaValidator
	var mockControllerCli clientsMocks.ControllerClient

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
		mockControllerCli = clientsMocks.ControllerClient{}

		job = NewSchemaFetchJob(1, &mockSchemaMutator, &mockSchemaValidator, &mockControllerCli, "cluster1", "123")
	})

	ginkgo.It("should work with no schema changes", func() {
		mockControllerCli.On("GetSchemaHash", "cluster1").Return("123", nil)
		job.FetchSchema()
	})

	ginkgo.It("should work with schema changes", func() {
		mockControllerCli.On("GetSchemaHash", "cluster1").Return("456", nil).Once()
		mockControllerCli.On("GetAllSchema", "cluster1").Return([]common.Table{testTable1, testTable2m, testTable3}, nil).Once()
		//                   creation     update      no-op        deletion
		// existing tables [          , testTable2, testTable3, testTable4]
		// from controller [testTable1, testTable2m,testTable3, (deletion)]
		mockSchemaMutator.On("ListTables").Return([]string{"testTable2", "testTable3", "testTable4"}, nil).Once()
		mockSchemaMutator.On("CreateTable", mock.Anything).Return(nil).Once()
		mockSchemaMutator.On("GetTable", "testTable2").Return(&testTable2, nil).Once()
		mockSchemaMutator.On("GetTable", "testTable3").Return(&testTable3, nil).Once()
		mockSchemaMutator.On("UpdateTable", mock.Anything).Return(nil).Once()
		mockSchemaMutator.On("DeleteTable", "testTable4").Return(nil).Once()
		mockSchemaValidator.On("SetNewTable", mock.Anything).Return(nil)
		mockSchemaValidator.On("SetOldTable", mock.Anything).Return(nil)
		mockSchemaValidator.On("Validate").Return(nil)
		job.FetchSchema()
	})
})
