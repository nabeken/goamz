package dynamodb_test

import (
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/dynamodb"
)

var dynamodb_local_region = aws.Region{
	DynamoDBEndpoint: "http://127.0.0.1:8000",
}

var dummy_auth = aws.Auth{
	AccessKey: "DUMMY_KEY",
	SecretKey: "EXAMPLE_KEY",
}

var dummy_server = dynamodb.Server{dummy_auth, dynamodb_local_region}

var dummy_tname = "DynamoDBTestMyTable"
var dummy_tdesc = dynamodb.TableDescriptionT{
	TableName: dummy_tname,
	AttributeDefinitions: []dynamodb.AttributeDefinitionT{
		dynamodb.AttributeDefinitionT{"TestHashKey", "S"},
		dynamodb.AttributeDefinitionT{"TestRangeKey", "N"},
	},
	KeySchema: []dynamodb.KeySchemaT{
		dynamodb.KeySchemaT{"TestHashKey", "HASH"},
		dynamodb.KeySchemaT{"TestRangeKey", "RANGE"},
	},
	ProvisionedThroughput: dynamodb.ProvisionedThroughputT{
		ReadCapacityUnits: 1,
		WriteCapacityUnits: 1,
	},
}

var dummy_tpk = dynamodb.PrimaryKey{
	dynamodb.NewStringAttribute("TestHashKey", ""),
	dynamodb.NewNumericAttribute("TestRangeKey", ""),
}

var dummy_table = dummy_server.NewTable(dummy_tname, dummy_tpk)

func createTable() error {
	_, err := dummy_server.CreateTable(dummy_tdesc)
	return err
}

func initializeTable() error {
	tables, err := dummy_server.ListTables()
	if err != nil {
		return err
	}
	for _, t := range tables {
		if _, err := dummy_server.DeleteTable(dynamodb.TableDescriptionT{TableName: t}); err != nil {
			return err
		}
	}
	return err
}

func initializeAndCreateTable() error {
	err := initializeTable()
	if err != nil {
		return err
	}
	return createTable()
}
