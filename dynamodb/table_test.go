package dynamodb_test

import (
	"flag"
	"github.com/crowdmob/goamz/dynamodb"
	"reflect"
	"testing"
)

var local = flag.Bool("local", false, "Enable tests against DynamoDB local")

func skipIfDisable(t *testing.T) {
	if !*local {
		t.Skip("DynamoDB local tests not enabled")
	}
}

func TestCreateTable(t *testing.T) {
	skipIfDisable(t)
	if err := initializeTable(); err != nil {
		t.Fatal(err)
	}

	status, err := dummy_server.CreateTable(dummy_tdesc)
	if err != nil {
		t.Fatal(err)
	}
	if status != "ACTIVE" && status != "CREATING" {
		t.Fatal("Expect status to be ACTIVE or CREATING")
	}
}

func TestListTables(t *testing.T) {
	skipIfDisable(t)
	if err := initializeAndCreateTable(); err != nil {
		t.Fatal(err)
	}

	tables, err := dummy_server.ListTables()
	if err != nil {
		t.Fatal(err)
	}

	if len(tables) != 1 {
		t.Fatal("Expected table to be returned")
	}
	for _, tb := range tables {
		if tb != "DynamoDBTestMyTable" {
			t.Fatal("Expect table is DynamoDBTestMyTable")
		}
	}
}

func TestPutItem(t *testing.T) {
	skipIfDisable(t)
	if err := initializeAndCreateTable(); err != nil {
		t.Fatal(err)
	}

	attrs := []dynamodb.Attribute{
		*dynamodb.NewStringAttribute("Attr1", "ATTR1VAL"),
	}
	if ok, err := dummy_table.PutItem("NewHashKey", "1", attrs); !ok {
		t.Fatal(err)
	}
}

func TestGetItem(t *testing.T) {
	skipIfDisable(t)
	initializeTable()

	tdesc := dynamodb.TableDescriptionT{
		TableName: dummy_tname,
		AttributeDefinitions: []dynamodb.AttributeDefinitionT{
			dynamodb.AttributeDefinitionT{"TestHashKey", "S"},
		},
		KeySchema: []dynamodb.KeySchemaT{
			dynamodb.KeySchemaT{"TestHashKey", "HASH"},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughputT{
			ReadCapacityUnits: 1,
			WriteCapacityUnits: 1,
		},
	}
	if _, err := dummy_server.CreateTable(tdesc); err != nil {
		t.Fatal(err)
	}

	primary := dynamodb.NewStringAttribute("TestHashKey", "")
	pk := dynamodb.PrimaryKey{primary, nil}
	table := dummy_server.NewTable(dummy_tname, pk)

	// Put
	attrs := []dynamodb.Attribute{
		*dynamodb.NewStringAttribute("Attr1", "ATTR1VAL"),
	}
	if ok, err := table.PutItem("NewHashKey", "", attrs); !ok {
		t.Fatal(err)
	}

	item, err := table.GetItem(&dynamodb.Key{HashKey: "NewHashKey"})
	if err != nil {
		t.Fatal(err)
	}

	expected := map[string]*dynamodb.Attribute{
		"TestHashKey":  dynamodb.NewStringAttribute("TestHashKey", "NewHashKey"),
		"Attr1":        dynamodb.NewStringAttribute("Attr1", "ATTR1VAL"),
	}
	if !reflect.DeepEqual(expected, item) {
		t.Fatalf("Expect an item to be deeply equal. expected: %v, actual: %v", expected, item)
	}
}

func TestGetItemRange(t *testing.T) {
	skipIfDisable(t)
	if err := initializeAndCreateTable(); err != nil {
		t.Fatal(err)
	}

	// Put
	attrs := []dynamodb.Attribute{
		*dynamodb.NewStringAttribute("Attr1", "ATTR1VAL"),
	}
	if ok, err := dummy_table.PutItem("NewHashKey", "1", attrs); !ok {
		t.Fatal(err)
	}

	pk := &dynamodb.Key{
		HashKey:  "NewHashKey",
		RangeKey: "1",
	}
	item, err := dummy_table.GetItem(pk)
	if err != nil {
		t.Fatal(err)
	}

	expected := map[string]*dynamodb.Attribute{
		"TestHashKey":  dynamodb.NewStringAttribute("TestHashKey", "NewHashKey"),
		"TestRangeKey": dynamodb.NewNumericAttribute("TestRangeKey", "1"),
		"Attr1":        dynamodb.NewStringAttribute("Attr1", "ATTR1VAL"),
	}
	if !reflect.DeepEqual(expected, item) {
		t.Fatalf("Expect an item to be deeply equal. expected: %v, actual: %v", expected, item)
	}
}
