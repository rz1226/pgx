package pgx

import (
	"fmt"
	"strings"
)

type TableName string

func NewTableName(name string) TableName {
	if strings.Contains(name, ".") {
		return TableName(name)
	}
	fmt.Println("table name must contains point [ . ]")
	return "."
}

func (n TableName) ShowInfo(dbKit *DB) {

	arr := strings.Split(string(n), ".")
	dbName := arr[0]
	tableName := arr[1]

	str := GetBmStrFromTable(dbKit, dbName, tableName)
	fmt.Println(str)
}
