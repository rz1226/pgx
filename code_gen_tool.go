package pgx

import (
	"fmt"
	"strings"
)

type OriTableInfo struct {
	COLUMNNAME    string `orm:"COLUMN_NAME"`
	DATATYPE      string `orm:"DATA_TYPE"`
	ISNULLABLE    string `orm:"IS_NULLABLE"`
	COLUMNCOMMENT string `orm:"COLUMN_COMMENT"`
	EXTRA         string `orm:"EXTRA"`
}

type TableInfo struct {
	Field    string
	DataType string
	Tag      string
	Comment  string
}

// 辅助根据表结构生成 bm

func GetBmStrFromTable(dbKit *DB, dbName, tableName string) string {
	tableInfos := getTableInfos(dbKit, dbName, tableName)

	str := ""
	for _, v := range tableInfos {
		str += pad(v.Field, 30) + " "
		str += pad(v.DataType, 12) + " "
		str += pad(v.Tag, 20) + " // " + v.Comment

		str += "\n"
	}
	str += "\n\n\n\n 建表语句=\n"
	str += getTableCreateSQL(dbKit, dbName+"."+tableName)
	return str
}

func getTableCreateSQL(dbKit *DB, tableName string) string {
	sql := SQLStr("show create table  " + tableName)
	res, err := sql.Query(dbKit)

	if err != nil {
		fmt.Println("getTableCreateSql err:", err)
		return ""
	}
	str, err := res.ToStringByField("Create Table")

	if err != nil {
		fmt.Println("getTableCreateSql err:", err)
		return ""
	}
	return str
}

func getTableInfos(dbKit *DB, dbName, tableName string) []*TableInfo {
	str := "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE,  COLUMN_COMMENT, EXTRA FROM INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = ? AND table_name = ? order by ordinal_position asc"
	sql := SQLStr(str).AddParams(dbName, tableName)

	res, err := sql.Query(dbKit)
	if err != nil {
		fmt.Println("getFieldInfoStr err: ", err)
		return nil
	}
	var oriTableInfo []*OriTableInfo
	err = res.ToStruct(&oriTableInfo)
	if err != nil {
		fmt.Println("getFieldInfoStr err: ", err)
		return nil
	}

	tableInfos := make([]*TableInfo, 0, len(oriTableInfo))
	for _, v := range oriTableInfo {
		tableInfo := &TableInfo{}

		columnName := strings.ToUpper(v.COLUMNNAME[0:1]) + v.COLUMNNAME[1:]
		tableInfo.Field = columnName

		dataType := ""
		switch strings.ToUpper(v.DATATYPE) {
		case "INT", "BIGINT", "TINYINT", "MEDIUMINT":
			dataType = "int64"
		case "FLOAT", "DOUBLE", "DECIMAL":
			dataType = "float64"
		case "CHAR", "VARCHAR", "TIME", "TEXT", "BLOB", "GEOMETRY", "BIT", "DATETIME", "DATE", "TIMESTAMP":
			dataType = "string"
		default:
			dataType = v.DATATYPE
		}
		tableInfo.DataType = dataType

		strTag := "`orm:\"" + v.COLUMNNAME + "\""
		if v.EXTRA == "auto_increment" {
			strTag += " auto:\"1\""
		}
		strTag += "`"
		tableInfo.Tag = strTag
		tableInfo.Comment = v.COLUMNCOMMENT
		tableInfos = append(tableInfos, tableInfo)
	}
	return tableInfos
}

func pad(str string, lenAll int) string {
	length := len(str)
	if length >= lenAll {
		lenAll = length
	}
	if lenPad := lenAll - length; lenPad > 0 {
		return str + strings.Repeat(" ", lenPad)
	}

	return str

}
