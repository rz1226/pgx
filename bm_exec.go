package pgx

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

//  如果想要灵活的生成insert sql, 不应该用struct去生成，因为这里面用到了struct的tag，field，这些是无法通过直接代码改变的。
//  从struct改为一种容易直接修改的数据结构Line , Lines来生成sql。
// {field = name, auto : 1 , value = xx}, ...
//  一个拍扁了的orm struct
type Field struct {
	BMField string      // bm struct字段名
	Name    string      //  数据库字段名
	IsAuto  bool        // 是否类似auto_increment, 或者create_time 不需要手动设置的数据
	Value   interface{} // 值
}
type Lines []Line

func (l Line) Map(fieldName string, f func(interface{}) interface{}) {
	for _, v := range l {
		if v.BMField == fieldName {
			v.Value = f(v.Value)
		}
	}
}

func (l Lines) Map(fieldName string, f func(interface{}) interface{}) {
	for _, v := range l {
		field, err := v.GetField(fieldName)
		if err == nil {
			if field.BMField == fieldName {
				field.Value = f(field.Value)
			}
		}
	}
}
func (l Lines) ToSQLInsert(tableName string) *SQL {
	sqlstr, params := sqlFromLinesForInsert(l, tableName)
	res := SQLStr(sqlstr).AddParams(params...)
	return &res
}

//  key 是字段名
type Line []*Field

func (l Line) ToSQLInsert(tableName string) *SQL {
	sqlstr, params := sqlFromLineForInsert(l, tableName)
	sql := SQLStr(sqlstr).AddParams(params...)
	return &sql
}
func (l Line) ToSQLUpdate(tableName, condition string, updateFields map[string]int) *SQL {
	sqlstr, params := sqlFromLineForUpdate(l, tableName, condition, updateFields)
	sql := SQLStr(sqlstr).AddParams(params...)
	return &sql
}
func (l Line) GetField(field string) (*Field, error) {
	for _, v := range l {
		if v.BMField == field {
			return v, nil
		}
	}
	return nil, errors.New("not found field :" + field)
}
func (l Line) Show() {
	for k, v := range l {
		fmt.Println("field index=", k, " v=", *v)
	}
}
func (l Lines) Show() {
	for k, v := range l {
		fmt.Println("line index:", k)
		v.Show()
	}
}

/**
这个结构体叫做 业务模型 BM
type Tai struct {
	Id          int64 `orm:"id" auto:"1""`
	Name          string `orm:"name22"`
	Age          int64 `orm:"age"`
	Weight float64 `orm:"weight"`
	Create_time string `orm:"create_time" auto:"1"`
}

*/
//  参数类型直接是struct or 其指针
func lineFromBM(sourceStruct interface{}) (Line, error) {
	v := reflect.ValueOf(sourceStruct)
	res := make([]*Field, 0, 20)
	t := v.Type()
	switch v.Kind() {
	case reflect.Ptr:
		return lineFromBM(v.Elem().Interface())
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			fd := &Field{}
			bmFieldName := t.Field(i).Name
			fieldName := t.Field(i).Tag.Get(Conf.TagName)
			if fieldName == "" {
				// 没找到映射tag 直接忽略
				continue
			}
			value := v.Field(i).Interface()
			fd.Value = value
			fd.Name = fieldName
			fd.BMField = bmFieldName
			fd.IsAuto = Conf.FuncAuto(t.Field(i).Tag)
			res = append(res, fd)
		}
	default:
		return res, errors.New("err LineFromBM 参数1 必须是struct或者其指针")
	}
	return res, nil
}

// 上面函数的批量方法
//  参数是*[]*struct
func linesFromBM(sourceStructArray interface{}) (Lines, error) {
	res := make([]Line, 0, 100)
	v := reflect.ValueOf(sourceStructArray)

	switch v.Kind() {
	case reflect.Ptr:
		return linesFromBM(v.Elem().Interface())
	case reflect.Slice:
		length := v.Len()
		for i := 0; i < length; i++ {
			ele := v.Index(i).Interface()
			line, err := lineFromBM(ele)
			if err != nil {
				return res, err
			}
			res = append(res, line)
		}
	}
	return res, nil
}

// auto代表插入的时候不管  影响生成insert语句
// type User struct{
// 	Id int64 `orm:"id" auto:"1"`
// 	Phone string `orm:"phone"`
// 	Passwd string `orm:"passwd"
// 	CreateTime string `orm:"create_time" auto:"1"`
// }

//  1 insertFieldList , 2 insert ? 占位符 3 insert parmas 4 update语句+占位符
//  如果想要灵活的生成insert sql, 不应该用struct去生成，因为这里面用到了struct的tag，field，这些是无法通过直接代码改变的。
//  所以这里要变，把第一个参数从struct改为一种容易直接修改的数据结构。
// 第二个参数是过滤字段，如果是nil不过滤，如果key有值，表示只处理标记的字段，其他的无视,fields只有键有用，值无视
func sqlFromLine(d Line, fields map[string]int) (strFieldList, strInsert string, dataParams []interface{}, strUpdate string) {
	insertFieldList := "("
	insertMarksStr := "("
	updateStr := ""
	insertValuesSli := make([]interface{}, 0, 30)
	// type lineData map[string]*fieldData
	for _, v := range d {
		// 略过过滤
		lengthOfFields := len(fields)
		if lengthOfFields > 0 {
			if _, ok := fields[v.Name]; !ok {
				continue
			}
		}

		auto := v.IsAuto
		if !auto {
			insertFieldList += "`" + v.Name + "`" + ","
			insertMarksStr += "?,"
			insertValuesSli = append(insertValuesSli, v.Value)
			updateStr += "`" + v.Name + "` = ?" + ","
		}
	}
	return strings.TrimRight(insertFieldList, ",") + ")",
		strings.TrimRight(insertMarksStr, ",") + ")",
		insertValuesSli, strings.TrimRight(updateStr, ",")
}

// 生成一个insert语句
func sqlFromLineForInsert(d Line, tableName string) (strSQL string, params []interface{}) {
	insertFields, insertMarks, insertParams, _ := sqlFromLine(d, nil)
	insertSQL := "insert into " + tableName + "  " + insertFields + " values " + insertMarks
	return insertSQL, insertParams
}

// 生成一个update语句
func sqlFromLineForUpdate(d Line, tableName, condition string, updateFields map[string]int) (strSQL string, params []interface{}) {
	_, _, insertParams, updateStr := sqlFromLine(d, updateFields)
	var insertSQL string
	if strings.Trim(condition, " ") == "" {
		insertSQL = "update " + tableName + " set " + updateStr
	} else {
		insertSQL = "update " + tableName + " set " + updateStr + " where " + condition
	}

	return insertSQL, insertParams
}

func sqlFromLinesForInsert(d Lines, tableName string) (strSQL string, params []interface{}) {
	length := len(d)
	if length == 0 {
		return "", nil
	}
	var marksBuf bytes.Buffer
	insertFields, insertMarks, insertParams, _ := sqlFromLine(d[0], nil)
	marksBuf.WriteString(insertMarks)
	marksBuf.WriteString(",")
	for i := 1; i < length; i++ {
		_, marks, params, _ := sqlFromLine(d[i], nil)
		marksBuf.WriteString(marks)
		marksBuf.WriteString(",")
		insertParams = append(insertParams, params...)
	}
	insertSQL := "insert into " + tableName + "  " + insertFields + " values " + strings.Trim(marksBuf.String(), ",")
	return insertSQL, insertParams
}
