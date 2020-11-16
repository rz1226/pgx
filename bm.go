package pgx

import (
	"errors"
	"log"
	"reflect"
)

/*

bm is business model  ， is struct
   成员只支持三种类型  int64 float64 string
type User struct{
	Id int64 `orm:"id" auto:"1"`
	Name string `orm:"name"`
	Pass string `orm:"pass"`
	Weight float64 `orm:"weight"`
}
*/

type ChangeForInsert struct {
	fieldName string
	f         func(data interface{}) interface{}
}

func NewChangeForInsert(fieldName string, f func(data interface{}) interface{}) *ChangeForInsert {
	c := new(ChangeForInsert)
	c.fieldName = fieldName
	c.f = f
	return c
}

type BM struct {
	v                interface{} //  实际上是一个*struct, 或者*[]*struct
	isMany           bool
	changeForInserts []*ChangeForInsert
}

func NewBM(data interface{}) *BM {
	isMany, err := isBmMany(data)
	if err != nil {
		log.Fatal(err.Error())
	}
	bm := new(BM)
	bm.v = data
	bm.changeForInserts = make([]*ChangeForInsert, 0, 2)
	bm.isMany = isMany
	return bm
}

//  加入可能在生成 nsert语句的时候，要改变值的东西
func (b *BM) ChangeForInsert(changeForInsert *ChangeForInsert) *BM {
	b.changeForInserts = append(b.changeForInserts, changeForInsert)
	return b
}

//  取出数据
func (b *BM) Data() interface{} {
	return b.v
}

func (b *BM) ToSQLUpdate(tableName string, updateFields map[string]int, condition string) (*SQL, error) {
	if !b.isMany {
		l, err := lineFromBM(b.Data())
		if err != nil {
			return nil, err
		}
		return l.ToSQLUpdate(tableName, condition, updateFields), nil
	}
	return nil, errors.New("只能生成单条数据的update")
}

//  使用单个和多个条目
//  第二个参数是, 用来改变生成的sql语句的值，例如有一个字段类型是datetime,值是空，就插不进去，改成nil对应数据库的NULL

func (b *BM) ToSQLInsert(tableName string) (*SQL, error) {
	if !b.isMany {
		l, err := lineFromBM(b.Data())
		if err != nil {
			return nil, err
		}
		//  是否设置了changeforInsert
		if len(b.changeForInserts) > 0 {
			for _, v := range b.changeForInserts {
				l.Map(v.fieldName, v.f)
			}
		}
		return l.ToSQLInsert(tableName), nil
	}
	lines, err := linesFromBM(b.Data())
	if err != nil {
		return nil, err
	}
	if len(b.changeForInserts) > 0 {
		for _, v := range b.changeForInserts {
			lines.Map(v.fieldName, v.f)
		}
	}
	return lines.ToSQLInsert(tableName), nil
}

//  判断struct是单数还是复数，只有两种格式是对的*struct,  *[]*struct
func isBmMany(data interface{}) (bool, error) {
	v := reflect.TypeOf(data)
	return _isBmMany(v)
}

func _isBmMany(v reflect.Type) (bool, error) {
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	} else {
		return false, errors.New("struct bm只有两种形式是对的，*struct,  *[]*struct")
	}
	switch v.Kind() {
	case reflect.Slice:
		res, err := _isBmMany(v.Elem())
		if err != nil {
			return false, err
		}
		if !res {
			return true, nil
		}
		return false, errors.New("struct bm只有两种形式是对的，*struct,  *[]*struct")

	case reflect.Struct:
		return false, nil

	}
	return false, errors.New("struct bm只有两种形式是对的，*struct,  *[]*struct")
}
