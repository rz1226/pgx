package pgx

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

//    query的结果是QueryRes ，本质是一个map，可以批量修改，然后QueryRes 可以转化成Struct,可单个也可以批量。
/**

这种结构体叫做BM， 业务模型
orm tag用于从map转换到自身的时候map的key和自身的feild的对应关系
auto tag用于生成update，或者insert语句   取值范围是1或者0
type Tai struct {
	Id          int64 `orm:"id" auto:"1""`
	Name          string `orm:"name22"`
	Age          int64 `orm:"age"`
	Weight float64 `orm:"weight"`
	Create_time string `orm:"create_time" auto:"1"`
}


res, err := db.Kit.Query("select * from tai where id = 4 limit 100")
u := new(Tai)
// 如果字段名不一致，自己很容易调整
res.Map(func(r map[string]interface{}){
	r["name22"] = r["name"]
})
fmt.Println(mysqlx.Map2Struct(res[0], u  ))

// 批量
var u  []*Tai
mysqlx.Map2StructBatch(res , &u  )


*/

/********************************************************************/
// 查询结果，数据结构, 可以用函数遍历,其内核是一个数组包着map， 其和普通数组map不同在于，可以用Map()遍历修改数据
type QueryRes struct {
	res []map[string]interface{}
	err error
}

func NewQueryRes(res []map[string]interface{}, err error) *QueryRes {
	q := new(QueryRes)
	q.res = res
	q.err = err
	return q
}
func (r *QueryRes) Error() error {
	return r.err
}

// 还原为数组
func (r *QueryRes) Data() []map[string]interface{} {
	return r.res
}

// 用函数遍历内部的数据, 用来修改自己本身
func (r *QueryRes) Map(f func(map[string]interface{})) {
	for _, v := range r.res {
		f(v)
	}
}

// 过滤掉一部分数据
func (r *QueryRes) Erase(f func(map[string]interface{}) bool) *QueryRes {
	newRes := make([]map[string]interface{}, 0, 10)
	for _, v := range r.res {
		if !f(v) {
			newRes = append(newRes, v)
		}
	}
	r.res = newRes
	return r
}

// 保留一部分数据
func (r *QueryRes) Keep(f func(map[string]interface{}) bool) *QueryRes {
	newRes := make([]map[string]interface{}, 0, 10)
	for _, v := range r.res {
		if f(v) {
			newRes = append(newRes, v)
		}
	}
	r.res = newRes
	return r
}

/********************************************************************/
// 执行exec   参数是*DB  or *DbTx
func (s SQL) Query(source interface{}) (*QueryRes, error) {
	res, err := queryCommon(source, s.str, s.params)
	return NewQueryRes(res, err), err
}

// 统一处理事务内，和非事务内query
func queryCommon(source interface{}, sqlStr string, args []interface{}) ([]map[string]interface{}, error) {
	if source == blankDB || source == blankDBTx {
		return nil, errors.New("query 无法进行， 请先初始化数据库")
	}

	if Conf.Log {
		fmt.Println("running....query sql = ", sqlStr, "\n args=", args)
	}

	p, ok := source.(*DB)
	if ok {
		rows, err := p.realPool.Query(sqlStr, args...)
		if err != nil {
			return nil, err
		}
		return queryResFromRows(rows)
	}
	// 多个sql事务
	t, ok := source.(*DBTx)
	if ok {
		rows, err := t.realtx.Query(sqlStr, args...)
		if err != nil {
			return nil, err
		}
		return queryResFromRows(rows)
	}
	return nil, errors.New("only support DbPool , DbTx")
}

//  scan的行为null 对应nil  数字对数字  其他对字符串 ,所以所有的字段数据类型归结为简单的几种。这可能不能处理非常规情况。
// 联表查询，如果两个表中有同名字段的时候，不会报错，会忠实的输出数据
// 另外如果数据库里是null，怎会被转换成0，空字符串，可能会影响业务逻辑，需要开发者自己注意
func queryResFromRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	defer rows.Close()
	res := make([]map[string]interface{}, 0, 100)
	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	lengthRow := len(fields)
	for {
		if result := rows.Next(); result {
			scanRes := make([]sql.Scanner, lengthRow)
			for i := 0; i < lengthRow; i++ {
				vType := columns[i].DatabaseTypeName()

				switch vType {
				case "INT", "BIGINT", "TINYINT", "MEDIUMINT":
					scanRes[i] = &sql.NullInt64{}
				case "FLOAT", "DOUBLE":
					scanRes[i] = &sql.NullFloat64{}
				case "CHAR", "VARCHAR", "TIME", "TEXT", "DECIMAL", "BLOB", "GEOMETRY", "BIT", "DATETIME", "DATE", "TIMESTAMP":
					scanRes[i] = &sql.NullString{}
				default:
					scanRes[i] = &sql.NullString{}
				}
			}
			resultData := make(map[string]interface{}, lengthRow)
			vScanRes := reflect.ValueOf(&scanRes)
			fn := reflect.ValueOf(rows.Scan)
			fnParams := make([]reflect.Value, lengthRow)
			for i := 0; i < lengthRow; i++ {
				fnParams[i] = vScanRes.Elem().Index(i)
			}
			callResult := fn.Call(fnParams)
			if callResult[0].Interface() != nil {
				return nil, callResult[0].Interface().(error)
			}
			for i := 0; i < lengthRow; i++ {
				resultData[fields[i]] = fetchFromScanner(scanRes[i])
			}
			res = append(res, resultData)
		} else {
			break
		}
	}
	return res, nil
}

// 把诸如*sql.NullXX  转化为正常的XX值，null一般转化为XX的零值
// 所以设计数据库的时候，要注意这套代码实际上是无法区分null和该字段类型的零值的
func fetchFromScanner(data sql.Scanner) interface{} {
	switch v := data.(type) { // v表示b1 接口转换成Bag对象的值
	case *sql.NullInt64:
		if v.Valid {
			return v.Int64
		}
		return int64(0)

	case *sql.NullFloat64:
		if v.Valid {
			return v.Float64
		}
		return float64(0)

	case *sql.NullString:
		if v.Valid {
			return v.String
		}
		return ""

	default:
		// 不可能会运行到这里
		return nil
	}
}
