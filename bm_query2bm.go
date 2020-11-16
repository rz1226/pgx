package pgx

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// 取第一条数据的某个字段  如果field为空，取第一个
func (q *QueryRes) toInterfaceByField(field string) (interface{}, error) {
	if q.err != nil {
		return nil, q.err
	}
	data := q.Data()
	if len(data) == 0 {
		return nil, errors.New("empty value")
	}
	firstData := data[0]
	for k, v := range firstData {
		if field == "" {
			return v, nil
		} else if k == field {
			return v, nil
		}
	}
	return nil, errors.New("can not find value")
}

//  这种queryRes通常只有一个字段，取出string
func (q *QueryRes) ToString() (string, error) {
	value, err := q.toInterfaceByField("")
	if err != nil {
		return "", err
	}
	vData, ok := value.(string)
	if ok {
		return vData, nil
	}
	return fmt.Sprint(vData), nil

}

func (q *QueryRes) ToInt64() (int64, error) {
	value, err := q.toInterfaceByField("")
	if err != nil {
		return 0, err
	}
	valueInt64, err := strconv.ParseInt(fmt.Sprint(value), 10, 64)
	if err != nil {
		return 0, errors.New("err ToInt64 error:数据库里的字段不是int64类型")
	}
	return valueInt64, nil

}
func (q *QueryRes) ToFloat64() (float64, error) {
	value, err := q.toInterfaceByField("")
	if err != nil {
		return 0, err
	}
	valueF64, err := strconv.ParseFloat(fmt.Sprint(value), 64)
	if err != nil {
		return 0, errors.New("err ToFloat64 error:数据库里的字段不是float64类型")
	}
	return valueF64, nil
}
func (q *QueryRes) ToStringByField(field string) (string, error) {
	value, err := q.toInterfaceByField(field)
	if err != nil {
		return "", err
	}
	vData, ok := value.(string)
	if ok {
		return vData, nil
	}
	return fmt.Sprint(vData), nil

}

func (q *QueryRes) ToInt64ByField(field string) (int64, error) {
	value, err := q.toInterfaceByField(field)
	if err != nil {
		return 0, err
	}
	valueInt64, err := strconv.ParseInt(fmt.Sprint(value), 10, 64)
	if err != nil {
		return 0, errors.New("err ToInt64 error:数据库里的字段不是int64类型")
	}
	return valueInt64, nil

}
func (q *QueryRes) ToFloat64ByField(field string) (float64, error) {
	value, err := q.toInterfaceByField(field)
	if err != nil {
		return 0, err
	}
	valueF64, err := strconv.ParseFloat(fmt.Sprint(value), 64)
	if err != nil {
		return 0, errors.New("err ToFloat64 error:数据库里的字段不是float64类型")
	}
	return valueF64, nil
}

// 多个，把queryRes里的数据转化成*[]*struct  或者  *struct ，适应两种格式，单个和多个
func (q *QueryRes) ToStruct(dstStructs interface{}) error {
	if q.err != nil {
		return q.err
	}
	data := q.Data()
	if len(data) == 0 {
		return errors.New("empty")
	}
	isMany, err := isBmMany(dstStructs)
	if err != nil {
		return err
	}
	if isMany {
		return queryRes2StructBatch(data, dstStructs, nil)
	}
	return queryRes2Struct(data[0], dstStructs, nil)
}

//  map数据映射到struct, 同名映射到struct的key不区分大小写，检查类型  。dstStruct是接收数据的struct的指针
// 可以加多个struct接收数据, 一般只支持 整数，浮点，字符串
//  第二个参数是struct 指针
func queryRes2Struct(sourceData map[string]interface{}, dstStruct interface{}, f func(map[string]interface{})) error {
	if f != nil {
		f(sourceData)
	}
	err := structFromQueryRes(sourceData, dstStruct)
	if err != nil {
		return err
	}
	return nil
}

// 第二个参数是&[]*SomeStruct
func queryRes2StructBatch(sourceDatas []map[string]interface{}, dstStructs interface{}, f func(map[string]interface{})) error {

	strusRV := reflect.Indirect(reflect.ValueOf(dstStructs))
	if strings.Contains(fmt.Sprint(strusRV), "<invalid reflect.Value>") {
		return errors.New("invalid reflect.Value , 应该把struct集合的类型声明为[]*SomeStruct,然后调用这里的时候加&")
	}

	elemRT := strusRV.Type().Elem()
	for _, v := range sourceDatas {
		eleData := reflect.New(elemRT.Elem()).Interface()
		err := queryRes2Struct(v, eleData, f)
		if err != nil {
			return err
		}
		strusRV = reflect.Append(strusRV, reflect.ValueOf(eleData))
	}
	reflect.Indirect(reflect.ValueOf(dstStructs)).Set(strusRV)
	return nil
}

// only support int64, float64, string, []byte
func structFromQueryRes(sourceData map[string]interface{}, dstStruct interface{}) (resErr error) {
	// 当前处理到哪个key了。panic返回报错用的
	currentField := ""
	defer func() {
		if co := recover(); co != nil {
			errStr := "发生panic, field=" + currentField + ":" + fmt.Sprint(co)
			if strings.Contains(fmt.Sprint(co), "reflect.Value.NumField on zero Value") {
				errStr += " 提示，如果是单个struct数据转化，要先用new(X)初始化，而不是只有var X 声明"
			}

			resErr = errors.New(errStr)
		}
	}()
	length := len(sourceData)
	if length <= 0 {
		return errors.New("no sourceData ,len zero ")
	}
	v := reflect.ValueOf(dstStruct)
	t := v.Type().Elem()
	switch v.Kind() {
	case reflect.Ptr:
		for i := 0; i < v.Elem().NumField(); i++ {
			key := t.Field(i).Tag.Get(Conf.TagName)
			if key == "" {
				// 找不到业务模型struct的数据库映射tag,忽略
				continue
			}
			currentField = key
			valueFromMap, ok := sourceData[key]
			if !ok {
				continue
			}
			vType := t.Field(i).Type
			switch vType.Name() {
			case "int64":
				valueInt64, ok := valueFromMap.(int64)

				if !ok {
					if valueStr, ok := valueFromMap.(string); ok {
						valueInt64New, err := strconv.ParseInt(valueStr, 10, 64)
						if err == nil {
							v.Elem().Field(i).Set(reflect.ValueOf(valueInt64New))
						}
					} else {
						return errors.New("field " + key + " can not store as integer , is " + fmt.Sprint(reflect.TypeOf(valueFromMap)))
					}

				} else {
					v.Elem().Field(i).Set(reflect.ValueOf(valueInt64))
				}

			case "float64":
				valueF64, ok := valueFromMap.(float64)
				if !ok {
					if valueStr, ok := valueFromMap.(string); ok {
						// decimal在这里可以转化为f64
						valueF64New, err := strconv.ParseFloat(valueStr, 64)
						if err == nil {
							v.Elem().Field(i).Set(reflect.ValueOf(valueF64New))
						}
					} else {
						return errors.New("field " + key + " can not store as float ,is " + fmt.Sprint(reflect.TypeOf(valueFromMap)))
					}

				} else {
					v.Elem().Field(i).Set(reflect.ValueOf(valueF64))
				}

			case "string":
				valueString, ok := valueFromMap.(string)
				if !ok {
					// 如果不是string类型，就强制转化
					// 处理nil, 当用不是本库从数据库生成的数据转化的时候，可能有nil的问题，
					if valueFromMap == nil {
						valueString = ""
					} else {
						valueString = fmt.Sprint(valueFromMap)
					}

				}
				v.Elem().Field(i).Set(reflect.ValueOf(valueString))
			default:
				return errors.New("only support int64, float64, string  ")
			}
		}
		return nil
	default:
		return errors.New("only support struct pointer")
	}
}
