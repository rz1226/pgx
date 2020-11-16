package pgx

import (
	"reflect"
)

//  bm 和map直接的转化
func (b BM) ToArray() []map[string]interface{} {
	if !b.isMany {
		return []map[string]interface{}{struct2Map(b.v)}
	}
	res := make([]map[string]interface{}, 0, 20)
	v := reflect.ValueOf(b.v).Elem()
	length := v.Len()
	for i := 0; i < length; i++ {
		res = append(res, struct2Map(v.Index(i).Interface()))
	}
	return res

}

func (b BM) ToMap() map[string]interface{} {
	if !b.isMany {
		return struct2Map(b.v)
	}
	first := reflect.ValueOf(b.v).Elem().Index(0).Interface()
	return struct2Map(first)

}

//  *struct  to map
func struct2Map(data interface{}) map[string]interface{} {
	v := reflect.ValueOf(data)
	res := make(map[string]interface{}, 10)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()
	if v.Kind() == reflect.Struct {
		for i := 0; i < v.NumField(); i++ {
			fieldName := t.Field(i).Tag.Get(Conf.TagName)
			if fieldName == "" {
				// 没找到映射tag 直接忽略
				continue
			}
			value := v.Field(i).Interface()
			res[fieldName] = value
		}
	}
	return res
}

//  map  2 *struct
//  map  2 *struct
func Array2Struct(arr []map[string]interface{}, dst interface{}) error {
	queryRes := NewQueryRes(arr, nil)
	return queryRes.ToStruct(dst)
}

func Map2Struct(m map[string]interface{}, dst interface{}) error {
	queryRes := NewQueryRes([]map[string]interface{}{m}, nil)
	return queryRes.ToStruct(dst)
}
