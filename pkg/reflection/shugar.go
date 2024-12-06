package reflection

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"reflect"
)
import "github.com/ydb-platform/ydb-go-sdk/v3/table/types"

type Sugar[T any] struct {
	Columns     []*Column
	PrimaryKeys []*Column
}

// ======================== Params for bulk upsert ========================

func (s *Sugar[T]) ToStructValue(v *T) (types.Value, error) {
	vals := make([]types.StructValueOption, 0, len(s.Columns))
	e := reflect.ValueOf(v).Elem()
	for i := range s.Columns {
		col := s.Columns[i]
		val, err := col.MakeValue(e.FieldByName(col.StructFiledName).Interface())
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	return types.StructValue(vals...), nil
}

func (s *Sugar[T]) ToListStructValues(v []T) (types.Value, error) {
	var res []types.Value
	for i := range v {
		val, err := s.ToStructValue(&v[i])
		if err != nil {
			return nil, err
		}
		res = append(res, val)
	}
	return types.ListValue(res...), nil
}

// ======================== Query parameters ========================

func (s *Sugar[T]) MakeQueryParams(v *T) (params []named.Value) {
	for i := range s.PrimaryKeys {
		params = append(params, named.Required(s.PrimaryKeys[i].Name, reflect.ValueOf(v).Elem().FieldByName(s.PrimaryKeys[i].StructFiledName).Addr().Interface()))
	}
	return
}

// ======================== Params to read query results ========================

func (s *Sugar[T]) MakeReadValue(obj *T) (v []named.Value) {
	for i := range s.Columns {
		v = append(v, named.OptionalWithDefault(s.Columns[i].Name, reflect.ValueOf(obj).Elem().Field(i).Addr().Interface()))
	}
	return
}
