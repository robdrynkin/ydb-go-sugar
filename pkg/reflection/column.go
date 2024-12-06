package reflection

import (
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"reflect"
)

type Column struct {
	Name            string
	Type            string
	StructFiledName string
}

// Value for bulk upsert

func (c Column) MakeValue(v interface{}) (types.StructValueOption, error) {
	var value types.Value
	switch c.Type {
	case "int32":
		value = types.Int32Value(v.(int32))
	case "uint64":
		value = types.Uint64Value(v.(uint64))
	case "string":
		value = types.BytesValue(v.([]byte))
	default:
		return nil, fmt.Errorf("unsupported type %s", c.Type)
	}
	return types.StructFieldValue(c.Name, value), nil
}

// Query parameters

func (c Column) MakeValueParam(v interface{}) (table.ParameterOption, error) {
	var value types.Value
	switch c.Type {
	case "int32":
		value = types.Int32Value(v.(int32))
	case "uint64":
		value = types.Uint64Value(v.(uint64))
	case "string":
		value = types.BytesValue(v.([]byte))
	default:
		return nil, fmt.Errorf("unsupported type %s", c.Type)
	}
	return table.ValueParam(c.Name, value), nil
}

func MakeQueryParams(v interface{}, columns []*Column) (*table.QueryParameters, error) {
	params := make([]table.ParameterOption, 0, len(columns))
	for i := range columns {
		param, err := columns[i].MakeValueParam(reflect.ValueOf(v).Elem().FieldByName(columns[i].StructFiledName).Interface())
		if err != nil {
			return nil, err
		}
		params = append(params, param)
	}
	return table.NewQueryParameters(params...), nil
}
