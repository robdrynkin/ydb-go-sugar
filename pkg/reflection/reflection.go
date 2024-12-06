package reflection

import (
	"fmt"
	"reflect"
	"strings"
)

func NewSugar[T any]() (Sugar[T], error) {
	t := reflect.TypeOf((*T)(nil)).Elem()
	s := Sugar[T]{}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		meta := field.Tag.Get("ydb")
		vals := strings.Split(meta, ",")
		if len(vals) < 2 {
			return s, fmt.Errorf("ydb tag must contain column name and type")
		}
		column := Column{
			Name:            vals[0],
			Type:            vals[1],
			StructFiledName: field.Name,
		}
		if len(vals) > 2 {
			for j := 2; j < len(vals); j++ {
				if vals[j] == "pk" {
					s.PrimaryKeys = append(s.PrimaryKeys, &column)
				}
			}
		}

		s.Columns = append(s.Columns, &column)
	}
	return s, nil
}
