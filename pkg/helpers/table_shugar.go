package helpers

import "github.com/robdrynkin/ydb-go-sugar/pkg/reflection"

type TableSugar[T any] struct {
	reflection.Sugar[T]
	TableName string
}

func NewTableSugar[T any](tableName string) (TableSugar[T], error) {
	sugar, err := reflection.NewSugar[T]()
	if err != nil {
		return TableSugar[T]{}, err
	}
	return TableSugar[T]{Sugar: sugar, TableName: tableName}, nil
}
