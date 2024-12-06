package helpers

import (
	"context"
	"fmt"
	"github.com/robdrynkin/ydb-go-sugar/pkg/reflection"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"strings"
)

func MakeSelectByColumnsQuery(tableName string, columns []*reflection.Column) string {
	declares := make([]string, 0, len(columns))
	for i := range columns {
		declares = append(declares, fmt.Sprintf("DECLARE $%s as %s;", columns[i].Name, columns[i].Type))
	}
	wheres := make([]string, 0, len(columns))
	for i := range columns {
		wheres = append(wheres, fmt.Sprintf("%s = $%s", columns[i].Name, columns[i].Name))
	}
	return fmt.Sprintf("%s\nSELECT * FROM %s WHERE %s", strings.Join(declares, "\n"), tableName, strings.Join(wheres, " AND "))
}

func (t *TableSugar[T]) SelectByColumns(ctx context.Context, session table.Session, tx *table.TransactionControl, columns []*reflection.Column, k *T) ([]T, error) {
	query := MakeSelectByColumnsQuery(t.TableName, columns)
	params, err := reflection.MakeQueryParams(k, columns)
	if err != nil {
		return nil, err
	}
	_, res, err := session.Execute(ctx, tx, query, params)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	if err = res.NextResultSetErr(ctx); err != nil {
		return nil, err
	}

	rows := make([]T, 0)
	for res.NextRow() {
		var row T
		readParams := t.MakeReadValue(&row)
		if err = res.ScanNamed(readParams...); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (t *TableSugar[T]) SelectByPK(ctx context.Context, session table.Session, tx *table.TransactionControl, k *T) (*T, error) {
	res, err := t.SelectByColumns(ctx, session, tx, t.PrimaryKeys, k)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	if len(res) > 1 {
		return nil, fmt.Errorf("multiple rows by pk returned")
	}
	return &res[0], nil
}
