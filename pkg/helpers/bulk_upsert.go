package helpers

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func (t *TableSugar[T]) BulkUpsert(ctx context.Context, c table.Client, dbName string, rows []T) error {
	vals, err := t.ToListStructValues(rows)
	if err != nil {
		return err
	}
	return c.BulkUpsert(ctx, fmt.Sprintf("%s/%s", dbName, t.TableName), table.BulkUpsertDataRows(vals))
}
