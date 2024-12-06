package helpers

import (
	"context"
	"fmt"
	"github.com/robdrynkin/ydb-go-sugar/pkg/reflection"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"strings"
)

func createRow(c *reflection.Column) string {
	return fmt.Sprintf("%s %s", c.Name, c.Type)
}

func (t *TableSugar[T]) createTableQuery(tableName string) string {
	columns := make([]string, 0, len(t.Columns))
	for i := range t.Columns {
		columns = append(columns, createRow(t.Columns[i]))
	}
	pks := make([]string, 0, len(t.PrimaryKeys))
	for i := range t.PrimaryKeys {
		pks = append(pks, t.PrimaryKeys[i].Name)
	}
	primaryKeys := strings.Join(pks, ",")
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s,\nPRIMARY KEY (%s))", tableName, strings.Join(columns, ",\n"), primaryKeys)
}

func (t *TableSugar[T]) CreateTable(ctx context.Context, client scripting.Client) error {
	_, err := client.Execute(ctx, t.createTableQuery(t.TableName), nil)
	return err
}

func (t *TableSugar[T]) DropTable(ctx context.Context, client scripting.Client) error {
	_, err := client.Execute(ctx, fmt.Sprintf("DROP TABLE %s", t.TableName), nil)
	return err
}
